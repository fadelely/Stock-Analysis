import numpy as np
import pandas as pd
import os
from db_utils import save_to_db

# reading csv
def read_csv(file_path):
    return pd.read_csv(file_path)

def check_duplicates(df):
    print(df.duplicated().sum())

def check_null_count(df):
    print(df.isnull().sum())
    print('-----------------------')

def explore_unique_values(df_trades, df_prices, df_customers, df_stocks, df_dates):
    print("[TRADES]")
    print("\n", df_trades["stock_ticker"].value_counts(dropna=False))

    print("[DAILY_TRADE_PRICES]")
    print(f"Unique dates: {df_prices['date'].nunique()} / {len(df_prices)} rows")

    print("[CUSTOMERS]")
    print(df_customers["account_type"].value_counts(dropna=False))

    print("[STOCKS]")
    df_stocks["stock_ticker"].is_unique

    print("[DATES]")
    for col in ["month_name", "day_name", "is_weekend", "is_holiday"]:
        if col in df_dates.columns:
            print(f"\nColumn: {col}")
            print(df_dates[col].value_counts(dropna=False))

def detect_inconsistencies(df_trades, df_prices, df_customers, df_stocks, df_dates):
    print("\n[TRADES]")
    invalid_qty = df_trades[(df_trades["quantity"] <= 0) | (df_trades["average_trade_size"] <= 0)]
    if not invalid_qty.empty:
        print(f"- {len(invalid_qty)} rows with invalid quantities or trade sizes")

    valid_tickers = set(df_stocks["stock_ticker"])
    valid_customers = set(df_customers["customer_id"])
    valid_dates = set(df_dates["date"])

    invalid_fk = df_trades[
        (~df_trades["stock_ticker"].isin(valid_tickers)) |
        (~df_trades["customer_id"].isin(valid_customers)) |
        (~df_trades["timestamp"].isin(valid_dates))
    ]
    if not invalid_fk.empty:
        print(f"- {len(invalid_fk)} rows with invalid foreign keys (customer/stock/date)")

    unknown_types = df_trades[~df_trades["transaction_type"].isin(["BUY", "SELL"])]
    if not unknown_types.empty:
        print(f"- {len(unknown_types)} rows with invalid transaction_type values")

    duplicate_trx = df_trades[df_trades.duplicated(subset=["transaction_id"], keep=False)]
    if not duplicate_trx.empty:
        print(f"- {len(duplicate_trx)} duplicate transaction_id entries")

    print("\n[DAILY_TRADE_PRICES]")
    all_cols = set(df_prices.columns) - {"date"}
    extra_cols = all_cols - set(df_stocks["stock_ticker"])
    if extra_cols:
        print(f"- Columns not found: {extra_cols}")

    invalid_dates_prices = df_prices[~df_prices["date"].isin(valid_dates)]
    if not invalid_dates_prices.empty:
        print(f"- {len(invalid_dates_prices)} rows with invalid dates")

    for col in df_prices.select_dtypes(include="number").columns:
        nonpositive = (df_prices[col] <= 0).sum()
        if nonpositive > 0:
            print(f"- Column {col} has {nonpositive} nonpositive (<=0) values")

    print("\n[CUSTOMERS]")
    dup_customers = df_customers[df_customers.duplicated(subset=["customer_id"], keep=False)]
    if not dup_customers.empty:
        print(f"- {len(dup_customers)} duplicate customer_id entries")

    print("\n[STOCKS]")
    dup_stocks = df_stocks[df_stocks.duplicated(subset=["stock_ticker"], keep=False)]
    if not dup_stocks.empty:
        print(f"- {len(dup_stocks)} duplicate stock_ticker entries")

    print("\n[DATES]")
    dup_dates = df_dates[df_dates.duplicated(subset=["date"], keep=False)]
    if not dup_dates.empty:
        print(f"- {len(dup_dates)} duplicate date entries")

    inconsistent_weekends = df_dates[
        (df_dates["day_name"].isin(["Saturday", "Sunday"])) & (df_dates["is_weekend"] == False)
        | (~df_dates["day_name"].isin(["Saturday", "Sunday"])) & (df_dates["is_weekend"] == True)
    ]
    if not inconsistent_weekends.empty:
        print(f"- {len(inconsistent_weekends)} inconsistent in weekends")

def fill_daily_trade_prices(df_prices, df_trades):
    df_filled = df_prices.copy()
    df_filled = df_filled.interpolate(method='linear', axis=0).ffill().bfill()
    return df_filled

def get_outlier_percentage(df, column):
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1

    lowerBound = Q1 - 1.5 * IQR
    upperBound = Q3 + 1.5 * IQR
    outlier = (df[column] < lowerBound) | (df[column] > upperBound)
    outlierCount = outlier.sum()
    if(outlierCount > 0):
        percentage = outlierCount/df[column].count() * 100
        percentage = round(percentage, 2)
        return percentage

    return 0.0


def handle_outlier(df, columnName):
    result = df.copy()
    new_col = columnName + "_outlier"
    if (df[columnName] == 0).any():
        result[new_col] = np.log1p(df[columnName])
    else:
        result[new_col] = np.log(df[columnName])
    return result

def discretize_average_trade(df):
    result = df.copy()
    labels = ['Small', 'Medium', 'Large', 'Very Large', 'Extreme']
    bins = [0, 176.27, 201.73, 212.63, 500, df['average_trade_size'].max()]
    result['average_trade_size_outlier'] = pd.cut(df['average_trade_size'], bins=bins, labels=labels, include_lowest=True)
    return result
    
def get_stock_price(df_prices, row):
    stock_name = row["stock_ticker"]
    date = row["timestamp"]
    date_prices = df_prices[df_prices["date"] == date]
    stock_price = date_prices[stock_name].values[0]
    if(stock_name == "STK001"):
        stock_price = date_prices["STK001_outlier"].values[0]

    return stock_price

def merge_tables(df_trades, df_prices, df_customers, df_stocks, df_dates):
    # start merging from trades.csv
    merged_df = df_trades.copy()
    merged_df = merged_df.drop(["cumulative_portfolio_value", "cumulative_portfolio_value_outlier", "average_trade_size"], axis=1)
    merged_df = merged_df.rename(columns={"average_trade_size_outlier":"average_trade_size"})

    # lambda function since stock_tickers are stored as columns not rows i.e can't do a simple merge
    merged_df["stock_price"] = merged_df.apply(lambda row: get_stock_price(df_prices, row), axis=1)
    merged_df["total_trade_amount"] = merged_df["stock_price"] * merged_df["quantity"]

    # customer id and key are exactly the same, so a join with either column works
    df_customers_joinable = df_customers.copy() 
    df_customers_joinable = df_customers_joinable.drop(["customer_key", "avg_trade_size_baseline"], axis=1)
    df_customers_joinable = df_customers_joinable.rename(columns={"account_type":"customer_account_type"})
    # there are customers with no trade inside the csv file, do we completely ignore them or add them as nulls?
    merged_df = pd.merge(merged_df, df_customers_joinable, on="customer_id", how="inner")

    # dates has no issues, all dates are accounted for :)
    df_dates_joinable = df_dates.copy() 
    df_dates_joinable = df_dates_joinable.rename(columns={"date":"timestamp"})
    df_dates_joinable = df_dates_joinable.drop(["date_key","day", "month", "month_name", "quarter", "year", "day_of_week"], axis=1)
    merged_df = pd.merge(merged_df, df_dates_joinable, on="timestamp", how="inner")

    # stocks has no issues, all stocks are accounted for :)
    df_stocks_joinable = df_stocks.copy()
    df_stocks_joinable = df_stocks_joinable.drop(["stock_key", "company_name"], axis=1)
    df_stocks_joinable = df_stocks_joinable.rename(columns={
        "liquidity_tier":"stock_liquidity_tier",
        "sector":"stock_sector",
        "industry":"stock_industry"
        })
    merged_df = pd.merge(merged_df, df_stocks_joinable, on="stock_ticker", how="inner")

    # since total_trade_amount has 10.5% outliers
    merged_df["total_trade_amount"] = np.log(merged_df["total_trade_amount"])
    
    return merged_df

def encode_data(df):
    df_encode = df.copy()

    # one-hot encode columns that have more than 2 values and have no importance in order
    df_one_hot_encoded = pd.get_dummies(
        df_encode, 
        columns=["stock_ticker", "day_name", "stock_sector", "stock_industry"]
    )

    # encode both transaction type and customer account type to 1/0 since they are only two possible values
    df_encoded_trans = df_one_hot_encoded.copy()
    df_encoded_trans["transaction_type"] = df_encoded_trans["transaction_type"].map({"BUY": 1, "SELL": 0})
    df_encoded_account = df_encoded_trans.copy()
    df_encoded_account["customer_account_type"] = df_encoded_account["customer_account_type"].map({"Retail": 1, "Institutional": 0})

    # convert the true/false to 1/0, but is this really necessary?
    # also replaces spaces with _ and lower cases the entire column name for consistency :)
    one_hot_cols = [col for col in df_one_hot_encoded.columns 
              if col.startswith("stock_ticker_") 
              or col.startswith("day_name_") 
              or col.startswith("stock_sector_") 
              or col.startswith("stock_industry_")
              or col.startswith("is_")]
    df_final = df_encoded_account.copy()
    df_final[one_hot_cols] = df_final[one_hot_cols].astype(int)
    df_final.rename(columns={col: col.lower().replace(" ", "_") for col in one_hot_cols}, inplace=True)

    return df_final

def create_lookup_table(df):
    lookup_table = pd.DataFrame(columns=["Column Name", "Original Value", "Encoded Value"])

    # lookup table or transaction_type
    for orig, encoded in {"BUY":1, "SELL":0}.items():
        lookup_table = pd.concat([
            lookup_table,
            pd.DataFrame({
                "Column Name": ["transaction_type"],
                "Original Value": [orig],
                "Encoded Value": [encoded]
            })
        ], ignore_index=True)

    # lookup table for customer_account_type 
    for orig, encoded in {"Retail":1, "Institutional":0}.items():
        lookup_table = pd.concat([
            lookup_table,
            pd.DataFrame({
                "Column Name": ["customer_account_type"],
                "Original Value": [orig],
                "Encoded Value": [encoded]
            })
        ], ignore_index=True)



    # for the boolean columns
    bool_cols = ["is_weekend", "is_holiday"]
    for col in bool_cols:
        for orig, encoded in {True:1, False:0}.items():
            lookup_table = pd.concat([
                lookup_table,
                pd.DataFrame({
                    "Column Name": [col],
                    "Original Value": [orig],
                    "Encoded Value": [encoded]
                })
            ], ignore_index=True)
    one_hot_cols = [col for col in df.columns 
        if col.startswith("stock_ticker_") 
        or col.startswith("day_name_") 
        or col.startswith("stock_sector_") 
        or col.startswith("stock_industry_")]

    # for the one hot encoded features, this will show 1 for each value, not very useful?
    for col in one_hot_cols:
        # since we lowercased it and replaced "_" with " ", we need to seperate it by searching using the prefix
        for prefix in ["stock_ticker_", "day_name_", "stock_sector_", "stock_industry_"]:
            if col.startswith(prefix):
                column_name = prefix[:-1]  
                original_value = col[len(prefix):]  

                # return the original value to how it was before being lowercased 
                if prefix == "stock_ticker_":
                    original_value = original_value.upper()
                else:
                    original_value = " ".join([word.capitalize() for word in original_value.split("_")])
                break

        lookup_table = pd.concat([
            lookup_table,
            pd.DataFrame({
                "Column Name": [column_name], 
                "Original Value": [original_value],
                "Encoded Value": [1]  
            })
        ], ignore_index=True)

    return lookup_table




def main():
    base_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "datasets")

    df_trades = pd.read_csv(os.path.join(base_path, "trades.csv"))
    df_prices = pd.read_csv(os.path.join(base_path, "daily_trade_prices.csv"))
    df_customers = pd.read_csv(os.path.join(base_path, "dim_customer.csv"))
    df_stocks = pd.read_csv(os.path.join(base_path, "dim_stock.csv"))
    df_dates = pd.read_csv(os.path.join(base_path, "dim_date.csv"))

    # do not use the above variables at any point, only use the ones in the dict below
    dfs = {
        "trades": df_trades,
        "prices": df_prices,
        "customers": df_customers,
        "stocks": df_stocks,
        "dates": df_dates
    }

    print("Checking for duplicates...")
    for _, df in dfs.items():
        check_duplicates(df)
        
    print("\nChecking for null values...")
    for _, df in dfs.items():
        check_null_count(df)

    print("\nFilling daily trade missing values...")
    dfs["prices"] = fill_daily_trade_prices(dfs["prices"], dfs["trades"])

    print("\nChecking for null values after imputation...")
    for _, df in dfs.items():
        check_null_count(df)

    print("\nChecking and handling outliers...")
    for name, df in dfs.items():
        for column in df.select_dtypes(include='number').columns:
            outlier_percentage = get_outlier_percentage(dfs[name], column) 
            if(outlier_percentage > 10):
                print(f"{column} has outliers! the total percentage is: {outlier_percentage}%")
                print(f"Handling outliers in {column}...")

                if(column == "average_trade_size"):
                    dfs[name] = discretize_average_trade(dfs[name])
                    print(f"{column} has been discretized.")
                    continue

                dfs[name] = handle_outlier(dfs[name], column)
                outlier_percentage = get_outlier_percentage(dfs[name], column + "_outlier")
                if(outlier_percentage > 10):
                    print(f"{column} still has {outlier_percentage}% outliers.")
                else:
                    print(f"{column} now has less than 10 percent outliers.")


    print("\nChecking for inconsistencies")
    explore_unique_values(df_trades, df_prices, df_customers, df_stocks, df_dates)
    detect_inconsistencies(df_trades, df_prices, df_customers, df_stocks, df_dates)
    print("\nDetection Complete")

    print("\nMerging tables...")
    merged_df = merge_tables(
        dfs["trades"],
        dfs["prices"],
        dfs["customers"],
        dfs["stocks"],
        dfs["dates"]
    )
    
    pd.set_option('display.max_columns', None)

    encoded_df = encode_data(merged_df)
    lookup_table = create_lookup_table(encoded_df)
    print(lookup_table)

    print("\nSaving merged dataframe...")
    save_to_db(encoded_df,'merged_df_final')




if __name__ == "__main__":
    main()