import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# reading csv
def read_csv(file_path):
    return pd.read_csv(file_path)

def check_duplicates(df):
    print(df.duplicated().sum())

def check_null_count(df):
    print(df.isnull().sum())
    print('-----------------------')


def fill_daily_trade_prices(df_prices, df_trades):
    # fill by a trade performed in that date
    df_filled = df_prices.copy()
    cols_with_nulls = df_filled.columns[df_filled.isnull().any()].tolist()

    for col in cols_with_nulls:
        est_map = (
            df_trades[df_trades['stock_ticker'] == col]
            .groupby('timestamp')
            .apply(lambda x: (x['cumulative_portfolio_value'] / x['quantity']).mean())
        )

        df_filled[col] = df_filled[col].fillna(
            df_filled['date'].map(est_map)
        )

    # for values that are still null
    # mean of the price before and after that specific day and for edges bfill and ffill although not needed
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
    if (df[columnName] == 0).any():
        result[columnName] = np.log1p(df[columnName])
    else:
        result[columnName] = np.log(df[columnName])
    return result

def get_stock_price(df_prices, row):
    stock_name = row["stock_ticker"]
    date = row["timestamp"]
    date_prices = df_prices[df_prices["date"] == date]
    stock_price = date_prices[stock_name].values[0]
    return stock_price

def merge_tables(df_trades, df_prices, df_customers, df_stocks, df_dates):
    # start merging from trades.csv
    merged_df = df_trades.copy()
    merged_df = merged_df.drop(["cumulative_portfolio_value"], axis=1)

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
    
    return merged_df

def save_df(df, file_name):
    df.to_csv(file_name, index=False)
    print(f"{file_name} has been saved!")

def main():
    df_trades = read_csv('datasets/trades.csv')
    df_prices = read_csv('datasets/daily_trade_prices.csv')
    df_customers = read_csv('datasets/dim_customer.csv')
    df_stocks = read_csv('datasets/dim_stock.csv')
    df_dates = read_csv('datasets/dim_date.csv')
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
    for _, df in dfs.items():
        for column in df.select_dtypes(include='number').columns:
            outlier_percentage = get_outlier_percentage(df, column) 
            if(outlier_percentage > 10):
                print(f"{column} has outliers! the total percentage is: {outlier_percentage}%")
                print(f"Handling outliers in {column}...")
                df = handle_outlier(df, column)
                # check if after handling outlier, the percentage is less than 10% or not
                outlier_percentage = get_outlier_percentage(df, column)
                if(outlier_percentage > 10):
                    print(f"{column} still has {outlier_percentage}% outliers...")
                else:
                    print(f"{column} now has less than 10 percent outliers...")


    print("\nMerging tables...")
    merged_df = merge_tables(
        dfs["trades"],
        dfs["prices"],
        dfs["customers"],
        dfs["stocks"],
        dfs["dates"]
    )

    print(merged_df.head(10)) 
    print(merged_df.shape) 


    print("\nSaving merged dataframe...")




    

main()
