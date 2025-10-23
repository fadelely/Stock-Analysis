import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# reading csv
df_trades=pd.read_csv('datasets/trades.csv')
df_prices=pd.read_csv('datasets/daily_trade_prices.csv')
df_customers=pd.read_csv('datasets/dim_customer.csv')
df_stocks=pd.read_csv('datasets/dim_stock.csv')
df_dates=pd.read_csv('datasets/dim_date.csv')

def check_duplicates():
    print(df_trades.duplicated().sum())
    print(df_prices.duplicated().sum())
    print(df_customers.duplicated().sum())
    print(df_stocks.duplicated().sum())
    print(df_dates.duplicated().sum())

def check_null_count():
    print(df_trades.isnull().sum())
    print('-----------------------')
    print(df_prices.isnull().sum())
    print('-----------------------')
    print(df_customers.isnull().sum())
    print('-----------------------')
    print(df_stocks.isnull().sum())
    print('-----------------------')
    print(df_dates.isnull().sum())

def fill_daily_trade_prices():
    global df_prices, df_trades
    # fill by a trade performed in that date
    cols_with_nulls = df_prices.columns[df_prices.isnull().any()].tolist()

    for col in cols_with_nulls:
        est_map = (
            df_trades[df_trades['stock_ticker'] == col]
            .groupby('timestamp')
            .apply(lambda x: (x['cumulative_portfolio_value'] / x['quantity']).mean())
        )

        df_prices[col] = df_prices[col].fillna(
            df_prices['date'].map(est_map)
        )

    # for values that are still null
    # mean of the price before and after that specific day and for edges bfill and ffill although not needed
    df_prices = df_prices.interpolate(method='linear', axis=0).ffill().bfill()

def detect_outlier(df):
    outlierPercentage = []
    for column in df.select_dtypes(include='number').columns:
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 -  Q1

        lowerBound = Q1 - 1.5 * IQR
        upperBound = Q3 + 1.5 * IQR
        outlier = (df[column] < lowerBound) | (df[column] > upperBound)
        outlierCount = outlier.sum()
        if(outlierCount > 0):
            percentage = outlierCount/df[column].count() * 100
            percentage = round(percentage, 2)
            if(percentage > 10):
                print(f"{column} has outliers! the total percentage is: {percentage}%")
        

def handle_outlier(df, columnName):
    result = df.copy()
    if (df[columnName] == 0).any():
        result[columnName] = np.log1p(df[columnName])
    else:
        result[columnName] = np.log(df[columnName])
    return result

def get_stock_price(row):
    stock_name = row["stock_ticker"]
    date = row["timestamp"]
    date_prices = df_prices[df_prices["date"] == date]
    stock_price = date_prices[stock_name].values[0]
    return stock_price

def merge_tables():
    # start merging from trades.csv
    merged_df = df_trades.copy()
    merged_df = merged_df.drop(["cumulative_portfolio_value"], axis=1)

    # lambda function since stock_tickers are stored as columns not rows i.e can't do a simple merge
    merged_df["stock_price"] = merged_df.apply(get_stock_price, axis=1)
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


def main():
    print("Checking for duplicates...")
    check_duplicates()
    print("\nChecking for null values...")
    check_null_count()
    print("\nFilling daily trade missing values...")
    fill_daily_trade_prices()
    print("\nChecking for null values...")
    check_null_count()

    print("\nChecking for outliers...")
    # average trade size & cumlative porto has outliers > 10%
    #detect_outlier(df_trades)
    # STK001 has outliers > 10%
    #detect_outlier(df_prices)
    #detect_outlier(df_customers)
    #detect_outlier(df_stocks)
    #detect_outlier(df_dates)
    print("\nHandling outliers...")
    #df_prices = handle_outlier(df_prices, "STK001")
    #df_trades = handle_outlier(df_trades, "average_trade_size")
    #df_trades = handle_outlier(df_trades_outliers, "cumulative_portfolio_value")
    #detect_outlier(df_trades)
    #detect_outlier(df_prices)
    print("\nMerging tables...")
    merged_df = merge_tables()
    print(merged_df)
    

    



        


main()
