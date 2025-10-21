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
    detect_outlier(df_trades)
    # STK001 has outliers > 10%
    detect_outlier(df_prices)
    #detect_outlier(df_customers)
    #detect_outlier(df_stocks)
    #detect_outlier(df_dates)
    print("\nHandling outliers...")
    df_prices_outliers = handle_outlier(df_prices, "STK001")
    df_trades_outliers = handle_outlier(df_trades, "average_trade_size")
    df_trades_outliers = handle_outlier(df_trades_outliers, "cumulative_portfolio_value")
    detect_outlier(df_trades_outliers)
    detect_outlier(df_prices_outliers)




        


main()
