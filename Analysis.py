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

def main():
    print("Checking for duplicates...")
    check_duplicates()
    print("\nChecking for null values...")
    check_null_count()

main()