import pandas as pd
import numpy as np
import os

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from sqlalchemy import create_engine, text
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def clean_missing_values(prices_filename):
    df_prices = pd.read_csv(prices_filename)
    df_filled = df_prices.interpolate(method='linear', axis=0).ffill().bfill()
    df_filled.to_csv("/opt/airflow/data/staging/daily_trade_prices_filled.csv")

def detect_outliers(prices_filename, customer_filename, date_filename, stock_filename, trades_filename):
    dfs = {}

    dfs["prices"]   = pd.read_csv(prices_filename)
    dfs["customer"] = pd.read_csv(customer_filename)
    dfs["date"]     = pd.read_csv(date_filename)
    dfs["stock"]    = pd.read_csv(stock_filename)
    dfs["trades"]   = pd.read_csv(trades_filename)


    for name, df in dfs.items():                   
        # handle average trade size with binning, otherwise just regular log
        if name == "trades" and "average_trade_size" in df.columns:
            labels = ['Small', 'Medium', 'Large', 'Very Large', 'Extreme']
            bins = [0, 176.27, 201.73, 212.63, 500, df['average_trade_size'].max()]
            df['average_trade_size'] = pd.cut(
                df['average_trade_size'],
                bins=bins,
                labels=labels,
                include_lowest=True
            )

        for column in df.select_dtypes(include=[np.number]).columns:   
            Q1 = df[column].quantile(0.25)
            Q3 = df[column].quantile(0.75)
            IQR = Q3 - Q1

            lowerBound = Q1 - 1.5 * IQR
            upperBound = Q3 + 1.5 * IQR

            outlier = (df[column] < lowerBound) | (df[column] > upperBound)
            outlierCount = outlier.sum()

            if outlierCount > 0:
                percentage = (outlierCount / df[column].count()) * 100
                percentage = round(percentage, 2)

                if percentage > 10:
                    if (df[column] == 0).any():
                        df[column] = np.log1p(df[column])
                    else:
                        df[column] = np.log(df[column])

        dfs[name] = df      
    
    staging_path = "/opt/airflow/data/staging"
    os.makedirs(staging_path, exist_ok=True)

    for name, df in dfs.items():
        filename = f"{staging_path}/{name}_cleaned.csv"  
        df.to_csv(filename, index=False)

def integrate_datasets(prices_filename, customer_filename, date_filename, stock_filename, trades_filename):
    def get_stock_price(df_prices, row):
        stock_name = row["stock_ticker"]
        date = row["timestamp"]
        date_prices = df_prices[df_prices["date"] == date]
        stock_price = date_prices[stock_name].values[0]
        return stock_price

    dfs = {}

    df_prices = pd.read_csv(prices_filename)
    df_customers = pd.read_csv(customer_filename)
    df_dates = pd.read_csv(date_filename)
    df_stocks = pd.read_csv(stock_filename)
    df_trades = pd.read_csv(trades_filename)

    merged_df = df_trades.copy()
    merged_df = merged_df.drop(["cumulative_portfolio_value", "cumulative_portfolio_value", "average_trade_size"], axis=1)
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
    merged_df.to_csv("/opt/airflow/data/cleaned/merged.csv")

def load_to_postgres(merged_filepath):
    try:
        conn = psycopg2.connect(
            host="pgdatabase",
            user="root",
            password="root",
            port=5432,
            dbname="postgres"
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()

        # Check database existence
        cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'stockanalysis'")
        exists = cursor.fetchone()

        if not exists:
            cursor.execute("CREATE DATABASE stockanalysis")
            print("Database created successfully")
        else:
            print("Database already exists")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error creating database: {e}")

    cleaned = pd.read_csv(merged_filepath)

    engine = create_engine("postgresql://root:root@pgdatabase:5432/stockanalysis")

    try:
        with engine.connect() as conn:
            print("Connected to Database")
            print("Writing cleaned dataset to database")
            cleaned.to_sql("merged_df", con=engine, if_exists='replace', index=False)
            print("Done writing to database")
    except ValueError:
        print("Cleaned table already exists")
    except Exception as ex:
        print(f"Error saving to database: {ex}")

default_args = {
    "owner": "nabila_and_yousef",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "retries": 1
}



with DAG(
    dag_id = "stock_portfolio_pipeline_Team_11",
    schedule_interval = '@once', 
    default_args = default_args,
    tags = ['stock-portfolio-pipeline'],
)as dag:
    clean_missing_values_task = PythonOperator(
        task_id = 'clean_missing_values',
        python_callable = clean_missing_values,
        op_kwargs = {
            'prices_filename': '/opt/airflow/data/raw/daily_trade_prices.csv'
        }
    )

    detect_outliers_task = PythonOperator(
        task_id = 'detect_outliers',
        python_callable = detect_outliers,
        op_kwargs = {
            "prices_filename": "/opt/airflow/data/staging/daily_trade_prices_filled.csv",
            "customer_filename": "/opt/airflow/data/raw/dim_customer.csv",
            "date_filename": "/opt/airflow/data/raw/dim_date.csv",
            "stock_filename": "/opt/airflow/data/raw/dim_stock.csv",
            "trades_filename": "/opt/airflow/data/raw/trades.csv"
        }
    )

    integrate_datasets_task = PythonOperator(
        task_id = "integrate_datasets",
        python_callable = integrate_datasets,
        op_kwargs = {
            "prices_filename": "/opt/airflow/data/staging/prices_cleaned.csv",
            "customer_filename": "/opt/airflow/data/staging/customer_cleaned.csv",
            "date_filename": "/opt/airflow/data/staging/date_cleaned.csv",
            "stock_filename": "/opt/airflow/data/staging/stock_cleaned.csv",
            "trades_filename": "/opt/airflow/data/staging/trades_cleaned.csv"
        }
    )

    load_to_postgres_task = PythonOperator(
        task_id = "load_to_postgres_merged",
        python_callable = load_to_postgres,
        op_kwargs = {
            "merged_filepath": "/opt/airflow/data/cleaned/merged.csv"
        }
    )

    clean_missing_values_task >> detect_outliers_task >> integrate_datasets_task >> load_to_postgres_task
