import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine, text
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def clean_missing_values(prices_filename):
    df_prices = pd.read_csv(prices_filename)
    df_filled = df_prices.interpolate(method='linear', axis=0).ffill().bfill()
    df_filled.to_csv("/opt/airflow/data/staging/daily_trade_prices_filled.csv", index=False)

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
    merged_df = merged_df.drop(["cumulative_portfolio_value", "cumulative_portfolio_value"], axis=1)
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
    merged_df.to_csv("/opt/airflow/data/cleaned/merged.csv", index=False)

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


def prepare_streaming_data(merged_filepath):
    df = pd.read_csv(merged_filepath)
    stream_df = df.sample(frac=0.05, random_state=42)
    df_remaining = df.drop(stream_df.index)

    stream_df.to_csv("/opt/airflow/data/stream/stream.csv", index=False)
    df_remaining.to_csv("/opt/airflow/data/cleaned/remaining.csv", index=False)

def encode_categorical_data(remaining_filepath):
    df_encode = pd.read_csv(remaining_filepath)

    # one-hot encode columns that have more than 2 values and have no importance in order
    df_one_hot_encoded = pd.get_dummies(
        df_encode, 
        columns=["stock_ticker", "day_name", "stock_sector", "stock_industry"]
    )

    # encode both transaction type and customer account type to 1/0 since they are only two possible values
    df_encoded_trans = df_one_hot_encoded.copy()
    df_encoded_trans["transaction_type"] = df_encoded_trans["transaction_type"].map({"BUY": 1, "SELL": 0}).astype(int)
    df_encoded_account = df_encoded_trans.copy()
    df_encoded_account["customer_account_type"] = df_encoded_account["customer_account_type"].map({"Retail": 1, "Institutional": 0}).astype(int)
    # both stock liquidty and average trade were also label encoded even though it is not required by the milestone document, done for the sake of consistency
    df_encoded_average = df_encoded_account.copy()
    df_encoded_average["average_trade_size"] = df_encoded_average["average_trade_size"].map({
        "Small": 0,
        "Medium": 1,
        "Large": 2,
        "Very Large": 3,
        "Extreme": 4
    }).astype(int)
    df_encoded_stock = df_encoded_average.copy()
    df_encoded_stock["stock_liquidity_tier"] = df_encoded_stock["stock_liquidity_tier"].map({
        "Low":0,
        "Mid":1,
        "High":2
    }).astype(int)


    # convert the true/false to 1/0
    # also replaces spaces with _ and lower cases the entire column name for consistency :)
    one_hot_cols = [col for col in df_one_hot_encoded.columns 
              if col.startswith("stock_ticker_") 
              or col.startswith("day_name_") 
              or col.startswith("stock_sector_") 
              or col.startswith("stock_industry_")
              or col.startswith("is_")]
    df_final = df_encoded_stock.copy()
    df_final[one_hot_cols] = df_final[one_hot_cols].astype(int)
    df_final.rename(columns={col: col.lower().replace(" ", "_") for col in one_hot_cols}, inplace=True)

    df_final.to_csv("/opt/airflow/data/cleaned/df_encoded.csv", index=False)

    lookup_table = pd.DataFrame(columns=["Column Name", "Original Value", "Encoded Value"])

    # lookup table or transaction_type
    lookup_table = pd.concat([
        lookup_table,
        pd.DataFrame({
            "Column Name": ["transaction_type", "transaction_type"],
            "Original Value": ["BUY", "SELL"],
            "Encoded Value": [1, 0]
        })
    ], ignore_index=True)


    # lookup table for customer_account_type 
    lookup_table = pd.concat([
        lookup_table,
        pd.DataFrame({
            "Column Name": ["customer_account_type", "customer_account_type"],
            "Original Value": ["Retail", "Institutional"],
            "Encoded Value": [1, 0]
        })
    ], ignore_index=True)

    # add stock_liquidity_tier
    lookup_table = pd.concat([
        lookup_table,
        pd.DataFrame({
            "Column Name": ["stock_liquidity_tier", "stock_liquidity_tier", "stock_liquidity_tier"],
            "Original Value": ["Low", "Mid", "High"],
            "Encoded Value": [0, 1, 2]
        })
    ], ignore_index=True)

    # add average_trade_size
    lookup_table = pd.concat([
        lookup_table,
        pd.DataFrame({
            "Column Name": ["average_trade_size"] * 5,
            "Original Value": ["Small", "Medium", "Large", "Very Large", "Extreme"],
            "Encoded Value": [0, 1, 2, 3, 4]
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

    lookup_table.to_csv("/opt/airflow/data/lookup_table/lookup_table.csv", index=False)



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
    with TaskGroup("Milestone_1", tooltip="Initial Cleaning and Integration") as milestone_1:
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
    
    with TaskGroup("Milestone_2", tooltip="Preparing Stream and Encoding Table") as milestone_2_encoding:
        prepare_streaming_data_task = PythonOperator(
            task_id = 'extract_5_percent',
            python_callable = prepare_streaming_data,
            op_kwargs = {
                'merged_filepath': '/opt/airflow/data/cleaned/merged.csv'


            }
        )
        encode_categorical_data_task = PythonOperator(
            task_id = 'encode_data',
            python_callable = encode_categorical_data,
            op_kwargs = {
                'remaining_filepath': '/opt/airflow/data/cleaned/remaining.csv'
            }
        )

        prepare_streaming_data_task >> encode_categorical_data_task

    milestone_1 >> milestone_2_encoding 

