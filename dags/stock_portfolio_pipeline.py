import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import psycopg2
import os
import sys

# there's some way to add them using docker instead of using sys but too lazy 
sys.path.append(os.path.join(os.path.dirname(__file__), '../scripts/src'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../scripts/agent'))
from Analysis import fill_daily_trade_prices, merge_tables, encode_data, create_lookup_table, get_outlier_percentage, handle_outlier, discretize_average_trade, process_stream 
from spark import connect_to_master, run_spark_analysis
from db_utils import save_to_db
from ai_agent import process_with_ai_agent

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# don't think OS basename is neccessary since docker should have the same path no matter the operating system
BASE_STAGING_PATH = "/opt/airflow/data/staging"
BASE_CLEAN_PATH   = "/opt/airflow/data/clean"
BASE_MISC_PATH   = "/opt/airflow/data/misc"
BASE_RAW_PATH   = "/opt/airflow/data/raw"


### TASK GROUP 1 ###

def clean_missing_values(prices_filepath):
    df_prices = pd.read_csv(prices_filepath)
    df_filled = fill_daily_trade_prices(df_prices)

    df_path = os.path.join(BASE_STAGING_PATH, "daily_trade_prices_filled.csv")
    os.makedirs(os.path.dirname(df_path), exist_ok=True)
    df_filled.to_csv(df_path, index=False)

def detect_outliers(prices_filepath, customer_filepath, date_filepath, stock_filepath, trades_filepath):
    dfs = {}

    dfs["prices"]   = pd.read_csv(prices_filepath)
    dfs["customer"] = pd.read_csv(customer_filepath)
    dfs["date"]     = pd.read_csv(date_filepath)
    dfs["stock"]    = pd.read_csv(stock_filepath)
    dfs["trades"]   = pd.read_csv(trades_filepath)


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
    


    for name, df in dfs.items():
        df_path = os.path.join(BASE_STAGING_PATH, f"{name}_cleaned.csv")
        os.makedirs(os.path.dirname(df_path), exist_ok=True)
        df.to_csv(df_path, index=False)

def integrate_datasets(prices_filepath, customer_filepath, date_filepath, stock_filepath, trades_filepath):
    df_prices = pd.read_csv(prices_filepath)
    df_customers = pd.read_csv(customer_filepath)
    df_dates = pd.read_csv(date_filepath)
    df_stocks = pd.read_csv(stock_filepath)
    df_trades = pd.read_csv(trades_filepath)

    merged_df = merge_tables(df_trades, df_prices, df_customers, df_stocks, df_dates)

    merged_path = os.path.join(BASE_CLEAN_PATH, "merged.csv")
    os.makedirs(os.path.dirname(merged_path), exist_ok=True)
    merged_df.to_csv(merged_path, index=False)

def load_to_postgres(merged_filepath):
    cleaned = pd.read_csv(merged_filepath)
    save_to_db(cleaned, "merged_stocks")


### TASK GROUP 2 ###

def prepare_streaming_data(merged_filepath):
    df = pd.read_csv(merged_filepath)
    stream_df = df.sample(frac=0.05, random_state=42)
    df_remaining = df.drop(stream_df.index)

    stream_filepath = os.path.join(BASE_MISC_PATH, "stream.csv")
    os.makedirs(os.path.dirname(stream_filepath), exist_ok=True)
    stream_df.to_csv(stream_filepath, index=False)

    df_remaining_filepath = os.path.join(BASE_CLEAN_PATH, "remaining.csv")
    df_remaining.to_csv(df_remaining_filepath, index=False)

def encode_categorical_data(remaining_filepath):
    df = pd.read_csv(remaining_filepath)
    df_encoded = encode_data(df)

    df_encoded_path = os.path.join(BASE_CLEAN_PATH, "df_encoded.csv")
    os.makedirs(os.path.dirname(df_encoded_path), exist_ok=True)
    df_encoded.to_csv(df_encoded_path, index=False)

    lookup_table = create_lookup_table(df_encoded)

    lookup_table_path = os.path.join(BASE_MISC_PATH, "lookup_table.csv")
    os.makedirs(os.path.dirname(lookup_table_path), exist_ok=True)
    lookup_table.to_csv(lookup_table_path, index=False)


### TASK GROUP 3 ###

def consume_and_process_stream(lookup_table_filepath, encoded_filepath):
    df = pd.read_csv(encoded_filepath)
    lookup_table = pd.read_csv(lookup_table_filepath)
    final_df = process_stream(lookup_table=lookup_table, df=df)

    final_df_path = os.path.join(BASE_CLEAN_PATH, "FINAL_STOCKS.csv")
    os.makedirs(os.path.dirname(final_df_path), exist_ok=True)
    final_df.to_csv(final_df_path, index=False)

def save_final_to_postgres(final_df_filepath):
    df = pd.read_csv(final_df_filepath)
    save_to_db(df, "final_stocks")


### TASK GROUP 4 ###

def intialize_spark_session():
    connect_to_master()

def run_spark_analytics(lookup_table_filepath, final_stocks_filepath):
    lookup_table = pd.read_csv(lookup_table_filepath)
    final_stocks = pd.read_csv(final_stocks_filepath)
    run_spark_analysis(lookup_table, final_stocks)

### VISUALIZATION SETUP ###
def prepare_visualization(lookup_table_filepath, final_stocks_filepath):
    lookup_df = pd.read_csv(lookup_table_filepath)
    df = pd.read_csv(final_stocks_filepath)

    label_columns = [
        "transaction_type",
        "customer_account_type",
        "average_trade_size",
        "stock_liquidity_tier",
        "is_weekend",
        "is_holiday"
    ]

    for colname in label_columns:
        current_lookup_df = lookup_df[lookup_df["Column Name"] == colname][["Encoded Value", "Original Value"]].copy()
        current_lookup_df.columns = [f"{colname}_KEY", f"{colname}_DECODED"]
        df = df.merge(
            current_lookup_df,
            left_on=colname,
            right_on=f"{colname}_KEY",
            how="left"
        )
        df = df.drop(columns=[colname, f"{colname}_KEY"])
        df = df.rename(columns={f"{colname}_DECODED": colname})

    # Decode ONE-HOT columns
    onehot_groups = {
        "day_name": "day_name_",
        "stock_sector": "stock_sector_",
        "stock_industry": "stock_industry_",
        "stock_ticker": "stock_ticker_"
    }

    for new_col, prefix in onehot_groups.items():
        onehot_cols = [c for c in df.columns if c.startswith(prefix)]
        
        if onehot_cols:
            def decode_onehot(row):
                for colname in onehot_cols:
                    if row[colname] == 1:
                        return colname[len(prefix):]
                return None
            
            df[new_col] = df.apply(decode_onehot, axis=1)
            
            # Drop the one-hot encoded columns
            df = df.drop(columns=onehot_cols)

    print("Reversing log transformations for STK001 stock_price and total_trade_amount")
    df.loc[df["stock_ticker"] == "STK001", "stock_price"] = np.exp(df.loc[df["stock_ticker"] == "STK001", "stock_price"])
    df["total_trade_amount"] = np.exp(df["total_trade_amount"])

    # Format stock_ticker: fully uppercase
    df["stock_ticker"] = df["stock_ticker"].str.upper()

    # Format other columns: replace _ with space and title case
    for col in ["day_name", "stock_sector", "stock_industry", "transaction_type", 
                "customer_account_type", "average_trade_size", "stock_liquidity_tier"]:
        if col in df.columns:
            df[col] = df[col].str.replace("_", " ").str.title()

    save_to_db(df, "visualization")

default_args = {
    "owner": "nabila_and_yousef",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "email_on_retry": False,
    "retries": 1
}

DAG(
'stock_portfolio_pipeline',
default_args=default_args,
schedule_interval='@daily',
catchup=False,
)

with DAG(
    dag_id = "stock_portfolio_pipeline_Team_11",
    description='End-to-end stock portfolio analytics pipeline',
    schedule_interval = '@daily', 
    catchup=False,
    default_args = default_args,
    tags=['data-engineering', 'stocks', 'analytics', 'stock-portfolio-pipeline'],
)as dag:
    with TaskGroup("Milestone_1", tooltip="Initial Cleaning and Integration") as milestone_1:
        clean_missing_values_task = PythonOperator(
            task_id = 'clean_missing_values',
            python_callable = clean_missing_values,
            op_kwargs = {
                'prices_filepath': f"{BASE_RAW_PATH}/daily_trade_prices.csv"
            }
        )

        detect_outliers_task = PythonOperator(
            task_id = 'detect_outliers',
            python_callable = detect_outliers,
            op_kwargs = {
                "prices_filepath": f"{BASE_STAGING_PATH}/daily_trade_prices_filled.csv",
                "customer_filepath": f"{BASE_RAW_PATH}/dim_customer.csv",
                "date_filepath": f"{BASE_RAW_PATH}/dim_date.csv",
                "stock_filepath": f"{BASE_RAW_PATH}/dim_stock.csv",
                "trades_filepath": f"{BASE_RAW_PATH}/trades.csv"
            }
        )

        integrate_datasets_task = PythonOperator(
            task_id = "integrate_datasets",
            python_callable = integrate_datasets,
            op_kwargs = {
                "prices_filepath": f"{BASE_STAGING_PATH}/prices_cleaned.csv",
                "customer_filepath": f"{BASE_STAGING_PATH}/customer_cleaned.csv",
                "date_filepath": f"{BASE_STAGING_PATH}/date_cleaned.csv",
                "stock_filepath": f"{BASE_STAGING_PATH}/stock_cleaned.csv",
                "trades_filepath": f"{BASE_STAGING_PATH}/trades_cleaned.csv"
            }
        )

        load_to_postgres_task = PythonOperator(
            task_id = "load_to_postgres_merged",
            python_callable = load_to_postgres,
            op_kwargs = {
                "merged_filepath": f"{BASE_CLEAN_PATH}/merged.csv"
            }
        )

        clean_missing_values_task >> detect_outliers_task >> integrate_datasets_task >> load_to_postgres_task
    
    with TaskGroup("Milestone_2_Encoding", tooltip="Preparing Stream and Encoding Table") as milestone_2_encoding:
        prepare_streaming_data_task = PythonOperator(
            task_id = 'extract_5_percent',
            python_callable = prepare_streaming_data,
            op_kwargs = {
                "merged_filepath": f"{BASE_CLEAN_PATH}/merged.csv"
            }
        )
        encode_categorical_data_task = PythonOperator(
            task_id = 'encode_data',
            python_callable = encode_categorical_data,
            op_kwargs = {
                "remaining_filepath": f"{BASE_CLEAN_PATH}/remaining.csv"
            }
        )

        prepare_streaming_data_task >> encode_categorical_data_task
    
    with TaskGroup("Milestone_2_Streaming", tooltip="Streaming Using Kafka") as milestone_2_streaming:
        start_kafka_producer_task = BashOperator(
            task_id = "start_kafka_producer",
            bash_command = "python /opt/airflow/scripts/src/kafka_producer.py"
        )
        consume_and_process_stream_task = PythonOperator(
            task_id = 'consume_kafka',
            python_callable = consume_and_process_stream,
            op_kwargs = {
                "lookup_table_filepath": f"{BASE_MISC_PATH}/lookup_table.csv",
                "encoded_filepath": f"{BASE_CLEAN_PATH}/df_encoded.csv"
            }
        )
        save_final_to_postgres_task = PythonOperator(
            task_id = 'save_final_to_postgres',
            python_callable = save_final_to_postgres,
            op_kwargs = {
                "final_df_filepath": f"{BASE_CLEAN_PATH}/FINAL_STOCKS.csv"

            }
        )

        start_kafka_producer_task >> consume_and_process_stream_task >> save_final_to_postgres_task

    with TaskGroup("Milestone_2_Spark", tooltip="Analyzing Data with Spark") as milestone_2_spark:
        initialize_spark_session_task = PythonOperator(
            task_id = 'intialize_spark',
            python_callable = intialize_spark_session,
        )

        run_spark_analytics_task = PythonOperator(
            task_id = 'analysis_spark',
            python_callable = run_spark_analytics,
            op_kwargs = {
                "lookup_table_filepath": f"{BASE_MISC_PATH}/lookup_table.csv",
                "final_stocks_filepath": f"{BASE_CLEAN_PATH}/FINAL_STOCKS.csv"
            }
        )

        initialize_spark_session_task >> run_spark_analytics_task
    with TaskGroup("Milestone_3_Visualization", tooltip="Visualizing Data") as milestone_3_visualization:
        prepare_visualization_task = PythonOperator(
            task_id = 'prepare_visualization',
            python_callable = prepare_visualization,
            op_kwargs = {
                "lookup_table_filepath": f"{BASE_MISC_PATH}/lookup_table.csv",
                "final_stocks_filepath": f"{BASE_CLEAN_PATH}/FINAL_STOCKS.csv"
            }
        )

        start_visualization_service_task = BashOperator(
            task_id = "start_visualization_service",
            bash_command = "streamlit run /opt/airflow/scripts/src/visualization/dashboard.py"

        )

        prepare_visualization_task >> start_visualization_service_task
    with TaskGroup("Milestone_3_Agent", tooltip="AI Agent") as milestone_3_agent:
        process_ai_task = PythonOperator(
            task_id="process_with_ai_agent",
            python_callable=process_with_ai_agent,
            op_kwargs = {
                "final_stocks_filepath": f"{BASE_CLEAN_PATH}/FINAL_STOCKS.csv"
            }
        )

        process_ai_task



    
    milestone_1 >> milestone_2_encoding  >> milestone_2_streaming >> milestone_2_spark >> [milestone_3_agent, milestone_3_visualization] 


