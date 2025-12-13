import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, sum as _sum, avg, desc, count
from db_utils import save_to_db_spark
import pandas as pd

def connect_to_master():
    spark = SparkSession.builder \
        .appName("M3_SPARK_APP_TEAM_11") \
        .master("spark://spark-master:7077") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.driver.host", "localhost") \
        .getOrCreate()

def run_spark_analysis(lookup_table: pd.DataFrame, df_spark_filepath: str):
    print("Connecting to Spark Session...")

    spark = SparkSession.builder \
        .appName("M3_SPARK_APP_TEAM_11") \
        .master("spark://spark-master:7077") \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.driver.host", "localhost") \
        .getOrCreate()
        

    print(df_spark_filepath)
    df_spark = spark.read.csv(df_spark_filepath)

    print("Decoding encoded columns using lookup table...")

    # Decode LABEL-ENCODED columns
    label_columns = [
        "transaction_type",
        "customer_account_type",
        "average_trade_size",
        "stock_liquidity_tier",
        "is_weekend",
        "is_holiday"
    ]

    lookup_spark_df = spark.createDataFrame(lookup_table)
    for colname in label_columns:
        current_lookup_df = lookup_spark_df.filter(col("Column Name") == colname).select(
                                        col("Encoded Value").alias(f"{colname}_KEY"),
                                        col("Original Value").alias(f"{colname}_DECODED")
                                    )

        df_spark = df_spark.alias("d").join(
            current_lookup_df.alias("l"),
            col(f"d.{colname}") == col(f"l.{colname}_KEY"),
            "left"
        )

        df_spark = df_spark.drop(col(f"d.{colname}")) \
                        .withColumnRenamed(f"{colname}_DECODED", colname) \
                        .drop(f"{colname}_KEY")
        
        df_spark = df_spark.alias("d")

    # Decode ONE-HOT columns
    onehot_groups = {
        "day_name": "day_name_",
        "stock_sector": "stock_sector_",
        "stock_industry": "stock_industry_",
        "stock_ticker": "stock_ticker_"
    }

    for new_col, prefix in onehot_groups.items():
        onehot_cols = [c for c in df_spark.columns if c.startswith(prefix)]
        if onehot_cols:
            expr = "CASE "
            for colname in onehot_cols:
                category = colname[len(prefix):]
                expr += f"WHEN `{colname}`=1 THEN '{category}' "
            expr += "END"
            df_spark = df_spark.withColumn(new_col, F.expr(expr))

    print("Reversing log transformations for STK001 stock_price and total_trade_amount")
    df_spark = df_spark.withColumn("stock_price",F.when(col("stock_ticker") == "STK001", F.exp(col("stock_price"))).otherwise(col("stock_price")))
    df_spark = df_spark.withColumn("total_trade_amount",F.exp(col("total_trade_amount")))

    print("Showing sample of decoded Spark DataFrame:")
    df_spark.show(10)

    print("3.3.2 SPARK DATAFRAME QUESTIONS")

    print("Total trading volume per stock ticker:")
    volume_per_ticker = df_spark.groupBy("stock_ticker") \
                                .agg(_sum("quantity").alias("total_volume")) 
    volume_per_ticker.show() 
    save_to_db_spark(volume_per_ticker, "spark_analytics_1")

    print("Average stock price per sector:")
    avg_stock_spark = df_spark.groupBy("stock_sector") \
                      .agg(avg("stock_price").alias("avg_price")) 
    avg_stock_spark.show()
    save_to_db_spark(avg_stock_spark, "spark_analytics_2")

    print("Buy vs Sell transactions on weekends:")
    weekend_transactions = df_spark.filter(col("is_weekend") == 'True') \
                                .groupBy("transaction_type") \
                                .count()
    weekend_transactions.show()
    save_to_db_spark(weekend_transactions, "spark_analytics_3")
    
    print("Customers with more than 10 transactions:")
    high_transaction_customers = df_spark.groupBy("customer_id") \
                                        .count() \
                                        .filter(col("count") > 10) 
    high_transaction_customers.show(n=10000)
    save_to_db_spark(high_transaction_customers, "spark_analytics_4")

    print("Total trade amount per day of the week:")
    trade_amount_per_day = df_spark.groupBy("day_name") \
                                .agg(_sum("total_trade_amount").alias("total_amt")) \
                                .orderBy(desc("total_amt"))
    trade_amount_per_day.show()
    save_to_db_spark(trade_amount_per_day, "spark_analytics_5")

    print("3.3.3 SPARK SQL QUESTIONS")

    df_spark.createOrReplaceTempView("stocks")

    print("Top 5 stock tickers by total quantity traded:")
    top_tickers = spark.sql("""
        SELECT stock_ticker, SUM(quantity) AS total_qty 
        FROM stocks 
        GROUP BY stock_ticker 
        ORDER BY total_qty DESC 
        LIMIT 5
    """)
    top_tickers.show()
    save_to_db_spark(top_tickers, "spark_sql_1")

    print("Average total trade amount by customer account type:")
    avg_trade_by_account = spark.sql("""
        SELECT customer_account_type, AVG(total_trade_amount) AS avg_trade_amt
        FROM stocks
        GROUP BY customer_account_type
    """)
    avg_trade_by_account.show()
    save_to_db_spark(avg_trade_by_account, "spark_sql_2")
    
    print("Number of transactions on holidays vs non-holidays:")
    holiday_transactions = spark.sql("""
        SELECT is_holiday, COUNT(*) AS trans_count
        FROM stocks
        GROUP BY is_holiday
    """)
    holiday_transactions.show()
    save_to_db_spark(holiday_transactions, "spark_sql_3")
    
    print("Total trading volume by stock sector on weekends sorted descendengly to display  highest total first:")
    weekend_volume_by_sector = spark.sql("""
        SELECT stock_sector, SUM(quantity) AS total_vol
        FROM stocks
        WHERE is_weekend = 'True'
        GROUP BY stock_sector
        ORDER BY total_vol DESC
    """)
    weekend_volume_by_sector.show()
    save_to_db_spark(weekend_volume_by_sector, "spark_sql_4")
    
    print("total buy vs sell amount for each stock liquidity tier")
    trade_by_liquidity_tier = spark.sql("""
        SELECT stock_liquidity_tier, transaction_type, SUM(total_trade_amount) AS total_amt
        FROM stocks
        GROUP BY stock_liquidity_tier, transaction_type
    """)
    trade_by_liquidity_tier.show()
    save_to_db_spark(trade_by_liquidity_tier, "spark_sql_5")

    spark.stop()
