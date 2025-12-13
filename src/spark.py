import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, sum as _sum, avg, desc, count

def run_spark_analysis(lookup_table, analytics_filepath=None, ):
    print("Initializing Spark Session...")

    spark = SparkSession.builder \
        .appName("StockAnalysis") \
        .master("spark://spark-master:7077") \
        .config("spark.driver.host", "stock-analysis") \
        .getOrCreate()

    file_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "merged_datasets",
        "FULL_STOCKS.csv"
    )

    df_spark = spark.read.csv(file_path, header=True, inferSchema=True)

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
    df_spark.groupBy("stock_ticker") \
            .agg(_sum("quantity").alias("total_volume")) \
            .show()

    print("Average stock price per sector:")
    df_spark.groupBy("stock_sector") \
            .agg(avg("stock_price").alias("avg_price")) \
            .show()

    print("Buy vs Sell transactions on weekends:")
    df_spark.filter(col("is_weekend") == 'True') \
            .groupBy("transaction_type") \
            .count() \
            .show()
    
    # print("Buy vs Sell transactions on weekends:")
    # df_spark.groupBy("is_weekend", "transaction_type") \
    #         .count() \
    #         .show()

    print("Customers with more than 10 transactions:")
    df_spark.groupBy("customer_id") \
            .count() \
            .filter(col("count") > 10) \
            .show(n=10000) # default is only 20 rows, so set to a high number to show all

    print("Total trade amount per day of the week:")
    df_spark.groupBy("day_name") \
            .agg(_sum("total_trade_amount").alias("total_amt")) \
            .orderBy(desc("total_amt")) \
            .show()

    print("3.3.3 SPARK SQL QUESTIONS")

    df_spark.createOrReplaceTempView("stocks")

    print("Top 5 stock tickers by total quantity traded:")
    spark.sql("""
        SELECT stock_ticker, SUM(quantity) AS total_qty 
        FROM stocks 
        GROUP BY stock_ticker 
        ORDER BY total_qty DESC 
        LIMIT 5
    """).show()

    print("Average total trade amount by customer account type:")
    spark.sql("""
        SELECT customer_account_type, AVG(total_trade_amount) AS avg_trade_amt
        FROM stocks
        GROUP BY customer_account_type
    """).show()

    print("Number of transactions on holidays vs non-holidays:")
    spark.sql("""
        SELECT is_holiday, COUNT(*) AS trans_count
        FROM stocks
        GROUP BY is_holiday
    """).show()

    print("Total trading volume by stock sector on weekends sorted descendengly to display  highest total first:")
    spark.sql("""
        SELECT stock_sector, SUM(quantity) AS total_vol
        FROM stocks
        WHERE is_weekend = 'True'
        GROUP BY stock_sector
        ORDER BY total_vol DESC
    """).show()

    print("total buy vs sell amount for each stock liquidity tier")
    spark.sql("""
        SELECT stock_liquidity_tier, transaction_type, SUM(total_trade_amount) AS total_amt
        FROM stocks
        GROUP BY stock_liquidity_tier, transaction_type
    """).show()

    spark.stop()
