from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from schema import TRANSACTION_SCHEMA
from analytics import calculate_daily_merchant_metrics


def main():
    spark = (
        SparkSession.builder
        .appName("CustomerTransactionDataLake")
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )

    input_path = "data/raw/transactions.csv"

    # Read raw transaction data
    df = (
        spark.read
        .option("header", True)
        .schema(TRANSACTION_SCHEMA)
        .csv(input_path)
    )

    print("=== RAW DATA ===")
    df.show()

    print("=== SCHEMA ===")
    df.printSchema()

    # Clean and transform data
    clean_df = (
        df
        .withColumn(
            "transaction_timestamp",
            F.to_timestamp("transaction_timestamp")
        )
        .withColumn(
            "amount",
            F.col("amount").cast("double")
        )
        .filter(F.col("amount") > 0)
        .filter(F.col("status") == "COMPLETED")
        .dropDuplicates(["transaction_id"])
    )

    print("=== CLEAN DATA ===")
    clean_df.show()

        # Write curated data as Parquet
    clean_df.write.mode("overwrite").parquet(
        "s3a://epsilon-demo-data-lake/curated/transactions"
    )

    print("Curated data written to data/curated/transactions")

    daily_metrics = calculate_daily_merchant_metrics(clean_df)

    print("\nDaily Merchant Metrics:")
    daily_metrics.show()

    # Write analytics data as Parquet
    daily_metrics.write.mode("overwrite").parquet(
        "s3a://epsilon-demo-data-lake/analytics/daily_merchant_metrics"
    )

    print("Analytics data written to data/analytics/daily_merchant_metrics")

    spark.stop()


if __name__ == "__main__":
    main()