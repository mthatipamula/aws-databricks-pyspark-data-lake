from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("CustomerTransactionDataLake")
        .master("local[*]")
        .getOrCreate()
    )

    input_path = "data/raw/transactions.csv"

    # Read raw transaction data
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
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
        "data/curated/transactions"
    )

    print("Curated data written to data/curated/transactions")

    # Daily merchant-level analytics
    daily_metrics = (
        clean_df
        .groupBy(
            F.to_date("transaction_timestamp").alias("transaction_date"),
            "merchant_id"
        )
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("average_amount")
        )
        .orderBy("transaction_date", "merchant_id")
    )

    print("=== DAILY MERCHANT METRICS ===")
    daily_metrics.show()

    spark.stop()


if __name__ == "__main__":
    main()