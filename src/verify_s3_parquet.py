from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .appName("VerifyS3Parquet")
        .getOrCreate()
    )

    df = spark.read.parquet(
        "s3a://epsilon-demo-data-lake/curated/transactions"
    )

    df.show()
    df.printSchema()

    spark.stop()


if __name__ == "__main__":
    main()