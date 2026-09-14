from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def calculate_daily_merchant_metrics(df: DataFrame) -> DataFrame:
    """
    Calculate daily transaction metrics grouped by merchant.
    """

    return (
        df
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