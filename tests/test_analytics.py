import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import pytest

from pyspark.sql import SparkSession

from analytics import calculate_daily_merchant_metrics


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("AnalyticsTest")
        .master("local[2]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )

    yield spark

    spark.stop()


def test_daily_merchant_metrics(spark):
    data = [
        ("TXN001", "CUST001", "M001", "2026-09-01 09:15:00", 100.0),
        ("TXN002", "CUST002", "M001", "2026-09-01 10:15:00", 200.0),
        ("TXN003", "CUST003", "M002", "2026-09-01 11:15:00", 50.0),
    ]

    columns = [
        "transaction_id",
        "customer_id",
        "merchant_id",
        "transaction_timestamp",
        "amount",
    ]

    df = spark.createDataFrame(data, columns)

    df = df.withColumn(
        "transaction_timestamp",
        df["transaction_timestamp"].cast("timestamp")
    )

    result = calculate_daily_merchant_metrics(df)

    rows = result.collect()

    assert len(rows) == 2

    assert rows[0]["merchant_id"] == "M001"
    assert rows[0]["transaction_count"] == 2
    assert rows[0]["total_amount"] == 300.0
    assert rows[0]["average_amount"] == 150.0

    assert rows[1]["merchant_id"] == "M002"
    assert rows[1]["transaction_count"] == 1
    assert rows[1]["total_amount"] == 50.0
    assert rows[1]["average_amount"] == 50.0