# AWS Databricks PySpark Data Lake

A hands-on data engineering project demonstrating a small transaction data pipeline using **Python, PySpark, Parquet, Data Lake architecture, AWS S3, and Databricks**.

The project starts with a local PySpark implementation and will gradually evolve into an AWS + Databricks data platform.

## Current Architecture

```text
transactions.csv
      |
      v
   PySpark
      |
      v
Clean / Transform
      |
      v
Curated Parquet
      |
      v
Daily Merchant Analytics
      |
      v
Analytics Parquet
```

## Data Lake Layers

```text
data/
|
+-- raw/
|   +-- transactions.csv
|
+-- curated/
|   +-- transactions/
|       +-- *.parquet
|
+-- analytics/
    +-- daily_merchant_metrics/
        +-- *.parquet
```

### Raw Layer

Contains the original transaction data:

```text
data/raw/transactions.csv
```

### Curated Layer

Contains cleaned and transformed transaction data stored as Parquet:

```text
data/curated/transactions/
```

### Analytics Layer

Contains aggregated business metrics stored as Parquet:

```text
data/analytics/daily_merchant_metrics/
```

## Transaction Data

The sample dataset contains:

| Column | Description |
|---|---|
| `transaction_id` | Unique transaction identifier |
| `customer_id` | Customer identifier |
| `merchant_id` | Merchant identifier |
| `transaction_timestamp` | Transaction timestamp |
| `amount` | Transaction amount |
| `currency` | Currency code |
| `transaction_type` | Transaction type |
| `status` | Transaction status |

## PySpark Processing

The pipeline:

1. Reads transaction data using PySpark.
2. Applies an explicit Spark schema.
3. Converts timestamps to `TimestampType`.
4. Converts amounts to `DoubleType`.
5. Removes invalid/non-positive amounts.
6. Keeps only completed transactions.
7. Removes duplicate transaction IDs.
8. Writes cleaned data as Parquet.
9. Calculates daily merchant metrics.
10. Writes analytics results as Parquet.

## Data Quality Rules

```python
.filter(F.col("amount") > 0)
.filter(F.col("status") == "COMPLETED")
.dropDuplicates(["transaction_id"])
```

## Analytics

The pipeline calculates:

```text
transaction_date
merchant_id
transaction_count
total_amount
average_amount
```

Example:

```text
2026-09-01  M001  2  335.50  167.75
2026-09-01  M002  2  395.25  197.625
2026-09-01  M003  1   50.75   50.75
```

## Why Parquet?

Parquet is a **columnar storage format** designed for analytical workloads.

Advantages include:

- Efficient column-based reads
- Data type preservation
- Compression
- Efficient analytical processing
- Strong integration with Spark and Databricks

The project uses:

```text
Raw       -> CSV
Curated   -> Parquet
Analytics -> Parquet
```

## Project Structure

```text
aws-databricks-pyspark-data-lake/
|
+-- .gitignore
+-- README.md
+-- requirements.txt
|
+-- data/
|   +-- raw/
|       +-- transactions.csv
|
+-- src/
|   +-- __init__.py
|   +-- schema.py
|   +-- analytics.py
|   +-- local_transform.py
|
+-- notebooks/
|
+-- tests/
    +-- test_analytics.py
```

Generated Parquet output under `data/curated/` and `data/analytics/` is excluded from Git.

## Running Locally

### Create virtual environment

```bash
python3 -m venv .venv
```

### Activate

```bash
source .venv/bin/activate
```

### Install dependencies

```bash
pip install -r requirements.txt
```

### Run the PySpark pipeline

```bash
python src/local_transform.py
```

The pipeline generates:

```text
data/curated/transactions/
data/analytics/daily_merchant_metrics/
```

## Running Tests

```bash
pytest -q
```

Current result:

```text
1 passed
```

## Technology Stack

### Current

- Python
- PySpark
- Apache Spark
- Parquet
- PyTest
- Git / GitHub

### Planned

- AWS S3
- Databricks
- Databricks notebooks
- Databricks compute
- AWS Data Lake architecture

## Planned AWS Architecture

```text
                  AWS S3 Data Lake
                         |
             +-----------+-----------+
             |                       |
            Raw                   Curated
             |                       |
      transactions/           transactions/
             |                       |
             +-----------+-----------+
                         |
                    Databricks
                         |
                      PySpark
                         |
                         v
                     Analytics
                         |
                         v
                  S3 analytics/
```

Planned S3 structure:

```text
s3://epsilon-demo-data-lake/

+-- raw/
|   +-- transactions/
|
+-- curated/
|   +-- transactions/
|
+-- analytics/
    +-- daily/
```

## Project Roadmap

### Phase 1 — Local PySpark

- [x] Create transaction dataset
- [x] Create Spark schema
- [x] Read CSV with PySpark
- [x] Clean and transform data
- [x] Write curated Parquet
- [x] Calculate daily merchant metrics
- [x] Write analytics Parquet
- [x] Add PySpark unit test

### Phase 2 — AWS S3

- [ ] Create S3 Data Lake
- [ ] Create raw/curated/analytics layers
- [ ] Upload raw transaction data
- [ ] Write PySpark output to S3
- [ ] Read/write Parquet from S3

### Phase 3 — Databricks

- [ ] Create Databricks workspace
- [ ] Create compute
- [ ] Create Databricks notebook
- [ ] Run PySpark transformations
- [ ] Read data from S3
- [ ] Write curated and analytics data to S3

### Phase 4 — Advanced Data Engineering

- [ ] Spark partitions
- [ ] Partition pruning
- [ ] Predicate pushdown
- [ ] Shuffle
- [ ] Caching
- [ ] Performance optimization
- [ ] Incremental processing

### Phase 5 — AWS Event-Driven Architecture

Potential future extensions:

```text
Transaction Source
       |
       v
      S3
       |
       v
   EventBridge
       |
       v
     Lambda
       |
       v
   Databricks
       |
       v
   Data Lake
```

Potential additional services:

- Lambda
- EventBridge
- SQS
- SNS
- API Gateway
- DynamoDB

## Status

**Phase 1 — Local PySpark Data Lake completed.**

The project currently demonstrates an end-to-end local pipeline:

```text
CSV
 |
 v
PySpark
 |
 v
Clean / Transform
 |
 v
Curated Parquet
 |
 v
Analytics
 |
 v
Analytics Parquet
```

The next major milestone is migrating this Data Lake architecture to **AWS S3**, followed by **Databricks** processing.
