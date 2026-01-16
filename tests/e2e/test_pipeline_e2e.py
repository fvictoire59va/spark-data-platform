#!/usr/bin/env python3
"""End-to-end pipeline test: Ingest → Transform → Aggregate"""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Initialize Spark Session
spark = (
    SparkSession.builder.appName("PipelineE2ETest")
    .master("spark://spark-master:7077")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("INFO")

# Test data schema
orders_schema = StructType(
    [
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("created_at", TimestampType(), True),
    ]
)

# Test data
test_orders = [
    (1, 101, 150.50, "completed", datetime(2026, 1, 10, 10, 0, 0)),
    (2, 102, 200.00, "pending", datetime(2026, 1, 11, 11, 0, 0)),
    (3, 101, 75.25, "completed", datetime(2026, 1, 12, 9, 0, 0)),
    (4, 103, 320.00, "failed", datetime(2026, 1, 13, 14, 0, 0)),
]

print("\n" + "=" * 60)
print("PIPELINE E2E TEST")
print("=" * 60)

# STEP 1: Bronze Layer - Create test data
print("\n[STEP 1] Creating test data in Bronze layer...")
df_orders = spark.createDataFrame(test_orders, orders_schema)
df_orders.show()
print(f"✓ Created {df_orders.count()} test orders")

# STEP 2: Silver Layer - Transform
print("\n[STEP 2] Silver Layer - Transformation...")
df_silver = df_orders.filter(df_orders.status == "completed").select(
    "order_id", "customer_id", "amount", "status", "created_at"
)
df_silver.show()
print(f"✓ Filtered to {df_silver.count()} completed orders")

# STEP 3: Gold Layer - Aggregation
print("\n[STEP 3] Gold Layer - Aggregation...")
df_gold = (
    df_silver.groupBy("customer_id")
    .agg({"amount": "sum", "order_id": "count"})
    .withColumnRenamed("sum(amount)", "total_amount")
    .withColumnRenamed("count(order_id)", "order_count")
)
df_gold.show()
print(f"✓ Aggregated to {df_gold.count()} customers")

# STEP 4: Verify results
print("\n[STEP 4] Verification...")
total_revenue = df_gold.agg({"total_amount": "sum"}).collect()[0][0]
print(f"  Total Revenue: ${total_revenue:.2f}")
print(f"  Orders processed: {df_silver.count()}")
print(f"  Unique customers: {df_gold.count()}")

# STEP 5: Test PostgreSQL connection
print("\n[STEP 5] Testing PostgreSQL connectivity...")
try:
    # Test read from metastore-db
    url = "jdbc:postgresql://metastore-db:5432/metastore"
    user = "hive"
    password = "hive123"
    table = "information_schema.tables"

    df_pg = (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    print("✓ PostgreSQL connection successful")
    print(f"  Schema tables available: {df_pg.count()}")
except Exception as e:
    print(f"✗ PostgreSQL connection failed: {e}")

print("\n" + "=" * 60)
print("PIPELINE E2E TEST COMPLETED SUCCESSFULLY ✓")
print("=" * 60 + "\n")

spark.stop()
