"""Schémas pour les données de commandes."""
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Schéma Bronze (données brutes)
ORDERS_BRONZE_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_date", TimestampType(), False),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DecimalType(10, 2), True),
        StructField("discount", DecimalType(5, 2), True),
        StructField("status", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)

# Schéma Silver (données nettoyées et validées)
ORDERS_SILVER_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_date", TimestampType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DecimalType(10, 2), False),
        StructField("discount", DecimalType(5, 2), False),
        StructField("total_amount", DecimalType(12, 2), False),
        StructField("status", StringType(), False),
        StructField("processing_timestamp", TimestampType(), False),
    ]
)

# Schéma Gold (agrégations)
DAILY_SALES_SCHEMA = StructType(
    [
        StructField("report_date", TimestampType(), False),
        StructField("total_orders", IntegerType(), False),
        StructField("total_revenue", DecimalType(15, 2), False),
        StructField("average_order_value", DecimalType(10, 2), False),
        StructField("total_customers", IntegerType(), False),
    ]
)
