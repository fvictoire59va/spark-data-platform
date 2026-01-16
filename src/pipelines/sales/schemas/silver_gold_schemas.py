"""Schémas pour les couches Silver et Gold."""

from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ============================================================================
# COUCHE SILVER - Données enrichies et validées
# ============================================================================

# Schéma Silver pour les commandes enrichies avec données client
ORDERS_SILVER_ENRICHED_SCHEMA = StructType(
    [
        # Identifiants
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        # Données commerciales
        StructField("order_date", TimestampType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DecimalType(10, 2), False),
        StructField("discount_percent", DecimalType(5, 2), False),
        StructField("discount_amount", DecimalType(10, 2), False),
        StructField("subtotal", DecimalType(12, 2), False),
        StructField("tax_amount", DecimalType(10, 2), False),
        StructField("total_amount", DecimalType(12, 2), False),
        # Métadonnées de statut
        StructField("order_status", StringType(), False),
        StructField("payment_status", StringType(), True),
        StructField("fulfillment_status", StringType(), True),
        # Données client enrichies
        StructField("customer_name", StringType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("customer_country", StringType(), True),
        StructField("is_vip_customer", StringType(), False),
        # Données produit enrichies
        StructField("product_name", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("product_brand", StringType(), True),
        StructField("product_price", DecimalType(10, 2), True),
        # Indicateurs calculés
        StructField("margin_percent", DoubleType(), False),
        StructField("is_high_value_order", StringType(), False),
        StructField("is_repeat_order", StringType(), False),
        StructField("days_to_delivery", IntegerType(), True),
        # Dates de traitement
        StructField("processing_timestamp", TimestampType(), False),
        StructField("delivery_date", TimestampType(), True),
        # Audit
        StructField("silver_processed_at", TimestampType(), False),
        StructField("silver_version", IntegerType(), False),
    ]
)

# Schéma Silver pour les clients master
CUSTOMERS_SILVER_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), False),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("country", StringType(), False),
        StructField("customer_segment", StringType(), False),
        StructField("lifetime_value", DecimalType(15, 2), False),
        StructField("total_orders", IntegerType(), False),
        StructField("is_active", StringType(), False),
        StructField("last_order_date", TimestampType(), True),
        StructField("is_vip", StringType(), False),
        StructField("registration_date", TimestampType(), False),
        StructField("silver_processed_at", TimestampType(), False),
    ]
)

# Schéma Silver pour les produits master
PRODUCTS_SILVER_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), False),
        StructField("brand", StringType(), False),
        StructField("unit_price", DecimalType(10, 2), False),
        StructField("cost_price", DecimalType(10, 2), True),
        StructField("stock_quantity", IntegerType(), False),
        StructField("is_active", StringType(), False),
        StructField("creation_date", TimestampType(), False),
        StructField("last_update", TimestampType(), False),
        StructField("silver_processed_at", TimestampType(), False),
    ]
)

# ============================================================================
# COUCHE GOLD - Agrégations et KPIs métier
# ============================================================================

# Schéma Gold pour les ventes quotidiennes
DAILY_SALES_GOLD_SCHEMA = StructType(
    [
        StructField("report_date", DateType(), False),
        StructField("total_orders", IntegerType(), False),
        StructField("total_revenue", DecimalType(15, 2), False),
        StructField("total_discount_given", DecimalType(15, 2), False),
        StructField("total_tax", DecimalType(15, 2), False),
        StructField("average_order_value", DecimalType(12, 2), False),
        StructField("unique_customers", IntegerType(), False),
        StructField("total_items_sold", IntegerType(), False),
        StructField("high_value_orders_count", IntegerType(), False),
        StructField("cancelled_orders_count", IntegerType(), False),
        StructField("average_margin_percent", DoubleType(), False),
        StructField("vip_orders_count", IntegerType(), False),
        StructField("gold_processed_at", TimestampType(), False),
    ]
)

# Schéma Gold pour les ventes par produit
PRODUCT_SALES_GOLD_SCHEMA = StructType(
    [
        StructField("report_date", DateType(), False),
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("product_category", StringType(), True),
        StructField("total_quantity_sold", IntegerType(), False),
        StructField("total_revenue", DecimalType(15, 2), False),
        StructField("average_unit_price", DecimalType(10, 2), False),
        StructField("total_discount", DecimalType(15, 2), False),
        StructField("total_margin", DecimalType(15, 2), False),
        StructField("number_of_transactions", IntegerType(), False),
        StructField("unique_customers", IntegerType(), False),
        StructField("average_quantity_per_order", DoubleType(), False),
        StructField("rank_by_revenue", IntegerType(), False),
        StructField("gold_processed_at", TimestampType(), False),
    ]
)

# Schéma Gold pour les ventes par client
CUSTOMER_SALES_GOLD_SCHEMA = StructType(
    [
        StructField("report_date", DateType(), False),
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("total_spend", DecimalType(15, 2), False),
        StructField("total_orders", IntegerType(), False),
        StructField("average_order_value", DecimalType(12, 2), False),
        StructField("total_items_purchased", IntegerType(), False),
        StructField("unique_products", IntegerType(), False),
        StructField("total_discount_received", DecimalType(15, 2), False),
        StructField("repeat_purchase_rate", DoubleType(), False),
        StructField("days_since_last_purchase", IntegerType(), True),
        StructField("is_high_value_customer", StringType(), False),
        StructField("gold_processed_at", TimestampType(), False),
    ]
)

# Schéma Gold pour les tendances mensuelles
MONTHLY_TRENDS_GOLD_SCHEMA = StructType(
    [
        StructField("year_month", StringType(), False),
        StructField("report_date", DateType(), False),
        StructField("total_revenue", DecimalType(15, 2), False),
        StructField("total_orders", IntegerType(), False),
        StructField("total_customers", IntegerType(), False),
        StructField("month_over_month_growth", DoubleType(), True),
        StructField("average_order_value", DecimalType(12, 2), False),
        StructField("top_product_id", StringType(), True),
        StructField("top_product_revenue", DecimalType(15, 2), True),
        StructField("total_discount", DecimalType(15, 2), False),
        StructField("return_rate", DoubleType(), False),
        StructField("gold_processed_at", TimestampType(), False),
    ]
)

# Schéma Gold pour les segments client
CUSTOMER_SEGMENT_GOLD_SCHEMA = StructType(
    [
        StructField("report_date", DateType(), False),
        StructField("segment", StringType(), False),
        StructField("total_customers", IntegerType(), False),
        StructField("total_revenue", DecimalType(15, 2), False),
        StructField("average_lifetime_value", DecimalType(12, 2), False),
        StructField("average_order_frequency", DoubleType(), False),
        StructField("total_orders", IntegerType(), False),
        StructField("repeat_customer_count", IntegerType(), False),
        StructField("repeat_customer_rate", DoubleType(), False),
        StructField("segment_revenue_share", DoubleType(), False),
        StructField("gold_processed_at", TimestampType(), False),
    ]
)

# Schéma Gold pour le top produits
TOP_PRODUCTS_GOLD_SCHEMA = StructType(
    [
        StructField("report_date", DateType(), False),
        StructField("rank", IntegerType(), False),
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("total_quantity_sold", IntegerType(), False),
        StructField("total_revenue", DecimalType(15, 2), False),
        StructField("unique_customers", IntegerType(), False),
        StructField("growth_rate", DoubleType(), True),
        StructField("contribution_to_total", DoubleType(), False),
        StructField("gold_processed_at", TimestampType(), False),
    ]
)

# Schéma Gold pour le RFM (Recency, Frequency, Monetary)
RFM_ANALYSIS_GOLD_SCHEMA = StructType(
    [
        StructField("report_date", DateType(), False),
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), True),
        StructField("days_since_last_purchase", IntegerType(), False),
        StructField("purchase_frequency", IntegerType(), False),
        StructField("monetary_value", DecimalType(15, 2), False),
        StructField("rfm_score", StringType(), False),
        StructField("customer_value_segment", StringType(), False),
        StructField("gold_processed_at", TimestampType(), False),
    ]
)
