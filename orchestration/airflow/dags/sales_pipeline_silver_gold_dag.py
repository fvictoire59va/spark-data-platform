"""
DAG Airflow pour le pipeline Silver/Gold complet.

Pipeline:
Bronze Layer (Ingestion)
    ↓
Silver Layer (Enrichissement)
    ↓
Gold Layer (Agrégations multiples - parallèles)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.spark_submit_operator import SparkSubmitOperator

# Configuration
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 15),
    "email": ["data-alerts@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# DAG
dag = DAG(
    dag_id="sales_pipeline_silver_gold",
    description="Pipeline de données Sales: Bronze -> Silver -> Gold",
    default_args=default_args,
    schedule_interval="0 6 * * *",  # Quotidien à 6h du matin
    catchup=False,
    tags=["sales", "data-engineering", "medallion"],
)

# ============================================================================
# BRONZE LAYER - Ingestion
# ============================================================================

ingest_orders = SparkSubmitOperator(
    task_id="bronze_layer.ingest_orders",
    application="{{ var.value.spark_jobs_path }}/ingest_orders.py",
    conf={
        "spark.driver.memory": "4g",
        "spark.executor.memory": "4g",
        "spark.executor.cores": "4",
        "spark.sql.shuffle.partitions": "100",
    },
    application_args=["--environment", "{{ var.value.environment }}"],
    spark_binary="/usr/bin/spark-submit",
    verbose=True,
    dag=dag,
)

ingest_customers = SparkSubmitOperator(
    task_id="bronze_layer.ingest_customers",
    application="{{ var.value.spark_jobs_path }}/ingest_customers.py",
    conf={
        "spark.driver.memory": "4g",
        "spark.executor.memory": "4g",
        "spark.executor.cores": "4",
    },
    application_args=["--environment", "{{ var.value.environment }}"],
    spark_binary="/usr/bin/spark-submit",
    verbose=True,
    dag=dag,
)

ingest_products = SparkSubmitOperator(
    task_id="bronze_layer.ingest_products",
    application="{{ var.value.spark_jobs_path }}/ingest_products.py",
    conf={
        "spark.driver.memory": "4g",
        "spark.executor.memory": "4g",
        "spark.executor.cores": "4",
    },
    application_args=["--environment", "{{ var.value.environment }}"],
    spark_binary="/usr/bin/spark-submit",
    verbose=True,
    dag=dag,
)

# Marker pour fin de Bronze layer
bronze_done = DummyOperator(
    task_id="bronze_layer.complete",
    dag=dag,
)

# ============================================================================
# SILVER LAYER - Enrichissement
# ============================================================================

enrich_orders_silver = SparkSubmitOperator(
    task_id="silver_layer.transform_orders",
    application="{{ var.value.spark_jobs_path }}/enrich_orders_silver.py",
    conf={
        "spark.driver.memory": "8g",
        "spark.executor.memory": "8g",
        "spark.executor.cores": "8",
        "spark.sql.shuffle.partitions": "200",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
    },
    application_args=["--environment", "{{ var.value.environment }}"],
    spark_binary="/usr/bin/spark-submit",
    verbose=True,
    dag=dag,
)

# Marker pour fin de Silver layer
silver_done = DummyOperator(
    task_id="silver_layer.complete",
    dag=dag,
)

# ============================================================================
# GOLD LAYER - Agrégations (Parallèles)
# ============================================================================

# 1. Agrégation quotidienne des ventes
daily_sales_gold = SparkSubmitOperator(
    task_id="gold_layer.aggregate_sales_daily",
    application="{{ var.value.spark_jobs_path }}/aggregate_gold.py",
    conf={
        "spark.driver.memory": "8g",
        "spark.executor.memory": "8g",
        "spark.executor.cores": "8",
        "spark.sql.shuffle.partitions": "200",
    },
    application_args=[
        "--environment",
        "{{ var.value.environment }}",
        "--job",
        "daily_sales",
    ],
    spark_binary="/usr/bin/spark-submit",
    verbose=True,
    dag=dag,
)

# 2. Ventes par produit
product_sales_gold = SparkSubmitOperator(
    task_id="gold_layer.aggregate_sales_product",
    application="{{ var.value.spark_jobs_path }}/aggregate_gold.py",
    conf={
        "spark.driver.memory": "8g",
        "spark.executor.memory": "8g",
        "spark.executor.cores": "8",
        "spark.sql.shuffle.partitions": "200",
    },
    application_args=[
        "--environment",
        "{{ var.value.environment }}",
        "--job",
        "product_sales",
    ],
    spark_binary="/usr/bin/spark-submit",
    verbose=True,
    dag=dag,
)

# 3. Ventes par client
customer_sales_gold = SparkSubmitOperator(
    task_id="gold_layer.aggregate_sales_customer",
    application="{{ var.value.spark_jobs_path }}/aggregate_gold.py",
    conf={
        "spark.driver.memory": "8g",
        "spark.executor.memory": "8g",
        "spark.executor.cores": "8",
        "spark.sql.shuffle.partitions": "200",
    },
    application_args=[
        "--environment",
        "{{ var.value.environment }}",
        "--job",
        "customer_sales",
    ],
    spark_binary="/usr/bin/spark-submit",
    verbose=True,
    dag=dag,
)

# 4. Tendances mensuelles
monthly_trends_gold = SparkSubmitOperator(
    task_id="gold_layer.aggregate_monthly_trends",
    application="{{ var.value.spark_jobs_path }}/aggregate_gold.py",
    conf={
        "spark.driver.memory": "8g",
        "spark.executor.memory": "8g",
        "spark.executor.cores": "8",
        "spark.sql.shuffle.partitions": "100",
    },
    application_args=[
        "--environment",
        "{{ var.value.environment }}",
        "--job",
        "monthly_trends",
    ],
    spark_binary="/usr/bin/spark-submit",
    verbose=True,
    dag=dag,
)

# 5. Segmentation client
segment_analysis_gold = SparkSubmitOperator(
    task_id="gold_layer.aggregate_customer_segment",
    application="{{ var.value.spark_jobs_path }}/aggregate_gold.py",
    conf={
        "spark.driver.memory": "8g",
        "spark.executor.memory": "8g",
        "spark.executor.cores": "8",
        "spark.sql.shuffle.partitions": "100",
    },
    application_args=[
        "--environment",
        "{{ var.value.environment }}",
        "--job",
        "customer_segment",
    ],
    spark_binary="/usr/bin/spark-submit",
    verbose=True,
    dag=dag,
)

# 6. Top produits
top_products_gold = SparkSubmitOperator(
    task_id="gold_layer.aggregate_top_products",
    application="{{ var.value.spark_jobs_path }}/aggregate_gold.py",
    conf={
        "spark.driver.memory": "8g",
        "spark.executor.memory": "8g",
        "spark.executor.cores": "8",
        "spark.sql.shuffle.partitions": "100",
    },
    application_args=[
        "--environment",
        "{{ var.value.environment }}",
        "--job",
        "top_products",
        "--top_n",
        "20",
    ],
    spark_binary="/usr/bin/spark-submit",
    verbose=True,
    dag=dag,
)

# 7. Analyse RFM
rfm_analysis_gold = SparkSubmitOperator(
    task_id="gold_layer.rfm_analysis",
    application="{{ var.value.spark_jobs_path }}/aggregate_gold.py",
    conf={
        "spark.driver.memory": "8g",
        "spark.executor.memory": "8g",
        "spark.executor.cores": "8",
        "spark.sql.shuffle.partitions": "200",
    },
    application_args=[
        "--environment",
        "{{ var.value.environment }}",
        "--job",
        "rfm_analysis",
    ],
    spark_binary="/usr/bin/spark-submit",
    verbose=True,
    dag=dag,
)

# Marker pour fin de Gold layer
gold_done = DummyOperator(
    task_id="gold_layer.complete",
    dag=dag,
)

# ============================================================================
# Pipeline End
# ============================================================================

pipeline_complete = DummyOperator(
    task_id="end",
    trigger_rule="all_done",
    dag=dag,
)

# ============================================================================
# Dépendances
# ============================================================================

# Bronze layer
[ingest_orders, ingest_customers, ingest_products] >> bronze_done

# Silver layer
bronze_done >> enrich_orders_silver >> silver_done

# Gold layer (parallèle)
(
    silver_done
    >> [
        daily_sales_gold,
        product_sales_gold,
        customer_sales_gold,
        monthly_trends_gold,
        segment_analysis_gold,
        top_products_gold,
        rfm_analysis_gold,
    ]
    >> gold_done
)

# End
gold_done >> pipeline_complete
