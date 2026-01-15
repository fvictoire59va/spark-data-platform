"""DAG Airflow pour le pipeline Sales."""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup

# Configuration par défaut
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["data-alerts@company.com"],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# Configuration Spark commune
SPARK_CONN_ID = "spark_default"
SPARK_MASTER = "spark://spark-master:7077"
SPARK_CONFIG = {
    "spark.executor.memory": "2g",
    "spark.executor.cores": "2",
    "spark.executor.instances": "2",
    "spark.sql.shuffle.partitions": "100",
}


with DAG(
    dag_id="sales_pipeline",
    default_args=default_args,
    description="Pipeline de données Sales: Bronze -> Silver -> Gold",
    schedule_interval="0 6 * * *",  # Tous les jours à 6h
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sales", "etl", "daily"],
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # ============ BRONZE LAYER ============
    with TaskGroup("bronze_layer", tooltip="Ingestion vers Bronze") as bronze_group:

        ingest_orders = SparkSubmitOperator(
            task_id="ingest_orders",
            application="local:///opt/spark-apps/ingest_orders.py",
            conf={
                **SPARK_CONFIG,
                "spark.app.name": "sales_ingest_orders",
            },
            conn_id=SPARK_CONN_ID,
            verbose=True,
            packages="org.postgresql:postgresql:42.6.0",
        )

        ingest_customers = SparkSubmitOperator(
            task_id="ingest_customers",
            application="local:///opt/spark-apps/ingest_customers.py",
            conf={
                **SPARK_CONFIG,
                "spark.app.name": "sales_ingest_customers",
            },
            conn_id=SPARK_CONN_ID,
            verbose=True,
        )

        [ingest_orders, ingest_customers]

    # ============ SILVER LAYER ============
    with TaskGroup("silver_layer", tooltip="Transformation vers Silver") as silver_group:

        transform_orders = SparkSubmitOperator(
            task_id="transform_orders",
            application="local:///opt/spark-apps/transform_orders.py",
            conf={
                **SPARK_CONFIG,
                "spark.app.name": "sales_transform_orders",
            },
            conn_id=SPARK_CONN_ID,
            verbose=True,
        )

    # ============ GOLD LAYER ============
    with TaskGroup("gold_layer", tooltip="Agrégation vers Gold") as gold_group:

        aggregate_sales = SparkSubmitOperator(
            task_id="aggregate_sales",
            application="local:///opt/spark-apps/aggregate_sales.py",
            conf={
                **SPARK_CONFIG,
                "spark.app.name": "sales_aggregate",
            },
            conn_id=SPARK_CONN_ID,
            verbose=True,
        )

    # ============ DEPENDENCIES ============
    start >> bronze_group >> silver_group >> gold_group >> end
