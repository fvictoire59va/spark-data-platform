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
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# Configuration Spark commune
SPARK_CONN_ID = "spark_default"
SPARK_CONFIG = {
    "spark.executor.memory": "4g",
    "spark.executor.cores": "2",
    "spark.executor.instances": "4",
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.adaptive.enabled": "true",
}


def get_spark_submit_operator(
    task_id: str,
    job_class: str,
    **kwargs,
) -> SparkSubmitOperator:
    """Factory pour créer des SparkSubmitOperator standardisés."""
    return SparkSubmitOperator(
        task_id=task_id,
        conn_id=SPARK_CONN_ID,
        application="${SPARK_HOME}/jobs/spark-data-platform.jar",
        java_class=job_class,
        conf=SPARK_CONFIG,
        verbose=True,
        **kwargs,
    )


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
            conn_id=SPARK_CONN_ID,
            application="/opt/spark/jobs/src/pipelines/sales/jobs/ingest_orders.py",
            conf={
                **SPARK_CONFIG,
                "spark.app.name": "sales_ingest_orders",
            },
            application_args=[
                "--env", "{{ var.value.environment }}",
                "--date", "{{ ds }}",
            ],
            verbose=True,
        )

        ingest_customers = SparkSubmitOperator(
            task_id="ingest_customers",
            conn_id=SPARK_CONN_ID,
            application="/opt/spark/jobs/src/pipelines/sales/jobs/ingest_customers.py",
            conf={
                **SPARK_CONFIG,
                "spark.app.name": "sales_ingest_customers",
            },
            application_args=[
                "--env", "{{ var.value.environment }}",
                "--date", "{{ ds }}",
            ],
            verbose=True,
        )

        [ingest_orders, ingest_customers]

    # ============ SILVER LAYER ============
    with TaskGroup("silver_layer", tooltip="Transformation vers Silver") as silver_group:
        
        transform_orders = SparkSubmitOperator(
            task_id="transform_orders",
            conn_id=SPARK_CONN_ID,
            application="/opt/spark/jobs/src/pipelines/sales/jobs/transform_orders.py",
            conf={
                **SPARK_CONFIG,
                "spark.app.name": "sales_transform_orders",
            },
            application_args=[
                "--env", "{{ var.value.environment }}",
                "--date", "{{ ds }}",
            ],
            verbose=True,
        )

    # ============ GOLD LAYER ============
    with TaskGroup("gold_layer", tooltip="Agrégation vers Gold") as gold_group:
        
        aggregate_sales = SparkSubmitOperator(
            task_id="aggregate_sales",
            conn_id=SPARK_CONN_ID,
            application="/opt/spark/jobs/src/pipelines/sales/jobs/aggregate_sales.py",
            conf={
                **SPARK_CONFIG,
                "spark.app.name": "sales_aggregate",
            },
            application_args=[
                "--env", "{{ var.value.environment }}",
                "--date", "{{ ds }}",
            ],
            verbose=True,
        )

    # ============ DATA QUALITY ============
    with TaskGroup("data_quality", tooltip="Contrôles qualité") as quality_group:
        
        quality_checks = SparkSubmitOperator(
            task_id="quality_checks",
            conn_id=SPARK_CONN_ID,
            application="/opt/spark/jobs/src/pipelines/sales/jobs/quality_checks.py",
            conf={
                **SPARK_CONFIG,
                "spark.app.name": "sales_quality_checks",
            },
            application_args=[
                "--env", "{{ var.value.environment }}",
                "--date", "{{ ds }}",
            ],
            verbose=True,
        )

    # ============ DEPENDENCIES ============
    start >> bronze_group >> silver_group >> gold_group >> quality_group >> end
