"""DAG Airflow pour le pipeline Sales."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
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
SPARK_MASTER = "spark://spark-master:7077"
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
SPARK_CONF = (
    "--master {{ params.spark_master }} "
    "--deploy-mode client "
    "--executor-memory 2g "
    "--executor-cores 2 "
    "--num-executors 2 "
    "--conf spark.sql.shuffle.partitions=100 "
    "--packages org.postgresql:postgresql:42.6.0,io.delta:delta-spark_2.12:3.2.0 "
    "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
)


def spark_submit_cmd(app_name: str, app_path: str) -> str:
    """Génère la commande spark-submit."""
    return f"{SPARK_SUBMIT} {SPARK_CONF} --name {app_name} {app_path}"


with DAG(
    dag_id="sales_pipeline",
    default_args=default_args,
    description="Pipeline de données Sales: Bronze -> Silver -> Gold",
    schedule_interval="0 6 * * *",  # Tous les jours à 6h
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["sales", "etl", "daily"],
    max_active_runs=1,
    params={"spark_master": SPARK_MASTER},
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # ============ BRONZE LAYER ============
    with TaskGroup("bronze_layer", tooltip="Ingestion vers Bronze") as bronze_group:

        ingest_orders = BashOperator(
            task_id="ingest_orders",
            bash_command=spark_submit_cmd(
                "sales_ingest_orders", "/opt/spark-apps/ingest_orders.py"
            ),
        )

        ingest_customers = BashOperator(
            task_id="ingest_customers",
            bash_command=spark_submit_cmd(
                "sales_ingest_customers", "/opt/spark-apps/ingest_customers.py"
            ),
        )

        ingest_orders >> ingest_customers

    # ============ SILVER LAYER ============
    with TaskGroup("silver_layer", tooltip="Transformation vers Silver") as silver_group:

        transform_orders = BashOperator(
            task_id="transform_orders",
            bash_command=spark_submit_cmd(
                "sales_transform_orders", "/opt/spark-apps/transform_orders.py"
            ),
        )

    # ============ GOLD LAYER ============
    with TaskGroup("gold_layer", tooltip="Agrégation vers Gold") as gold_group:

        aggregate_sales = BashOperator(
            task_id="aggregate_sales",
            bash_command=spark_submit_cmd("sales_aggregate", "/opt/spark-apps/aggregate_sales.py"),
        )

    # ============ DEPENDENCIES ============
    start >> bronze_group >> silver_group >> gold_group >> end
