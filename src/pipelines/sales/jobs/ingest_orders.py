"""Job d'ingestion des commandes vers Bronze."""
from __future__ import annotations

from pyspark.sql import DataFrame, functions as F

from src.common.readers.jdbc_reader import JDBCReader
from src.common.writers.delta_writer import DeltaWriter
from src.core.config_manager import Environment
from src.core.base_job import BaseSparkJob
from src.pipelines.sales.schemas.orders_schema import ORDERS_BRONZE_SCHEMA


class IngestOrdersJob(BaseSparkJob):
    """Job d'ingestion des commandes depuis la base source."""

    def __init__(self, environment: Environment | None = None):
        super().__init__(
            job_name="ingest_orders",
            domain="sales",
            environment=environment,
        )

    def extract(self) -> DataFrame:
        """Extrait les commandes depuis la base de données source."""
        source_config = self.config.source_config.get("orders", {})
        
        reader = JDBCReader(self.spark, source_config)
        df = reader.with_schema(ORDERS_BRONZE_SCHEMA).read()
        
        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """Ajoute les métadonnées d'ingestion."""
        return df.withColumn(
            "_ingestion_timestamp", F.current_timestamp()
        ).withColumn(
            "_source", F.lit("jdbc_orders")
        )

    def load(self, df: DataFrame) -> None:
        """Écrit les données dans la couche Bronze."""
        target_config = self.config.target_config.get("bronze", {})
        
        writer = DeltaWriter(self.spark, target_config)
        writer.write(df)


def main() -> None:
    """Point d'entrée du job."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Ingest Orders Job")
    parser.add_argument(
        "--environment",
        type=str,
        default="dev",
        choices=["local", "dev", "staging", "prod"],
        help="Environnement d'exécution",
    )
    
    args = parser.parse_args()
    
    job = IngestOrdersJob(environment=Environment(args.environment))
    
    try:
        result = job.run()
        print(f"Job terminé: {result}")
    finally:
        job.cleanup()


if __name__ == "__main__":
    main()
