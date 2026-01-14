"""Job de transformation des commandes Bronze -> Silver."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.quality.data_quality import CheckSeverity, DataQualityChecker
from src.common.readers.delta_reader import DeltaReader
from src.common.transformers.base_transformer import TransformationPipeline
from src.common.transformers.cleaning import (
    DropDuplicatesTransformer,
    FillNullsTransformer,
    TrimStringsTransformer,
)
from src.common.writers.delta_writer import DeltaWriter
from src.core.base_job import BaseSparkJob
from src.core.config_manager import Environment


class TransformOrdersJob(BaseSparkJob):
    """Job de transformation des commandes."""

    def __init__(self, environment: Environment | None = None):
        super().__init__(
            job_name="transform_orders",
            domain="sales",
            environment=environment,
        )

    def extract(self) -> DataFrame:
        """Lit les données de la couche Bronze."""
        bronze_config = self.config.target_config.get("bronze", {})
        reader = DeltaReader(self.spark, bronze_config)
        return reader.read()

    def transform(self, df: DataFrame) -> DataFrame:
        """Applique les transformations métier."""
        # Pipeline de transformations
        pipeline = TransformationPipeline()

        # Nettoyage
        pipeline.add(TrimStringsTransformer())
        pipeline.add(DropDuplicatesTransformer(columns=["order_id"]))
        pipeline.add(FillNullsTransformer(fills={"discount": 0.0, "quantity": 0}))

        df = pipeline.execute(df)

        # Calculs métier
        df = df.withColumn(
            "total_amount",
            F.round(F.col("quantity") * F.col("unit_price") * (1 - F.col("discount") / 100), 2),
        )

        # Standardisation du statut
        df = df.withColumn("status", F.lower(F.trim(F.col("status"))))

        # Timestamp de traitement
        df = df.withColumn("processing_timestamp", F.current_timestamp())

        return df

    def validate(self, df: DataFrame) -> DataFrame:
        """Valide la qualité des données."""
        checker = DataQualityChecker(df)

        checker.check_not_null(
            columns=["order_id", "customer_id", "order_date", "product_id"],
            severity=CheckSeverity.CRITICAL,
        ).check_unique(
            columns=["order_id"],
            severity=CheckSeverity.ERROR,
        ).check_range(
            column="quantity",
            min_value=1,
            max_value=10000,
            severity=CheckSeverity.ERROR,
        ).check_values_in_set(
            column="status",
            allowed_values={"pending", "confirmed", "shipped", "delivered", "cancelled"},
            severity=CheckSeverity.WARNING,
        ).check_row_count(
            min_count=1,
            severity=CheckSeverity.ERROR,
        )

        # Exécuter les checks
        checker.run(fail_on_error=True)

        return df

    def load(self, df: DataFrame) -> None:
        """Écrit dans la couche Silver avec merge."""
        silver_config = self.config.target_config.get("silver", {})

        writer = DeltaWriter(self.spark, silver_config)
        writer.write(df)

        # Optimisation périodique
        writer.optimize(z_order_columns=["customer_id", "order_date"])


def main() -> None:
    """Point d'entrée du job."""
    import argparse

    parser = argparse.ArgumentParser(description="Transform Orders Job")
    parser.add_argument(
        "--environment",
        type=str,
        default="dev",
        choices=["local", "dev", "staging", "prod"],
    )

    args = parser.parse_args()

    job = TransformOrdersJob(environment=Environment(args.environment))

    try:
        result = job.run()
        print(f"Job terminé: {result}")
    finally:
        job.cleanup()


if __name__ == "__main__":
    main()
