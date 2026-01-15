"""Job de test d'ingestion avec données CSV."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.core.base_job import BaseSparkJob
from src.core.config_manager import Environment
from src.pipelines.sales.schemas.orders_schema import ORDERS_BRONZE_SCHEMA


class TestIngestJob(BaseSparkJob):
    """Job de test d'ingestion avec données de sample."""

    def __init__(self, environment: Environment | None = None):
        super().__init__(
            job_name="test_ingest",
            domain="sales",
            environment=environment,
        )

    def extract(self) -> DataFrame:
        """Crée des données de test."""
        from decimal import Decimal

        # Créer des données de test
        data = [
            (
                "ORD001",
                "CUST001",
                datetime(2024, 1, 15, 10, 30),
                "PROD001",
                2,
                Decimal("19.99"),
                Decimal("0.00"),
                "COMPLETED",
                datetime(2024, 1, 15, 10, 30),
                datetime(2024, 1, 15, 10, 30),
            ),
            (
                "ORD002",
                "CUST002",
                datetime(2024, 1, 15, 11, 45),
                "PROD002",
                1,
                Decimal("49.99"),
                Decimal("5.00"),
                "COMPLETED",
                datetime(2024, 1, 15, 11, 45),
                datetime(2024, 1, 15, 11, 45),
            ),
            (
                "ORD003",
                "CUST001",
                datetime(2024, 1, 15, 14, 20),
                "PROD003",
                3,
                Decimal("12.50"),
                Decimal("1.25"),
                "PENDING",
                datetime(2024, 1, 15, 14, 20),
                datetime(2024, 1, 15, 14, 20),
            ),
            (
                "ORD004",
                "CUST003",
                datetime(2024, 1, 15, 16, 00),
                "PROD001",
                1,
                Decimal("19.99"),
                Decimal("0.00"),
                "COMPLETED",
                datetime(2024, 1, 15, 16, 00),
                datetime(2024, 1, 15, 16, 00),
            ),
        ]

        df = self.spark.createDataFrame(data, schema=ORDERS_BRONZE_SCHEMA)
        print(f"Données de test créées: {df.count()} lignes")

        return df

    def transform(self, df: DataFrame) -> DataFrame:
        """Ajoute les métadonnées d'ingestion."""
        return df.withColumn("_ingestion_timestamp", F.current_timestamp()).withColumn(
            "_source", F.lit("test_data")
        )

    def load(self, df: DataFrame) -> None:
        """Affiche les données (test mode)."""
        # Afficher les données
        print("\n📊 DONNÉES INGÉRÉES:")
        df.show(truncate=False)

        # Afficher les statistiques
        print("\n📈 STATISTIQUES:")
        print(f"  ✓ Total de lignes: {df.count()}")
        print(f"  ✓ Colonnes: {', '.join(df.columns)}")

        # Afficher les agrégations
        print("\n📉 AGRÉGATIONS PAR STATUT:")
        df.groupBy("status").agg(
            F.count("*").alias("nombre_commandes"),
            F.sum("quantity").alias("quantité_totale"),
            F.sum(F.col("quantity") * F.col("unit_price")).alias("montant_total"),
        ).show()


def main() -> None:
    """Point d'entrée du job."""
    import argparse

    parser = argparse.ArgumentParser(description="Test Ingest Job")
    parser.add_argument(
        "--environment",
        type=str,
        default="local",
        choices=["local", "dev", "staging", "prod"],
        help="Environnement d'exécution",
    )

    args = parser.parse_args()

    job = TestIngestJob(environment=Environment(args.environment))

    try:
        print("\n🚀 Lancement du job de test d'ingestion...\n")
        result = job.run()
        print("\n✅ Job terminé avec succès!")
        print(f"   Status: {result['status']}")
        print(f"   Lignes traitées: {result['records_processed']}")
        print(f"   Durée: {result['duration_seconds']}s\n")
    except Exception as e:
        print(f"\n❌ Erreur lors de l'exécution du job: {e}")
        raise
    finally:
        job.cleanup()


if __name__ == "__main__":
    main()
