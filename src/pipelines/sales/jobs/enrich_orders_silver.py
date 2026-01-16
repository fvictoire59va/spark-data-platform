"""Job d'enrichissement et transformation Silver pour les commandes."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.quality.data_quality import CheckSeverity, DataQualityChecker
from src.common.readers.delta_reader import DeltaReader
from src.common.transformers.silver_transformers import (
    DiscountAnalysisTransformer,
    FulfillmentStatusTransformer,
    MarginCalculationTransformer,
    OrderValueSegmentationTransformer,
    PaymentStatusTransformer,
    RepeatOrderDetectionTransformer,
    TaxCalculationTransformer,
)
from src.common.writers.delta_writer import DeltaWriter
from src.core.base_job import BaseSparkJob
from src.core.config_manager import Environment


class EnrichOrdersSilverJob(BaseSparkJob):
    """Job d'enrichissement des commandes vers Silver."""

    def __init__(self, environment: Environment | None = None):
        super().__init__(
            job_name="enrich_orders_silver",
            domain="sales",
            environment=environment,
        )

    def extract(self) -> DataFrame:
        """Lit les données de la couche Bronze."""
        bronze_config = self.config.target_config.get("bronze", {})
        reader = DeltaReader(self.spark, bronze_config)

        # Lire les commandes
        orders_df = reader.read()

        return orders_df

    def transform(self, df: DataFrame) -> DataFrame:
        """Applique les enrichissements métier."""
        # Calcul du sous-total (avant taxes et remises finales)
        df = df.withColumn(
            "subtotal",
            F.round(F.col("quantity") * F.col("unit_price"), 2),
        )

        # Appliquer les transformateurs Silver
        # 1. Analyse des remises
        df = DiscountAnalysisTransformer().transform(df)

        # 2. Calcul des taxes
        tax_transformer = TaxCalculationTransformer(
            tax_rates={
                "FR": 0.20,
                "US": 0.10,
                "DE": 0.19,
                "IT": 0.22,
                "ES": 0.21,
                "GB": 0.20,
            }
        )
        df = tax_transformer.transform(df)

        # 3. Calcul des marges (si cost_price disponible)
        if "cost_price" in df.columns:
            df = MarginCalculationTransformer().transform(df)
        else:
            df = df.withColumn("margin_percent", F.lit(30.0))

        # 4. Segmentation par valeur de commande
        df = OrderValueSegmentationTransformer(high_value_threshold=500.0).transform(df)

        # 5. Détection des commandes répétées
        df = RepeatOrderDetectionTransformer().transform(df)

        # 6. Standardisation des statuts
        df = PaymentStatusTransformer().transform(df)
        df = FulfillmentStatusTransformer().transform(df)

        # 7. Renommage et sélection des colonnes finales
        df = df.withColumn("order_status", F.col("status"))
        df = df.withColumn("silver_processed_at", F.current_timestamp())
        df = df.withColumn("silver_version", F.lit(1))

        # Sélectionner les colonnes Silver attendues
        silver_columns = [
            "order_id",
            "customer_id",
            "order_date",
            "product_id",
            "quantity",
            "unit_price",
            "discount_percent",
            "discount_amount",
            "subtotal",
            "tax_amount",
            "total_amount",
            "order_status",
            "payment_status",
            "fulfillment_status",
            "customer_name",
            "customer_segment",
            "customer_country",
            "is_vip_customer",
            "product_name",
            "product_category",
            "product_brand",
            "product_price",
            "margin_percent",
            "is_high_value_order",
            "is_repeat_order",
            "days_to_delivery",
            "processing_timestamp",
            "delivery_date",
            "silver_processed_at",
            "silver_version",
        ]

        # Gardez seulement les colonnes qui existent
        existing_columns = [col for col in silver_columns if col in df.columns]
        df = df.select(existing_columns)

        return df

    def validate(self, df: DataFrame) -> DataFrame:
        """Valide les données Silver."""
        checker = DataQualityChecker(df)

        # Validations critiques
        checker.check_not_null(
            columns=["order_id", "customer_id", "order_date", "product_id"],
            severity=CheckSeverity.CRITICAL,
        ).check_unique(
            columns=["order_id"],
            severity=CheckSeverity.CRITICAL,
        ).check_range(
            column="quantity",
            min_value=1,
            max_value=10000,
            severity=CheckSeverity.ERROR,
        ).check_range(
            column="margin_percent",
            min_value=-100,
            max_value=100,
            severity=CheckSeverity.WARNING,
        ).check_values_in_set(
            column="payment_status",
            allowed_values={"pending", "completed", "cancelled", "unknown"},
            severity=CheckSeverity.ERROR,
        )

        # Validations métier
        checker.run(fail_on_error=True)

        return df

    def load(self, df: DataFrame) -> None:
        """Écrit dans la couche Silver avec merge."""
        silver_config = self.config.target_config.get("silver", {})

        writer = DeltaWriter(self.spark, silver_config)
        writer.write(df)

        # Optimisation périodique
        writer.optimize(z_order_columns=["customer_id", "order_date", "product_id"])


def main() -> None:
    """Point d'entrée du job."""
    import argparse

    parser = argparse.ArgumentParser(description="Enrich Orders Silver Job")
    parser.add_argument(
        "--environment",
        type=str,
        default="dev",
        choices=["local", "dev", "staging", "prod"],
        help="Environnement d'exécution",
    )

    args = parser.parse_args()

    job = EnrichOrdersSilverJob(environment=Environment(args.environment))

    try:
        result = job.run()
        print(f"Job terminé: {result}")
    finally:
        job.cleanup()


if __name__ == "__main__":
    main()
