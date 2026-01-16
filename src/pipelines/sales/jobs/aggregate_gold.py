"""Jobs d'agrégation Gold pour les KPIs de ventes."""

from __future__ import annotations

from pyspark.sql import DataFrame

from src.common.quality.data_quality import CheckSeverity, DataQualityChecker
from src.common.readers.delta_reader import DeltaReader
from src.common.transformers.gold_transformers import (
    CustomerSalesAggregationTransformer,
    CustomerSegmentAggregationTransformer,
    DailySalesAggregationTransformer,
    MonthlyTrendsTransformer,
    ProductSalesAggregationTransformer,
    RFMAnalysisTransformer,
    TopProductsTransformer,
)
from src.common.writers.delta_writer import DeltaWriter
from src.core.base_job import BaseSparkJob
from src.core.config_manager import Environment


class DailySalesAggregationJob(BaseSparkJob):
    """Job d'agrégation des ventes quotidiennes."""

    def __init__(self, environment: Environment | None = None):
        super().__init__(
            job_name="aggregate_sales_daily_gold",
            domain="sales",
            environment=environment,
        )

    def extract(self) -> DataFrame:
        """Lit les données de la couche Silver."""
        silver_config = self.config.target_config.get("silver", {})
        reader = DeltaReader(self.spark, silver_config)
        return reader.read()

    def transform(self, df: DataFrame) -> DataFrame:
        """Agrège les ventes quotidiennes."""
        return DailySalesAggregationTransformer().transform(df)

    def validate(self, df: DataFrame) -> DataFrame:
        """Valide les agrégations."""
        checker = DataQualityChecker(df)

        checker.check_not_null(
            columns=["report_date", "total_orders", "total_revenue"],
            severity=CheckSeverity.CRITICAL,
        ).check_row_count(
            min_count=1,
            severity=CheckSeverity.ERROR,
        ).check_range(
            column="total_orders",
            min_value=0,
            severity=CheckSeverity.ERROR,
        ).check_range(
            column="total_revenue",
            min_value=0,
            severity=CheckSeverity.ERROR,
        )

        checker.run(fail_on_error=True)
        return df

    def load(self, df: DataFrame) -> None:
        """Écrit l'agrégation dans Gold."""
        gold_config = self.config.target_config.get("gold", {})
        gold_config["table_name"] = "daily_sales"

        writer = DeltaWriter(self.spark, gold_config)
        writer.write(df)
        writer.optimize(z_order_columns=["report_date"])


class ProductSalesAggregationJob(BaseSparkJob):
    """Job d'agrégation des ventes par produit."""

    def __init__(self, environment: Environment | None = None):
        super().__init__(
            job_name="aggregate_sales_product_gold",
            domain="sales",
            environment=environment,
        )

    def extract(self) -> DataFrame:
        """Lit les données de la couche Silver."""
        silver_config = self.config.target_config.get("silver", {})
        reader = DeltaReader(self.spark, silver_config)
        return reader.read()

    def transform(self, df: DataFrame) -> DataFrame:
        """Agrège les ventes par produit."""
        return ProductSalesAggregationTransformer().transform(df)

    def validate(self, df: DataFrame) -> DataFrame:
        """Valide les agrégations."""
        checker = DataQualityChecker(df)

        checker.check_not_null(
            columns=["report_date", "product_id", "total_quantity_sold", "total_revenue"],
            severity=CheckSeverity.CRITICAL,
        ).check_range(
            column="total_quantity_sold",
            min_value=0,
            severity=CheckSeverity.ERROR,
        ).check_range(
            column="rank_by_revenue",
            min_value=1,
            severity=CheckSeverity.WARNING,
        )

        checker.run(fail_on_error=True)
        return df

    def load(self, df: DataFrame) -> None:
        """Écrit l'agrégation dans Gold."""
        gold_config = self.config.target_config.get("gold", {})
        gold_config["table_name"] = "product_sales"

        writer = DeltaWriter(self.spark, gold_config)
        writer.write(df)
        writer.optimize(z_order_columns=["report_date", "product_id"])


class CustomerSalesAggregationJob(BaseSparkJob):
    """Job d'agrégation des ventes par client."""

    def __init__(self, environment: Environment | None = None):
        super().__init__(
            job_name="aggregate_sales_customer_gold",
            domain="sales",
            environment=environment,
        )

    def extract(self) -> DataFrame:
        """Lit les données de la couche Silver."""
        silver_config = self.config.target_config.get("silver", {})
        reader = DeltaReader(self.spark, silver_config)
        return reader.read()

    def transform(self, df: DataFrame) -> DataFrame:
        """Agrège les ventes par client."""
        return CustomerSalesAggregationTransformer().transform(df)

    def validate(self, df: DataFrame) -> DataFrame:
        """Valide les agrégations."""
        checker = DataQualityChecker(df)

        checker.check_not_null(
            columns=["report_date", "customer_id", "total_spend", "total_orders"],
            severity=CheckSeverity.CRITICAL,
        ).check_range(
            column="total_orders",
            min_value=0,
            severity=CheckSeverity.ERROR,
        )

        checker.run(fail_on_error=True)
        return df

    def load(self, df: DataFrame) -> None:
        """Écrit l'agrégation dans Gold."""
        gold_config = self.config.target_config.get("gold", {})
        gold_config["table_name"] = "customer_sales"

        writer = DeltaWriter(self.spark, gold_config)
        writer.write(df)
        writer.optimize(z_order_columns=["report_date", "customer_id"])


class MonthlyTrendsJob(BaseSparkJob):
    """Job de création des tendances mensuelles."""

    def __init__(self, environment: Environment | None = None):
        super().__init__(
            job_name="aggregate_monthly_trends_gold",
            domain="sales",
            environment=environment,
        )

    def extract(self) -> DataFrame:
        """Lit les données de la couche Silver."""
        silver_config = self.config.target_config.get("silver", {})
        reader = DeltaReader(self.spark, silver_config)
        return reader.read()

    def transform(self, df: DataFrame) -> DataFrame:
        """Agrège les tendances mensuelles."""
        return MonthlyTrendsTransformer().transform(df)

    def validate(self, df: DataFrame) -> DataFrame:
        """Valide les agrégations."""
        checker = DataQualityChecker(df)

        checker.check_not_null(
            columns=["year_month", "report_date", "total_revenue"],
            severity=CheckSeverity.CRITICAL,
        )

        checker.run(fail_on_error=True)
        return df

    def load(self, df: DataFrame) -> None:
        """Écrit l'agrégation dans Gold."""
        gold_config = self.config.target_config.get("gold", {})
        gold_config["table_name"] = "monthly_trends"

        writer = DeltaWriter(self.spark, gold_config)
        writer.write(df)
        writer.optimize(z_order_columns=["year_month"])


class CustomerSegmentAggregationJob(BaseSparkJob):
    """Job d'agrégation des ventes par segment client."""

    def __init__(self, environment: Environment | None = None):
        super().__init__(
            job_name="aggregate_customer_segment_gold",
            domain="sales",
            environment=environment,
        )

    def extract(self) -> DataFrame:
        """Lit les données de la couche Silver."""
        silver_config = self.config.target_config.get("silver", {})
        reader = DeltaReader(self.spark, silver_config)
        return reader.read()

    def transform(self, df: DataFrame) -> DataFrame:
        """Agrège par segment client."""
        return CustomerSegmentAggregationTransformer().transform(df)

    def validate(self, df: DataFrame) -> DataFrame:
        """Valide les agrégations."""
        checker = DataQualityChecker(df)

        checker.check_not_null(
            columns=["report_date", "segment", "total_customers", "total_revenue"],
            severity=CheckSeverity.CRITICAL,
        )

        checker.run(fail_on_error=True)
        return df

    def load(self, df: DataFrame) -> None:
        """Écrit l'agrégation dans Gold."""
        gold_config = self.config.target_config.get("gold", {})
        gold_config["table_name"] = "customer_segment_analysis"

        writer = DeltaWriter(self.spark, gold_config)
        writer.write(df)
        writer.optimize(z_order_columns=["report_date", "segment"])


class TopProductsJob(BaseSparkJob):
    """Job d'identification des meilleurs produits."""

    def __init__(self, environment: Environment | None = None, top_n: int = 20):
        super().__init__(
            job_name="aggregate_top_products_gold",
            domain="sales",
            environment=environment,
        )
        self.top_n = top_n

    def extract(self) -> DataFrame:
        """Lit les données de la couche Silver."""
        silver_config = self.config.target_config.get("silver", {})
        reader = DeltaReader(self.spark, silver_config)
        return reader.read()

    def transform(self, df: DataFrame) -> DataFrame:
        """Identifie les top produits."""
        return TopProductsTransformer(top_n=self.top_n, spark=self.spark).transform(df)

    def validate(self, df: DataFrame) -> DataFrame:
        """Valide les agrégations."""
        checker = DataQualityChecker(df)

        checker.check_not_null(
            columns=["report_date", "product_id", "rank"],
            severity=CheckSeverity.CRITICAL,
        ).check_range(
            column="rank",
            min_value=1,
            max_value=self.top_n,
            severity=CheckSeverity.ERROR,
        )

        checker.run(fail_on_error=True)
        return df

    def load(self, df: DataFrame) -> None:
        """Écrit l'agrégation dans Gold."""
        gold_config = self.config.target_config.get("gold", {})
        gold_config["table_name"] = "top_products"

        writer = DeltaWriter(self.spark, gold_config)
        writer.write(df)
        writer.optimize(z_order_columns=["report_date", "rank"])


class RFMAnalysisJob(BaseSparkJob):
    """Job d'analyse RFM des clients."""

    def __init__(self, environment: Environment | None = None):
        super().__init__(
            job_name="rfm_analysis_gold",
            domain="sales",
            environment=environment,
        )

    def extract(self) -> DataFrame:
        """Lit les données de la couche Silver."""
        silver_config = self.config.target_config.get("silver", {})
        reader = DeltaReader(self.spark, silver_config)
        return reader.read()

    def transform(self, df: DataFrame) -> DataFrame:
        """Crée l'analyse RFM."""
        return RFMAnalysisTransformer(spark=self.spark).transform(df)

    def validate(self, df: DataFrame) -> DataFrame:
        """Valide les agrégations."""
        checker = DataQualityChecker(df)

        checker.check_not_null(
            columns=["report_date", "customer_id", "rfm_score", "customer_value_segment"],
            severity=CheckSeverity.CRITICAL,
        )

        checker.run(fail_on_error=True)
        return df

    def load(self, df: DataFrame) -> None:
        """Écrit l'analyse dans Gold."""
        gold_config = self.config.target_config.get("gold", {})
        gold_config["table_name"] = "rfm_analysis"

        writer = DeltaWriter(self.spark, gold_config)
        writer.write(df)
        writer.optimize(z_order_columns=["report_date", "customer_id"])


def main_daily_sales() -> None:
    """Point d'entrée pour l'agrégation quotidienne."""
    import argparse

    parser = argparse.ArgumentParser(description="Daily Sales Aggregation Job")
    parser.add_argument(
        "--environment",
        type=str,
        default="dev",
        choices=["local", "dev", "staging", "prod"],
        help="Environnement d'exécution",
    )

    args = parser.parse_args()
    job = DailySalesAggregationJob(environment=Environment(args.environment))

    try:
        result = job.run()
        print(f"Job terminé: {result}")
    finally:
        job.cleanup()


if __name__ == "__main__":
    main_daily_sales()
