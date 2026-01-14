"""Job d'agrégation des ventes Silver -> Gold."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common.quality import DataQualityChecker, QualityCheck
from src.common.writers import DeltaWriter
from src.core.base_job import BaseSparkJob
from src.core.logger import get_logger

logger = get_logger(__name__)


class AggregateSalesJob(BaseSparkJob):
    """Agrège les ventes par différentes dimensions pour la couche Gold."""

    @property
    def job_name(self) -> str:
        return "aggregate_sales"

    @property
    def domain(self) -> str:
        return "sales"

    def extract(self) -> dict[str, DataFrame]:
        """
        Lit les données transformées depuis Silver.

        Returns:
            Dict avec les DataFrames sources
        """
        silver_path = self._get_config("paths.silver.orders")

        df_orders = self._spark.read.format("delta").load(silver_path)

        logger.info(
            "Extraction Silver terminée",
            orders_count=df_orders.count(),
        )

        return {"orders": df_orders}

    def transform(self, data: dict[str, DataFrame]) -> dict[str, DataFrame]:
        """
        Crée les agrégations pour Gold.

        Returns:
            Dict avec les différentes tables Gold
        """
        df_orders = data["orders"]

        # Agrégation journalière par produit
        df_daily_product = self._aggregate_daily_by_product(df_orders)

        # Agrégation mensuelle par client
        df_monthly_customer = self._aggregate_monthly_by_customer(df_orders)

        # Agrégation des KPIs globaux
        df_kpis = self._aggregate_global_kpis(df_orders)

        # Top produits avec classement
        df_top_products = self._calculate_top_products(df_orders)

        return {
            "daily_product_sales": df_daily_product,
            "monthly_customer_sales": df_monthly_customer,
            "sales_kpis": df_kpis,
            "top_products": df_top_products,
        }

    def load(self, data: dict[str, DataFrame]) -> None:
        """
        Écrit les tables Gold.

        Args:
            data: Dict des DataFrames à écrire
        """
        gold_base_path = self._get_config("paths.gold")

        for table_name, df in data.items():
            path = f"{gold_base_path}/{table_name}"

            writer = DeltaWriter(
                self._spark,
                {
                    "path": path,
                    "mode": "overwrite",
                    "partition_by": self._get_partition_columns(table_name),
                },
            )
            writer.write(df)

            logger.info(
                f"Table Gold écrite: {table_name}",
                path=path,
                rows=df.count(),
            )

    def validate(self, data: dict[str, DataFrame]) -> bool:
        """Valide les agrégations."""
        checker = DataQualityChecker(self._spark)

        for table_name in data:
            checker.add_check(
                QualityCheck(
                    name=f"{table_name}_not_empty",
                    check_type="row_count",
                    params={"min_count": 1},
                    description=f"Table {table_name} non vide",
                )
            )

        df_to_check = list(data.values())[0]  # Check sur le premier
        results = checker.run(df_to_check)

        return all(r.passed for r in results.checks)

    def _aggregate_daily_by_product(self, df: DataFrame) -> DataFrame:
        """Agrégation journalière par produit."""
        return (
            df.groupBy(
                F.col("order_date").alias("date"),
                "product_id",
            )
            .agg(
                F.count("order_id").alias("total_orders"),
                F.sum("quantity").alias("total_quantity"),
                F.sum("total_amount").alias("total_revenue"),
                F.avg("total_amount").alias("avg_order_value"),
                F.countDistinct("customer_id").alias("unique_customers"),
            )
            .withColumn("created_at", F.current_timestamp())
        )

    def _aggregate_monthly_by_customer(self, df: DataFrame) -> DataFrame:
        """Agrégation mensuelle par client."""
        return (
            df.withColumn("year_month", F.date_format("order_date", "yyyy-MM"))
            .groupBy("year_month", "customer_id")
            .agg(
                F.count("order_id").alias("total_orders"),
                F.sum("total_amount").alias("total_spent"),
                F.avg("total_amount").alias("avg_order_value"),
                F.min("order_date").alias("first_order_date"),
                F.max("order_date").alias("last_order_date"),
            )
            .withColumn("created_at", F.current_timestamp())
        )

    def _aggregate_global_kpis(self, df: DataFrame) -> DataFrame:
        """KPIs globaux."""
        return (
            df.groupBy(F.col("order_date").alias("date"))
            .agg(
                F.count("order_id").alias("total_orders"),
                F.sum("total_amount").alias("total_revenue"),
                F.avg("total_amount").alias("aov"),  # Average Order Value
                F.countDistinct("customer_id").alias("unique_customers"),
                F.countDistinct("product_id").alias("unique_products"),
                F.sum("quantity").alias("total_items_sold"),
            )
            .withColumn("created_at", F.current_timestamp())
        )

    def _calculate_top_products(self, df: DataFrame, top_n: int = 100) -> DataFrame:
        """Top N produits par revenu."""
        window = Window.orderBy(F.desc("total_revenue"))

        return (
            df.groupBy("product_id")
            .agg(
                F.sum("total_amount").alias("total_revenue"),
                F.sum("quantity").alias("total_quantity"),
                F.count("order_id").alias("total_orders"),
            )
            .withColumn("rank", F.row_number().over(window))
            .filter(F.col("rank") <= top_n)
            .withColumn("created_at", F.current_timestamp())
        )

    def _get_partition_columns(self, table_name: str) -> list[str]:
        """Retourne les colonnes de partition selon la table."""
        partitions_map = {
            "daily_product_sales": ["date"],
            "monthly_customer_sales": ["year_month"],
            "sales_kpis": ["date"],
            "top_products": [],
        }
        return partitions_map.get(table_name, [])


if __name__ == "__main__":
    job = AggregateSalesJob()
    job.run()
