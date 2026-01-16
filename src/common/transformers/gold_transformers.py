"""Transformers métier pour la couche Gold - Agrégations et KPIs."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class GoldTransformer(ABC):
    """Classe de base pour les transformers Gold."""

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Applique l'agrégation."""
        pass


class DailySalesAggregationTransformer(GoldTransformer):
    """Agrège les ventes quotidiennes."""

    def transform(self, df: DataFrame) -> DataFrame:
        """Crée une vue agrégée quotidienne des ventes."""
        return (
            df.withColumn("report_date", F.to_date(F.col("order_date")))
            .groupBy("report_date")
            .agg(
                F.count("order_id").alias("total_orders"),
                F.sum("total_amount").alias("total_revenue"),
                F.sum("discount_amount").alias("total_discount_given"),
                F.sum("tax_amount").alias("total_tax"),
                F.round(F.avg("total_amount"), 2).alias("average_order_value"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.sum("quantity").alias("total_items_sold"),
                F.sum(F.when(F.col("is_high_value_order") == "Y", 1).otherwise(0)).alias(
                    "high_value_orders_count"
                ),
                F.sum(F.when(F.col("order_status") == "cancelled", 1).otherwise(0)).alias(
                    "cancelled_orders_count"
                ),
                F.round(F.avg("margin_percent"), 2).alias("average_margin_percent"),
                F.sum(F.when(F.col("is_vip_customer") == "Y", 1).otherwise(0)).alias(
                    "vip_orders_count"
                ),
                F.current_timestamp().alias("gold_processed_at"),
            )
        )


class ProductSalesAggregationTransformer(GoldTransformer):
    """Agrège les ventes par produit."""

    def transform(self, df: DataFrame) -> DataFrame:
        """Crée une vue agrégée des ventes par produit."""
        daily_product = (
            df.withColumn("report_date", F.to_date(F.col("order_date")))
            .groupBy("report_date", "product_id", "product_name", "product_category")
            .agg(
                F.sum("quantity").alias("total_quantity_sold"),
                F.sum("total_amount").alias("total_revenue"),
                F.round(F.avg("unit_price"), 2).alias("average_unit_price"),
                F.sum("discount_amount").alias("total_discount"),
                F.sum(
                    F.col("total_amount") - F.col("total_amount") * (F.col("margin_percent") / 100)
                ).alias("total_margin"),
                F.count("order_id").alias("number_of_transactions"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.round(F.avg("quantity"), 2).alias("average_quantity_per_order"),
                F.current_timestamp().alias("gold_processed_at"),
            )
        )

        # Ajouter le ranking par revenu
        window_rank = Window.partitionBy("report_date").orderBy(F.desc("total_revenue"))
        return daily_product.withColumn("rank_by_revenue", F.row_number().over(window_rank))


class CustomerSalesAggregationTransformer(GoldTransformer):
    """Agrège les ventes par client."""

    def transform(self, df: DataFrame) -> DataFrame:
        """Crée une vue agrégée des ventes par client."""
        daily_customer = (
            df.withColumn("report_date", F.to_date(F.col("order_date")))
            .groupBy("report_date", "customer_id", "customer_name", "customer_segment")
            .agg(
                F.sum("total_amount").alias("total_spend"),
                F.count("order_id").alias("total_orders"),
                F.round(F.avg("total_amount"), 2).alias("average_order_value"),
                F.sum("quantity").alias("total_items_purchased"),
                F.countDistinct("product_id").alias("unique_products"),
                F.sum("discount_amount").alias("total_discount_received"),
                F.round(F.avg(F.when(F.col("is_repeat_order") == "Y", 1).otherwise(0)), 2).alias(
                    "repeat_purchase_rate"
                ),
                F.datediff(
                    F.col("report_date"),
                    F.max("order_date"),
                ).alias("days_since_last_purchase"),
                F.current_timestamp().alias("gold_processed_at"),
            )
        )

        # Ajouter flag high value customer
        return daily_customer.withColumn(
            "is_high_value_customer",
            F.when(
                F.col("total_spend")
                > F.percentile_approx("total_spend", 0.75).over(Window.partitionBy()),
                "Y",
            ).otherwise("N"),
        )


class MonthlyTrendsTransformer(GoldTransformer):
    """Crée des tendances mensuelles."""

    def transform(self, df: DataFrame) -> DataFrame:
        """Crée une vue des tendances mensuelles."""
        # Calcul des statistiques mensuelles
        monthly = (
            df.withColumn("year_month", F.date_format(F.col("order_date"), "yyyy-MM"))
            .withColumn("report_date", F.to_date(F.col("order_date")))
            .groupBy("year_month", "report_date")
            .agg(
                F.sum("total_amount").alias("total_revenue"),
                F.count("order_id").alias("total_orders"),
                F.countDistinct("customer_id").alias("total_customers"),
                F.round(F.avg("total_amount"), 2).alias("average_order_value"),
                F.sum("discount_amount").alias("total_discount"),
                F.round(
                    F.sum(F.when(F.col("order_status") == "cancelled", 1).otherwise(0))
                    / F.count("order_id")
                    * 100,
                    2,
                ).alias("return_rate"),
                F.current_timestamp().alias("gold_processed_at"),
            )
        )

        # Ajouter le growth rate mois sur mois
        window_month = Window.orderBy("year_month")
        monthly = (
            monthly.withColumn(
                "prev_month_revenue",
                F.lag("total_revenue").over(window_month),
            )
            .withColumn(
                "month_over_month_growth",
                F.when(
                    F.col("prev_month_revenue").isNotNull() & (F.col("prev_month_revenue") > 0),
                    F.round(
                        (
                            (F.col("total_revenue") - F.col("prev_month_revenue"))
                            / F.col("prev_month_revenue")
                        )
                        * 100,
                        2,
                    ),
                ).otherwise(F.lit(None)),
            )
            .drop("prev_month_revenue")
        )

        # Ajouter le top produit du mois
        product_window = Window.partitionBy("year_month").orderBy(F.desc("total_revenue"))
        # Note: On va créer une agrégation par produit et mois d'abord
        monthly_product = (
            df.withColumn("year_month", F.date_format(F.col("order_date"), "yyyy-MM"))
            .groupBy("year_month", "product_id")
            .agg(
                F.sum("total_amount").alias("product_revenue"),
            )
            .withColumn(
                "rank",
                F.row_number().over(product_window),
            )
            .filter(F.col("rank") == 1)
            .select(
                "year_month",
                F.col("product_id").alias("top_product_id"),
                F.col("product_revenue").alias("top_product_revenue"),
            )
        )

        # Joindre les top produits
        return monthly.join(monthly_product, on="year_month", how="left")


class CustomerSegmentAggregationTransformer(GoldTransformer):
    """Agrège les ventes par segment client."""

    def transform(self, df: DataFrame) -> DataFrame:
        """Crée une vue agrégée par segment client."""
        daily_segment = (
            df.withColumn("report_date", F.to_date(F.col("order_date")))
            .groupBy("report_date", "customer_segment")
            .agg(
                F.countDistinct("customer_id").alias("total_customers"),
                F.sum("total_amount").alias("total_revenue"),
                F.round(F.avg("lifetime_value"), 2).alias("average_lifetime_value"),
                F.round(
                    F.count("order_id") / F.countDistinct("customer_id"),
                    2,
                ).alias("average_order_frequency"),
                F.count("order_id").alias("total_orders"),
                F.sum(F.when(F.col("is_repeat_order") == "Y", 1).otherwise(0)).alias(
                    "repeat_customer_count"
                ),
                F.current_timestamp().alias("gold_processed_at"),
            )
        )

        # Ajouter le taux de client répétés et la part de revenu
        window_all = Window.partitionBy("report_date")
        return daily_segment.withColumn(
            "repeat_customer_rate",
            F.round(
                F.col("repeat_customer_count") / F.col("total_customers") * 100,
                2,
            ),
        ).withColumn(
            "segment_revenue_share",
            F.round(
                F.col("total_revenue") / F.sum("total_revenue").over(window_all) * 100,
                2,
            ),
        )


class TopProductsTransformer(GoldTransformer):
    """Identifie les meilleurs produits."""

    def __init__(self, top_n: int = 20, spark: SparkSession | None = None):
        self.top_n = top_n
        self.spark = spark

    def transform(self, df: DataFrame) -> DataFrame:
        """Crée un classement des top produits."""
        # Aggréger par produit et date
        daily_product = (
            df.withColumn("report_date", F.to_date(F.col("order_date")))
            .groupBy("report_date", "product_id", "product_name", "product_category")
            .agg(
                F.sum("quantity").alias("total_quantity_sold"),
                F.sum("total_amount").alias("total_revenue"),
                F.countDistinct("customer_id").alias("unique_customers"),
                F.current_timestamp().alias("gold_processed_at"),
            )
        )

        # Ajouter le ranking et contribution
        window_rank = Window.partitionBy("report_date").orderBy(F.desc("total_revenue"))
        window_all = Window.partitionBy("report_date")

        top_products = (
            daily_product.withColumn("rank", F.row_number().over(window_rank))
            .filter(F.col("rank") <= self.top_n)
            .withColumn(
                "contribution_to_total",
                F.round(
                    F.col("total_revenue") / F.sum("total_revenue").over(window_all) * 100,
                    2,
                ),
            )
        )

        # Calculer growth rate vs période précédente
        window_prev = Window.partitionBy("product_id").orderBy("report_date")
        top_products = (
            top_products.withColumn(
                "prev_revenue",
                F.lag("total_revenue").over(window_prev),
            )
            .withColumn(
                "growth_rate",
                F.when(
                    F.col("prev_revenue").isNotNull() & (F.col("prev_revenue") > 0),
                    F.round(
                        ((F.col("total_revenue") - F.col("prev_revenue")) / F.col("prev_revenue"))
                        * 100,
                        2,
                    ),
                ).otherwise(F.lit(None)),
            )
            .drop("prev_revenue")
        )

        return top_products


class RFMAnalysisTransformer(GoldTransformer):
    """Crée une analyse RFM (Recency, Frequency, Monetary)."""

    def __init__(self, spark: SparkSession | None = None):
        self.spark = spark

    def transform(self, df: DataFrame) -> DataFrame:
        """Crée une analyse RFM des clients."""
        rfm = (
            df.withColumn("report_date", F.to_date(F.col("order_date")))
            .groupBy("report_date", "customer_id", "customer_name")
            .agg(
                F.datediff(
                    F.max("report_date"),
                    F.max("order_date"),
                ).alias("days_since_last_purchase"),
                F.count("order_id").alias("purchase_frequency"),
                F.sum("total_amount").alias("monetary_value"),
                F.current_timestamp().alias("gold_processed_at"),
            )
        )

        # Créer les quintiles pour RFM
        recency_window = Window.partitionBy("report_date").orderBy("days_since_last_purchase")
        frequency_window = Window.partitionBy("report_date").orderBy("purchase_frequency")
        monetary_window = Window.partitionBy("report_date").orderBy("monetary_value")

        rfm = (
            rfm.withColumn(
                "r_score",
                F.ntile(5).over(recency_window),
            )
            .withColumn(
                "f_score",
                F.ntile(5).over(frequency_window),
            )
            .withColumn(
                "m_score",
                F.ntile(5).over(monetary_window),
            )
        )

        # Combiner les scores
        rfm = rfm.withColumn(
            "rfm_score",
            F.concat_ws(
                "",
                F.col("r_score"),
                F.col("f_score"),
                F.col("m_score"),
            ),
        )

        # Segmenter basé sur RFM
        rfm = rfm.withColumn(
            "customer_value_segment",
            F.when(
                (F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4),
                F.lit("Champions"),
            )
            .when(
                (F.col("r_score") >= 3) & (F.col("f_score") >= 3) & (F.col("m_score") >= 3),
                F.lit("Loyal Customers"),
            )
            .when(
                (F.col("r_score") >= 4) & (F.col("f_score") <= 2),
                F.lit("At Risk"),
            )
            .when(
                (F.col("r_score") <= 2) & (F.col("f_score") >= 4),
                F.lit("Cannot Lose Them"),
            )
            .when(
                (F.col("r_score") >= 4) & (F.col("m_score") >= 3),
                F.lit("Promising"),
            )
            .otherwise(F.lit("Needs Attention")),
        )

        return rfm.drop("r_score", "f_score", "m_score")
