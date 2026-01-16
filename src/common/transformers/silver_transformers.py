"""Transformers métier pour la couche Silver."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class SilverTransformer(ABC):
    """Classe de base pour les transformers Silver."""

    def __init__(self, spark: SparkSession | None = None):
        self.spark = spark

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """Applique la transformation."""
        pass


class OrderEnrichmentTransformer(SilverTransformer):
    """Enrichit les commandes avec données client et produit."""

    def transform(
        self,
        orders_df: DataFrame,
        customers_df: DataFrame | None = None,
        products_df: DataFrame | None = None,
    ) -> DataFrame:
        """
        Enrichit les commandes.

        Args:
            orders_df: DataFrame des commandes
            customers_df: DataFrame des clients (optionnel)
            products_df: DataFrame des produits (optionnel)

        Returns:
            DataFrame enrichie
        """
        result = orders_df

        # Enrichissement avec données client
        if customers_df is not None:
            result = result.join(
                customers_df.select(
                    "customer_id",
                    F.col("customer_name").alias("customer_name"),
                    F.col("customer_segment").alias("customer_segment"),
                    F.col("country").alias("customer_country"),
                    F.col("is_vip").alias("is_vip_customer"),
                ),
                on="customer_id",
                how="left",
            )
        else:
            # Valeurs par défaut
            result = result.withColumn("customer_name", F.lit(None))
            result = result.withColumn("customer_segment", F.lit(None))
            result = result.withColumn("customer_country", F.lit(None))
            result = result.withColumn("is_vip_customer", F.lit("N"))

        # Enrichissement avec données produit
        if products_df is not None:
            result = result.join(
                products_df.select(
                    "product_id",
                    F.col("product_name").alias("product_name"),
                    F.col("category").alias("product_category"),
                    F.col("brand").alias("product_brand"),
                    F.col("unit_price").alias("product_price"),
                ),
                on="product_id",
                how="left",
            )
        else:
            # Valeurs par défaut
            result = result.withColumn("product_name", F.lit(None))
            result = result.withColumn("product_category", F.lit(None))
            result = result.withColumn("product_brand", F.lit(None))
            result = result.withColumn("product_price", F.col("unit_price"))

        return result


class MarginCalculationTransformer(SilverTransformer):
    """Calcule les marges et indicateurs de profitabilité."""

    def __init__(self, cost_price_column: str = "cost_price", spark: SparkSession | None = None):
        super().__init__(spark)
        self.cost_price_column = cost_price_column

    def transform(self, df: DataFrame) -> DataFrame:
        """Calcule les marges."""
        # Calcul de la marge brute (si cost_price disponible)
        if self.cost_price_column in df.columns:
            df = df.withColumn(
                "margin_percent",
                F.when(
                    F.col("product_price").isNotNull() & (F.col("product_price") > 0),
                    F.round(
                        (
                            (F.col("product_price") - F.col(self.cost_price_column))
                            / F.col("product_price")
                        )
                        * 100,
                        2,
                    ),
                ).otherwise(0),
            )
        else:
            # Si cost_price non disponible, on estime une marge par défaut
            df = df.withColumn("margin_percent", F.lit(30.0))

        return df


class OrderValueSegmentationTransformer(SilverTransformer):
    """Segmente les commandes par valeur."""

    def __init__(self, high_value_threshold: float = 500.0, spark: SparkSession | None = None):
        super().__init__(spark)
        self.high_value_threshold = high_value_threshold

    def transform(self, df: DataFrame) -> DataFrame:
        """Segmente les commandes par valeur."""
        return df.withColumn(
            "is_high_value_order",
            F.when(F.col("total_amount") >= self.high_value_threshold, "Y").otherwise("N"),
        )


class RepeatOrderDetectionTransformer(SilverTransformer):
    """Détecte les commandes répétées du même client."""

    def transform(self, df: DataFrame) -> DataFrame:
        """Détecte les commandes répétées."""
        # Fenêtre pour numéroter les commandes par client
        window_spec = Window.partitionBy("customer_id").orderBy("order_date")

        # Ajouter un numéro de commande par client
        df = df.withColumn("customer_order_number", F.row_number().over(window_spec))

        # Déterminer si c'est une commande répétée
        return df.withColumn(
            "is_repeat_order",
            F.when(F.col("customer_order_number") > 1, "Y").otherwise("N"),
        )


class DeliveryTimeCalculationTransformer(SilverTransformer):
    """Calcule le temps de livraison."""

    def transform(self, df: DataFrame) -> DataFrame:
        """Calcule les jours jusqu'à la livraison."""
        return df.withColumn(
            "days_to_delivery",
            F.when(
                F.col("delivery_date").isNotNull(),
                F.datediff(F.col("delivery_date"), F.col("order_date")),
            ).otherwise(F.lit(None)),
        )


class TaxCalculationTransformer(SilverTransformer):
    """Calcule les taxes basées sur le pays/région."""

    def __init__(
        self, tax_rates: dict[str, float] | None = None, spark: SparkSession | None = None
    ):
        super().__init__(spark)
        # Taux de TVA par pays (exemple)
        self.tax_rates = tax_rates or {
            "FR": 0.20,
            "US": 0.10,
            "DE": 0.19,
            "IT": 0.22,
            "ES": 0.21,
            "GB": 0.20,
        }

    def transform(self, df: DataFrame) -> DataFrame:
        """Ajoute le calcul des taxes."""
        # Ajouter une colonne de taux de taxe basée sur le pays
        tax_rate_col = None
        for country, rate in self.tax_rates.items():
            condition = F.col("customer_country") == country
            tax_rate = F.lit(rate)

            if tax_rate_col is None:
                tax_rate_col = F.when(condition, tax_rate)
            else:
                tax_rate_col = tax_rate_col.when(condition, tax_rate)

        if tax_rate_col is not None:
            tax_rate_col = tax_rate_col.otherwise(F.lit(0.20))  # Taux par défaut
        else:
            tax_rate_col = F.lit(0.20)

        df = df.withColumn("tax_rate", tax_rate_col)

        # Calcul du montant de taxe
        df = df.withColumn(
            "tax_amount",
            F.when(
                F.col("subtotal").isNotNull(),
                F.round(F.col("subtotal") * F.col("tax_rate"), 2),
            ).otherwise(0),
        )

        # Recalculer le total avec taxes
        df = df.withColumn(
            "total_amount",
            F.when(
                F.col("subtotal").isNotNull(),
                F.round(F.col("subtotal") + F.col("tax_amount"), 2),
            ).otherwise(F.col("subtotal")),
        )

        return df.drop("tax_rate")


class DiscountAnalysisTransformer(SilverTransformer):
    """Analyse les remises appliquées."""

    def transform(self, df: DataFrame) -> DataFrame:
        """Détaille les remises."""
        df = df.withColumn(
            "discount_amount",
            F.when(
                (F.col("subtotal").isNotNull()) & (F.col("discount_percent").isNotNull()),
                F.round(F.col("subtotal") * (F.col("discount_percent") / 100), 2),
            ).otherwise(0),
        )

        return df


class PaymentStatusTransformer(SilverTransformer):
    """Standardise le statut de paiement."""

    def __init__(self, spark: SparkSession | None = None):
        super().__init__(spark)

    def transform(self, df: DataFrame) -> DataFrame:
        """Standardise les statuts de paiement."""
        return df.withColumn(
            "payment_status",
            F.when(F.col("order_status") == "cancelled", F.lit("cancelled"))
            .when(F.col("order_status").isin("pending", "processing"), F.lit("pending"))
            .when(F.col("order_status") == "delivered", F.lit("completed"))
            .otherwise(F.lit("unknown")),
        )


class FulfillmentStatusTransformer(SilverTransformer):
    """Standardise le statut de fulfillment."""

    def transform(self, df: DataFrame) -> DataFrame:
        """Standardise les statuts de fulfillment."""
        return df.withColumn(
            "fulfillment_status",
            F.when(F.col("order_status") == "pending", F.lit("pending"))
            .when(F.col("order_status") == "confirmed", F.lit("processing"))
            .when(F.col("order_status") == "shipped", F.lit("in_transit"))
            .when(F.col("order_status") == "delivered", F.lit("delivered"))
            .when(F.col("order_status") == "cancelled", F.lit("cancelled"))
            .otherwise(F.lit("unknown")),
        )
