"""
Exemples pratiques d'utilisation des transformers Silver et Gold.

Ce module montre comment utiliser les transformers dans différents scénarios.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.transformers.gold_transformers import (
    DailySalesAggregationTransformer,
    ProductSalesAggregationTransformer,
    RFMAnalysisTransformer,
)
from src.common.transformers.silver_transformers import (
    MarginCalculationTransformer,
    OrderValueSegmentationTransformer,
    RepeatOrderDetectionTransformer,
    TaxCalculationTransformer,
)


def example_1_basic_silver_pipeline():
    """Exemple 1: Pipeline Silver basique avec enrichissements."""

    # Créer une session Spark
    spark = SparkSession.builder.appName("example_silver").master("local[4]").getOrCreate()

    # Charger données Bronze (exemple fictif)
    orders_df = spark.createDataFrame(
        [
            ("ORD001", "CUST001", "2024-01-15", "PROD001", 2, 100.00, 10.0, "delivered"),
            ("ORD002", "CUST002", "2024-01-16", "PROD002", 1, 500.00, 5.0, "shipped"),
        ],
        [
            "order_id",
            "customer_id",
            "order_date",
            "product_id",
            "quantity",
            "unit_price",
            "discount_percent",
            "status",
        ],
    )

    # Transformer 1: Ajouter sous-total
    orders_df = orders_df.withColumn(
        "subtotal", F.round(F.col("quantity") * F.col("unit_price"), 2)
    )

    # Transformer 2: Appliquer taxes
    tax_transformer = TaxCalculationTransformer(tax_rates={"FR": 0.20})
    orders_df = tax_transformer.transform(orders_df)

    # Transformer 3: Segmenter par valeur
    value_transformer = OrderValueSegmentationTransformer(high_value_threshold=400.0)
    orders_df = value_transformer.transform(orders_df)

    # Afficher résultat
    orders_df.select("order_id", "total_amount", "is_high_value_order").show()

    return orders_df


def example_2_margin_analysis():
    """Exemple 2: Analyser les marges par produit."""

    spark = SparkSession.builder.appName("example_margins").master("local[4]").getOrCreate()

    # Données avec coûts
    orders_df = spark.createDataFrame(
        [
            ("PROD001", 100.00, 60.00, 5),  # prix, coût, marge attendue = 40%
            ("PROD002", 500.00, 300.00, 1),  # marge = 40%
            ("PROD003", 50.00, 40.00, 10),  # marge = 20%
        ],
        ["product_id", "unit_price", "cost_price", "quantity"],
    )

    # Ajouter le subtotal
    orders_df = orders_df.withColumn("subtotal", F.col("quantity") * F.col("unit_price"))

    # Calculer marges
    margin_transformer = MarginCalculationTransformer()
    orders_df = margin_transformer.transform(orders_df)

    # Analyser
    margin_stats = orders_df.groupBy().agg(
        F.round(F.avg("margin_percent"), 2).alias("avg_margin"),
        F.min("margin_percent").alias("min_margin"),
        F.max("margin_percent").alias("max_margin"),
    )

    margin_stats.show()

    return orders_df


def example_3_repeat_customer_analysis():
    """Exemple 3: Identifier les clients fidèles."""

    spark = SparkSession.builder.appName("example_repeat").master("local[4]").getOrCreate()

    # Historique de commandes
    orders_df = spark.createDataFrame(
        [
            ("ORD001", "CUST001", "2024-01-10"),
            ("ORD002", "CUST001", "2024-01-20"),  # 2e commande
            ("ORD003", "CUST001", "2024-02-05"),  # 3e commande
            ("ORD004", "CUST002", "2024-01-15"),
            ("ORD005", "CUST002", "2024-02-10"),  # 2e commande
            ("ORD006", "CUST003", "2024-01-25"),  # Seule commande
        ],
        ["order_id", "customer_id", "order_date"],
    )

    # Convertir en timestamp
    orders_df = orders_df.withColumn("order_date", F.to_timestamp("order_date"))

    # Détecter commandes répétées
    repeat_transformer = RepeatOrderDetectionTransformer()
    orders_df = repeat_transformer.transform(orders_df)

    # Compter clients répétés
    repeat_stats = orders_df.groupBy("customer_id").agg(
        F.count("order_id").alias("order_count"),
        F.sum(F.when(F.col("is_repeat_order") == "Y", 1).otherwise(0)).alias("repeat_order_count"),
    )

    print("Statistiques de fidélité client:")
    repeat_stats.show()

    return orders_df


def example_4_daily_sales_gold():
    """Exemple 4: Créer l'agrégation quotidienne Gold."""

    spark = SparkSession.builder.appName("example_gold_daily").master("local[4]").getOrCreate()

    # Données Silver préparées
    orders_df = spark.createDataFrame(
        [
            ("2024-01-15", "ORD001", "CUST001", 180.00, 30, "Y", "Y", "N"),
            ("2024-01-15", "ORD002", "CUST002", 475.00, 35, "N", "Y", "Y"),
            ("2024-01-15", "ORD003", "CUST003", 500.00, 30, "Y", "Y", "N"),
            ("2024-01-15", "ORD004", "CUST004", 77.00, 40, "N", "N", "N"),
            ("2024-01-16", "ORD005", "CUST005", 250.00, 25, "N", "N", "Y"),
        ],
        [
            "order_date",
            "order_id",
            "customer_id",
            "total_amount",
            "margin_percent",
            "is_high_value_order",
            "is_vip_customer",
            "order_status",
        ],
    )

    # Convertir colonne en timestamp
    orders_df = orders_df.withColumn("order_date", F.to_timestamp("order_date"))

    # Ajouter colonnes manquantes (simplification)
    orders_df = (
        orders_df.withColumn("quantity", F.lit(1).cast("integer"))
        .withColumn("discount_amount", F.lit(0).cast("decimal(10,2)"))
        .withColumn("tax_amount", F.lit(0).cast("decimal(10,2)"))
    )

    # Appliquer transformer Gold
    daily_transformer = DailySalesAggregationTransformer()
    daily_sales = daily_transformer.transform(orders_df)

    print("Agrégation quotidienne:")
    daily_sales.select(
        "report_date", "total_orders", "total_revenue", "average_order_value", "unique_customers"
    ).show()

    return daily_sales


def example_5_product_ranking():
    """Exemple 5: Classer les produits par performance."""

    spark = SparkSession.builder.appName("example_products").master("local[4]").getOrCreate()

    # Données ventes par produit
    orders_df = spark.createDataFrame(
        [
            ("2024-01-15", "PROD001", "Product A", 5, 100.00),
            ("2024-01-15", "PROD002", "Product B", 3, 500.00),
            ("2024-01-15", "PROD001", "Product A", 2, 100.00),
            ("2024-01-15", "PROD003", "Product C", 10, 50.00),
        ],
        ["order_date", "product_id", "product_name", "quantity", "unit_price"],
    )

    # Convertir
    orders_df = orders_df.withColumn("order_date", F.to_timestamp("order_date")).withColumn(
        "total_amount", F.col("quantity") * F.col("unit_price")
    )

    # Ajouter colonnes requis
    orders_df = (
        orders_df.withColumn("customer_id", F.lit("CUST001"))
        .withColumn("discount_amount", F.lit(0).cast("decimal(10,2)"))
        .withColumn("margin_percent", F.lit(30))
    )

    # Transformer
    product_transformer = ProductSalesAggregationTransformer()
    product_sales = product_transformer.transform(orders_df)

    print("Performance par produit:")
    product_sales.select(
        "product_id", "product_name", "total_quantity_sold", "total_revenue", "rank_by_revenue"
    ).show()

    return product_sales


def example_6_rfm_segmentation():
    """Exemple 6: Segmenter clients avec RFM."""

    spark = SparkSession.builder.appName("example_rfm").master("local[4]").getOrCreate()

    from datetime import datetime

    base_date = datetime(2024, 1, 15)

    # Historique d'achats
    orders_df = spark.createDataFrame(
        [
            # Client 1: Champion (récent, fréquent, valeur haute)
            ("2024-01-10", "CUST001", 500.00),
            ("2024-01-12", "CUST001", 600.00),
            ("2024-01-14", "CUST001", 550.00),
            # Client 2: At Risk (ancien, faible fréquence)
            ("2023-11-20", "CUST002", 200.00),
            ("2023-10-15", "CUST002", 150.00),
            # Client 3: Loyal (récent, fréquent)
            ("2024-01-08", "CUST003", 300.00),
            ("2024-01-11", "CUST003", 350.00),
            ("2024-01-15", "CUST003", 320.00),
            # Client 4: Prometteur (très récent)
            ("2024-01-14", "CUST004", 800.00),
        ],
        ["order_date", "customer_id", "total_amount"],
    )

    # Convertir
    orders_df = (
        orders_df.withColumn("order_date", F.to_timestamp("order_date"))
        .withColumn("customer_name", F.lit("Customer"))
        .withColumn("quantity", F.lit(1).cast("integer"))
        .withColumn("discount_amount", F.lit(0).cast("decimal(10,2)"))
        .withColumn("margin_percent", F.lit(30))
        .withColumn("is_repeat_order", F.lit("Y"))
    )

    # Transformer RFM
    rfm_transformer = RFMAnalysisTransformer(spark=spark)
    rfm_analysis = rfm_transformer.transform(orders_df)

    print("Segmentation RFM:")
    rfm_analysis.select(
        "customer_id", "rfm_score", "customer_value_segment", "purchase_frequency", "monetary_value"
    ).show()

    return rfm_analysis


def example_7_advanced_silver_pipeline():
    """Exemple 7: Pipeline Silver avancé avec tous les enrichissements."""

    spark = SparkSession.builder.appName("example_full_silver").master("local[4]").getOrCreate()

    # Données Bronze de base
    orders_df = spark.createDataFrame(
        [
            ("ORD001", "CUST001", "2024-01-15", "PROD001", 2, 100.00, 10.0, 60.00, "delivered"),
            ("ORD002", "CUST001", "2024-01-20", "PROD002", 1, 500.00, 5.0, 300.00, "shipped"),
            ("ORD003", "CUST002", "2024-01-15", "PROD001", 5, 100.00, 0.0, 60.00, "pending"),
        ],
        [
            "order_id",
            "customer_id",
            "order_date",
            "product_id",
            "quantity",
            "unit_price",
            "discount_percent",
            "cost_price",
            "status",
        ],
    )

    # Convertir
    orders_df = orders_df.withColumn("order_date", F.to_timestamp("order_date"))

    # 1. Sous-total
    orders_df = orders_df.withColumn(
        "subtotal", F.round(F.col("quantity") * F.col("unit_price"), 2)
    )

    # 2. Remises
    from src.common.transformers.silver_transformers import DiscountAnalysisTransformer

    orders_df = DiscountAnalysisTransformer().transform(orders_df)

    # 3. Taxes
    orders_df = TaxCalculationTransformer({"FR": 0.20}).transform(orders_df)

    # 4. Marges
    orders_df = MarginCalculationTransformer().transform(orders_df)

    # 5. Segmentation valeur
    orders_df = OrderValueSegmentationTransformer(400.0).transform(orders_df)

    # 6. Commandes répétées
    orders_df = RepeatOrderDetectionTransformer().transform(orders_df)

    print("Pipeline Silver complet:")
    orders_df.select(
        "order_id", "total_amount", "margin_percent", "is_high_value_order", "is_repeat_order"
    ).show()

    return orders_df


if __name__ == "__main__":
    print("=" * 80)
    print("EXEMPLE 1: Pipeline Silver Basique")
    print("=" * 80)
    example_1_basic_silver_pipeline()

    print("\n" + "=" * 80)
    print("EXEMPLE 2: Analyse des Marges")
    print("=" * 80)
    example_2_margin_analysis()

    print("\n" + "=" * 80)
    print("EXEMPLE 3: Analyse Clients Fidèles")
    print("=" * 80)
    example_3_repeat_customer_analysis()

    print("\n" + "=" * 80)
    print("EXEMPLE 4: Agrégation Quotidienne Gold")
    print("=" * 80)
    example_4_daily_sales_gold()

    print("\n" + "=" * 80)
    print("EXEMPLE 5: Classement des Produits")
    print("=" * 80)
    example_5_product_ranking()

    print("\n" + "=" * 80)
    print("EXEMPLE 6: Segmentation RFM")
    print("=" * 80)
    example_6_rfm_segmentation()

    print("\n" + "=" * 80)
    print("EXEMPLE 7: Pipeline Silver Avancé")
    print("=" * 80)
    example_7_advanced_silver_pipeline()

    print("\n✅ Tous les exemples ont été exécutés avec succès!")
