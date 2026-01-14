"""Tests d'intégration pour le pipeline Sales."""
from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def integration_paths(tmp_path_factory):
    """Chemins pour les tests d'intégration."""
    base = tmp_path_factory.mktemp("integration")
    paths = {
        "bronze": str(base / "bronze" / "orders"),
        "silver": str(base / "silver" / "orders"),
        "gold": str(base / "gold"),
    }
    return paths


@pytest.fixture
def sample_source_data(spark: SparkSession):
    """Données source pour les tests."""
    data = [
        ("ORD001", "CUST001", "2024-01-15", "PROD001", 2, 29.99, 0.0, "confirmed"),
        ("ORD002", "CUST002", "2024-01-15", "PROD002", 1, 49.99, 10.0, "confirmed"),
        ("ORD003", "CUST001", "2024-01-16", "PROD001", 3, 29.99, 5.0, "shipped"),
        ("ORD004", "CUST003", "2024-01-16", "PROD003", 1, 99.99, 0.0, "delivered"),
        ("ORD005", "CUST002", "2024-01-17", "PROD002", 2, 49.99, 5.0, "confirmed"),
    ]

    columns = [
        "order_id",
        "customer_id",
        "order_date",
        "product_id",
        "quantity",
        "unit_price",
        "discount",
        "status",
    ]

    return spark.createDataFrame(data, columns)


@pytest.mark.integration
class TestSalesPipelineIntegration:
    """Tests d'intégration du pipeline Sales complet."""

    def test_bronze_to_silver_pipeline(
        self,
        spark: SparkSession,
        sample_source_data,
        integration_paths,
    ):
        """Test du flux Bronze -> Silver."""
        # Écrire les données Bronze
        sample_source_data.write.format("delta").mode("overwrite").save(integration_paths["bronze"])

        # Lire depuis Bronze
        bronze_df = spark.read.format("delta").load(integration_paths["bronze"])
        assert bronze_df.count() == 5

        # Simuler la transformation Silver
        silver_df = (
            bronze_df.withColumn("order_date", F.to_date("order_date"))
            .withColumn(
                "total_amount",
                F.col("quantity") * F.col("unit_price") - F.col("discount"),
            )
            .withColumn("_processed_at", F.current_timestamp())
            .dropDuplicates(["order_id"])
        )

        # Écrire Silver
        silver_df.write.format("delta").mode("overwrite").save(integration_paths["silver"])

        # Vérifier Silver
        result = spark.read.format("delta").load(integration_paths["silver"])
        assert result.count() == 5
        assert "total_amount" in result.columns

    def test_silver_to_gold_aggregations(
        self,
        spark: SparkSession,
        integration_paths,
    ):
        """Test des agrégations Silver -> Gold."""
        # Préparer les données Silver
        silver_data = [
            ("ORD001", "CUST001", "2024-01-15", "PROD001", 2, 59.98, "confirmed"),
            ("ORD002", "CUST002", "2024-01-15", "PROD002", 1, 39.99, "confirmed"),
            ("ORD003", "CUST001", "2024-01-15", "PROD001", 3, 84.97, "shipped"),
        ]

        silver_df = spark.createDataFrame(
            silver_data,
            [
                "order_id",
                "customer_id",
                "order_date",
                "product_id",
                "quantity",
                "total_amount",
                "status",
            ],
        ).withColumn("order_date", F.to_date("order_date"))

        silver_df.write.format("delta").mode("overwrite").save(integration_paths["silver"])

        # Agrégation daily par produit
        daily_product = silver_df.groupBy("order_date", "product_id").agg(
            F.count("order_id").alias("total_orders"),
            F.sum("total_amount").alias("total_revenue"),
        )

        gold_daily_path = f"{integration_paths['gold']}/daily_product_sales"
        daily_product.write.format("delta").mode("overwrite").save(gold_daily_path)

        # Vérifier
        result = spark.read.format("delta").load(gold_daily_path)
        assert result.count() == 2  # 2 produits distincts

        # PROD001 devrait avoir 2 commandes
        prod001 = result.filter(F.col("product_id") == "PROD001").first()
        assert prod001["total_orders"] == 2

    def test_data_lineage_through_layers(
        self,
        spark: SparkSession,
        sample_source_data,
        integration_paths,
    ):
        """Vérifie la traçabilité des données à travers les couches."""
        # Bronze
        bronze_df = sample_source_data.withColumn("_ingested_at", F.current_timestamp()).withColumn(
            "_source", F.lit("test_source")
        )

        bronze_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(
            integration_paths["bronze"]
        )

        # Silver
        silver_df = (
            spark.read.format("delta")
            .load(integration_paths["bronze"])
            .withColumn("_processed_at", F.current_timestamp())
            .withColumn(
                "total_amount",
                F.col("quantity") * F.col("unit_price") - F.col("discount"),
            )
        )

        silver_df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(
            integration_paths["silver"]
        )

        # Vérifier les métadonnées
        result = spark.read.format("delta").load(integration_paths["silver"])

        assert "_ingested_at" in result.columns
        assert "_source" in result.columns
        assert "_processed_at" in result.columns

    def test_incremental_processing(
        self,
        spark: SparkSession,
        integration_paths,
    ):
        """Test du traitement incrémental."""
        # Batch 1
        batch1 = spark.createDataFrame(
            [
                ("ORD001", "CUST001", "2024-01-15", "PROD001", 1, 29.99),
            ],
            ["order_id", "customer_id", "order_date", "product_id", "quantity", "price"],
        )

        batch1.write.format("delta").mode("overwrite").save(integration_paths["bronze"])

        # Batch 2 (append)
        batch2 = spark.createDataFrame(
            [
                ("ORD002", "CUST002", "2024-01-16", "PROD002", 2, 49.99),
            ],
            ["order_id", "customer_id", "order_date", "product_id", "quantity", "price"],
        )

        batch2.write.format("delta").mode("append").save(integration_paths["bronze"])

        # Vérifier le total
        result = spark.read.format("delta").load(integration_paths["bronze"])
        assert result.count() == 2

        # Vérifier l'historique Delta
        from delta.tables import DeltaTable

        dt = DeltaTable.forPath(spark, integration_paths["bronze"])
        history = dt.history()
        assert history.count() >= 2
