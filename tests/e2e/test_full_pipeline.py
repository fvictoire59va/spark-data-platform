"""Tests end-to-end du pipeline complet."""
from __future__ import annotations

import pytest
from pyspark.sql import SparkSession


@pytest.mark.e2e
@pytest.mark.slow
class TestFullSalesPipeline:
    """Tests E2E du pipeline Sales complet."""

    def test_bronze_to_gold_pipeline(
        self,
        spark: SparkSession,
        temp_path: str,
        sample_orders_df,
        sample_customers_df,
    ):
        """
        Test du pipeline complet Bronze -> Silver -> Gold.
        """
        # Setup paths
        bronze_orders = f"{temp_path}/bronze/orders"
        bronze_customers = f"{temp_path}/bronze/customers"
        silver_orders = f"{temp_path}/silver/orders"
        gold_sales = f"{temp_path}/gold/sales_summary"

        # ============ BRONZE ============
        # Écriture des données brutes
        sample_orders_df.write.format("delta").mode("overwrite").save(bronze_orders)
        sample_customers_df.write.format("delta").mode("overwrite").save(bronze_customers)

        # Vérification Bronze
        bronze_orders_df = spark.read.format("delta").load(bronze_orders)
        assert bronze_orders_df.count() == 5

        # ============ SILVER ============
        # Transformation: nettoyage et enrichissement
        from pyspark.sql import functions as F

        orders_df = spark.read.format("delta").load(bronze_orders)
        customers_df = spark.read.format("delta").load(bronze_customers)

        silver_df = (
            orders_df
            .filter(F.col("status") == "completed")
            .join(customers_df, "customer_id", "left")
            .select(
                "order_id",
                "customer_id",
                "name",
                "country",
                F.to_date("order_date").alias("order_date"),
                F.col("amount").cast("decimal(10,2)").alias("amount"),
            )
            .withColumn("processed_at", F.current_timestamp())
        )

        silver_df.write.format("delta").mode("overwrite").save(silver_orders)

        # Vérification Silver
        silver_result = spark.read.format("delta").load(silver_orders)
        assert silver_result.count() == 3  # Seulement completed
        assert "name" in silver_result.columns
        assert "processed_at" in silver_result.columns

        # ============ GOLD ============
        # Agrégation par pays et date
        silver_df = spark.read.format("delta").load(silver_orders)

        gold_df = (
            silver_df
            .groupBy("country", "order_date")
            .agg(
                F.count("order_id").alias("total_orders"),
                F.sum("amount").alias("total_revenue"),
                F.avg("amount").alias("avg_order_value"),
            )
            .withColumn("created_at", F.current_timestamp())
        )

        gold_df.write.format("delta").mode("overwrite").save(gold_sales)

        # Vérification Gold
        gold_result = spark.read.format("delta").load(gold_sales)
        assert gold_result.count() > 0
        assert "total_orders" in gold_result.columns
        assert "total_revenue" in gold_result.columns

        # Vérification des valeurs
        fr_sales = gold_result.filter(F.col("country") == "FR").collect()
        assert len(fr_sales) > 0

    def test_data_quality_checks(
        self,
        spark: SparkSession,
        temp_path: str,
        sample_orders_df,
    ):
        """Test des contrôles de qualité des données."""
        from pyspark.sql import functions as F

        # Écriture des données
        path = f"{temp_path}/quality_test"
        sample_orders_df.write.format("delta").mode("overwrite").save(path)

        df = spark.read.format("delta").load(path)

        # Contrôles de qualité
        quality_checks = {
            "no_null_order_id": df.filter(F.col("order_id").isNull()).count() == 0,
            "valid_amounts": df.filter(F.col("amount") < 0).count() == 0,
            "valid_status": df.filter(
                ~F.col("status").isin(["completed", "pending", "cancelled"])
            ).count() == 0,
        }

        # Toutes les vérifications doivent passer
        for check_name, passed in quality_checks.items():
            assert passed, f"Quality check failed: {check_name}"

    def test_incremental_load(
        self,
        spark: SparkSession,
        temp_path: str,
    ):
        """Test du chargement incrémental."""
        from pyspark.sql import functions as F

        path = f"{temp_path}/incremental"

        # Lot initial
        initial_data = [
            ("ORD001", "2024-01-15", 100.0),
            ("ORD002", "2024-01-15", 200.0),
        ]
        initial_df = spark.createDataFrame(
            initial_data, ["order_id", "order_date", "amount"]
        )
        initial_df.write.format("delta").mode("overwrite").save(path)

        # Vérification initiale
        assert spark.read.format("delta").load(path).count() == 2

        # Lot incrémental (merge)
        incremental_data = [
            ("ORD002", "2024-01-15", 250.0),  # Mise à jour
            ("ORD003", "2024-01-16", 300.0),  # Nouveau
        ]
        incremental_df = spark.createDataFrame(
            incremental_data, ["order_id", "order_date", "amount"]
        )

        # Simulation d'un MERGE
        from delta.tables import DeltaTable

        delta_table = DeltaTable.forPath(spark, path)
        
        (
            delta_table.alias("target")
            .merge(
                incremental_df.alias("source"),
                "target.order_id = source.order_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        # Vérification finale
        final_df = spark.read.format("delta").load(path)
        assert final_df.count() == 3

        # Vérification de la mise à jour
        updated_row = final_df.filter(F.col("order_id") == "ORD002").collect()[0]
        assert updated_row["amount"] == 250.0
