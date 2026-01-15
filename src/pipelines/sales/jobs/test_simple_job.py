"""Job de test simple pour vérifier l'infrastructure Spark."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def main() -> None:
    """Point d'entrée du job de test."""
    print("=" * 60)
    print("TEST SIMPLE JOB - SPARK PIPELINE")
    print("=" * 60)

    # Créer la session Spark
    spark = SparkSession.builder.appName("TestSimpleJob").getOrCreate()

    print("\n[STEP 1] SparkSession créée avec succès")
    print(f"  - Spark version: {spark.version}")
    print(f"  - App name: {spark.sparkContext.appName}")

    # Définir le schéma de test
    schema = StructType(
        [
            StructField("order_id", IntegerType(), False),
            StructField("customer_id", IntegerType(), False),
            StructField("amount", DoubleType(), False),
            StructField("status", StringType(), False),
            StructField("created_at", TimestampType(), False),
        ]
    )

    # Créer des données de test
    test_data = [
        (1, 101, 150.50, "completed", datetime(2026, 1, 10, 10, 0, 0)),
        (2, 102, 200.00, "pending", datetime(2026, 1, 11, 11, 0, 0)),
        (3, 101, 75.25, "completed", datetime(2026, 1, 12, 9, 0, 0)),
        (4, 103, 320.00, "failed", datetime(2026, 1, 13, 14, 0, 0)),
        (5, 104, 89.99, "completed", datetime(2026, 1, 14, 16, 30, 0)),
    ]

    print("\n[STEP 2] Création des données de test...")
    df_orders = spark.createDataFrame(test_data, schema)
    df_orders.show()
    print(f"  ✓ {df_orders.count()} commandes créées (Bronze Layer)")

    # Transformation Silver - Filtrer les commandes complétées
    print("\n[STEP 3] Transformation Silver Layer...")
    df_silver = df_orders.filter(F.col("status") == "completed")
    df_silver.show()
    print(f"  ✓ {df_silver.count()} commandes complétées (Silver Layer)")

    # Agrégation Gold - Revenue par client
    print("\n[STEP 4] Agrégation Gold Layer...")
    df_gold = df_silver.groupBy("customer_id").agg(
        F.sum("amount").alias("total_revenue"),
        F.count("order_id").alias("order_count"),
        F.avg("amount").alias("avg_order_value"),
    )
    df_gold.show()
    print(f"  ✓ {df_gold.count()} clients agrégés (Gold Layer)")

    # Résumé final
    total_revenue = df_gold.agg(F.sum("total_revenue")).collect()[0][0]

    print("\n" + "=" * 60)
    print("RÉSULTATS DU TEST")
    print("=" * 60)
    print(f"  - Bronze: {df_orders.count()} commandes")
    print(f"  - Silver: {df_silver.count()} commandes complétées")
    print(f"  - Gold: {df_gold.count()} clients")
    print(f"  - Revenue total: ${total_revenue:.2f}")
    print("\n✓ TEST SIMPLE JOB TERMINÉ AVEC SUCCÈS ✓")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
