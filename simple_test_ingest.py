"""Test simple de l'ingestion Spark sans dépendances de configuration."""

from datetime import datetime
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def create_spark_session() -> SparkSession:
    """Crée une session Spark minimale."""
    builder = SparkSession.builder
    builder = builder.appName("TestIngestJob")
    builder = builder.master("spark://spark-master:7077")
    builder = builder.config("spark.executor.instances", "2")
    builder = builder.config("spark.executor.memory", "2g")
    builder = builder.config("spark.executor.cores", "2")
    builder = builder.config("spark.driver.memory", "2g")
    return builder.getOrCreate()


def test_ingest() -> None:
    """Teste l'ingestion avec des données d'exemple."""
    spark = create_spark_session()
    print("✅ Session Spark créée avec succès")

    # Définir le schéma
    schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_datetime", TimestampType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DecimalType(10, 2), True),
            StructField("discount", DecimalType(10, 2), True),
            StructField("status", StringType(), True),
            StructField("created_at", TimestampType(), True),
            StructField("updated_at", TimestampType(), True),
        ]
    )

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
            5,
            Decimal("9.99"),
            Decimal("0.00"),
            "PENDING",
            datetime(2024, 1, 15, 14, 20),
            datetime(2024, 1, 15, 14, 20),
        ),
        (
            "ORD004",
            "CUST003",
            datetime(2024, 1, 15, 16, 15),
            "PROD001",
            3,
            Decimal("19.99"),
            Decimal("2.00"),
            "CANCELLED",
            datetime(2024, 1, 15, 16, 15),
            datetime(2024, 1, 15, 16, 15),
        ),
    ]

    # Créer le DataFrame
    df = spark.createDataFrame(data, schema=schema)
    print("✅ DataFrame créé avec les données de test")

    # Ajouter des colonnes de métadonnées
    df = df.withColumn("_ingestion_timestamp", F.current_timestamp()).withColumn(
        "_source", F.lit("test_data")
    )

    print("\n📊 DONNÉES INGÉRÉES:")
    df.show(truncate=False)

    print("\n📈 STATISTIQUES:")
    print(f"  ✓ Total de lignes: {df.count()}")
    print(f"  ✓ Colonnes: {', '.join(df.columns)}")

    print("\n📉 AGRÉGATIONS PAR STATUT:")
    df.groupBy("status").agg(
        F.count("*").alias("nombre_commandes"),
        F.sum("quantity").alias("quantité_totale"),
        F.sum(F.col("quantity") * F.col("unit_price")).alias("montant_total"),
    ).show()

    spark.stop()
    print("\n✅ Test d'ingestion terminé avec succès!")


if __name__ == "__main__":
    test_ingest()
