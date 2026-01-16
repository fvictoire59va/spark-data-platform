"""Test du job IngestOrdersJob avec connexion JDBC à PostgreSQL."""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def test_ingest_orders() -> int:
    """Teste le job IngestOrdersJob via JDBC."""
    # Créer une session Spark
    builder = SparkSession.builder
    builder = builder.appName("IngestOrdersJobTest")
    builder = builder.master("spark://spark-master:7077")
    builder = builder.config("spark.executor.instances", "2")
    builder = builder.config("spark.executor.memory", "2g")
    builder = builder.config("spark.executor.cores", "2")
    builder = builder.config("spark.driver.memory", "2g")
    # Ajouter le JAR PostgreSQL JDBC
    builder = builder.config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
    spark = builder.getOrCreate()

    print("✅ Session Spark créée avec succès")

    # Configuration JDBC
    jdbc_url = "jdbc:postgresql://metastore-db:5432/dev_db"
    jdbc_user = "hive"
    jdbc_password = "hive123"
    jdbc_table = "sales.orders"
    jdbc_driver = "org.postgresql.Driver"

    print(f"🔗 Connexion JDBC à {jdbc_url}")

    try:
        # Lire les données depuis PostgreSQL
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", jdbc_table)
            .option("user", jdbc_user)
            .option("password", jdbc_password)
            .option("driver", jdbc_driver)
            .load()
        )

        print("✅ Données lues depuis PostgreSQL")

        # Afficher le schéma
        print("\n📋 SCHÉMA DES DONNÉES:")
        df.printSchema()

        # Ajouter les métadonnées d'ingestion
        df = df.withColumn("_ingestion_timestamp", F.current_timestamp()).withColumn(
            "_source", F.lit("jdbc_orders")
        )

        print("\n📊 DONNÉES INGÉRÉES (5 premières lignes):")
        df.show(5, truncate=False)

        # Afficher les statistiques
        print("\n📈 STATISTIQUES:")
        print(f"  ✓ Total de lignes: {df.count()}")
        print(f"  ✓ Colonnes: {', '.join(df.columns)}")

        # Agrégations par statut
        print("\n📉 AGRÉGATIONS PAR STATUT:")
        df.groupBy("status").agg(
            F.count("*").alias("nombre_commandes"),
            F.sum("quantity").alias("quantité_totale"),
            F.sum(F.col("quantity") * F.col("unit_price")).alias("montant_total"),
        ).show()

        # Agrégations par client
        print("\n👥 TOP 5 CLIENTS PAR MONTANT TOTAL:")
        df.groupBy("customer_id").agg(
            F.count("*").alias("nombre_commandes"),
            F.sum(F.col("quantity") * F.col("unit_price")).alias("montant_total"),
        ).orderBy(F.desc("montant_total")).show(5)

        spark.stop()
        print("\n✅ Test IngestOrdersJob terminé avec succès!")
        return 0

    except Exception as e:
        print(f"\n❌ Erreur lors de la lecture JDBC: {str(e)}")
        spark.stop()
        return 1


if __name__ == "__main__":
    sys.exit(test_ingest_orders())
