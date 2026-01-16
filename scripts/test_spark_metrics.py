#!/usr/bin/env python3
"""
Simple Spark job pour tester et générer des métriques
"""

import random

from pyspark.sql import SparkSession


def main():
    """Simple test job"""

    spark = (
        SparkSession.builder.appName("test-metrics-job")
        .master("spark://spark-master:7077")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )

    print("\n✅ SparkSession créée")

    try:
        # Créer quelques RDDs pour générer des statistiques
        print("📊 Génération de données de test...")

        # Dataset 1: Nombres aléatoires
        rdd1 = spark.sparkContext.parallelize(range(1000000))
        sum_result = rdd1.map(lambda x: x * 2).sum()
        print(f"   • Sum of doubled numbers: {sum_result}")

        # Dataset 2: DataFrame avec données structurées
        data = [(i, f"user_{i % 100}", random.randint(100, 10000)) for i in range(10000)]
        df = spark.createDataFrame(data, ["id", "user", "amount"])

        print(f"   • DataFrame créé avec {df.count()} lignes")

        # Aggrégations
        agg_result = df.groupBy("user").agg({"amount": "sum"}).count()
        print(f"   • Agrégations: {agg_result} users")

        # Cache et traitement
        df.cache()
        df.count()

        print("\n✅ JOB TEST TERMINÉ AVEC SUCCÈS")
        print("📈 Métriques envoyées à Prometheus")

    except Exception as e:
        print(f"❌ Erreur: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
