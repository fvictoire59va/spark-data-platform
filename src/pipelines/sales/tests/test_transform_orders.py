"""Tests pour le job de transformation des commandes."""
from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.pipelines.sales.jobs.transform_orders import TransformOrdersJob

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class TestTransformOrdersJob:
    """Tests pour TransformOrdersJob."""

    @pytest.fixture
    def job(self, spark: SparkSession) -> TransformOrdersJob:
        """Fixture du job."""
        with patch.object(TransformOrdersJob, "_load_config"):
            job = TransformOrdersJob()
            job._spark = spark
            job._config = {
                "paths": {
                    "bronze": {"orders": "/tmp/test/bronze/orders"},
                    "silver": {"orders": "/tmp/test/silver/orders"},
                },
            }
            return job

    @pytest.fixture
    def bronze_orders_df(self, spark: SparkSession) -> DataFrame:
        """Données Bronze simulées."""
        data = [
            ("ORD001", "CUST001", "2024-01-15", "PROD001", 2, 29.99, 0.0, "confirmed"),
            ("ORD002", "CUST002", "2024-01-15", "PROD002", 1, 49.99, 10.0, "pending"),
            ("ORD003", "CUST001", "2024-01-16", "PROD001", 3, 29.99, 5.0, "shipped"),
            ("ORD001", "CUST001", "2024-01-15", "PROD001", 2, 29.99, 0.0, "confirmed"),  # Doublon
        ]

        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("discount", DoubleType(), True),
            StructField("status", StringType(), True),
        ])

        return spark.createDataFrame(data, schema)

    def test_transform_calculates_total_amount(
        self,
        job: TransformOrdersJob,
        bronze_orders_df: DataFrame,
    ):
        """Vérifie le calcul du montant total."""
        result = job.transform(bronze_orders_df)

        # Vérifier ORD001: quantity=2, unit_price=29.99, discount=0
        # total = 2 * 29.99 - 0 = 59.98
        ord001 = result.filter(F.col("order_id") == "ORD001").first()
        assert abs(ord001["total_amount"] - 59.98) < 0.01

        # Vérifier ORD002: quantity=1, unit_price=49.99, discount=10
        # total = 1 * 49.99 - 10 = 39.99
        ord002 = result.filter(F.col("order_id") == "ORD002").first()
        assert abs(ord002["total_amount"] - 39.99) < 0.01

    def test_transform_removes_duplicates(
        self,
        job: TransformOrdersJob,
        bronze_orders_df: DataFrame,
    ):
        """Vérifie la suppression des doublons."""
        result = job.transform(bronze_orders_df)

        # ORD001 apparaît 2 fois dans les données source
        ord001_count = result.filter(F.col("order_id") == "ORD001").count()
        assert ord001_count == 1

        # Total de 3 lignes uniques
        assert result.count() == 3

    def test_transform_casts_order_date(
        self,
        job: TransformOrdersJob,
        bronze_orders_df: DataFrame,
    ):
        """Vérifie le cast de order_date en DateType."""
        result = job.transform(bronze_orders_df)

        date_field = [f for f in result.schema.fields if f.name == "order_date"][0]
        assert date_field.dataType == DateType()

    def test_transform_standardizes_status(
        self,
        job: TransformOrdersJob,
        spark: SparkSession,
    ):
        """Vérifie la standardisation des statuts."""
        data = [
            ("ORD001", "CUST001", "2024-01-15", "PROD001", 1, 10.0, 0.0, "CONFIRMED"),
            ("ORD002", "CUST001", "2024-01-15", "PROD001", 1, 10.0, 0.0, "Pending"),
            ("ORD003", "CUST001", "2024-01-15", "PROD001", 1, 10.0, 0.0, "SHIPPED"),
        ]

        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("discount", DoubleType(), True),
            StructField("status", StringType(), True),
        ])

        df = spark.createDataFrame(data, schema)
        result = job.transform(df)

        statuses = [row["status"] for row in result.collect()]
        assert all(s.islower() for s in statuses)

    def test_transform_adds_processing_timestamp(
        self,
        job: TransformOrdersJob,
        bronze_orders_df: DataFrame,
    ):
        """Vérifie l'ajout du timestamp de traitement."""
        result = job.transform(bronze_orders_df)

        assert "_processed_at" in result.columns

        processed_field = [f for f in result.schema.fields if f.name == "_processed_at"][0]
        assert processed_field.dataType == TimestampType()

    def test_validate_fails_with_null_order_ids(
        self,
        job: TransformOrdersJob,
        spark: SparkSession,
    ):
        """Vérifie que la validation échoue avec des order_id null."""
        data = [
            (None, "CUST001", "2024-01-15", "PROD001", 1, 10.0, 0.0, "confirmed"),
        ]

        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("discount", DoubleType(), True),
            StructField("status", StringType(), True),
        ])

        df = spark.createDataFrame(data, schema)
        result = job.validate(df)

        assert result is False

    def test_validate_fails_with_negative_amounts(
        self,
        job: TransformOrdersJob,
        spark: SparkSession,
    ):
        """Vérifie que la validation échoue avec des montants négatifs."""
        data = [
            ("ORD001", "CUST001", "2024-01-15", "PROD001", -1, 10.0, 0.0, "confirmed"),
        ]

        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("discount", DoubleType(), True),
            StructField("status", StringType(), True),
        ])

        df = spark.createDataFrame(data, schema)
        result = job.validate(df)

        assert result is False

    @pytest.mark.integration
    def test_full_transform_pipeline(
        self,
        job: TransformOrdersJob,
        bronze_orders_df: DataFrame,
        temp_delta_path: str,
    ):
        """Test d'intégration de la transformation complète."""
        job._config["paths"]["silver"]["orders"] = temp_delta_path

        # Simuler la lecture Bronze
        with patch.object(job, "extract", return_value=bronze_orders_df):
            transformed = job.transform(bronze_orders_df)
            
            assert job.validate(transformed) is True
            
            job.load(transformed)

            # Vérifier l'écriture
            result = job._spark.read.format("delta").load(temp_delta_path)
            assert result.count() == 3
            assert "total_amount" in result.columns
