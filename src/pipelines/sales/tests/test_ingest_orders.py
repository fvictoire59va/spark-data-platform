"""Tests pour le job d'ingestion des commandes."""
from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from src.pipelines.sales.jobs.ingest_orders import IngestOrdersJob

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class TestIngestOrdersJob:
    """Tests pour IngestOrdersJob."""

    @pytest.fixture
    def job(self, spark: SparkSession) -> IngestOrdersJob:
        """Fixture du job avec mock de config."""
        with patch.object(IngestOrdersJob, "_load_config"):
            job = IngestOrdersJob()
            job._spark = spark
            job._config = {
                "source": {
                    "type": "jdbc",
                    "url": "jdbc:postgresql://localhost:5432/test",
                    "table": "orders",
                    "user": "test",
                    "password": "test",
                },
                "paths": {
                    "bronze": {
                        "orders": "/tmp/test/bronze/orders",
                    },
                },
            }
            return job

    @pytest.fixture
    def raw_orders_df(self, spark: SparkSession) -> DataFrame:
        """Données brutes de commandes."""
        data = [
            ("ORD001", "CUST001", "2024-01-15", "PROD001", 2, 29.99, 0.0, "confirmed"),
            ("ORD002", "CUST002", "2024-01-15", "PROD002", 1, 49.99, 10.0, "pending"),
            ("ORD003", "CUST001", "2024-01-16", "PROD001", 3, 29.99, 5.0, "shipped"),
            (None, "CUST003", "2024-01-16", "PROD003", 1, 99.99, 0.0, "delivered"),
            ("ORD005", None, "2024-01-17", "PROD001", 1, 29.99, 0.0, "confirmed"),
        ]

        schema = StructType(
            [
                StructField("order_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("order_date", StringType(), True),
                StructField("product_id", StringType(), True),
                StructField("quantity", IntegerType(), True),
                StructField("unit_price", DoubleType(), True),
                StructField("discount", DoubleType(), True),
                StructField("status", StringType(), True),
            ]
        )

        return spark.createDataFrame(data, schema)

    def test_extract_returns_dataframe(
        self,
        job: IngestOrdersJob,
        raw_orders_df: DataFrame,
    ):
        """Vérifie que extract retourne un DataFrame."""
        with patch.object(job, "_read_from_source", return_value=raw_orders_df):
            result = job.extract()

            assert result is not None
            assert result.count() == 5

    def test_transform_adds_metadata_columns(
        self,
        job: IngestOrdersJob,
        raw_orders_df: DataFrame,
    ):
        """Vérifie l'ajout des colonnes de métadonnées."""
        result = job.transform(raw_orders_df)

        assert "_ingested_at" in result.columns
        assert "_source_system" in result.columns
        assert "_batch_id" in result.columns

    def test_transform_preserves_original_columns(
        self,
        job: IngestOrdersJob,
        raw_orders_df: DataFrame,
    ):
        """Vérifie que les colonnes originales sont préservées."""
        original_columns = raw_orders_df.columns
        result = job.transform(raw_orders_df)

        for col in original_columns:
            assert col in result.columns

    def test_validate_passes_for_valid_data(
        self,
        job: IngestOrdersJob,
        raw_orders_df: DataFrame,
    ):
        """Vérifie que la validation passe pour des données valides."""
        result = job.validate(raw_orders_df)
        assert result is True

    def test_validate_fails_for_empty_dataframe(
        self,
        job: IngestOrdersJob,
        spark: SparkSession,
        raw_orders_df: DataFrame,
    ):
        """Vérifie que la validation échoue pour un DataFrame vide."""
        empty_df = spark.createDataFrame([], raw_orders_df.schema)

        result = job.validate(empty_df)
        assert result is False

    @pytest.mark.integration
    def test_full_pipeline_integration(
        self,
        job: IngestOrdersJob,
        raw_orders_df: DataFrame,
        temp_delta_path: str,
    ):
        """Test d'intégration du pipeline complet."""
        job._config["paths"]["bronze"]["orders"] = temp_delta_path

        with patch.object(job, "_read_from_source", return_value=raw_orders_df):
            # Exécuter le pipeline
            extracted = job.extract()
            transformed = job.transform(extracted)
            job.load(transformed)

            # Vérifier l'écriture
            result = job._spark.read.format("delta").load(temp_delta_path)
            assert result.count() == 5
