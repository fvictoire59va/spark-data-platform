"""Configuration globale des tests pytest."""
from __future__ import annotations

import os
from typing import TYPE_CHECKING, Generator

import pytest
from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """
    Crée une SparkSession pour les tests.
    
    Scope session pour réutiliser la même session.
    """
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest-spark")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    
    yield spark
    
    spark.stop()


@pytest.fixture
def sample_orders_df(spark: SparkSession) -> DataFrame:
    """DataFrame d'exemple avec des commandes."""
    data = [
        ("ORD001", "CUST001", "2024-01-15", 100.50, "completed"),
        ("ORD002", "CUST002", "2024-01-15", 250.00, "completed"),
        ("ORD003", "CUST001", "2024-01-16", 75.25, "pending"),
        ("ORD004", "CUST003", "2024-01-16", 500.00, "completed"),
        ("ORD005", "CUST002", "2024-01-17", 150.75, "cancelled"),
    ]
    
    return spark.createDataFrame(
        data,
        ["order_id", "customer_id", "order_date", "amount", "status"]
    )


@pytest.fixture
def sample_customers_df(spark: SparkSession) -> DataFrame:
    """DataFrame d'exemple avec des clients."""
    data = [
        ("CUST001", "John Doe", "john@example.com", "FR"),
        ("CUST002", "Jane Smith", "jane@example.com", "UK"),
        ("CUST003", "Bob Wilson", "bob@example.com", "DE"),
    ]
    
    return spark.createDataFrame(
        data,
        ["customer_id", "name", "email", "country"]
    )


@pytest.fixture
def temp_path(tmp_path) -> str:
    """Chemin temporaire pour les tests."""
    return str(tmp_path)


@pytest.fixture(autouse=True)
def env_setup():
    """Configure les variables d'environnement pour les tests."""
    os.environ["ENVIRONMENT"] = "test"
    os.environ["LOG_LEVEL"] = "WARNING"
    yield
    # Cleanup si nécessaire
