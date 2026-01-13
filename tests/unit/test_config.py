# tests/unit/conftest.py
"""Configuration et fixtures pour les tests unitaires."""

from __future__ import annotations

import pytest
import os
import tempfile
import shutil
from datetime import datetime, date
from typing import Generator, Any
from unittest.mock import MagicMock

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType,
    ArrayType,
    MapType,
)

from tests.fixtures.sample_data import (
    SAMPLE_CUSTOMERS_VALID,
    SAMPLE_CUSTOMERS_INVALID,
    SAMPLE_PRODUCTS_VALID,
    SAMPLE_TRANSACTIONS_VALID,
    DataGenerator,
)


# =============================================================================
# SPARK SESSION FIXTURE
# =============================================================================

@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """
    Crée une session Spark pour les tests unitaires.
    
    Scope 'session' pour réutiliser la même session dans tous les tests.
    """
    spark_session = (
        SparkSession.builder
        .master("local[2]")
        .appName("unit-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp())
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    
    # Réduire le niveau de log pour les tests
    spark_session.sparkContext.setLogLevel("WARN")
    
    yield spark_session
    
    spark_session.stop()


@pytest.fixture(scope="function")
def spark_context(spark: SparkSession):
    """Retourne le SparkContext."""
    return spark.sparkContext


# =============================================================================
# TEMPORARY DIRECTORIES
# =============================================================================

@pytest.fixture
def tmp_dir() -> Generator[str, None, None]:
    """Crée un répertoire temporaire pour les tests."""
    dirpath = tempfile.mkdtemp()
    yield dirpath
    shutil.rmtree(dirpath, ignore_errors=True)


@pytest.fixture
def tmp_file(tmp_dir: str) -> Generator[str, None, None]:
    """Crée un fichier temporaire."""
    filepath = os.path.join(tmp_dir, "test_file.txt")
    yield filepath


@pytest.fixture
def csv_file(tmp_dir: str) -> str:
    """Crée un fichier CSV de test."""
    filepath = os.path.join(tmp_dir, "test.csv")
    content = """id,name,value
1,Alice,100
2,Bob,200
3,Charlie,300
"""
    with open(filepath, "w") as f:
        f.write(content)
    return filepath


@pytest.fixture
def json_file(tmp_dir: str) -> str:
    """Crée un fichier JSON de test."""
    filepath = os.path.join(tmp_dir, "test.json")
    content = """{"id": 1, "name": "Alice", "value": 100}
{"id": 2, "name": "Bob", "value": 200}
{"id": 3, "name": "Charlie", "value": 300}
"""
    with open(filepath, "w") as f:
        f.write(content)
    return filepath


@pytest.fixture
def parquet_dir(spark: SparkSession, tmp_dir: str) -> str:
    """Crée un répertoire avec des fichiers Parquet de test."""
    path = os.path.join(tmp_dir, "parquet_data")
    df = spark.createDataFrame([
        {"id": 1, "name": "Alice", "value": 100},
        {"id": 2, "name": "Bob", "value": 200},
        {"id": 3, "name": "Charlie", "value": 300},
    ])
    df.write.parquet(path)
    return path


# =============================================================================
# DATAFRAME FIXTURES
# =============================================================================

@pytest.fixture
def empty_df(spark: SparkSession) -> DataFrame:
    """Crée un DataFrame vide avec schéma."""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("value", DoubleType(), True),
    ])
    return spark.createDataFrame([], schema)


@pytest.fixture
def simple_df(spark: SparkSession) -> DataFrame:
    """Crée un DataFrame simple pour les tests."""
    data = [
        {"id": 1, "name": "Alice", "value": 100.0},
        {"id": 2, "name": "Bob", "value": 200.0},
        {"id": 3, "name": "Charlie", "value": 300.0},
    ]
    return spark.createDataFrame(data)


@pytest.fixture
def customers_df(spark: SparkSession) -> DataFrame:
    """Crée un DataFrame clients."""
    return spark.createDataFrame(SAMPLE_CUSTOMERS_VALID)


@pytest.fixture
def customers_invalid_df(spark: SparkSession) -> DataFrame:
    """Crée un DataFrame clients avec données invalides."""
    return spark.createDataFrame(SAMPLE_CUSTOMERS_INVALID)


@pytest.fixture
def products_df(spark: SparkSession) -> DataFrame:
    """Crée un DataFrame produits."""
    return spark.createDataFrame(SAMPLE_PRODUCTS_VALID)


@pytest.fixture
def transactions_df(spark: SparkSession) -> DataFrame:
    """Crée un DataFrame transactions."""
    return spark.createDataFrame(SAMPLE_TRANSACTIONS_VALID)


@pytest.fixture
def df_with_nulls(spark: SparkSession) -> DataFrame:
    """Crée un DataFrame avec des valeurs nulles."""
    data = [
        {"id": 1, "name": "Alice", "value": 100.0, "category": "A"},
        {"id": 2, "name": None, "value": 200.0, "category": "B"},
        {"id": 3, "name": "Charlie", "value": None, "category": "A"},
        {"id": 4, "name": None, "value": None, "category": None},
        {"id": 5, "name": "Eve", "value": 500.0, "category": "C"},
    ]
    return spark.createDataFrame(data)


@pytest.fixture
def df_with_duplicates(spark: SparkSession) -> DataFrame:
    """Crée un DataFrame avec des doublons."""
    data = [
        {"id": 1, "name": "Alice", "value": 100},
        {"id": 2, "name": "Bob", "value": 200},
        {"id": 1, "name": "Alice", "value": 100},  # Doublon exact
        {"id": 3, "name": "Charlie", "value": 300},
        {"id": 2, "name": "Bob", "value": 250},  # Doublon partiel (id)
    ]
    return spark.createDataFrame(data)


@pytest.fixture
def df_for_aggregation(spark: SparkSession) -> DataFrame:
    """Crée un DataFrame pour tests d'agrégation."""
    data = [
        {"category": "A", "region": "North", "sales": 100, "quantity": 10},
        {"category": "A", "region": "South", "sales": 150, "quantity": 15},
        {"category": "A", "region": "North", "sales": 200, "quantity": 20},
        {"category": "B", "region": "North", "sales": 300, "quantity": 30},
        {"category": "B", "region": "South", "sales": 250, "quantity": 25},
        {"category": "C", "region": "South", "sales": 400, "quantity": 40},
    ]
    return spark.createDataFrame(data)


@pytest.fixture
def df_for_join_left(spark: SparkSession) -> DataFrame:
    """DataFrame gauche pour tests de jointure."""
    data = [
        {"id": 1, "name": "Alice", "dept_id": 10},
        {"id": 2, "name": "Bob", "dept_id": 20},
        {"id": 3, "name": "Charlie", "dept_id": 10},
        {"id": 4, "name": "Diana", "dept_id": 30},
    ]
    return spark.createDataFrame(data)


@pytest.fixture
def df_for_join_right(spark: SparkSession) -> DataFrame:
    """DataFrame droit pour tests de jointure."""
    data = [
        {"dept_id": 10, "dept_name": "Engineering"},
        {"dept_id": 20, "dept_name": "Marketing"},
        {"dept_id": 40, "dept_name": "Sales"},
    ]
    return spark.createDataFrame(data)


@pytest.fixture
def df_with_dates(spark: SparkSession) -> DataFrame:
    """Crée un DataFrame avec des dates."""
    data = [
        {"id": 1, "event_date": "2023-01-15", "timestamp": "2023-01-15 10:30:00"},
        {"id": 2, "event_date": "2023-06-20", "timestamp": "2023-06-20 14:45:00"},
        {"id": 3, "event_date": "2023-12-25", "timestamp": "2023-12-25 08:00:00"},
    ]
    return spark.createDataFrame(data)


@pytest.fixture
def df_with_arrays(spark: SparkSession) -> DataFrame:
    """Crée un DataFrame avec des colonnes de type array."""
    data = [
        {"id": 1, "tags": ["python", "spark"], "scores": [85, 90, 88]},
        {"id": 2, "tags": ["java", "scala", "spark"], "scores": [92, 88]},
        {"id": 3, "tags": ["python"], "scores": [75, 80, 85, 90]},
    ]
    return spark.createDataFrame(data)


# =============================================================================
# SCHEMA FIXTURES
# =============================================================================

@pytest.fixture
def customer_schema() -> StructType:
    """Retourne le schéma client."""
    return StructType([
        StructField("customer_id", StringType(), False),
        StructField("email", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("birth_date", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("registration_date", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("customer_segment", StringType(), True),
    ])


@pytest.fixture
def transaction_schema() -> StructType:
    """Retourne le schéma transaction."""
    return StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("status", StringType(), True),
    ])


# =============================================================================
# MOCK FIXTURES
# =============================================================================

@pytest.fixture
def mock_spark_session() -> MagicMock:
    """Crée un mock de SparkSession."""
    mock = MagicMock(spec=SparkSession)
    mock.read = MagicMock()
    mock.write = MagicMock()
    return mock


@pytest.fixture
def mock_dataframe() -> MagicMock:
    """Crée un mock de DataFrame."""
    mock = MagicMock(spec=DataFrame)
    mock.count.return_value = 100
    mock.columns = ["id", "name", "value"]
    return mock


# =============================================================================
# HELPER FIXTURES
# =============================================================================

@pytest.fixture
def assert_dataframe_equal():
    """Fixture pour comparer deux DataFrames."""
    def _assert_equal(df1: DataFrame, df2: DataFrame, check_order: bool = False):
        """
        Compare deux DataFrames.
        
        Args:
            df1: Premier DataFrame
            df2: Deuxième DataFrame
            check_order: Si True, vérifie aussi l'ordre des lignes
        """
        # Vérifier les colonnes
        assert set(df1.columns) == set(df2.columns), \
            f"Colonnes différentes: {df1.columns} vs {df2.columns}"
        
        # Vérifier le nombre de lignes
        assert df1.count() == df2.count(), \
            f"Nombre de lignes différent: {df1.count()} vs {df2.count()}"
        
        # Vérifier le contenu
        if check_order:
            rows1 = [row.asDict() for row in df1.collect()]
            rows2 = [row.asDict() for row in df2.collect()]
            assert rows1 == rows2, "Contenu différent"
        else:
            set1 = {tuple(sorted(row.asDict().items())) for row in df1.collect()}
            set2 = {tuple(sorted(row.asDict().items())) for row in df2.collect()}
            assert set1 == set2, "Contenu différent"
    
    return _assert_equal


@pytest.fixture
def assert_column_values():
    """Fixture pour vérifier les valeurs d'une colonne."""
    def _assert_values(df: DataFrame, column: str, expected_values: list):
        """Vérifie que les valeurs d'une colonne correspondent."""
        actual_values = [row[column] for row in df.select(column).collect()]
        assert sorted(actual_values) == sorted(expected_values), \
            f"Valeurs différentes pour {column}: {actual_values} vs {expected_values}"
    
    return _assert_values
