# tests/integration/test_delta_operations.py
"""Tests d'intégration pour les opérations Delta Lake."""

from __future__ import annotations

import pytest
from datetime import datetime, timedelta
from typing import Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    BooleanType,
)

from delta import DeltaTable

from tests.fixtures.sample_data import (
    SAMPLE_CUSTOMERS_VALID,
    SAMPLE_PRODUCTS_VALID,
    SAMPLE_TRANSACTIONS_VALID,
    DataGenerator,
)


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture(scope="module")
def spark_delta() -> SparkSession:
    """Crée une session Spark avec support Delta Lake."""
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-delta-operations")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse-test")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    
    yield spark
    spark.stop()


@pytest.fixture
def delta_path(tmp_path) -> str:
    """Retourne un chemin temporaire pour les tables Delta."""
    return str(tmp_path / "delta_tables")


@pytest.fixture
def customers_df(spark_delta: SparkSession) -> DataFrame:
    """Crée un DataFrame clients."""
    return spark_delta.createDataFrame(SAMPLE_CUSTOMERS_VALID)


@pytest.fixture
def products_df(spark_delta: SparkSession) -> DataFrame:
    """Crée un DataFrame produits."""
    return spark_delta.createDataFrame(SAMPLE_PRODUCTS_VALID)


@pytest.fixture
def transactions_df(spark_delta: SparkSession) -> DataFrame:
    """Crée un DataFrame transactions."""
    return spark_delta.createDataFrame(SAMPLE_TRANSACTIONS_VALID)


# =============================================================================
# TESTS - ÉCRITURE DELTA
# =============================================================================

class TestDeltaWrite:
    """Tests pour l'écriture Delta."""
    
    def test_write_delta_basic(
        self,
        spark_delta: SparkSession,
        customers_df: DataFrame,
        delta_path: str,
    ):
        """Test écriture basique en Delta."""
        table_path = f"{delta_path}/customers"
        
        # Écrire en Delta
        customers_df.write.format("delta").mode("overwrite").save(table_path)
        
        # Vérifier
        result = spark_delta.read.format("delta").load(table_path)
        assert result.count() == customers_df.count()
    
    def test_write_delta_with_partitioning(
        self,
        spark_delta: SparkSession,
        customers_df: DataFrame,
        delta_path: str,
    ):
        """Test écriture Delta avec partitionnement."""
        table_path = f"{delta_path}/customers_partitioned"
        
        # Écrire avec partition par pays
        (
            customers_df
            .write
            .format("delta")
            .mode("overwrite")
            .partitionBy("country")
            .save(table_path)
        )
        
        # Vérifier la structure
        result = spark_delta.read.format("delta").load(table_path)
        assert result.count() == customers_df.count()
        
        # Vérifier qu'on peut filtrer efficacement par partition
        france_df = (
            spark_delta.read
            .format("delta")
            .load(table_path)
            .filter(F.col("country") == "FR")
        )
        expected_count = customers_df.filter(F.col("country") == "FR").count()
        assert france_df.count() == expected_count
    
    def test_write_delta_append_mode(
        self,
        spark_delta: SparkSession,
        delta_path: str,
    ):
        """Test écriture Delta en mode append."""
        table_path = f"{delta_path}/append_test"
        
        # Première écriture
        df1 = spark_delta.createDataFrame([
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ])
        df1.write.format("delta").mode("overwrite").save(table_path)
        
        # Deuxième écriture en append
        df2 = spark_delta.createDataFrame([
            {"id": 3, "name": "Charlie"},
            {"id": 4, "name": "Diana"},
        ])
        df2.write.format("delta").mode("append").save(table_path)
        
        # Vérifier
        result = spark_delta.read.format("delta").load(table_path)
        assert result.count() == 4
    
    def test_write_delta_with_schema_evolution(
        self,
        spark_delta: SparkSession,
        delta_path: str,
    ):
        """Test écriture Delta avec évolution de schéma."""
        table_path = f"{delta_path}/schema_evolution"
        
        # Première écriture
        df1 = spark_delta.createDataFrame([
            {"id": 1, "name": "Alice"},
        ])
        df1.write.format("delta").mode("overwrite").save(table_path)
        
        # Deuxième écriture avec nouvelle colonne
        df2 = spark_delta.createDataFrame([
            {"id": 2, "name": "Bob", "email": "bob@test.com"},
        ])
        (
            df2.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(table_path)
        )
        
        # Vérifier
        result = spark_delta.read.format("delta").load(table_path)
        assert result.count() == 2
        assert "email" in result.columns


# =============================================================================
# TESTS - LECTURE DELTA
# =============================================================================

class TestDeltaRead:
    """Tests pour la lecture Delta."""
    
    def test_read_delta_basic(
        self,
        spark_delta: SparkSession,
        customers_df: DataFrame,
        delta_path: str,
    ):
        """Test lecture basique Delta."""
        table_path = f"{delta_path}/read_basic"
        customers_df.write.format("delta").mode("overwrite").save(table_path)
        
        result = spark_delta.read.format("delta").load(table_path)
        assert result.count() == customers_df.count()
        assert set(result.columns) == set(customers_df.columns)
    
    def test_read_delta_specific_columns(
        self,
        spark_delta: SparkSession,
        customers_df: DataFrame,
        delta_path: str,
    ):
        """Test lecture Delta avec sélection de colonnes."""
        table_path = f"{delta_path}/read_columns"
        customers_df.write.format("delta").mode("overwrite").save(table_path)
        
        result = (
            spark_delta.read
            .format("delta")
            .load(table_path)
            .select("customer_id", "email", "first_name")
        )
        
        assert result.columns == ["customer_id", "email", "first_name"]
        assert result.count() == customers_df.count()
    
    def test_read_delta_with_filter(
        self,
        spark_delta: SparkSession,
        customers_df: DataFrame,
        delta_path: str,
    ):
        """Test lecture Delta avec filtre."""
        table_path = f"{delta_path}/read_filter"
        customers_df.write.format("delta").mode("overwrite").save(table_path)
        
        result = (
            spark_delta.read
            .format("delta")
            .load(table_path)
            .filter(F.col("is_active") == True)
        )
        
        expected = customers_df.filter(F.col("is_active") == True).count()
        assert result.count() == expected


# =============================================================================
# TESTS - TIME TRAVEL
# =============================================================================

class TestDeltaTimeTravel:
    """Tests pour le Time Travel Delta."""
    
    def test_time_travel_by_version(
        self,
        spark_delta: SparkSession,
        delta_path: str,
    ):
        """Test Time Travel par version."""
        table_path = f"{delta_path}/time_travel_version"
        
        # Version 0 : écriture initiale
        df_v0 = spark_delta.createDataFrame([
            {"id": 1, "name": "Alice", "status": "active"},
            {"id": 2, "name": "Bob", "status": "active"},
        ])
        df_v0.write.format("delta").mode("overwrite").save(table_path)
        
        # Version 1 : mise à jour
        df_v1 = spark_delta.createDataFrame([
            {"id": 1, "name": "Alice Updated", "status": "active"},
            {"id": 2, "name": "Bob", "status": "inactive"},
            {"id": 3, "name": "Charlie", "status": "active"},
        ])
        df_v1.write.format("delta").mode("overwrite").save(table_path)
        
        # Lire la version actuelle
        current = spark_delta.read.format("delta").load(table_path)
        assert current.count() == 3
        
        # Lire la version 0
        version_0 = (
            spark_delta.read
            .format("delta")
            .option("versionAsOf", 0)
            .load(table_path)
        )
        assert version_0.count() == 2
        
        # Vérifier le contenu de la version 0
        alice_v0 = version_0.filter(F.col("id") == 1).first()
        assert alice_v0["name"] == "Alice"
    
    def test_time_travel_by_timestamp(
        self,
        spark_delta: SparkSession,
        delta_path: str,
    ):
        """Test Time Travel par timestamp."""
        table_path = f"{delta_path}/time_travel_timestamp"
        
        # Écriture initiale
        df = spark_delta.createDataFrame([
            {"id": 1, "value": 100},
        ])
        df.write.format("delta").mode("overwrite").save(table_path)
        
        # Capturer le timestamp après création
        delta_table = DeltaTable.forPath(spark_delta, table_path)
        history = delta_table.history(1).collect()
        creation_timestamp = history[0]["timestamp"]
        
        # Mise à jour
        df2 = spark_delta.createDataFrame([
            {"id": 1, "value": 200},
        ])
        df2.write.format("delta").mode("overwrite").save(table_path)
        
        # Lire au timestamp de création
        old_data = (
            spark_delta.read
            .format("delta")
            .option("timestampAsOf", creation_timestamp)
            .load(table_path)
        )
        
        assert old_data.first()["value"] == 100
    
    def test_delta_history(
        self,
        spark_delta: SparkSession,
        delta_path: str,
    ):
        """Test consultation de l'historique Delta."""
        table_path = f"{delta_path}/history_test"
        
        # Plusieurs versions
        for i in range(5):
            df = spark_delta.createDataFrame([
                {"id": 1, "version": i},
            ])
            mode = "overwrite" if i == 0 else "append"
            df.write.format("delta").mode(mode).save(table_path)
        
        # Consulter l'historique
        delta_table = DeltaTable.forPath(spark_delta, table_path)
        history_df = delta_table.history()
        
        assert history_df.count() == 5
        
        # Vérifier les colonnes d'historique
        history_columns = history_df.columns
        assert "version" in history_columns
        assert "timestamp" in history_columns
        assert "operation" in history_columns


# =============================================================================
# TESTS - MERGE (UPSERT)
# =============================================================================

class TestDeltaMerge:
    """Tests pour les opérations MERGE Delta."""
    
    def test_merge_upsert(
        self,
        spark_delta: SparkSession,
        delta_path: str,
    ):
        """Test MERGE upsert (insert + update)."""
        table_path = f"{delta_path}/merge_upsert"
        
        # Données initiales
        initial_df = spark_delta.createDataFrame([
            {"customer_id": "C001", "name": "Alice", "score": 100},
            {"customer_id": "C002", "name": "Bob", "score": 200},
            {"customer_id": "C003", "name": "Charlie", "score": 150},
        ])
        initial_df.write.format("delta").mode("overwrite").save(table_path)
        
        # Nouvelles données (updates + inserts)
        updates_df = spark_delta.createDataFrame([
            {"customer_id": "C001", "name": "Alice", "score": 150},  # Update
            {"customer_id": "C002", "name": "Bob Updated", "score": 250},  # Update
            {"customer_id": "C004", "name": "Diana", "score": 300},  # Insert
        ])
        
        # Exécuter le MERGE
        delta_table = DeltaTable.forPath(spark_delta, table_path)
        
        (
            delta_table.alias("target")
            .merge(
                updates_df.alias("source"),
                "target.customer_id = source.customer_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        
        # Vérifier
        result = spark_delta.read.format("delta").load(table_path)
        assert result.count() == 4
        
        # Vérifier les updates
        alice = result.filter(F.col("customer_id") == "C001").first()
        assert alice["score"] == 150
        
        bob = result.filter(F.col("customer_id") == "C002").first()
        assert bob["name"] == "Bob Updated"
        
        # Vérifier l'insert
        diana = result.filter(F.col("customer_id") == "C004").first()
        assert diana is not None
        assert diana["score"] == 300
    
    def test_merge_with_delete(
        self,
        spark_delta: SparkSession,
        delta_path: str,
    ):
        """Test MERGE avec suppression."""
        table_path = f"{delta_path}/merge_delete"
        
        # Données initiales
        initial_df = spark_delta.createDataFrame([
            {"id": 1, "status": "active", "value": 100},
            {"id": 2, "status": "active", "value": 200},
            {"id": 3, "status": "active", "value": 300},
        ])
        initial_df.write.format("delta").mode("overwrite").save(table_path)
        
        # Données avec suppressions marquées
        updates_df = spark_delta.createDataFrame([
            {"id": 1, "status": "deleted", "value": 100},
            {"id": 2, "status": "active", "value": 250},
        ])
        
        # MERGE avec delete conditionnel
        delta_table = DeltaTable.forPath(spark_delta, table_path)
        
        (
            delta_table.alias("target")
            .merge(
                updates_df.alias("source"),
                "target.id = source.id"
            )
            .whenMatchedDelete(condition="source.status = 'deleted'")
            .whenMatchedUpdateAll()
            .execute()
        )
        
        # Vérifier
        result = spark_delta.read.format("delta").load(table_path)
        assert result.count() == 2  # id=1 supprimé
        
        ids = [row["id"] for row in result.collect()]
        assert 1 not in ids
        assert 2 in ids
        assert 3 in ids
    
    def test_merge_conditional_update(
        self,
        spark_delta: SparkSession,
        delta_path: str,
    ):
        """Test MERGE avec mise à jour conditionnelle."""
        table_path = f"{delta_path}/merge_conditional"
        
        # Données initiales
        initial_df = spark_delta.createDataFrame([
            {"id": 1, "name": "Alice", "score": 100, "updated_at": "2023-01-01"},
            {"id": 2, "name": "Bob", "score": 200, "updated_at": "2023-06-01"},
        ])
        initial_df.write.format("delta").mode("overwrite").save(table_path)
        
        # Updates (seulement si plus récent)
        updates_df = spark_delta.createDataFrame([
            {"id": 1, "name": "Alice New", "score": 150, "updated_at": "2023-07-01"},  # Plus récent
            {"id": 2, "name": "Bob Old", "score": 180, "updated_at": "2023-03-01"},  # Plus ancien
        ])
        
        # MERGE conditionnel
        delta_table = DeltaTable.forPath(spark_delta, table_path)
        
        (
            delta_table.alias("target")
            .merge(
                updates_df.alias("source"),
                "target.id = source.id"
            )
            .whenMatchedUpdate(
                condition="source.updated_at > target.updated_at",
                set={
                    "name": "source.name",
                    "score": "source.score",
                    "updated_at": "source.updated_at",
                }
            )
            .execute()
        )
        
        # Vérifier
        result = spark_delta.read.format("delta").load(table_path)
        
        alice = result.filter(F.col("id") == 1).first()
        assert alice["name"] == "Alice New"  # Mis à jour
        assert alice["score"] == 150
        
        bob = result.filter(F.col("id") == 2).first()
        assert bob["name"] == "Bob"  # Pas mis à jour (source plus ancien)
        assert bob["score"] == 200
    
    def test_merge_insert_only(
        self,
        spark_delta: SparkSession,
        delta_path: str,
    ):
        """Test MERGE insert only (ignore existing)."""
        table_path = f"{delta_path}/merge_insert_only"
        
        # Données initiales
        initial_df = spark_delta.createDataFrame([
            {"id": 1, "value": 100},
            {"id": 2, "value": 200},
        ])
        initial_df.write.format("delta").mode("overwrite").save(table_path)
        
        # Nouvelles données (certaines existent déjà)
        new_df = spark_delta.createDataFrame([
            {"id": 2, "value": 999},  # Existe, ignorer
            {"id": 3, "value": 300},  # Nouveau
            {"id": 4, "value": 400},  # Nouveau
        ])
        
        # MERGE insert only
        delta_table = DeltaTable.forPath(spark_delta, table_path)
        
        (
            delta_table.alias("target")
            .merge(
                new_df.alias("source"),
                "target.id = source.id"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        
        # Vérifier
        result = spark_delta.read.format("delta").load(table_path)
        assert result.count() == 4
        
        # id=2 garde sa valeur originale
        id_2 = result.filter(F.col("id") == 2).first()
        assert id_2["value"] == 200


# =============================================================================
# TESTS - DELETE ET UPDATE
# =============================================================================

class TestDeltaDeleteUpdate:
    """Tests pour DELETE et UPDATE Delta."""
    
    def test_delete_by_condition(
        self,
        spark_delta: SparkSession,
        delta_path: str,
    ):
        """Test DELETE par condition."""
        table_path = f"{delta_path}/delete_condition"
        
        # Données initiales
        df = spark_delta.createDataFrame([
            {"id": 1, "status": "active"},
            {"id": 2, "status": "inactive"},
            {"id": 3, "status": "active"},
            {"id": 4, "status": "inactive"},
        ])
        df.write.format("delta").mode("overwrite").save(table_path)
        
        # Supprimer les inactifs
        delta_table = DeltaTable.forPath(spark_delta, table_path)
        delta_table.delete("status = 'inactive'")
        
        # Vérifier
        result = spark_delta.read.format("delta").load(table_path)
        assert result.count() == 2
        
        statuses = [row["status"] for row in result.collect()]
        assert all(s == "active" for s in statuses)
    
    def test_update_by_condition(
        self,
        spark_delta: SparkSession,
        delta_path: str,
    ):
        """Test UPDATE par condition."""
        table_path = f"{delta_path}/update_condition"
        
        # Données initiales
        df = spark_delta.createDataFrame([
            {"id": 1, "price": 100.0, "category": "A"},
            {"id": 2, "price": 200.0, "category": "B"},
            {"id": 3, "price": 150.0, "category": "A"},
        ])
        df.write.format("delta").mode("overwrite").save(table_path)
        
        # Augmenter les prix de catégorie A de 10%
        delta_table = DeltaTable.forPath(spark_delta, table_path)
        delta_table.update(
            condition="category = 'A'",
            set={"price": F.col("price") * 1.1}
        )
        
        # Vérifier
        result = spark_delta.read.format("delta").load(table_path)
        
        cat_a = result.filter(F.col("category") == "A").collect()
        assert cat_a[0]["price"] == 110.0  # 100 * 1.1
        assert cat_a[1]["price"] == 165.0  # 150 * 1.1
        
        cat_b = result.filter(F.col("category") == "B").first()
        assert cat_b["price"] == 200.0  # Inchangé


# =============================================================================
# TESTS - VACUUM ET OPTIMISATION
# =============================================================================

class TestDeltaOptimization:
    """Tests pour l'optimisation Delta."""
    
    def test_optimize_table(
        self,
        spark_delta: SparkSession,
        delta_path: str,
    ):
        """Test OPTIMIZE (compaction)."""
        table_path = f"{delta_path}/optimize_test"
        
        # Créer plusieurs petits fichiers
        for i in range(10):
            df = spark_delta.createDataFrame([
                {"id": i, "value": i * 100}
            ])
            mode = "overwrite" if i == 0 else "append"
            df.write.format("delta").mode(mode).save(table_path)
        
        # Optimiser
        delta_table = DeltaTable.forPath(spark_delta, table_path)
        delta_table.optimize().executeCompaction()
        
        # Vérifier que les données sont intactes
        result = spark_delta.read.format("delta").load(table_path)
        assert result.count() == 10
    
    def test_optimize_with_zorder(
        self,
        spark_delta: SparkSession,
        delta_path: str,
    ):
        """Test OPTIMIZE avec Z-ORDER."""
        table_path = f"{delta_path}/zorder_test"
        
        # Créer des données
        data = DataGenerator.generate_transactions(100)
        df = spark_delta.createDataFrame(data)
        df.write.format("delta").mode("overwrite").save(table_path)
        
        # Optimiser avec Z-ORDER sur customer_id
        delta_table = DeltaTable.forPath(spark_delta, table_path)
        delta_table.optimize().executeZOrderBy("customer_id")
        
        # Vérifier que les données sont intactes
        result = spark_delta.read.format("delta").load(table_path)
        assert result.count() == 100
    
    def test_vacuum(
        self,
        spark_delta: SparkSession,
        delta_path: str,
    ):
        """Test VACUUM (nettoyage fichiers obsolètes)."""
        table_path = f"{delta_path}/vacuum_test"
        
        # Créer plusieurs versions
        for i in range(5):
            df = spark_delta.createDataFrame([
                {"id": 1, "version": i}
            ])
            df.write.format("delta").mode("overwrite").save(table_path)
        
        # Vacuum avec rétention 0 heures (pour les tests)
        # En production, utiliser au moins 168 heures (7 jours)
        delta_table = DeltaTable.forPath(spark_delta, table_path)
        
        # Désactiver la protection pour les tests
        spark_delta.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        
        delta_table.vacuum(0)
        
        # Réactiver la protection
        spark_delta.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")
        
        # Les données actuelles sont toujours accessibles
        result = spark_delta.read.format("delta").load(table_path)
        assert result.count() == 1


# =============================================================================
# TESTS - SCHEMA ENFORCEMENT
# =============================================================================

class TestDeltaSchemaEnforcement:
    """Tests pour le contrôle de schéma Delta."""
    
    def test_schema_enforcement_reject_extra_column(
        self,
        spark_delta: SparkSession,
        delta_path: str,
    ):
        """Test rejet d'écriture avec colonne supplémentaire."""
        table_path = f"{delta_path}/schema_enforcement"
        
        # Créer la table avec un schéma défini
        df1 = spark_delta.createDataFrame([
            {"id": 1, "name": "Alice"}
        ])
        df1.write.format("delta").mode("overwrite").save(table_path)
        
        # Tenter d'écrire avec une colonne en plus
        df2 = spark_delta.createDataFrame([
            {"id": 2, "name": "Bob", "extra_column": "value"}
        ])
        
        # Sans mergeSchema, cela devrait échouer
        with pytest.raises(Exception):
            df2.write.format("delta").mode("append").save(table_path)
    
    def test_schema_evolution_add_column(
        self,
        spark_delta: SparkSession,
        delta_path: str,
    ):
        """Test évolution de schéma avec ajout de colonne."""
        table_path = f"{delta_path}/schema_evolution_add"
        
        # Schéma initial
        df1 = spark_delta.createDataFrame([
            {"id": 1, "name": "Alice"}
        ])
        df1.write.format("delta").mode("overwrite").save(table_path)
        
        # Ajouter une colonne avec mergeSchema
        df2 = spark_delta.createDataFrame([
            {"id": 2, "name": "Bob", "email": "bob@test.com"}
        ])
        (
            df2.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(table_path)
        )
        
        # Vérifier le nouveau schéma
        result = spark_delta.read.format("delta").load(table_path)
        assert "email" in result.columns
        assert result.count() == 2
        
        # L'ancienne ligne a email=null
        alice = result.filter(F.col("id") == 1).first()
        assert alice["email"] is None


# =============================================================================
# TESTS - CHANGE DATA FEED (CDF)
# =============================================================================

class TestDeltaChangeDataFeed:
    """Tests pour le Change Data Feed Delta."""
    
    def test_enable_cdf(
        self,
        spark_delta: SparkSession,
        delta_path: str,
    ):
        """Test activation du Change Data Feed."""
        table_path = f"{delta_path}/cdf_enabled"
        
        # Créer une table avec CDF activé
        df = spark_delta.createDataFrame([
            {"id": 1, "value": 100}
        ])
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("delta.enableChangeDataFeed", "true")
            .save(table_path)
        )
        
        # Vérifier que la propriété est activée
        delta_table = DeltaTable.forPath(spark_delta, table_path)
        detail = delta_table.detail().collect()[0]
        
        # La propriété devrait être dans les propriétés de la table
        properties = detail["properties"]
        assert properties.get("delta.enableChangeDataFeed") == "true"
    
    def test_read_cdf_changes(
        self,
        spark_delta: SparkSession,
        delta_path: str,
    ):
        """Test lecture des changements via CDF."""
        table_path = f"{delta_path}/cdf_changes"
        
        # Créer une table avec CDF
        df = spark_delta.createDataFrame([
            {"id": 1, "name": "Alice", "score": 100},
            {"id": 2, "name": "Bob", "score": 200},
        ])
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("delta.enableChangeDataFeed", "true")
            .save(table_path)
        )
        
        # Faire des modifications
        delta_table = DeltaTable.forPath(spark_delta, table_path)
        
        # Update
        delta_table.update(
            condition="id = 1",
            set={"score": F.lit(150)}
        )
        
        # Delete
        delta_table.delete("id = 2")
        
        # Insert
        new_df = spark_delta.createDataFrame([
            {"id": 3, "name": "Charlie", "score": 300}
        ])
        new_df.write.format("delta").mode("append").save(table_path)
        
        # Lire les changements depuis la version 1
        changes = (
            spark_delta.read
            .format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", 1)
            .load(table_path)
        )
        
        # Vérifier les types de changements
        change_types = [row["_change_type"] for row in changes.collect()]
        
        # Devrait contenir update_preimage, update_postimage, delete, insert
        assert "update_preimage" in change_types or "update_postimage" in change_types
        assert "delete" in change_types
        assert "insert" in change_types


# =============================================================================
# TESTS - TRANSACTIONS ACID
# =============================================================================

class TestDeltaACID:
    """Tests pour les propriétés ACID de Delta."""
    
    def test_concurrent_writes_isolation(
        self,
        spark_delta: SparkSession,
        delta_path: str,
    ):
        """Test isolation des écritures concurrentes."""
        table_path = f"{delta_path}/acid_isolation"
        
        # Créer la table
        df = spark_delta.createDataFrame([
            {"id": 1, "value": 100}
        ])
        df.write.format("delta").mode("overwrite").save(table_path)
        
        # Simuler des écritures concurrentes
        # (dans un vrai test, on utiliserait des threads)
        for i in range(5):
            update_df = spark_delta.createDataFrame([
                {"id": i + 2, "value": (i + 2) * 100}
            ])
            update_df.write.format("delta").mode("append").save(table_path)
        
        # Vérifier l'intégrité
        result = spark_delta.read.format("delta").load(table_path)
        assert result.count() == 6
        
        # Chaque ID est unique
        ids = [row["id"] for row in result.collect()]
        assert len(ids) == len(set(ids))
    
    def test_atomic_write_failure(
        self,
        spark_delta: SparkSession,
        delta_path: str,
    ):
        """Test atomicité en cas d'échec."""
        table_path = f"{delta_path}/acid_atomic"
        
        # Créer la table initiale
        df = spark_delta.createDataFrame([
            {"id": 1, "value": 100}
        ])
        df.write.format("delta").mode("overwrite").save(table_path)
        
        initial_count = spark_delta.read.format("delta").load(table_path).count()
        
        # Tenter une écriture qui échoue (schéma incompatible sans mergeSchema)
        try:
            bad_df = spark_delta.createDataFrame([
                {"id": "not_an_int", "value": 200, "extra": "col"}
            ])
            bad_df.write.format("delta").mode("append").save(table_path)
        except Exception:
            pass
        
        # La table devrait être inchangée
        final_count = spark_delta.read.format("delta").load(table_path).count()
        assert final_count == initial_count


# =============================================================================
# TESTS - FONCTIONNALITÉS AVANCÉES
# =============================================================================

class TestDeltaAdvanced:
    """Tests pour fonctionnalités avancées Delta."""
    
    def test_generated_columns(
        self,
        spark_delta: SparkSession,
        delta_path: str,
    ):
        """Test colonnes générées."""
        table_path = f"{delta_path}/generated_columns"
        
        # Créer une table avec colonne générée via SQL
        spark_delta.sql(f"""
            CREATE TABLE delta.`{table_path}` (
                id INT,
                first_name STRING,
                last_name STRING,
                full_name STRING GENERATED ALWAYS AS (concat(first_name, ' ', last_name))
            ) USING DELTA
        """)
        
        # Insérer des données
        spark_delta.sql(f"""
            INSERT INTO delta.`{table_path}` (id, first_name, last_name)
            VALUES (1, 'John', 'Doe'), (2, 'Jane', 'Smith')
        """)
        
        # Vérifier
        result = spark_delta.read.format("delta").load(table_path)
        
        john = result.filter(F.col("id") == 1).first()
        assert john["full_name"] == "John Doe"
    
    def test_table_constraints(
        self,
        spark_delta: SparkSession,
        delta_path: str,
    ):
        """Test contraintes de table."""
        table_path = f"{delta_path}/constraints"
        
        # Créer une table
        df = spark_delta.createDataFrame([
            {"id": 1, "price": 100.0}
        ])
        df.write.format("delta").mode("overwrite").save(table_path)
        
        # Ajouter une contrainte CHECK
        delta_table = DeltaTable.forPath(spark_delta, table_path)
        
        # Via SQL
        spark_delta.sql(f"""
            ALTER TABLE delta.`{table_path}`
            ADD CONSTRAINT price_positive CHECK (price > 0)
        """)
        
        # Tenter d'insérer une valeur invalide
        with pytest.raises(Exception):
            bad_df = spark_delta.createDataFrame([
                {"id": 2, "price": -50.0}
            ])
            bad_df.write.format("delta").mode("append").save(table_path)
        
        # L'insertion valide fonctionne
        good_df = spark_delta.createDataFrame([
            {"id": 3, "price": 200.0}
        ])
        good_df.write.format("delta").mode("append").save(table_path)
        
        result = spark_delta.read.format("delta").load(table_path)
        assert result.count() == 2
    
    def test_clone_table(
        self,
        spark_delta: SparkSession,
        delta_path: str,
    ):
        """Test clonage de table (shallow et deep)."""
        source_path = f"{delta_path}/clone_source"
        shallow_path = f"{delta_path}/clone_shallow"
        deep_path = f"{delta_path}/clone_deep"
        
        # Créer la table source
        df = spark_delta.createDataFrame([
            {"id": 1, "value": 100},
            {"id": 2, "value": 200},
        ])
        df.write.format("delta").mode("overwrite").save(source_path)
        
        # Clone shallow (partage les fichiers)
        spark_delta.sql(f"""
            CREATE TABLE delta.`{shallow_path}`
            SHALLOW CLONE delta.`{source_path}`
        """)
        
        # Clone deep (copie les fichiers)
        spark_delta.sql(f"""
            CREATE TABLE delta.`{deep_path}`
            DEEP CLONE delta.`{source_path}`
        """)
        
        # Vérifier les clones
        shallow_result = spark_delta.read.format("delta").load(shallow_path)
        deep_result = spark_delta.read.format("delta").load(deep_path)
        
        assert shallow_result.count() == 2
        assert deep_result.count() == 2


# =============================================================================
# TESTS DE PERFORMANCE
# =============================================================================

class TestDeltaPerformance:
    """Tests de performance Delta."""
    
    @pytest.mark.slow
    def test_large_dataset_write_read(
        self,
        spark_delta: SparkSession,
        delta_path: str,
    ):
        """Test performance avec dataset volumineux."""
        table_path = f"{delta_path}/large_dataset"
        
        # Générer un grand dataset
        data = DataGenerator.generate_transactions(10000)
        df = spark_delta.createDataFrame(data)
        
        # Mesurer l'écriture
        import time
        start = time.time()
        df.write.format("delta").mode("overwrite").save(table_path)
        write_time = time.time() - start
        
        # Mesurer la lecture
        start = time.time()
        result = spark_delta.read.format("delta").load(table_path)
        count = result.count()
        read_time = time.time() - start
        
        assert count == 10000
        
        # Log des performances (optionnel)
        print(f"Write time: {write_time:.2f}s, Read time: {read_time:.2f}s")
    
    @pytest.mark.slow
    def test_partitioned_query_performance(
        self,
        spark_delta: SparkSession,
        delta_path: str,
    ):
        """Test performance avec partitionnement."""
        table_path = f"{delta_path}/partitioned_perf"
        
        # Générer des données
        data = DataGenerator.generate_transactions(5000)
        df = spark_delta.createDataFrame(data)
        
        # Écrire avec partitionnement
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("channel")
            .save(table_path)
        )
        
        # Requête avec filtre sur partition
        import time
        start = time.time()
        result = (
            spark_delta.read
            .format("delta")
            .load(table_path)
            .filter(F.col("channel") == "WEB")
        )
        count = result.count()
        query_time = time.time() - start
        
        # La requête devrait être rapide grâce au partition pruning
        assert count > 0
        print(f"Partitioned query time: {query_time:.2f}s for {count} records")
