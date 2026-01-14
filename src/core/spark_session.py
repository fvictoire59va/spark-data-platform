"""Factory pour créer des sessions Spark configurées."""
from __future__ import annotations

from functools import lru_cache
from typing import TYPE_CHECKING

from pyspark.sql import SparkSession

from src.core.config_manager import Settings, get_settings
from src.core.logger import get_logger

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)


class SparkSessionFactory:
    """Factory pour créer et gérer les sessions Spark."""

    _instance: SparkSession | None = None

    @classmethod
    def create(
        cls,
        app_name: str,
        settings: Settings | None = None,
        additional_configs: dict[str, str] | None = None,
    ) -> SparkSession:
        """
        Crée une session Spark avec la configuration appropriée.

        Args:
            app_name: Nom de l'application Spark
            settings: Configuration de l'environnement
            additional_configs: Configurations supplémentaires

        Returns:
            SparkSession configurée
        """
        if cls._instance is not None:
            logger.warning("Session Spark existante retournée")
            return cls._instance

        settings = settings or get_settings()

        builder = (
            SparkSession.builder.appName(app_name)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
            )
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.shuffle.partitions", str(settings.spark_shuffle_partitions))
            .config("spark.default.parallelism", str(settings.spark_default_parallelism))
        )

        # Configuration environnement
        if settings.environment == "local":
            builder = builder.master("local[*]")

        # Delta Lake configurations
        if settings.delta_enabled:
            builder = builder.config(
                "spark.databricks.delta.retentionDurationCheck.enabled", "false"
            ).config("spark.databricks.delta.schema.autoMerge.enabled", "true")

        # Configurations additionnelles
        if additional_configs:
            for key, value in additional_configs.items():
                builder = builder.config(key, value)

        cls._instance = builder.getOrCreate()

        # Configuration du niveau de log
        cls._instance.sparkContext.setLogLevel(settings.spark_log_level)

        logger.info(
            "Session Spark créée",
            app_name=app_name,
            environment=settings.environment,
            spark_version=cls._instance.version,
        )

        return cls._instance

    @classmethod
    def get_or_create(cls, app_name: str = "SparkApp") -> SparkSession:
        """Récupère la session existante ou en crée une nouvelle."""
        if cls._instance is None:
            return cls.create(app_name)
        return cls._instance

    @classmethod
    def stop(cls) -> None:
        """Arrête la session Spark."""
        if cls._instance is not None:
            cls._instance.stop()
            cls._instance = None
            logger.info("Session Spark arrêtée")


@lru_cache
def get_spark_session(app_name: str = "SparkApp") -> SparkSession:
    """Helper pour obtenir une session Spark (cached)."""
    return SparkSessionFactory.get_or_create(app_name)
