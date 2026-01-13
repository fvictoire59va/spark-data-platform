"""Classe de base pour tous les jobs Spark."""
from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from pyspark.sql import DataFrame

from src.core.config_manager import Environment, PipelineConfig, get_settings
from src.core.exceptions import SparkPlatformError
from src.core.logger import SparkJobLogger
from src.core.spark_session import SparkSessionFactory

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class BaseSparkJob(ABC):
    """Classe de base pour les jobs Spark."""

    def __init__(
        self,
        job_name: str,
        domain: str,
        environment: Environment | None = None,
    ):
        self.job_name = job_name
        self.domain = domain
        self.settings = get_settings()
        self.environment = environment or self.settings.environment
        
        # Configuration du pipeline
        self.config = PipelineConfig(job_name, domain, self.environment)
        
        # Logger
        self.logger = SparkJobLogger(job_name, domain)
        
        # Session Spark
        self._spark: SparkSession | None = None
        
        # Métriques
        self._start_time: float = 0
        self._records_processed: int = 0

    @property
    def spark(self) -> SparkSession:
        """Retourne la session Spark (lazy initialization)."""
        if self._spark is None:
            self._spark = SparkSessionFactory.create(
                f"{self.domain}_{self.job_name}",
                self.settings,
            )
        return self._spark

    @abstractmethod
    def extract(self) -> DataFrame:
        """
        Extrait les données sources.

        Returns:
            DataFrame avec les données extraites
        """
        pass

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Applique les transformations.

        Args:
            df: DataFrame source

        Returns:
            DataFrame transformé
        """
        pass

    @abstractmethod
    def load(self, df: DataFrame) -> None:
        """
        Charge les données dans la destination.

        Args:
            df: DataFrame à charger
        """
        pass

    def validate(self, df: DataFrame) -> DataFrame:
        """
        Valide les données (peut être surchargé).

        Args:
            df: DataFrame à valider

        Returns:
            DataFrame validé
        """
        return df

    def run(self) -> dict[str, Any]:
        """
        Exécute le job ETL complet.

        Returns:
            Dictionnaire avec les métriques du job
        """
        self._start_time = time.time()
        
        try:
            self.logger.job_started(
                environment=self.environment.value,
                config=self.config._config,
            )
            
            # Extract
            self.logger.step_completed("extract_start")
            df = self.extract()
            self.logger.step_completed("extract", count=df.count())
            
            # Transform
            df = self.transform(df)
            self.logger.step_completed("transform", count=df.count())
            
            # Validate
            df = self.validate(df)
            self._records_processed = df.count()
            self.logger.step_completed("validate", count=self._records_processed)
            
            # Load
            self.load(df)
            self.logger.step_completed("load")
            
            duration = time.time() - self._start_time
            
            self.logger.job_completed(
                records_processed=self._records_processed,
                duration_seconds=duration,
            )
            
            return {
                "status": "SUCCESS",
                "records_processed": self._records_processed,
                "duration_seconds": round(duration, 2),
            }
            
        except Exception as e:
            self.logger.job_failed(e)
            raise SparkPlatformError(
                f"Job {self.job_name} échoué: {e}",
                details={"job_name": self.job_name, "domain": self.domain},
            ) from e

    def cleanup(self) -> None:
        """Nettoie les ressources."""
        if self._spark is not None:
            SparkSessionFactory.stop()
