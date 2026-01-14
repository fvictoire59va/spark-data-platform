"""Interface de base pour les writers de données."""
from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum
from typing import TYPE_CHECKING, Any, TypeVar

from pyspark.sql import DataFrame

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

T = TypeVar("T", bound="BaseWriter")


class WriteMode(str, Enum):
    """Modes d'écriture supportés."""

    APPEND = "append"
    OVERWRITE = "overwrite"
    ERROR_IF_EXISTS = "errorifexists"
    IGNORE = "ignore"
    MERGE = "merge"


class BaseWriter(ABC):
    """Classe abstraite pour l'écriture de données."""

    def __init__(self, spark: SparkSession, config: dict[str, Any]):
        """
        Initialise le writer.

        Args:
            spark: Session Spark
            config: Configuration du writer
        """
        self.spark = spark
        self.config = config
        self._mode: WriteMode = WriteMode.APPEND
        self._partition_by: list[str] = []

    def with_mode(self: T, mode: WriteMode | str) -> T:
        """
        Définit le mode d'écriture.

        Args:
            mode: Mode d'écriture

        Returns:
            Self pour le chaînage
        """
        if isinstance(mode, str):
            mode = WriteMode(mode.lower())
        self._mode = mode
        return self

    def with_partitions(self: T, columns: list[str]) -> T:
        """
        Définit les colonnes de partitionnement.

        Args:
            columns: Colonnes de partition

        Returns:
            Self pour le chaînage
        """
        self._partition_by = columns
        return self

    @abstractmethod
    def write(self, df: DataFrame) -> None:
        """
        Écrit les données.

        Args:
            df: DataFrame à écrire
        """
        pass

    def _validate_config(self, required_keys: list[str]) -> None:
        """
        Valide la présence des clés requises dans la config.

        Args:
            required_keys: Liste des clés obligatoires

        Raises:
            ValueError: Si une clé est manquante
        """
        missing = [key for key in required_keys if key not in self.config]
        if missing:
            raise ValueError(f"Configuration manquante: {missing}")
