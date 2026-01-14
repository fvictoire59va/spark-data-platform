"""Interface de base pour les lecteurs de données."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, TypeVar

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

T = TypeVar("T", bound="BaseReader")


class BaseReader(ABC):
    """Classe abstraite pour la lecture de données."""

    def __init__(self, spark: SparkSession, config: dict[str, Any]):
        """
        Initialise le reader.

        Args:
            spark: Session Spark
            config: Configuration du reader
        """
        self.spark = spark
        self.config = config
        self._schema: StructType | None = None

    def with_schema(self: T, schema: StructType) -> T:
        """
        Définit le schéma à utiliser.

        Args:
            schema: Schéma Spark

        Returns:
            Self pour le chaînage
        """
        self._schema = schema
        return self

    @abstractmethod
    def read(self) -> DataFrame:
        """
        Lit les données et retourne un DataFrame.

        Returns:
            DataFrame Spark
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
