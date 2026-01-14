"""Classes de base pour les transformations."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame

from src.core.logger import get_logger

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)


class BaseTransformer(ABC):
    """Classe abstraite pour les transformations de données."""

    @property
    def name(self) -> str:
        """Retourne le nom du transformer."""
        return self.__class__.__name__

    @abstractmethod
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Applique la transformation.

        Args:
            df: DataFrame source

        Returns:
            DataFrame transformé
        """
        pass

    def __call__(self, df: DataFrame) -> DataFrame:
        """Permet d'utiliser le transformer comme fonction."""
        return self.transform(df)


class TransformationPipeline:
    """Pipeline de transformations chaînées."""

    def __init__(self):
        self._transformers: list[BaseTransformer] = []

    def add(self, transformer: BaseTransformer) -> TransformationPipeline:
        """
        Ajoute un transformer au pipeline.

        Args:
            transformer: Transformer à ajouter

        Returns:
            Self pour le chaînage
        """
        self._transformers.append(transformer)
        return self

    def execute(self, df: DataFrame) -> DataFrame:
        """
        Exécute tous les transformers du pipeline.

        Args:
            df: DataFrame source

        Returns:
            DataFrame transformé
        """
        result = df

        for transformer in self._transformers:
            logger.debug(f"Application du transformer: {transformer.name}")
            result = transformer.transform(result)
            logger.debug(f"Transformer {transformer.name} appliqué")

        logger.info(f"Pipeline exécuté avec {len(self._transformers)} transformations")

        return result

    def __len__(self) -> int:
        """Retourne le nombre de transformers."""
        return len(self._transformers)
