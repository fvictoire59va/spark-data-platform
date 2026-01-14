"""Transformers de nettoyage des données."""
from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.common.transformers.base_transformer import BaseTransformer


class TrimStringsTransformer(BaseTransformer):
    """Supprime les espaces en début/fin des colonnes string."""

    def __init__(self, columns: list[str] | None = None):
        """
        Args:
            columns: Colonnes à traiter (toutes si None)
        """
        self.columns = columns

    def transform(self, df: DataFrame) -> DataFrame:
        """Applique trim sur les colonnes string."""
        string_cols = [f.name for f in df.schema.fields if str(f.dataType) == "StringType"]

        if self.columns:
            string_cols = [c for c in string_cols if c in self.columns]

        for col in string_cols:
            df = df.withColumn(col, F.trim(F.col(col)))

        return df


class DropNullsTransformer(BaseTransformer):
    """Supprime les lignes avec des valeurs nulles."""

    def __init__(
        self,
        columns: list[str] | None = None,
        how: str = "any",
        thresh: int | None = None,
    ):
        """
        Args:
            columns: Colonnes à vérifier
            how: 'any' ou 'all'
            thresh: Nombre minimum de non-nulls requis
        """
        self.columns = columns
        self.how = how
        self.thresh = thresh

    def transform(self, df: DataFrame) -> DataFrame:
        """Supprime les lignes avec nulls."""
        return df.dropna(
            how=self.how,
            thresh=self.thresh,
            subset=self.columns,
        )


class FillNullsTransformer(BaseTransformer):
    """Remplace les valeurs nulles."""

    def __init__(self, fills: dict[str, Any]):
        """
        Args:
            fills: Dictionnaire {colonne: valeur_remplacement}
        """
        self.fills = fills

    def transform(self, df: DataFrame) -> DataFrame:
        """Remplace les nulls par les valeurs spécifiées."""
        return df.fillna(self.fills)


class DropDuplicatesTransformer(BaseTransformer):
    """Supprime les doublons."""

    def __init__(self, columns: list[str] | None = None):
        """
        Args:
            columns: Colonnes à considérer pour l'unicité
        """
        self.columns = columns

    def transform(self, df: DataFrame) -> DataFrame:
        """Supprime les doublons."""
        if self.columns:
            return df.dropDuplicates(self.columns)
        return df.dropDuplicates()


class LowerCaseTransformer(BaseTransformer):
    """Convertit les colonnes en minuscules."""

    def __init__(self, columns: list[str]):
        """
        Args:
            columns: Colonnes à convertir
        """
        self.columns = columns

    def transform(self, df: DataFrame) -> DataFrame:
        """Convertit en minuscules."""
        for col in self.columns:
            if col in df.columns:
                df = df.withColumn(col, F.lower(F.col(col)))
        return df


class UpperCaseTransformer(BaseTransformer):
    """Convertit les colonnes en majuscules."""

    def __init__(self, columns: list[str]):
        """
        Args:
            columns: Colonnes à convertir
        """
        self.columns = columns

    def transform(self, df: DataFrame) -> DataFrame:
        """Convertit en majuscules."""
        for col in self.columns:
            if col in df.columns:
                df = df.withColumn(col, F.upper(F.col(col)))
        return df
