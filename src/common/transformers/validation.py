"""Transformers de validation et enforcement de schéma."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from src.common.transformers.base_transformer import BaseTransformer
from src.core.exceptions import SchemaValidationError
from src.core.logger import get_logger

logger = get_logger(__name__)


class SchemaEnforcementTransformer(BaseTransformer):
    """Force un schéma spécifique sur le DataFrame."""

    def __init__(
        self,
        schema: StructType,
        strict: bool = True,
        fill_missing: bool = False,
    ):
        """
        Args:
            schema: Schéma cible
            strict: Lever une erreur si colonnes manquantes
            fill_missing: Ajouter les colonnes manquantes avec null
        """
        self.schema = schema
        self.strict = strict
        self.fill_missing = fill_missing

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Applique le schéma au DataFrame.

        Raises:
            SchemaValidationError: Si strict et colonnes manquantes
        """
        expected_cols = {f.name for f in self.schema.fields}
        actual_cols = set(df.columns)

        missing_cols = expected_cols - actual_cols
        extra_cols = actual_cols - expected_cols

        if missing_cols and self.strict and not self.fill_missing:
            raise SchemaValidationError(
                f"Colonnes manquantes: {missing_cols}",
                expected_schema=str(self.schema),
                actual_schema=str(df.schema),
            )

        # Ajouter les colonnes manquantes
        if missing_cols and self.fill_missing:
            for field in self.schema.fields:
                if field.name in missing_cols:
                    df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
                    logger.warning(f"Colonne ajoutée avec null: {field.name}")

        # Supprimer les colonnes extra
        if extra_cols:
            df = df.drop(*extra_cols)
            logger.warning(f"Colonnes supprimées: {extra_cols}")

        # Sélectionner dans l'ordre du schéma et caster
        select_exprs = []
        for field in self.schema.fields:
            if field.name in df.columns:
                select_exprs.append(F.col(field.name).cast(field.dataType).alias(field.name))

        return df.select(*select_exprs)


class TypeCastTransformer(BaseTransformer):
    """Cast des colonnes vers des types spécifiques."""

    def __init__(self, casts: dict[str, str]):
        """
        Args:
            casts: Dictionnaire {colonne: type_cible}
                   Types: 'string', 'integer', 'long', 'double', 'float',
                          'decimal(p,s)', 'date', 'timestamp', 'boolean'
        """
        self.casts = casts

    def transform(self, df: DataFrame) -> DataFrame:
        """Applique les casts de types."""
        for column, target_type in self.casts.items():
            if column in df.columns:
                df = df.withColumn(column, F.col(column).cast(target_type))
            else:
                logger.warning(f"Colonne '{column}' non trouvée pour cast")

        return df


class FilterValidRowsTransformer(BaseTransformer):
    """Filtre les lignes valides selon des règles."""

    def __init__(
        self,
        not_null_columns: list[str] | None = None,
        conditions: list[str] | None = None,
    ):
        """
        Args:
            not_null_columns: Colonnes qui ne doivent pas être nulles
            conditions: Conditions SQL à appliquer
        """
        self.not_null_columns = not_null_columns or []
        self.conditions = conditions or []

    def transform(self, df: DataFrame) -> DataFrame:
        """Filtre les lignes valides."""
        result = df

        # Filtrer les nulls
        for col in self.not_null_columns:
            if col in result.columns:
                result = result.filter(F.col(col).isNotNull())

        # Appliquer les conditions
        for condition in self.conditions:
            result = result.filter(condition)

        return result
