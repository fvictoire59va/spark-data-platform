"""Validation de schéma pour les DataFrames."""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql.types import StructType

from src.core.exceptions import SchemaValidationError
from src.core.logger import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = get_logger(__name__)


@dataclass
class SchemaValidationResult:
    """Résultat de validation de schéma."""

    is_valid: bool
    missing_columns: list[str]
    extra_columns: list[str]
    type_mismatches: dict[str, tuple[str, str]]  # {col: (expected, actual)}
    nullable_mismatches: dict[str, tuple[bool, bool]]  # {col: (expected, actual)}


class SchemaValidator:
    """Validateur de schéma DataFrame."""

    def __init__(self, expected_schema: StructType):
        """
        Args:
            expected_schema: Schéma attendu
        """
        self.expected_schema = expected_schema
        self._expected_fields = {f.name: f for f in expected_schema.fields}

    def validate(
        self,
        df: DataFrame,
        check_types: bool = True,
        check_nullable: bool = False,
        strict: bool = False,
    ) -> SchemaValidationResult:
        """
        Valide le schéma d'un DataFrame.

        Args:
            df: DataFrame à valider
            check_types: Vérifier les types de données
            check_nullable: Vérifier les contraintes nullable
            strict: Pas de colonnes extra autorisées

        Returns:
            SchemaValidationResult
        """
        actual_fields = {f.name: f for f in df.schema.fields}

        # Colonnes manquantes
        missing = [name for name in self._expected_fields if name not in actual_fields]

        # Colonnes extra
        extra = [name for name in actual_fields if name not in self._expected_fields]

        # Vérification des types
        type_mismatches = {}
        if check_types:
            for name, expected_field in self._expected_fields.items():
                if name in actual_fields:
                    actual_type = str(actual_fields[name].dataType)
                    expected_type = str(expected_field.dataType)
                    if actual_type != expected_type:
                        type_mismatches[name] = (expected_type, actual_type)

        # Vérification nullable
        nullable_mismatches = {}
        if check_nullable:
            for name, expected_field in self._expected_fields.items():
                if name in actual_fields:
                    actual_nullable = actual_fields[name].nullable
                    expected_nullable = expected_field.nullable
                    if actual_nullable != expected_nullable:
                        nullable_mismatches[name] = (expected_nullable, actual_nullable)

        # Déterminer la validité
        is_valid = (
            len(missing) == 0
            and len(type_mismatches) == 0
            and (not strict or len(extra) == 0)
            and (not check_nullable or len(nullable_mismatches) == 0)
        )

        result = SchemaValidationResult(
            is_valid=is_valid,
            missing_columns=missing,
            extra_columns=extra,
            type_mismatches=type_mismatches,
            nullable_mismatches=nullable_mismatches,
        )

        logger.info(
            "Validation de schéma terminée",
            is_valid=is_valid,
            missing_count=len(missing),
            extra_count=len(extra),
            type_mismatches_count=len(type_mismatches),
        )

        return result

    def validate_or_raise(
        self,
        df: DataFrame,
        check_types: bool = True,
        check_nullable: bool = False,
        strict: bool = False,
    ) -> None:
        """
        Valide et lève une exception si invalide.

        Raises:
            SchemaValidationError: Si le schéma est invalide
        """
        result = self.validate(df, check_types, check_nullable, strict)

        if not result.is_valid:
            error_details = []

            if result.missing_columns:
                error_details.append(f"Colonnes manquantes: {result.missing_columns}")
            if result.type_mismatches:
                error_details.append(f"Types incorrects: {result.type_mismatches}")
            if result.extra_columns and strict:
                error_details.append(f"Colonnes non autorisées: {result.extra_columns}")
            if result.nullable_mismatches:
                error_details.append(f"Nullable incorrect: {result.nullable_mismatches}")

            raise SchemaValidationError(
                f"Validation de schéma échouée: {'; '.join(error_details)}",
                expected_schema=str(self.expected_schema),
                actual_schema=str(df.schema),
            )
