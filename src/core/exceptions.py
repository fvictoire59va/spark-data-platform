"""Exceptions personnalisées pour la plateforme."""
from __future__ import annotations

from typing import Any


class SparkPlatformError(Exception):
    """Exception de base pour la plateforme."""

    def __init__(self, message: str, details: dict[str, Any] | None = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)


class ConfigurationError(SparkPlatformError):
    """Erreur de configuration."""

    pass


class DataQualityError(SparkPlatformError):
    """Erreur de qualité des données."""

    def __init__(
        self,
        message: str,
        failed_checks: list[str] | None = None,
        details: dict[str, Any] | None = None,
    ):
        self.failed_checks = failed_checks or []
        super().__init__(message, details)


class SchemaValidationError(SparkPlatformError):
    """Erreur de validation de schéma."""

    def __init__(
        self,
        message: str,
        expected_schema: str | None = None,
        actual_schema: str | None = None,
    ):
        details = {
            "expected_schema": expected_schema,
            "actual_schema": actual_schema,
        }
        super().__init__(message, details)


class DataSourceError(SparkPlatformError):
    """Erreur lors de l'accès à une source de données."""

    pass


class TransformationError(SparkPlatformError):
    """Erreur lors d'une transformation."""

    pass


class WriteError(SparkPlatformError):
    """Erreur lors de l'écriture des données."""

    pass
