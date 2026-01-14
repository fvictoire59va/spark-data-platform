"""Module de contrôle qualité des données."""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

from pyspark.sql import functions as F

from src.core.exceptions import DataQualityError
from src.core.logger import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = get_logger(__name__)


class CheckSeverity(str, Enum):
    """Niveau de sévérité des checks."""

    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class QualityCheck:
    """Définition d'un contrôle qualité."""

    name: str
    check_fn: Callable[[DataFrame], bool]
    severity: CheckSeverity = CheckSeverity.ERROR
    description: str = ""


@dataclass
class QualityResult:
    """Résultat d'un contrôle qualité."""

    check_name: str
    passed: bool
    severity: CheckSeverity
    details: dict[str, Any] = field(default_factory=dict)


class DataQualityChecker:
    """Gestionnaire des contrôles qualité."""

    def __init__(self, df: DataFrame):
        self.df = df
        self._checks: list[QualityCheck] = []
        self._results: list[QualityResult] = []

    def check_not_null(
        self,
        columns: list[str],
        severity: CheckSeverity = CheckSeverity.ERROR,
    ) -> DataQualityChecker:
        """Vérifie que les colonnes ne sont pas nulles."""
        for col in columns:
            self._checks.append(
                QualityCheck(
                    name=f"not_null_{col}",
                    check_fn=lambda df, c=col: df.filter(F.col(c).isNull()).count() == 0,
                    severity=severity,
                    description=f"Colonne {col} ne doit pas contenir de NULL",
                )
            )
        return self

    def check_unique(
        self,
        columns: list[str],
        severity: CheckSeverity = CheckSeverity.ERROR,
    ) -> DataQualityChecker:
        """Vérifie l'unicité sur les colonnes."""
        col_str = "_".join(columns)

        def check_fn(df: DataFrame) -> bool:
            total = df.count()
            distinct = df.select(*columns).distinct().count()
            return total == distinct

        self._checks.append(
            QualityCheck(
                name=f"unique_{col_str}",
                check_fn=check_fn,
                severity=severity,
                description=f"Unicité sur {columns}",
            )
        )
        return self

    def check_values_in_set(
        self,
        column: str,
        allowed_values: set[Any],
        severity: CheckSeverity = CheckSeverity.ERROR,
    ) -> DataQualityChecker:
        """Vérifie que les valeurs sont dans un ensemble défini."""

        def check_fn(df: DataFrame) -> bool:
            invalid = df.filter(~F.col(column).isin(list(allowed_values))).count()
            return invalid == 0

        self._checks.append(
            QualityCheck(
                name=f"values_in_set_{column}",
                check_fn=check_fn,
                severity=severity,
                description=f"{column} doit être dans {allowed_values}",
            )
        )
        return self

    def check_range(
        self,
        column: str,
        min_value: Any = None,
        max_value: Any = None,
        severity: CheckSeverity = CheckSeverity.ERROR,
    ) -> DataQualityChecker:
        """Vérifie que les valeurs sont dans une plage."""

        def check_fn(df: DataFrame) -> bool:
            conditions = []
            if min_value is not None:
                conditions.append(F.col(column) < min_value)
            if max_value is not None:
                conditions.append(F.col(column) > max_value)

            if conditions:
                combined = conditions[0]
                for cond in conditions[1:]:
                    combined = combined | cond
                return df.filter(combined).count() == 0
            return True

        self._checks.append(
            QualityCheck(
                name=f"range_{column}",
                check_fn=check_fn,
                severity=severity,
                description=f"{column} doit être entre {min_value} et {max_value}",
            )
        )
        return self

    def check_row_count(
        self,
        min_count: int = 0,
        max_count: int | None = None,
        severity: CheckSeverity = CheckSeverity.ERROR,
    ) -> DataQualityChecker:
        """Vérifie le nombre de lignes."""

        def check_fn(df: DataFrame) -> bool:
            count = df.count()
            if count < min_count:
                return False
            if max_count is not None and count > max_count:
                return False
            return True

        self._checks.append(
            QualityCheck(
                name="row_count",
                check_fn=check_fn,
                severity=severity,
                description=f"Nombre de lignes entre {min_count} et {max_count}",
            )
        )
        return self

    def add_custom_check(
        self,
        name: str,
        check_fn: Callable[[DataFrame], bool],
        severity: CheckSeverity = CheckSeverity.ERROR,
        description: str = "",
    ) -> DataQualityChecker:
        """Ajoute un check personnalisé."""
        self._checks.append(
            QualityCheck(
                name=name,
                check_fn=check_fn,
                severity=severity,
                description=description,
            )
        )
        return self

    def run(self, fail_on_error: bool = True) -> list[QualityResult]:
        """
        Exécute tous les contrôles.

        Args:
            fail_on_error: Lever une exception si un check ERROR/CRITICAL échoue

        Returns:
            Liste des résultats

        Raises:
            DataQualityError: Si fail_on_error et échec d'un check ERROR/CRITICAL
        """
        self._results = []
        failed_checks: list[str] = []

        for check in self._checks:
            try:
                passed = check.check_fn(self.df)
                result = QualityResult(
                    check_name=check.name,
                    passed=passed,
                    severity=check.severity,
                    details={"description": check.description},
                )
                self._results.append(result)

                if passed:
                    logger.info(f"Check réussi: {check.name}")
                else:
                    log_fn = (
                        logger.warning if check.severity == CheckSeverity.WARNING else logger.error
                    )
                    log_fn(
                        f"Check échoué: {check.name}",
                        severity=check.severity.value,
                        description=check.description,
                    )

                    if check.severity in (CheckSeverity.ERROR, CheckSeverity.CRITICAL):
                        failed_checks.append(check.name)

            except Exception as e:
                logger.error(f"Erreur lors du check {check.name}: {e}")
                self._results.append(
                    QualityResult(
                        check_name=check.name,
                        passed=False,
                        severity=check.severity,
                        details={"error": str(e)},
                    )
                )
                failed_checks.append(check.name)

        if fail_on_error and failed_checks:
            raise DataQualityError(
                f"Contrôles qualité échoués: {failed_checks}",
                failed_checks=failed_checks,
            )

        return self._results

    @property
    def results(self) -> list[QualityResult]:
        """Retourne les résultats des checks."""
        return self._results
