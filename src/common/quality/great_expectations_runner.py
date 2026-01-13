# src/common/quality/great_expectations_runner.py
"""Runner pour Great Expectations avec Spark."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any
from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from common.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ExpectationResult:
    """Résultat d'une expectation individuelle."""
    
    expectation_type: str
    success: bool
    column: str | None = None
    details: dict[str, Any] = field(default_factory=dict)
    observed_value: Any = None
    
    def to_dict(self) -> dict[str, Any]:
        """Convertit en dictionnaire."""
        return {
            "expectation_type": self.expectation_type,
            "success": self.success,
            "column": self.column,
            "details": self.details,
            "observed_value": self.observed_value,
        }


@dataclass
class ValidationResult:
    """Résultat complet d'une validation."""
    
    suite_name: str
    run_name: str
    expectation_results: list[ExpectationResult]
    run_time: datetime
    
    @property
    def success(self) -> bool:
        """Retourne True si toutes les expectations ont réussi."""
        return all(r.success for r in self.expectation_results)
    
    @property
    def success_count(self) -> int:
        """Nombre d'expectations réussies."""
        return sum(1 for r in self.expectation_results if r.success)
    
    @property
    def failure_count(self) -> int:
        """Nombre d'expectations échouées."""
        return sum(1 for r in self.expectation_results if not r.success)
    
    @property
    def success_rate(self) -> float:
        """Taux de réussite."""
        total = len(self.expectation_results)
        return self.success_count / total if total > 0 else 1.0
    
    def get_failures(self) -> list[ExpectationResult]:
        """Retourne les expectations échouées."""
        return [r for r in self.expectation_results if not r.success]
    
    def to_dict(self) -> dict[str, Any]:
        """Convertit en dictionnaire."""
        return {
            "suite_name": self.suite_name,
            "run_name": self.run_name,
            "run_time": self.run_time.isoformat(),
            "success": self.success,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "success_rate": self.success_rate,
            "results": [r.to_dict() for r in self.expectation_results],
        }
    
    def __repr__(self) -> str:
        return (
            f"ValidationResult("
            f"suite={self.suite_name}, "
            f"success={self.success}, "
            f"rate={self.success_rate:.1%})"
        )


class GreatExpectationsRunner:
    """
    Exécute les validations Great Expectations sur des DataFrames Spark.
    
    Cette classe fournit une interface simplifiée pour valider
    la qualité des données avec Great Expectations.
    
    Example:
        >>> runner = GreatExpectationsRunner(
        ...     expectations_path="configs/expectations",
        ...     results_path="output/validations",
        ... )
        >>> result = runner.validate(df, "orders_quality")
        >>> if not result.success:
        ...     print(f"Échecs: {result.failure_count}")
    """
    
    def __init__(
        self,
        expectations_path: str | Path | None = None,
        results_path: str | Path | None = None,
    ):
        """
        Initialise le runner.
        
        Args:
            expectations_path: Chemin vers les fichiers d'expectations
            results_path: Chemin pour sauvegarder les résultats
        """
        self.expectations_path = Path(expectations_path) if expectations_path else None
        self.results_path = Path(results_path) if results_path else None
        self._expectations_cache: dict[str, dict] = {}
        
        # Créer le dossier de résultats si nécessaire
        if self.results_path:
            self.results_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(
            "GreatExpectationsRunner initialisé",
            expectations_path=str(self.expectations_path),
            results_path=str(self.results_path),
        )
    
    def load_expectations(self, suite_name: str) -> dict[str, Any]:
        """
        Charge une suite d'expectations depuis un fichier JSON.
        
        Args:
            suite_name: Nom de la suite d'expectations
            
        Returns:
            Dictionnaire contenant les expectations
            
        Raises:
            ValueError: Si expectations_path non configuré
            FileNotFoundError: Si le fichier n'existe pas
        """
        if suite_name in self._expectations_cache:
            return self._expectations_cache[suite_name]
        
        if not self.expectations_path:
            raise ValueError("expectations_path non configuré")
        
        file_path = self.expectations_path / f"{suite_name}.json"
        
        if not file_path.exists():
            raise FileNotFoundError(f"Suite d'expectations non trouvée: {file_path}")
        
        with open(file_path, encoding="utf-8") as f:
            expectations = json.load(f)
        
        self._expectations_cache[suite_name] = expectations
        logger.info(f"Expectations chargées: {suite_name}")
        
        return expectations
    
    def validate(
        self,
        df: DataFrame,
        suite_name: str,
        run_name: str | None = None,
        save_results: bool = True,
    ) -> ValidationResult:
        """
        Valide un DataFrame avec une suite d'expectations.
        
        Args:
            df: DataFrame Spark à valider
            suite_name: Nom de la suite d'expectations
            run_name: Nom optionnel pour cette exécution
            save_results: Si True, sauvegarde les résultats
            
        Returns:
            ValidationResult avec tous les résultats
        """
        run_time = datetime.now()
        run_name = run_name or f"{suite_name}_{run_time.strftime('%Y%m%d_%H%M%S')}"
        
        logger.info(f"Démarrage validation: {suite_name}", run_name=run_name)
        
        # Charger les expectations
        suite = self.load_expectations(suite_name)
        expectations = suite.get("expectations", [])
        
        # Exécuter chaque expectation
        results: list[ExpectationResult] = []
        for exp in expectations:
            result = self._run_expectation(df, exp)
            results.append(result)
            
            if not result.success:
                logger.warning(
                    f"Expectation échouée: {result.expectation_type}",
                    column=result.column,
                    observed_value=result.observed_value,
                )
        
        # Créer le résultat final
        validation_result = ValidationResult(
            suite_name=suite_name,
            run_name=run_name,
            expectation_results=results,
            run_time=run_time,
        )
        
        # Sauvegarder si demandé
        if save_results and self.results_path:
            self._save_results(validation_result)
        
        logger.info(
            f"Validation terminée: {suite_name}",
            success=validation_result.success,
            success_rate=f"{validation_result.success_rate:.1%}",
        )
        
        return validation_result
    
    def validate_with_expectations(
        self,
        df: DataFrame,
        expectations: list[dict[str, Any]],
        suite_name: str = "inline",
        run_name: str | None = None,
    ) -> ValidationResult:
        """
        Valide un DataFrame avec des expectations passées directement.
        
        Args:
            df: DataFrame Spark à valider
            expectations: Liste d'expectations sous forme de dictionnaires
            suite_name: Nom de la suite (pour les logs)
            run_name: Nom optionnel pour cette exécution
            
        Returns:
            ValidationResult avec tous les résultats
        """
        run_time = datetime.now()
        run_name = run_name or f"{suite_name}_{run_time.strftime('%Y%m%d_%H%M%S')}"
        
        results: list[ExpectationResult] = []
        for exp in expectations:
            result = self._run_expectation(df, exp)
            results.append(result)
        
        return ValidationResult(
            suite_name=suite_name,
            run_name=run_name,
            expectation_results=results,
            run_time=run_time,
        )
    
    def _run_expectation(
        self,
        df: DataFrame,
        expectation: dict[str, Any],
    ) -> ExpectationResult:
        """
        Exécute une expectation individuelle.
        
        Args:
            df: DataFrame à valider
            expectation: Configuration de l'expectation
            
        Returns:
            ExpectationResult avec le résultat
        """
        exp_type = expectation.get("expectation_type", "unknown")
        kwargs = expectation.get("kwargs", {})
        
        try:
            # Router vers la méthode appropriée
            if exp_type == "expect_column_to_exist":
                return self._expect_column_to_exist(df, **kwargs)
            
            elif exp_type == "expect_column_values_to_not_be_null":
                return self._expect_column_values_to_not_be_null(df, **kwargs)
            
            elif exp_type == "expect_column_values_to_be_unique":
                return self._expect_column_values_to_be_unique(df, **kwargs)
            
            elif exp_type == "expect_column_values_to_be_in_set":
                return self._expect_column_values_to_be_in_set(df, **kwargs)
            
            elif exp_type == "expect_column_values_to_be_between":
                return self._expect_column_values_to_be_between(df, **kwargs)
            
            elif exp_type == "expect_column_values_to_match_regex":
                return self._expect_column_values_to_match_regex(df, **kwargs)
            
            elif exp_type == "expect_table_row_count_to_be_between":
                return self._expect_table_row_count_to_be_between(df, **kwargs)
            
            elif exp_type == "expect_column_mean_to_be_between":
                return self._expect_column_mean_to_be_between(df, **kwargs)
            
            elif exp_type == "expect_column_values_to_be_of_type":
                return self._expect_column_values_to_be_of_type(df, **kwargs)
            
            else:
                logger.warning(f"Expectation non supportée: {exp_type}")
                return ExpectationResult(
                    expectation_type=exp_type,
                    success=False,
                    details={"error": f"Type non supporté: {exp_type}"},
                )
                
        except Exception as e:
            logger.error(f"Erreur lors de l'exécution de {exp_type}: {e}")
            return ExpectationResult(
                expectation_type=exp_type,
                success=False,
                details={"error": str(e)},
            )
    
    # ========== EXPECTATIONS IMPLEMENTATIONS ==========
    
    def _expect_column_to_exist(
        self,
        df: DataFrame,
        column: str,
    ) -> ExpectationResult:
        """Vérifie qu'une colonne existe."""
        exists = column in df.columns
        return ExpectationResult(
            expectation_type="expect_column_to_exist",
            success=exists,
            column=column,
            details={"available_columns": df.columns},
            observed_value=exists,
        )
    
    def _expect_column_values_to_not_be_null(
        self,
        df: DataFrame,
        column: str,
        mostly: float = 1.0,
    ) -> ExpectationResult:
        """Vérifie que les valeurs ne sont pas nulles."""
        if column not in df.columns:
            return ExpectationResult(
                expectation_type="expect_column_values_to_not_be_null",
                success=False,
                column=column,
                details={"error": f"Colonne {column} inexistante"},
            )
        
        total_count = df.count()
        if total_count == 0:
            return ExpectationResult(
                expectation_type="expect_column_values_to_not_be_null",
                success=True,
                column=column,
                details={"message": "DataFrame vide"},
                observed_value=1.0,
            )
        
        non_null_count = df.filter(F.col(column).isNotNull()).count()
        non_null_ratio = non_null_count / total_count
        
        return ExpectationResult(
            expectation_type="expect_column_values_to_not_be_null",
            success=non_null_ratio >= mostly,
            column=column,
            details={
                "total_count": total_count,
                "non_null_count": non_null_count,
                "mostly": mostly,
            },
            observed_value=non_null_ratio,
        )
    
    def _expect_column_values_to_be_unique(
        self,
        df: DataFrame,
        column: str,
        mostly: float = 1.0,
    ) -> ExpectationResult:
        """Vérifie que les valeurs sont uniques."""
        if column not in df.columns:
            return ExpectationResult(
                expectation_type="expect_column_values_to_be_unique",
                success=False,
                column=column,
                details={"error": f"Colonne {column} inexistante"},
            )
        
        total_count = df.count()
        if total_count == 0:
            return ExpectationResult(
                expectation_type="expect_column_values_to_be_unique",
                success=True,
                column=column,
                observed_value=1.0,
            )
        
        distinct_count = df.select(column).distinct().count()
        unique_ratio = distinct_count / total_count
        
        return ExpectationResult(
            expectation_type="expect_column_values_to_be_unique",
            success=unique_ratio >= mostly,
            column=column,
            details={
                "total_count": total_count,
                "distinct_count": distinct_count,
                "mostly": mostly,
            },
            observed_value=unique_ratio,
        )
    
    def _expect_column_values_to_be_in_set(
        self,
        df: DataFrame,
        column: str,
        value_set: list[Any],
        mostly: float = 1.0,
    ) -> ExpectationResult:
        """Vérifie que les valeurs sont dans un ensemble donné."""
        if column not in df.columns:
            return ExpectationResult(
                expectation_type="expect_column_values_to_be_in_set",
                success=False,
                column=column,
                details={"error": f"Colonne {column} inexistante"},
            )
        
        total_count = df.count()
        if total_count == 0:
            return ExpectationResult(
                expectation_type="expect_column_values_to_be_in_set",
                success=True,
                column=column,
                observed_value=1.0,
            )
        
        in_set_count = df.filter(F.col(column).isin(value_set)).count()
        in_set_ratio = in_set_count / total_count
        
        # Trouver les valeurs hors ensemble
        out_of_set = (
            df.filter(~F.col(column).isin(value_set))
            .select(column)
            .distinct()
            .limit(10)
            .collect()
        )
        out_of_set_values = [row[column] for row in out_of_set]
        
        return ExpectationResult(
            expectation_type="expect_column_values_to_be_in_set",
            success=in_set_ratio >= mostly,
            column=column,
            details={
                "value_set": value_set,
                "out_of_set_values": out_of_set_values,
                "mostly": mostly,
            },
            observed_value=in_set_ratio,
        )
    
    def _expect_column_values_to_be_between(
        self,
        df: DataFrame,
        column: str,
        min_value: float | None = None,
        max_value: float | None = None,
        mostly: float = 1.0,
    ) -> ExpectationResult:
        """Vérifie que les valeurs sont dans une plage."""
        if column not in df.columns:
            return ExpectationResult(
                expectation_type="expect_column_values_to_be_between",
                success=False,
                column=column,
                details={"error": f"Colonne {column} inexistante"},
            )
        
        total_count = df.count()
        if total_count == 0:
            return ExpectationResult(
                expectation_type="expect_column_values_to_be_between",
                success=True,
                column=column,
                observed_value=1.0,
            )
        
        # Construire la condition
        conditions = []
        if min_value is not None:
            conditions.append(F.col(column) >= min_value)
        if max_value is not None:
            conditions.append(F.col(column) <= max_value)
        
        if not conditions:
            return ExpectationResult(
                expectation_type="expect_column_values_to_be_between",
                success=True,
                column=column,
                details={"message": "Aucune borne spécifiée"},
                observed_value=1.0,
            )
        
        combined_condition = conditions[0]
        for cond in conditions[1:]:
            combined_condition = combined_condition & cond
        
        in_range_count = df.filter(combined_condition).count()
        in_range_ratio = in_range_count / total_count
        
        # Stats sur les valeurs hors plage
        stats = df.agg(
            F.min(column).alias("min"),
            F.max(column).alias("max"),
        ).collect()[0]
        
        return ExpectationResult(
            expectation_type="expect_column_values_to_be_between",
            success=in_range_ratio >= mostly,
            column=column,
            details={
                "min_value": min_value,
                "max_value": max_value,
                "observed_min": stats["min"],
                "observed_max": stats["max"],
                "mostly": mostly,
            },
            observed_value=in_range_ratio,
        )
    
    def _expect_column_values_to_match_regex(
        self,
        df: DataFrame,
        column: str,
        regex: str,
        mostly: float = 1.0,
    ) -> ExpectationResult:
        """Vérifie que les valeurs correspondent à une regex."""
        if column not in df.columns:
            return ExpectationResult(
                expectation_type="expect_column_values_to_match_regex",
                success=False,
                column=column,
                details={"error": f"Colonne {column} inexistante"},
            )
        
        total_count = df.count()
        if total_count == 0:
            return ExpectationResult(
                expectation_type="expect_column_values_to_match_regex",
                success=True,
                column=column,
                observed_value=1.0,
            )
        
        match_count = df.filter(F.col(column).rlike(regex)).count()
        match_ratio = match_count / total_count
        
        # Exemples de non-correspondances
        non_matches = (
            df.filter(~F.col(column).rlike(regex))
            .select(column)
            .distinct()
            .limit(5)
            .collect()
        )
        non_match_examples = [row[column] for row in non_matches]
        
        return ExpectationResult(
            expectation_type="expect_column_values_to_match_regex",
            success=match_ratio >= mostly,
            column=column,
            details={
                "regex": regex,
                "non_match_examples": non_match_examples,
                "mostly": mostly,
            },
            observed_value=match_ratio,
        )
    
    def _expect_table_row_count_to_be_between(
        self,
        df: DataFrame,
        min_value: int | None = None,
        max_value: int | None = None,
    ) -> ExpectationResult:
        """Vérifie que le nombre de lignes est dans une plage."""
        row_count = df.count()
        
        success = True
        if min_value is not None and row_count < min_value:
            success = False
        if max_value is not None and row_count > max_value:
            success = False
        
        return ExpectationResult(
            expectation_type="expect_table_row_count_to_be_between",
            success=success,
            details={
                "min_value": min_value,
                "max_value": max_value,
            },
            observed_value=row_count,
        )
    
    def _expect_column_mean_to_be_between(
        self,
        df: DataFrame,
        column: str,
        min_value: float | None = None,
        max_value: float | None = None,
    ) -> ExpectationResult:
        """Vérifie que la moyenne d'une colonne est dans une plage."""
        if column not in df.columns:
            return ExpectationResult(
                expectation_type="expect_column_mean_to_be_between",
                success=False,
                column=column,
                details={"error": f"Colonne {column} inexistante"},
            )
        
        mean_value = df.agg(F.mean(column)).collect()[0][0]
        
        if mean_value is None:
            return ExpectationResult(
                expectation_type="expect_column_mean_to_be_between",
                success=False,
                column=column,
                details={"error": "Impossible de calculer la moyenne"},
            )
        
        success = True
        if min_value is not None and mean_value < min_value:
            success = False
        if max_value is not None and mean_value > max_value:
            success = False
        
        return ExpectationResult(
            expectation_type="expect_column_mean_to_be_between",
            success=success,
            column=column,
            details={
                "min_value": min_value,
                "max_value": max_value,
            },
            observed_value=mean_value,
        )
    
    def _expect_column_values_to_be_of_type(
        self,
        df: DataFrame,
        column: str,
        type_: str,
    ) -> ExpectationResult:
        """Vérifie le type d'une colonne."""
        if column not in df.columns:
            return ExpectationResult(
                expectation_type="expect_column_values_to_be_of_type",
                success=False,
                column=column,
                details={"error": f"Colonne {column} inexistante"},
            )
        
        # Récupérer le type de la colonne
        col_type = None
        for field in df.schema.fields:
            if field.name == column:
                col_type = str(field.dataType)
                break
        
        # Mapping des types
        type_mapping = {
            "string": ["StringType", "StringType()"],
            "int": ["IntegerType", "IntegerType()", "LongType", "LongType()"],
            "integer": ["IntegerType", "IntegerType()", "LongType", "LongType()"],
            "long": ["LongType", "LongType()"],
            "double": ["DoubleType", "DoubleType()"],
            "float": ["FloatType", "FloatType()", "DoubleType", "DoubleType()"],
            "boolean": ["BooleanType", "BooleanType()"],
            "date": ["DateType", "DateType()"],
            "timestamp": ["TimestampType", "TimestampType()"],
        }
        
        expected_types = type_mapping.get(type_.lower(), [type_])
        success = col_type in expected_types
        
        return ExpectationResult(
            expectation_type="expect_column_values_to_be_of_type",
            success=success,
            column=column,
            details={
                "expected_type": type_,
                "expected_types_list": expected_types,
            },
            observed_value=col_type,
        )
    
    # ========== PERSISTENCE ==========
    
    def _save_results(self, result: ValidationResult) -> None:
        """Sauvegarde les résultats de validation."""
        if not self.results_path:
            return
        
        filename = f"{result.run_name}.json"
        filepath = self.results_path / filename
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(result.to_dict(), f, indent=2, default=str)
        
        logger.info(f"Résultats sauvegardés: {filepath}")
    
    def list_available_suites(self) -> list[str]:
        """Liste les suites d'expectations disponibles."""
        if not self.expectations_path or not self.expectations_path.exists():
            return []
        
        return [
            f.stem
            for f in self.expectations_path.glob("*.json")
        ]
    
    def create_expectation_suite(
        self,
        suite_name: str,
        expectations: list[dict[str, Any]],
        meta: dict[str, Any] | None = None,
    ) -> Path:
        """
        Crée un fichier de suite d'expectations.
        
        Args:
            suite_name: Nom de la suite
            expectations: Liste des expectations
            meta: Métadonnées optionnelles
            
        Returns:
            Chemin du fichier créé
        """
        if not self.expectations_path:
            raise ValueError("expectations_path non configuré")
        
        self.expectations_path.mkdir(parents=True, exist_ok=True)
        
        suite = {
            "expectation_suite_name": suite_name,
            "meta": meta or {},
            "expectations": expectations,
            "created_at": datetime.now().isoformat(),
        }
        
        filepath = self.expectations_path / f"{suite_name}.json"
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(suite, f, indent=2)
        
        logger.info(f"Suite créée: {filepath}")
        
        return filepath
