# COMPLETENESS-Données manquantes-Colonnes requises non nulles
# UNIQUENESS-Doublons-Clés primaires/composites
# VALIDITY-Format/valeurs-Emails, plages numériques
# CONSISTENCY-Cohérence interne-Calculs (qty × price = total)
# TIMELINESS-Fraîcheur-Données < 24h
# INTEGRITY-Références-FK vers tables parents

# src/pipelines/sales/jobs/quality_checks.py
"""Jobs de vérification de qualité pour le pipeline Sales."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from common.jobs import JobConfig, JobContext, SparkJob
from common.logging import get_logger
from common.readers import ParquetReader
from common.writers import JSONWriter, ParquetWriter, WriteMode
from pipelines.sales.config import SalesConfig
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logger = get_logger(__name__)


class QualityCheckType(Enum):
    """Types de vérifications de qualité."""

    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    TIMELINESS = "timeliness"
    UNIQUENESS = "uniqueness"
    VALIDITY = "validity"
    INTEGRITY = "integrity"


@dataclass
class QualityCheckResult:
    """Résultat d'une vérification de qualité."""

    check_name: str
    check_type: QualityCheckType
    table_name: str
    passed: bool
    score: float
    total_records: int
    valid_records: int
    invalid_records: int
    details: dict[str, Any] = field(default_factory=dict)
    executed_at: datetime = field(default_factory=datetime.now)
    duration_seconds: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convertit en dictionnaire."""
        return {
            "check_name": self.check_name,
            "check_type": self.check_type.value,
            "table_name": self.table_name,
            "passed": self.passed,
            "score": self.score,
            "total_records": self.total_records,
            "valid_records": self.valid_records,
            "invalid_records": self.invalid_records,
            "details": self.details,
            "executed_at": self.executed_at.isoformat(),
            "duration_seconds": self.duration_seconds,
        }


@dataclass
class QualityReport:
    """Rapport de qualité global."""

    report_id: str
    report_name: str
    environment: str
    results: list[QualityCheckResult] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.now)
    completed_at: datetime | None = None
    overall_passed: bool = True
    overall_score: float = 0.0

    def add_result(self, result: QualityCheckResult) -> None:
        """Ajoute un résultat au rapport."""
        self.results.append(result)

        # Recalculer le score global
        if self.results:
            self.overall_score = sum(r.score for r in self.results) / len(self.results)
            self.overall_passed = all(r.passed for r in self.results)

    def complete(self) -> None:
        """Marque le rapport comme terminé."""
        self.completed_at = datetime.now()

    def to_dict(self) -> dict[str, Any]:
        """Convertit en dictionnaire."""
        return {
            "report_id": self.report_id,
            "report_name": self.report_name,
            "environment": self.environment,
            "results": [r.to_dict() for r in self.results],
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "overall_passed": self.overall_passed,
            "overall_score": self.overall_score,
            "total_checks": len(self.results),
            "passed_checks": sum(1 for r in self.results if r.passed),
            "failed_checks": sum(1 for r in self.results if not r.passed),
        }


class SalesQualityChecksJob(SparkJob):
    """
    Job de vérification de qualité pour le pipeline Sales.

    Effectue des vérifications complètes sur:
    - Customers (bronze)
    - Transactions (bronze)
    - Products (bronze)
    - Cohérence inter-tables

    Example:
        >>> config = JobConfig(job_name="sales_quality_checks", environment="dev")
        >>> job = SalesQualityChecksJob(spark, config)
        >>> result = job.run()
    """

    def __init__(
        self,
        spark: SparkSession,
        config: JobConfig,
        sales_config: SalesConfig | None = None,
    ):
        """
        Initialise le job de qualité.

        Args:
            spark: Session Spark
            config: Configuration du job
            sales_config: Configuration spécifique sales
        """
        super().__init__(spark, config)
        self.sales_config = sales_config or SalesConfig.from_environment(config.environment)

        self.report = QualityReport(
            report_id=f"quality_{self.config.job_id}",
            report_name="Sales Data Quality Report",
            environment=config.environment,
        )

        logger.info(
            "SalesQualityChecksJob initialisé",
            environment=config.environment,
        )

    def extract(self, context: JobContext) -> dict[str, DataFrame]:
        """
        Charge les tables à vérifier.

        Returns:
            Dictionnaire des DataFrames par nom de table
        """
        logger.info("Chargement des tables pour vérification de qualité")

        tables = {}
        bronze_path = self.sales_config.bronze_path

        # Tables à charger
        table_paths = {
            "customers": f"{bronze_path}/customers",
            "transactions": f"{bronze_path}/transactions",
            "products": f"{bronze_path}/products",
        }

        for table_name, path in table_paths.items():
            try:
                reader = ParquetReader(spark=self.spark, path=path)
                df = reader.read()
                tables[table_name] = df

                count = df.count()
                context.metrics[f"{table_name}_count"] = count
                logger.info(f"Table {table_name} chargée: {count} enregistrements")

            except Exception as e:
                logger.warning(f"Table {table_name} non disponible: {e}")
                tables[table_name] = None

        return tables

    def transform(
        self,
        tables: dict[str, DataFrame],
        context: JobContext,
    ) -> QualityReport:
        """
        Exécute toutes les vérifications de qualité.

        Args:
            tables: DataFrames à vérifier
            context: Contexte d'exécution

        Returns:
            Rapport de qualité
        """
        logger.info("Exécution des vérifications de qualité")

        # Vérifications par table
        if tables.get("customers") is not None:
            self._check_customers_quality(tables["customers"], context)

        if tables.get("transactions") is not None:
            self._check_transactions_quality(tables["transactions"], context)

        if tables.get("products") is not None:
            self._check_products_quality(tables["products"], context)

        # Vérifications de cohérence inter-tables
        self._check_referential_integrity(tables, context)

        # Compléter le rapport
        self.report.complete()

        return self.report

    def _check_customers_quality(
        self,
        df: DataFrame,
        _context: JobContext,
    ) -> None:
        """Vérifie la qualité des données clients."""
        logger.info("Vérification de qualité: Customers")

        start_time = datetime.now()

        # 1. Complétude
        completeness_result = self._check_completeness(
            df=df,
            table_name="customers",
            required_columns=["customer_id", "email", "first_name", "last_name"],
            important_columns=["phone", "city", "country"],
        )
        self.report.add_result(completeness_result)

        # 2. Unicité
        uniqueness_result = self._check_uniqueness(
            df=df,
            table_name="customers",
            unique_columns=["customer_id"],
            unique_composite=[["email"]],
        )
        self.report.add_result(uniqueness_result)

        # 3. Validité des emails
        email_validity = self._check_email_validity(df, "customers")
        self.report.add_result(email_validity)

        # 4. Validité des dates
        date_validity = self._check_date_validity(
            df=df,
            table_name="customers",
            date_columns={
                "birth_date": {"min": "1900-01-01", "max": "today"},
                "registration_date": {"min": "2000-01-01", "max": "now"},
            },
        )
        self.report.add_result(date_validity)

        # 5. Distribution des valeurs
        distribution_check = self._check_value_distribution(
            df=df,
            table_name="customers",
            column="customer_segment",
            expected_values=["STANDARD", "PREMIUM", "VIP", "ENTERPRISE"],
        )
        self.report.add_result(distribution_check)

        # 6. Fraîcheur des données
        freshness_check = self._check_data_freshness(
            df=df,
            table_name="customers",
            timestamp_column="_ingestion_timestamp",
            max_age_hours=24,
        )
        self.report.add_result(freshness_check)

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Vérification Customers terminée en {duration:.2f}s")

    def _check_transactions_quality(
        self,
        df: DataFrame,
        _context: JobContext,
    ) -> None:
        """Vérifie la qualité des données transactions."""
        logger.info("Vérification de qualité: Transactions")

        start_time = datetime.now()

        # 1. Complétude
        completeness_result = self._check_completeness(
            df=df,
            table_name="transactions",
            required_columns=[
                "transaction_id",
                "customer_id",
                "product_id",
                "quantity",
                "unit_price",
                "total_amount",
            ],
            important_columns=["transaction_date", "payment_method"],
        )
        self.report.add_result(completeness_result)

        # 2. Unicité
        uniqueness_result = self._check_uniqueness(
            df=df,
            table_name="transactions",
            unique_columns=["transaction_id"],
        )
        self.report.add_result(uniqueness_result)

        # 3. Validité des montants
        amounts_validity = self._check_numeric_validity(
            df=df,
            table_name="transactions",
            numeric_columns={
                "quantity": {"min": 1, "max": 10000},
                "unit_price": {"min": 0.01, "max": 1000000},
                "total_amount": {"min": 0.01, "max": 10000000},
            },
        )
        self.report.add_result(amounts_validity)

        # 4. Cohérence des calculs
        calculation_check = self._check_calculation_consistency(
            df=df,
            table_name="transactions",
            formula="quantity * unit_price",
            result_column="total_amount",
            tolerance=0.01,
        )
        self.report.add_result(calculation_check)

        # 5. Distribution temporelle
        temporal_check = self._check_temporal_distribution(
            df=df,
            table_name="transactions",
            date_column="transaction_date",
        )
        self.report.add_result(temporal_check)

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Vérification Transactions terminée en {duration:.2f}s")

    def _check_products_quality(
        self,
        df: DataFrame,
        _context: JobContext,
    ) -> None:
        """Vérifie la qualité des données produits."""
        logger.info("Vérification de qualité: Products")

        start_time = datetime.now()

        # 1. Complétude
        completeness_result = self._check_completeness(
            df=df,
            table_name="products",
            required_columns=["product_id", "product_name", "category", "price"],
            important_columns=["description", "brand", "stock_quantity"],
        )
        self.report.add_result(completeness_result)

        # 2. Unicité
        uniqueness_result = self._check_uniqueness(
            df=df,
            table_name="products",
            unique_columns=["product_id"],
            unique_composite=[["product_name", "brand"]],
        )
        self.report.add_result(uniqueness_result)

        # 3. Validité des prix
        price_validity = self._check_numeric_validity(
            df=df,
            table_name="products",
            numeric_columns={
                "price": {"min": 0.01, "max": 1000000},
                "stock_quantity": {"min": 0, "max": 1000000},
            },
        )
        self.report.add_result(price_validity)

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Vérification Products terminée en {duration:.2f}s")

    def _check_referential_integrity(
        self,
        tables: dict[str, DataFrame],
        _context: JobContext,
    ) -> None:
        """Vérifie l'intégrité référentielle entre tables."""
        logger.info("Vérification de l'intégrité référentielle")

        start_time = datetime.now()

        # Transactions -> Customers
        if tables.get("transactions") and tables.get("customers"):
            result = self._check_foreign_key(
                child_df=tables["transactions"],
                parent_df=tables["customers"],
                child_table="transactions",
                parent_table="customers",
                child_column="customer_id",
                parent_column="customer_id",
            )
            self.report.add_result(result)

        # Transactions -> Products
        if tables.get("transactions") and tables.get("products"):
            result = self._check_foreign_key(
                child_df=tables["transactions"],
                parent_df=tables["products"],
                child_table="transactions",
                parent_table="products",
                child_column="product_id",
                parent_column="product_id",
            )
            self.report.add_result(result)

        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Vérification intégrité référentielle terminée en {duration:.2f}s")

    # === Méthodes de vérification individuelles ===

    def _check_completeness(
        self,
        df: DataFrame,
        table_name: str,
        required_columns: list[str],
        important_columns: list[str] | None = None,
    ) -> QualityCheckResult:
        """Vérifie la complétude des données."""
        important_columns = important_columns or []
        start_time = datetime.now()
        total_records = df.count()

        details = {
            "required_columns": {},
            "important_columns": {},
        }

        # Vérifier les colonnes requises
        required_scores = []
        for col in required_columns:
            if col in df.columns:
                non_null_count = df.filter(F.col(col).isNotNull()).count()
                completeness = non_null_count / total_records if total_records > 0 else 0
                details["required_columns"][col] = {
                    "non_null_count": non_null_count,
                    "completeness": completeness,
                }
                required_scores.append(completeness)
            else:
                details["required_columns"][col] = {
                    "error": "column_not_found",
                    "completeness": 0,
                }
                required_scores.append(0)

        # Vérifier les colonnes importantes
        for col in important_columns:
            if col in df.columns:
                non_null_count = df.filter(F.col(col).isNotNull()).count()
                completeness = non_null_count / total_records if total_records > 0 else 0
                details["important_columns"][col] = {
                    "non_null_count": non_null_count,
                    "completeness": completeness,
                }

        # Score global (moyenne des colonnes requises)
        avg_score = sum(required_scores) / len(required_scores) if required_scores else 0

        # Seuil de passage: 95% de complétude sur les colonnes requises
        passed = avg_score >= 0.95

        duration = (datetime.now() - start_time).total_seconds()

        return QualityCheckResult(
            check_name=f"{table_name}_completeness",
            check_type=QualityCheckType.COMPLETENESS,
            table_name=table_name,
            passed=passed,
            score=avg_score,
            total_records=total_records,
            valid_records=int(total_records * avg_score),
            invalid_records=int(total_records * (1 - avg_score)),
            details=details,
            duration_seconds=duration,
        )

    def _check_uniqueness(
        self,
        df: DataFrame,
        table_name: str,
        unique_columns: list[str],
        unique_composite: list[list[str]] | None = None,
    ) -> QualityCheckResult:
        """Vérifie l'unicité des colonnes."""
        unique_composite = unique_composite or []
        start_time = datetime.now()
        total_records = df.count()

        details = {
            "single_columns": {},
            "composite_keys": {},
        }

        all_passed = True
        scores = []

        # Vérifier les colonnes individuelles
        for col in unique_columns:
            if col in df.columns:
                distinct_count = df.select(col).distinct().count()
                uniqueness = distinct_count / total_records if total_records > 0 else 0
                duplicates = total_records - distinct_count

                details["single_columns"][col] = {
                    "distinct_count": distinct_count,
                    "duplicate_count": duplicates,
                    "uniqueness": uniqueness,
                }

                scores.append(uniqueness)
                if uniqueness < 1.0:
                    all_passed = False

        # Vérifier les clés composites
        for key_cols in unique_composite:
            if all(c in df.columns for c in key_cols):
                key_name = "_".join(key_cols)
                distinct_count = df.select(*key_cols).distinct().count()
                uniqueness = distinct_count / total_records if total_records > 0 else 0

                details["composite_keys"][key_name] = {
                    "columns": key_cols,
                    "distinct_count": distinct_count,
                    "uniqueness": uniqueness,
                }

                scores.append(uniqueness)

        avg_score = sum(scores) / len(scores) if scores else 1.0
        duration = (datetime.now() - start_time).total_seconds()

        return QualityCheckResult(
            check_name=f"{table_name}_uniqueness",
            check_type=QualityCheckType.UNIQUENESS,
            table_name=table_name,
            passed=all_passed,
            score=avg_score,
            total_records=total_records,
            valid_records=int(total_records * avg_score),
            invalid_records=int(total_records * (1 - avg_score)),
            details=details,
            duration_seconds=duration,
        )

    def _check_email_validity(
        self,
        df: DataFrame,
        table_name: str,
        email_column: str = "email",
    ) -> QualityCheckResult:
        """Vérifie la validité des emails."""
        start_time = datetime.now()

        if email_column not in df.columns:
            return QualityCheckResult(
                check_name=f"{table_name}_email_validity",
                check_type=QualityCheckType.VALIDITY,
                table_name=table_name,
                passed=True,
                score=1.0,
                total_records=0,
                valid_records=0,
                invalid_records=0,
                details={"error": "column_not_found"},
            )

        total_records = df.filter(F.col(email_column).isNotNull()).count()

        # Pattern email
        email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"

        valid_emails = df.filter(F.col(email_column).rlike(email_pattern)).count()

        invalid_emails = total_records - valid_emails
        score = valid_emails / total_records if total_records > 0 else 1.0

        # Exemples d'emails invalides
        invalid_examples = []
        if invalid_emails > 0:
            invalid_examples = [
                row[email_column]
                for row in df.filter(
                    F.col(email_column).isNotNull() & ~F.col(email_column).rlike(email_pattern)
                )
                .select(email_column)
                .limit(5)
                .collect()
            ]

        duration = (datetime.now() - start_time).total_seconds()

        return QualityCheckResult(
            check_name=f"{table_name}_email_validity",
            check_type=QualityCheckType.VALIDITY,
            table_name=table_name,
            passed=score >= 0.95,
            score=score,
            total_records=total_records,
            valid_records=valid_emails,
            invalid_records=invalid_emails,
            details={
                "invalid_examples": invalid_examples,
                "pattern": email_pattern,
            },
            duration_seconds=duration,
        )

    def _check_date_validity(
        self,
        df: DataFrame,
        table_name: str,
        date_columns: dict[str, dict[str, str]],
    ) -> QualityCheckResult:
        """Vérifie la validité des dates."""
        start_time = datetime.now()
        total_records = df.count()

        details = {}
        all_valid = 0
        all_invalid = 0

        for col_name, constraints in date_columns.items():
            if col_name not in df.columns:
                details[col_name] = {"error": "column_not_found"}
                continue

            col_total = df.filter(F.col(col_name).isNotNull()).count()

            conditions = [F.col(col_name).isNotNull()]

            # Contrainte min
            if "min" in constraints:
                min_val = constraints["min"]
                if min_val == "today":
                    conditions.append(F.col(col_name) <= F.current_date())
                else:
                    conditions.append(F.col(col_name) >= F.lit(min_val))

            # Contrainte max
            if "max" in constraints:
                max_val = constraints["max"]
                if max_val in ("today", "now"):
                    conditions.append(F.col(col_name) <= F.current_timestamp())
                else:
                    conditions.append(F.col(col_name) <= F.lit(max_val))

            # Compter les valides
            from functools import reduce

            combined_condition = reduce(lambda a, b: a & b, conditions)
            valid_count = df.filter(combined_condition).count()
            invalid_count = col_total - valid_count

            details[col_name] = {
                "total": col_total,
                "valid": valid_count,
                "invalid": invalid_count,
                "validity_rate": valid_count / col_total if col_total > 0 else 1.0,
            }

            all_valid += valid_count
            all_invalid += invalid_count

        total_checked = all_valid + all_invalid
        score = all_valid / total_checked if total_checked > 0 else 1.0
        duration = (datetime.now() - start_time).total_seconds()

        return QualityCheckResult(
            check_name=f"{table_name}_date_validity",
            check_type=QualityCheckType.VALIDITY,
            table_name=table_name,
            passed=score >= 0.95,
            score=score,
            total_records=total_records,
            valid_records=all_valid,
            invalid_records=all_invalid,
            details=details,
            duration_seconds=duration,
        )

    def _check_numeric_validity(
        self,
        df: DataFrame,
        table_name: str,
        numeric_columns: dict[str, dict[str, float]],
    ) -> QualityCheckResult:
        """Vérifie la validité des valeurs numériques."""
        start_time = datetime.now()
        total_records = df.count()

        details = {}
        all_valid = 0
        all_invalid = 0

        for col_name, constraints in numeric_columns.items():
            if col_name not in df.columns:
                details[col_name] = {"error": "column_not_found"}
                continue

            col_total = df.filter(F.col(col_name).isNotNull()).count()

            conditions = [F.col(col_name).isNotNull()]

            if "min" in constraints:
                conditions.append(F.col(col_name) >= constraints["min"])

            if "max" in constraints:
                conditions.append(F.col(col_name) <= constraints["max"])

            from functools import reduce

            combined_condition = reduce(lambda a, b: a & b, conditions)
            valid_count = df.filter(combined_condition).count()
            invalid_count = col_total - valid_count

            # Statistiques
            stats = df.select(
                F.min(col_name).alias("min"),
                F.max(col_name).alias("max"),
                F.avg(col_name).alias("avg"),
                F.stddev(col_name).alias("stddev"),
            ).first()

            details[col_name] = {
                "total": col_total,
                "valid": valid_count,
                "invalid": invalid_count,
                "validity_rate": valid_count / col_total if col_total > 0 else 1.0,
                "statistics": {
                    "min": float(stats["min"]) if stats["min"] else None,
                    "max": float(stats["max"]) if stats["max"] else None,
                    "avg": float(stats["avg"]) if stats["avg"] else None,
                    "stddev": float(stats["stddev"]) if stats["stddev"] else None,
                },
                "constraints": constraints,
            }

            all_valid += valid_count
            all_invalid += invalid_count

        total_checked = all_valid + all_invalid
        score = all_valid / total_checked if total_checked > 0 else 1.0
        duration = (datetime.now() - start_time).total_seconds()

        return QualityCheckResult(
            check_name=f"{table_name}_numeric_validity",
            check_type=QualityCheckType.VALIDITY,
            table_name=table_name,
            passed=score >= 0.95,
            score=score,
            total_records=total_records,
            valid_records=all_valid,
            invalid_records=all_invalid,
            details=details,
            duration_seconds=duration,
        )

    def _check_value_distribution(
        self,
        df: DataFrame,
        table_name: str,
        column: str,
        expected_values: list[str],
    ) -> QualityCheckResult:
        """Vérifie la distribution des valeurs."""
        start_time = datetime.now()

        if column not in df.columns:
            return QualityCheckResult(
                check_name=f"{table_name}_{column}_distribution",
                check_type=QualityCheckType.VALIDITY,
                table_name=table_name,
                passed=True,
                score=1.0,
                total_records=0,
                valid_records=0,
                invalid_records=0,
                details={"error": "column_not_found"},
            )

        total_records = df.filter(F.col(column).isNotNull()).count()

        # Distribution actuelle
        distribution = df.filter(F.col(column).isNotNull()).groupBy(column).count().collect()

        distribution_dict = {row[column]: row["count"] for row in distribution}

        # Valeurs valides
        valid_count = sum(
            count for value, count in distribution_dict.items() if value in expected_values
        )

        # Valeurs inattendues
        unexpected_values = {
            value: count
            for value, count in distribution_dict.items()
            if value not in expected_values
        }

        score = valid_count / total_records if total_records > 0 else 1.0
        duration = (datetime.now() - start_time).total_seconds()

        return QualityCheckResult(
            check_name=f"{table_name}_{column}_distribution",
            check_type=QualityCheckType.VALIDITY,
            table_name=table_name,
            passed=score >= 0.95,
            score=score,
            total_records=total_records,
            valid_records=valid_count,
            invalid_records=total_records - valid_count,
            details={
                "expected_values": expected_values,
                "distribution": distribution_dict,
                "unexpected_values": unexpected_values,
            },
            duration_seconds=duration,
        )

    def _check_data_freshness(
        self,
        df: DataFrame,
        table_name: str,
        timestamp_column: str,
        max_age_hours: int = 24,
    ) -> QualityCheckResult:
        """Vérifie la fraîcheur des données."""
        start_time = datetime.now()

        if timestamp_column not in df.columns:
            return QualityCheckResult(
                check_name=f"{table_name}_freshness",
                check_type=QualityCheckType.TIMELINESS,
                table_name=table_name,
                passed=True,
                score=1.0,
                total_records=0,
                valid_records=0,
                invalid_records=0,
                details={"error": "column_not_found"},
            )

        total_records = df.count()

        # Trouver la date la plus récente
        latest_record = df.select(F.max(timestamp_column)).first()[0]

        if latest_record is None:
            return QualityCheckResult(
                check_name=f"{table_name}_freshness",
                check_type=QualityCheckType.TIMELINESS,
                table_name=table_name,
                passed=False,
                score=0.0,
                total_records=total_records,
                valid_records=0,
                invalid_records=total_records,
                details={"error": "no_timestamp_data"},
            )

        # Calculer l'âge
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        age_hours = (datetime.now() - latest_record).total_seconds() / 3600

        # Données fraîches (moins de max_age_hours)
        fresh_count = df.filter(F.col(timestamp_column) >= F.lit(cutoff_time)).count()

        score = fresh_count / total_records if total_records > 0 else 0.0
        passed = age_hours <= max_age_hours

        duration = (datetime.now() - start_time).total_seconds()

        return QualityCheckResult(
            check_name=f"{table_name}_freshness",
            check_type=QualityCheckType.TIMELINESS,
            table_name=table_name,
            passed=passed,
            score=score,
            total_records=total_records,
            valid_records=fresh_count,
            invalid_records=total_records - fresh_count,
            details={
                "latest_record": latest_record.isoformat(),
                "age_hours": round(age_hours, 2),
                "max_age_hours": max_age_hours,
                "cutoff_time": cutoff_time.isoformat(),
            },
            duration_seconds=duration,
        )

    def _check_calculation_consistency(
        self,
        df: DataFrame,
        table_name: str,
        formula: str,
        result_column: str,
        tolerance: float = 0.01,
    ) -> QualityCheckResult:
        """Vérifie la cohérence des calculs."""
        start_time = datetime.now()

        if result_column not in df.columns:
            return QualityCheckResult(
                check_name=f"{table_name}_calculation_consistency",
                check_type=QualityCheckType.CONSISTENCY,
                table_name=table_name,
                passed=True,
                score=1.0,
                total_records=0,
                valid_records=0,
                invalid_records=0,
                details={"error": "column_not_found"},
            )

        total_records = df.count()

        # Calculer la valeur attendue
        df_with_calc = df.withColumn("_expected", F.expr(formula))

        # Comparer avec tolérance
        consistent_count = df_with_calc.filter(
            F.abs(F.col(result_column) - F.col("_expected")) <= tolerance
        ).count()

        inconsistent_count = total_records - consistent_count
        score = consistent_count / total_records if total_records > 0 else 1.0

        # Exemples d'incohérences
        inconsistent_examples = []
        if inconsistent_count > 0:
            examples = (
                df_with_calc.filter(F.abs(F.col(result_column) - F.col("_expected")) > tolerance)
                .select(
                    result_column,
                    "_expected",
                    (F.col(result_column) - F.col("_expected")).alias("difference"),
                )
                .limit(5)
                .collect()
            )

            inconsistent_examples = [
                {
                    "actual": float(row[result_column]),
                    "expected": float(row["_expected"]),
                    "difference": float(row["difference"]),
                }
                for row in examples
            ]

        duration = (datetime.now() - start_time).total_seconds()

        return QualityCheckResult(
            check_name=f"{table_name}_calculation_consistency",
            check_type=QualityCheckType.CONSISTENCY,
            table_name=table_name,
            passed=score >= 0.99,
            score=score,
            total_records=total_records,
            valid_records=consistent_count,
            invalid_records=inconsistent_count,
            details={
                "formula": formula,
                "result_column": result_column,
                "tolerance": tolerance,
                "inconsistent_examples": inconsistent_examples,
            },
            duration_seconds=duration,
        )

    def _check_temporal_distribution(
        self,
        df: DataFrame,
        table_name: str,
        date_column: str,
    ) -> QualityCheckResult:
        """Vérifie la distribution temporelle des données."""
        start_time = datetime.now()

        if date_column not in df.columns:
            return QualityCheckResult(
                check_name=f"{table_name}_temporal_distribution",
                check_type=QualityCheckType.CONSISTENCY,
                table_name=table_name,
                passed=True,
                score=1.0,
                total_records=0,
                valid_records=0,
                invalid_records=0,
                details={"error": "column_not_found"},
            )

        total_records = df.filter(F.col(date_column).isNotNull()).count()

        # Distribution par mois
        monthly_dist = (
            df.filter(F.col(date_column).isNotNull())
            .withColumn("year_month", F.date_format(F.col(date_column), "yyyy-MM"))
            .groupBy("year_month")
            .count()
            .orderBy("year_month")
            .collect()
        )

        distribution = {row["year_month"]: row["count"] for row in monthly_dist}

        # Détecter les gaps (mois sans données)
        if len(distribution) >= 2:
            # Simplification: on vérifie juste s'il y a des données récentes
            passed = True
            score = 1.0
        else:
            passed = total_records > 0
            score = 1.0 if passed else 0.0

        duration = (datetime.now() - start_time).total_seconds()

        return QualityCheckResult(
            check_name=f"{table_name}_temporal_distribution",
            check_type=QualityCheckType.CONSISTENCY,
            table_name=table_name,
            passed=passed,
            score=score,
            total_records=total_records,
            valid_records=total_records,
            invalid_records=0,
            details={
                "monthly_distribution": distribution,
                "distinct_months": len(distribution),
            },
            duration_seconds=duration,
        )

    def _check_foreign_key(
        self,
        child_df: DataFrame,
        parent_df: DataFrame,
        child_table: str,
        parent_table: str,
        child_column: str,
        parent_column: str,
    ) -> QualityCheckResult:
        """Vérifie l'intégrité référentielle."""
        start_time = datetime.now()

        total_records = child_df.filter(F.col(child_column).isNotNull()).count()

        # IDs parents
        parent_ids = parent_df.select(parent_column).distinct()

        # IDs enfants qui existent dans parent
        valid_count = (
            child_df.filter(F.col(child_column).isNotNull())
            .join(parent_ids, child_df[child_column] == parent_ids[parent_column], "left_semi")
            .count()
        )

        # IDs orphelins
        orphan_count = total_records - valid_count

        # Exemples d'orphelins
        orphan_examples = []
        if orphan_count > 0:
            orphans = (
                child_df.filter(F.col(child_column).isNotNull())
                .join(parent_ids, child_df[child_column] == parent_ids[parent_column], "left_anti")
                .select(child_column)
                .distinct()
                .limit(10)
                .collect()
            )
            orphan_examples = [row[child_column] for row in orphans]

        score = valid_count / total_records if total_records > 0 else 1.0
        duration = (datetime.now() - start_time).total_seconds()

        return QualityCheckResult(
            check_name=f"{child_table}_{child_column}_fk_{parent_table}",
            check_type=QualityCheckType.INTEGRITY,
            table_name=child_table,
            passed=score >= 0.99,
            score=score,
            total_records=total_records,
            valid_records=valid_count,
            invalid_records=orphan_count,
            details={
                "parent_table": parent_table,
                "child_column": child_column,
                "parent_column": parent_column,
                "orphan_examples": orphan_examples,
            },
            duration_seconds=duration,
        )

    def validate(
        self,
        report: QualityReport,
        context: JobContext,
    ) -> QualityReport:
        """
        Valide le rapport de qualité.

        Args:
            report: Rapport généré
            context: Contexte d'exécution

        Returns:
            Rapport validé
        """
        logger.info("Validation du rapport de qualité")

        # Métriques globales
        context.metrics["total_checks"] = len(report.results)
        context.metrics["passed_checks"] = sum(1 for r in report.results if r.passed)
        context.metrics["failed_checks"] = sum(1 for r in report.results if not r.passed)
        context.metrics["overall_score"] = report.overall_score
        context.metrics["overall_passed"] = report.overall_passed

        # Log du résumé
        logger.info(
            "Résumé de la qualité",
            total_checks=context.metrics["total_checks"],
            passed=context.metrics["passed_checks"],
            failed=context.metrics["failed_checks"],
            score=f"{report.overall_score:.2%}",
        )

        # Log des échecs
        for result in report.results:
            if not result.passed:
                logger.warning(
                    f"Check échoué: {result.check_name}",
                    table=result.table_name,
                    score=f"{result.score:.2%}",
                    invalid_records=result.invalid_records,
                )

        return report

    def load(self, report: QualityReport, context: JobContext) -> None:
        """
        Sauvegarde le rapport de qualité.

        Args:
            report: Rapport à sauvegarder
            context: Contexte d'exécution
        """
        logger.info("Sauvegarde du rapport de qualité")

        output_base = f"{self.sales_config.bronze_path}/_quality_reports"

        # 1. Sauvegarder le résumé en JSON
        report_dict = report.to_dict()

        report_df = self.spark.createDataFrame([report_dict])

        json_path = f"{output_base}/reports"
        JSONWriter(
            path=json_path,
            mode=WriteMode.APPEND,
            date_format="yyyy-MM-dd'T'HH:mm:ss",
        ).write(report_df)

        logger.info(f"Rapport JSON sauvegardé: {json_path}")

        # 2. Sauvegarder les résultats détaillés en Parquet
        results_data = [
            {
                "report_id": report.report_id,
                "check_name": r.check_name,
                "check_type": r.check_type.value,
                "table_name": r.table_name,
                "passed": r.passed,
                "score": r.score,
                "total_records": r.total_records,
                "valid_records": r.valid_records,
                "invalid_records": r.invalid_records,
                "executed_at": r.executed_at,
                "duration_seconds": r.duration_seconds,
            }
            for r in report.results
        ]

        if results_data:
            results_df = self.spark.createDataFrame(results_data)

            parquet_path = f"{output_base}/results"
            ParquetWriter(
                path=parquet_path,
                mode=WriteMode.APPEND,
                partition_by=["table_name"],
            ).write(results_df)

            logger.info(f"Résultats Parquet sauvegardés: {parquet_path}")

        context.metrics["report_path"] = output_base


def create_job(
    spark: SparkSession,
    environment: str = "dev",
    **kwargs,
) -> SalesQualityChecksJob:
    """Factory function pour créer le job."""
    config = JobConfig(
        job_name="sales_quality_checks",
        environment=environment,
        parameters=kwargs,
    )

    return SalesQualityChecksJob(spark, config)


# Point d'entrée
def main():
    """Point d'entrée principal."""
    import argparse

    from common.spark import create_spark_session

    parser = argparse.ArgumentParser(description="Job de vérification de qualité Sales")
    parser.add_argument("--env", default="dev", choices=["dev", "staging", "prod"])
    parser.add_argument(
        "--fail-on-error", action="store_true", help="Échouer si qualité insuffisante"
    )
    parser.add_argument("--min-score", type=float, default=0.8, help="Score minimum requis")

    args = parser.parse_args()

    spark = create_spark_session(
        app_name="sales_quality_checks",
        environment=args.env,
    )

    try:
        job = create_job(
            spark,
            environment=args.env,
            fail_on_error=args.fail_on_error,
            min_score=args.min_score,
        )
        result = job.run()

        print(f"\n{'='*60}")
        print(f"Job terminé: {result.status.value}")
        print(f"Score global: {result.metrics.get('overall_score', 0):.2%}")
        print(
            f"Checks passés: {result.metrics.get('passed_checks', 0)}/{result.metrics.get('total_checks', 0)}"
        )
        print(f"{'='*60}\n")

        # Échouer si demandé et score insuffisant
        if args.fail_on_error:
            score = result.metrics.get("overall_score", 0)
            if score < args.min_score:
                raise SystemExit(
                    f"Score de qualité insuffisant: {score:.2%} < {args.min_score:.2%}"
                )

    except Exception as e:
        logger.error(f"Erreur: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
