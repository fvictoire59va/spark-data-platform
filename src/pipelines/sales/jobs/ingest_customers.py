# src/pipelines/sales/jobs/ingest_customers.py
"""Job d'ingestion des données clients."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    BooleanType,
    DateType,
)

from common.jobs import SparkJob, JobConfig, JobContext
from common.readers import CSVReader, JSONReader
from common.writers import ParquetWriter, WriteMode
from common.transformers import (
    ColumnTransformer,
    FilterTransformer,
    DeduplicationTransformer,
)
from common.quality import QualityChecker, QualityRule, QualitySeverity
from common.logging import get_logger

from pipelines.sales.config import SalesConfig
from pipelines.sales.schemas import CustomerSchema

logger = get_logger(__name__)


class IngestCustomersJob(SparkJob):
    """
    Job d'ingestion des données clients.
    
    Ce job:
    1. Lit les fichiers clients (CSV ou JSON) depuis la zone raw
    2. Applique le schéma et les transformations de base
    3. Déduplique les clients par customer_id
    4. Valide la qualité des données
    5. Écrit les données validées dans la zone bronze
    
    Example:
        >>> config = JobConfig(
        ...     job_name="ingest_customers",
        ...     environment="dev",
        ... )
        >>> job = IngestCustomersJob(spark, config)
        >>> job.run()
    """
    
    def __init__(
        self,
        spark: SparkSession,
        config: JobConfig,
        sales_config: SalesConfig | None = None,
    ):
        """
        Initialise le job d'ingestion clients.
        
        Args:
            spark: Session Spark
            config: Configuration du job
            sales_config: Configuration spécifique sales (optionnel)
        """
        super().__init__(spark, config)
        self.sales_config = sales_config or SalesConfig.from_environment(
            config.environment
        )
        
        # Chemins
        self.input_path = self._resolve_input_path()
        self.output_path = self._resolve_output_path()
        
        logger.info(
            "IngestCustomersJob initialisé",
            input_path=self.input_path,
            output_path=self.output_path,
            environment=config.environment,
        )
    
    def _resolve_input_path(self) -> str:
        """Résout le chemin d'entrée."""
        base_path = self.sales_config.raw_path
        
        # Vérifier si un chemin spécifique est fourni dans les paramètres
        if "input_path" in self.config.parameters:
            return self.config.parameters["input_path"]
        
        # Chemin par défaut
        return f"{base_path}/customers"
    
    def _resolve_output_path(self) -> str:
        """Résout le chemin de sortie."""
        base_path = self.sales_config.bronze_path
        return f"{base_path}/customers"
    
    def _get_input_schema(self) -> StructType:
        """
        Retourne le schéma des données d'entrée.
        
        Returns:
            Schéma Spark pour les fichiers clients raw
        """
        return StructType([
            StructField("customer_id", StringType(), False),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("birth_date", StringType(), True),
            StructField("gender", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("country", StringType(), True),
            StructField("registration_date", StringType(), True),
            StructField("is_active", StringType(), True),
            StructField("customer_segment", StringType(), True),
            StructField("preferred_language", StringType(), True),
            StructField("marketing_consent", StringType(), True),
        ])
    
    def extract(self, context: JobContext) -> DataFrame:
        """
        Extrait les données clients depuis la source.
        
        Args:
            context: Contexte d'exécution
            
        Returns:
            DataFrame brut des clients
        """
        logger.info(f"Extraction des données clients depuis: {self.input_path}")
        
        # Déterminer le format des fichiers
        file_format = self.config.parameters.get("file_format", "csv")
        
        if file_format == "csv":
            reader = CSVReader(
                spark=self.spark,
                path=self.input_path,
                header=True,
                delimiter=self.config.parameters.get("delimiter", ","),
                encoding=self.config.parameters.get("encoding", "utf-8"),
                schema=self._get_input_schema(),
                mode="PERMISSIVE",
                column_name_of_corrupt_record="_corrupt_record",
            )
        elif file_format == "json":
            reader = JSONReader(
                spark=self.spark,
                path=self.input_path,
                schema=self._get_input_schema(),
                multiline=self.config.parameters.get("multiline", False),
            )
        else:
            raise ValueError(f"Format de fichier non supporté: {file_format}")
        
        df = reader.read()
        
        # Log des statistiques d'extraction
        record_count = df.count()
        context.metrics["extracted_records"] = record_count
        logger.info(f"Extraction terminée: {record_count} enregistrements")
        
        # Vérifier les enregistrements corrompus
        if "_corrupt_record" in df.columns:
            corrupt_count = df.filter(F.col("_corrupt_record").isNotNull()).count()
            if corrupt_count > 0:
                logger.warning(f"Enregistrements corrompus détectés: {corrupt_count}")
                context.metrics["corrupt_records"] = corrupt_count
        
        return df
    
    def transform(self, df: DataFrame, context: JobContext) -> DataFrame:
        """
        Transforme les données clients.
        
        Args:
            df: DataFrame extrait
            context: Contexte d'exécution
            
        Returns:
            DataFrame transformé
        """
        logger.info("Transformation des données clients")
        
        # 1. Supprimer les enregistrements corrompus
        if "_corrupt_record" in df.columns:
            df = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
        
        # 2. Nettoyage et standardisation des colonnes texte
        df = self._clean_text_columns(df)
        
        # 3. Conversion des types de données
        df = self._convert_data_types(df)
        
        # 4. Standardisation des valeurs
        df = self._standardize_values(df)
        
        # 5. Création du nom complet
        df = self._add_full_name(df)
        
        # 6. Déduplication par customer_id
        df = self._deduplicate_customers(df, context)
        
        # 7. Ajout des métadonnées d'ingestion
        df = self._add_ingestion_metadata(df)
        
        # 8. Filtrer les clients valides (avec customer_id)
        df = df.filter(
            F.col("customer_id").isNotNull() & 
            (F.trim(F.col("customer_id")) != "")
        )
        
        # Log des statistiques
        final_count = df.count()
        context.metrics["transformed_records"] = final_count
        logger.info(f"Transformation terminée: {final_count} enregistrements")
        
        return df
    
    def _clean_text_columns(self, df: DataFrame) -> DataFrame:
        """Nettoie les colonnes texte."""
        text_columns = [
            "first_name", "last_name", "email", "phone",
            "address", "city", "state", "postal_code", "country",
            "customer_segment", "preferred_language",
        ]
        
        for col_name in text_columns:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    F.when(
                        F.col(col_name).isNotNull(),
                        F.trim(F.col(col_name))
                    ).otherwise(None)
                )
        
        return df
    
    def _convert_data_types(self, df: DataFrame) -> DataFrame:
        """Convertit les types de données."""
        # Conversion de la date de naissance
        if "birth_date" in df.columns:
            df = df.withColumn(
                "birth_date",
                F.coalesce(
                    F.to_date(F.col("birth_date"), "yyyy-MM-dd"),
                    F.to_date(F.col("birth_date"), "dd/MM/yyyy"),
                    F.to_date(F.col("birth_date"), "MM/dd/yyyy"),
                )
            )
        
        # Conversion de la date d'inscription
        if "registration_date" in df.columns:
            df = df.withColumn(
                "registration_date",
                F.coalesce(
                    F.to_timestamp(F.col("registration_date"), "yyyy-MM-dd HH:mm:ss"),
                    F.to_timestamp(F.col("registration_date"), "yyyy-MM-dd'T'HH:mm:ss"),
                    F.to_timestamp(F.col("registration_date"), "yyyy-MM-dd"),
                )
            )
        
        # Conversion des booléens
        boolean_columns = ["is_active", "marketing_consent"]
        for col_name in boolean_columns:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    F.when(
                        F.lower(F.col(col_name)).isin(["true", "1", "yes", "oui", "y"]),
                        F.lit(True)
                    ).when(
                        F.lower(F.col(col_name)).isin(["false", "0", "no", "non", "n"]),
                        F.lit(False)
                    ).otherwise(None).cast(BooleanType())
                )
        
        return df
    
    def _standardize_values(self, df: DataFrame) -> DataFrame:
        """Standardise les valeurs."""
        # Email en minuscules
        if "email" in df.columns:
            df = df.withColumn(
                "email",
                F.lower(F.col("email"))
            )
        
        # Genre standardisé
        if "gender" in df.columns:
            df = df.withColumn(
                "gender",
                F.when(
                    F.upper(F.col("gender")).isin(["M", "MALE", "HOMME", "H"]),
                    F.lit("M")
                ).when(
                    F.upper(F.col("gender")).isin(["F", "FEMALE", "FEMME"]),
                    F.lit("F")
                ).otherwise(F.lit("U"))  # Unknown
            )
        
        # Pays en majuscules (code ISO)
        if "country" in df.columns:
            df = df.withColumn(
                "country",
                F.upper(F.col("country"))
            )
        
        # Segment client standardisé
        if "customer_segment" in df.columns:
            df = df.withColumn(
                "customer_segment",
                F.when(
                    F.col("customer_segment").isNull(),
                    F.lit("STANDARD")
                ).otherwise(F.upper(F.col("customer_segment")))
            )
        
        # Langue préférée - défaut
        if "preferred_language" in df.columns:
            df = df.withColumn(
                "preferred_language",
                F.when(
                    F.col("preferred_language").isNull(),
                    F.lit("en")
                ).otherwise(F.lower(F.col("preferred_language")))
            )
        
        return df
    
    def _add_full_name(self, df: DataFrame) -> DataFrame:
        """Ajoute le nom complet du client."""
        if "first_name" in df.columns and "last_name" in df.columns:
            df = df.withColumn(
                "full_name",
                F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
            )
        return df
    
    def _deduplicate_customers(
        self,
        df: DataFrame,
        context: JobContext,
    ) -> DataFrame:
        """
        Déduplique les clients par customer_id.
        
        Garde l'enregistrement le plus récent basé sur registration_date.
        """
        before_count = df.count()
        
        # Utiliser une window function pour garder le plus récent
        window = Window.partitionBy("customer_id").orderBy(
            F.col("registration_date").desc_nulls_last()
        )
        
        df = df.withColumn("_row_num", F.row_number().over(window))
        df = df.filter(F.col("_row_num") == 1).drop("_row_num")
        
        after_count = df.count()
        duplicates_removed = before_count - after_count
        
        if duplicates_removed > 0:
            logger.info(f"Doublons supprimés: {duplicates_removed}")
            context.metrics["duplicates_removed"] = duplicates_removed
        
        return df
    
    def _add_ingestion_metadata(self, df: DataFrame) -> DataFrame:
        """Ajoute les métadonnées d'ingestion."""
        return df.withColumns({
            "_ingestion_timestamp": F.current_timestamp(),
            "_source_file": F.input_file_name(),
            "_job_id": F.lit(self.config.job_id),
            "_job_name": F.lit(self.config.job_name),
        })
    
    def validate(self, df: DataFrame, context: JobContext) -> DataFrame:
        """
        Valide la qualité des données clients.
        
        Args:
            df: DataFrame transformé
            context: Contexte d'exécution
            
        Returns:
            DataFrame validé
        """
        logger.info("Validation de la qualité des données clients")
        
        # Définir les règles de qualité
        rules = [
            # Règles critiques
            QualityRule(
                name="customer_id_not_null",
                column="customer_id",
                rule_type="not_null",
                severity=QualitySeverity.CRITICAL,
                description="L'identifiant client ne doit pas être null",
            ),
            QualityRule(
                name="customer_id_unique",
                column="customer_id",
                rule_type="unique",
                severity=QualitySeverity.CRITICAL,
                description="L'identifiant client doit être unique",
            ),
            
            # Règles importantes
            QualityRule(
                name="email_format",
                column="email",
                rule_type="regex",
                parameters={"pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"},
                severity=QualitySeverity.WARNING,
                description="L'email doit avoir un format valide",
                filter_condition="email IS NOT NULL",
            ),
            QualityRule(
                name="email_not_null",
                column="email",
                rule_type="not_null",
                severity=QualitySeverity.WARNING,
                description="L'email devrait être renseigné",
            ),
            QualityRule(
                name="first_name_not_null",
                column="first_name",
                rule_type="not_null",
                severity=QualitySeverity.WARNING,
                description="Le prénom devrait être renseigné",
            ),
            QualityRule(
                name="last_name_not_null",
                column="last_name",
                rule_type="not_null",
                severity=QualitySeverity.WARNING,
                description="Le nom devrait être renseigné",
            ),
            
            # Règles de cohérence
            QualityRule(
                name="gender_valid_values",
                column="gender",
                rule_type="in_set",
                parameters={"values": ["M", "F", "U"]},
                severity=QualitySeverity.WARNING,
                description="Le genre doit être M, F ou U",
            ),
            QualityRule(
                name="birth_date_reasonable",
                column="birth_date",
                rule_type="custom",
                parameters={
                    "expression": "birth_date IS NULL OR (birth_date > '1900-01-01' AND birth_date < current_date())"
                },
                severity=QualitySeverity.WARNING,
                description="La date de naissance doit être raisonnable",
            ),
            QualityRule(
                name="registration_date_not_future",
                column="registration_date",
                rule_type="custom",
                parameters={
                    "expression": "registration_date IS NULL OR registration_date <= current_timestamp()"
                },
                severity=QualitySeverity.WARNING,
                description="La date d'inscription ne peut pas être dans le futur",
            ),
            QualityRule(
                name="country_code_length",
                column="country",
                rule_type="custom",
                parameters={
                    "expression": "country IS NULL OR length(country) BETWEEN 2 AND 3"
                },
                severity=QualitySeverity.INFO,
                description="Le code pays devrait avoir 2-3 caractères",
            ),
        ]
        
        # Exécuter la validation
        checker = QualityChecker(rules=rules)
        validation_result = checker.validate(df)
        
        # Enregistrer les métriques
        context.metrics["quality_rules_total"] = len(rules)
        context.metrics["quality_rules_passed"] = validation_result.passed_count
        context.metrics["quality_rules_failed"] = validation_result.failed_count
        context.metrics["quality_score"] = validation_result.overall_score
        
        # Log du résumé
        logger.info(
            "Validation terminée",
            score=validation_result.overall_score,
            passed=validation_result.passed_count,
            failed=validation_result.failed_count,
        )
        
        # Log des échecs
        for result in validation_result.results:
            if not result.passed:
                log_method = (
                    logger.error if result.severity == QualitySeverity.CRITICAL
                    else logger.warning if result.severity == QualitySeverity.WARNING
                    else logger.info
                )
                log_method(
                    f"Règle échouée: {result.rule_name}",
                    valid_count=result.valid_count,
                    invalid_count=result.invalid_count,
                    valid_percentage=result.valid_percentage,
                )
        
        # Échouer si des règles critiques ont échoué
        critical_failures = [
            r for r in validation_result.results
            if not r.passed and r.severity == QualitySeverity.CRITICAL
        ]
        
        if critical_failures:
            error_msg = f"Règles critiques échouées: {[r.rule_name for r in critical_failures]}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        return df
    
    def load(self, df: DataFrame, context: JobContext) -> None:
        """
        Charge les données clients dans la zone bronze.
        
        Args:
            df: DataFrame validé
            context: Contexte d'exécution
        """
        logger.info(f"Chargement des données vers: {self.output_path}")
        
        # Sélectionner les colonnes finales dans l'ordre
        output_columns = [
            "customer_id",
            "first_name",
            "last_name",
            "full_name",
            "email",
            "phone",
            "birth_date",
            "gender",
            "address",
            "city",
            "state",
            "postal_code",
            "country",
            "registration_date",
            "is_active",
            "customer_segment",
            "preferred_language",
            "marketing_consent",
            "_ingestion_timestamp",
            "_source_file",
            "_job_id",
            "_job_name",
        ]
        
        # Ne garder que les colonnes qui existent
        existing_columns = [c for c in output_columns if c in df.columns]
        df = df.select(*existing_columns)
        
        # Déterminer le mode d'écriture
        write_mode = WriteMode.OVERWRITE
        if self.config.parameters.get("append_mode", False):
            write_mode = WriteMode.APPEND
        
        # Écrire en Parquet
        writer = ParquetWriter(
            path=self.output_path,
            mode=write_mode,
            partition_by=self.config.parameters.get("partition_by", None),
            compression="snappy",
        )
        
        output_path = writer.write(df)
        
        # Métriques de chargement
        context.metrics["output_path"] = output_path
        context.metrics["loaded_records"] = df.count()
        
        logger.info(
            "Chargement terminé",
            output_path=output_path,
            records=context.metrics["loaded_records"],
        )


def create_job(
    spark: SparkSession,
    environment: str = "dev",
    **kwargs: Any,
) -> IngestCustomersJob:
    """
    Factory function pour créer le job.
    
    Args:
        spark: Session Spark
        environment: Environnement d'exécution
        **kwargs: Paramètres supplémentaires du job
        
    Returns:
        Instance configurée du job
    """
    config = JobConfig(
        job_name="ingest_customers",
        environment=environment,
        parameters=kwargs,
    )
    
    return IngestCustomersJob(spark, config)


# Point d'entrée pour spark-submit
def main():
    """Point d'entrée principal."""
    import argparse
    from common.spark import create_spark_session
    
    parser = argparse.ArgumentParser(description="Job d'ingestion des clients")
    parser.add_argument(
        "--env",
        default="dev",
        choices=["dev", "staging", "prod"],
        help="Environnement d'exécution",
    )
    parser.add_argument(
        "--input-path",
        help="Chemin des fichiers d'entrée (optionnel)",
    )
    parser.add_argument(
        "--file-format",
        default="csv",
        choices=["csv", "json"],
        help="Format des fichiers d'entrée",
    )
    parser.add_argument(
        "--delimiter",
        default=",",
        help="Délimiteur CSV",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Mode append au lieu de overwrite",
    )
    
    args = parser.parse_args()
    
    # Construire les paramètres
    params = {
        "file_format": args.file_format,
        "delimiter": args.delimiter,
        "append_mode": args.append,
    }
    
    if args.input_path:
        params["input_path"] = args.input_path
    
    # Créer la session Spark
    spark = create_spark_session(
        app_name="ingest_customers",
        environment=args.env,
    )
    
    try:
        # Créer et exécuter le job
        job = create_job(spark, environment=args.env, **params)
        result = job.run()
        
        # Afficher le résumé
        print(f"\n{'='*60}")
        print(f"Job terminé: {result.status.value}")
        print(f"Durée: {result.duration_seconds:.2f} secondes")
        print(f"Métriques: {result.metrics}")
        print(f"{'='*60}\n")
        
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution du job: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
