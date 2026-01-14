"""Writer pour les bases de données JDBC."""
from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from pyspark.sql import DataFrame

from src.common.writers.base_writer import BaseWriter, WriteMode
from src.core.exceptions import WriteError
from src.core.logger import get_logger

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = get_logger(__name__)


class JDBCWriter(BaseWriter):
    """Writer pour les bases de données via JDBC."""

    def __init__(self, spark: SparkSession, config: dict[str, Any]):
        """
        Initialise le JDBC writer.

        Config attendue:
            - url: URL JDBC
            - table: Table cible
            - user: Utilisateur
            - password: Mot de passe
            - driver: (optionnel) Classe du driver
            - batch_size: (optionnel) Taille des batchs
            - truncate: (optionnel) Truncate avant overwrite
        """
        super().__init__(spark, config)
        self._validate_config(["url", "table", "user", "password"])

    def write(self, df: DataFrame) -> None:
        """
        Écrit les données dans la table JDBC.

        Args:
            df: DataFrame à écrire

        Raises:
            WriteError: En cas d'erreur d'écriture
        """
        try:
            jdbc_options = {
                "url": self.config["url"],
                "dbtable": self.config["table"],
                "user": self.config["user"],
                "password": self.config["password"],
            }

            if "driver" in self.config:
                jdbc_options["driver"] = self.config["driver"]

            if "batch_size" in self.config:
                jdbc_options["batchsize"] = str(self.config["batch_size"])

            writer = df.write.format("jdbc").options(**jdbc_options)

            # Mode d'écriture
            if self._mode == WriteMode.OVERWRITE:
                if self.config.get("truncate", False):
                    jdbc_options["truncate"] = "true"
                writer = writer.mode("overwrite")
            elif self._mode == WriteMode.APPEND:
                writer = writer.mode("append")
            else:
                writer = writer.mode(self._mode.value)

            writer.save()

            logger.info(
                "Écriture JDBC réussie",
                table=self.config["table"],
                mode=self._mode.value,
                rows=df.count(),
            )

        except Exception as e:
            raise WriteError(
                f"Erreur lors de l'écriture JDBC: {e}",
                details={"table": self.config["table"]},
            ) from e

    def upsert(
        self,
        df: DataFrame,
        key_columns: list[str],
        update_columns: list[str] | None = None,
    ) -> None:
        """
        Effectue un upsert (INSERT ... ON CONFLICT UPDATE).

        Note: Implémentation spécifique selon le SGBD.
        Cette version est pour PostgreSQL.

        Args:
            df: DataFrame à upsert
            key_columns: Colonnes formant la clé primaire
            update_columns: Colonnes à mettre à jour (toutes si None)
        """
        # Créer une table temporaire
        temp_table = f"temp_{self.config['table']}_{int(time.time())}"

        try:
            # Écrire dans la table temp
            temp_config = {**self.config, "table": temp_table}
            temp_writer = JDBCWriter(self.spark, temp_config)
            temp_writer.with_mode(WriteMode.OVERWRITE).write(df)

            # Construire la requête d'upsert
            all_columns = df.columns
            update_cols = update_columns or [c for c in all_columns if c not in key_columns]

            update_set = ", ".join([f"{col} = source.{col}" for col in update_cols])

            insert_cols = ", ".join(all_columns)

            # Exécuter via JDBC
            upsert_sql = f"""
                INSERT INTO {self.config['table']} ({insert_cols})
                SELECT {insert_cols} FROM {temp_table} source
                ON CONFLICT ({', '.join(key_columns)})
                DO UPDATE SET {update_set}
            """

            self._execute_sql(upsert_sql)

            logger.info(
                "Upsert JDBC réussi",
                table=self.config["table"],
                key_columns=key_columns,
            )

        finally:
            # Nettoyer la table temp
            self._execute_sql(f"DROP TABLE IF EXISTS {temp_table}")

    def _execute_sql(self, sql: str) -> None:
        """Exécute une requête SQL via JDBC."""
        import jaydebeapi

        conn = jaydebeapi.connect(
            self.config.get("driver", ""),
            self.config["url"],
            [self.config["user"], self.config["password"]],
        )
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            conn.commit()
        finally:
            conn.close()
