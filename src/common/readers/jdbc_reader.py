"""Reader pour les sources JDBC."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pyspark.sql import DataFrame

from src.common.readers.base_reader import BaseReader
from src.core.exceptions import DataSourceError
from src.core.logger import get_logger

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = get_logger(__name__)


class JDBCReader(BaseReader):
    """Lecteur pour les bases de données via JDBC."""

    def __init__(self, spark: SparkSession, config: dict[str, Any]):
        """
        Initialise le JDBC reader.

        Config attendue:
            - url: URL JDBC de connexion
            - table: Nom de la table ou requête
            - user: Utilisateur
            - password: Mot de passe
            - driver: (optionnel) Classe du driver JDBC
            - fetch_size: (optionnel) Taille du fetch
            - partition_column: (optionnel) Colonne pour partitionnement
            - lower_bound: (optionnel) Borne inférieure
            - upper_bound: (optionnel) Borne supérieure
            - num_partitions: (optionnel) Nombre de partitions
        """
        super().__init__(spark, config)
        self._validate_config(["url", "table", "user", "password"])

    def read(self) -> DataFrame:
        """
        Lit les données depuis la base JDBC.

        Returns:
            DataFrame avec les données

        Raises:
            DataSourceError: En cas d'erreur de lecture
        """
        try:
            jdbc_options = {
                "url": self.config["url"],
                "dbtable": self.config["table"],
                "user": self.config["user"],
                "password": self.config["password"],
            }

            # Options optionnelles
            if "driver" in self.config:
                jdbc_options["driver"] = self.config["driver"]
            
            if "fetch_size" in self.config:
                jdbc_options["fetchsize"] = str(self.config["fetch_size"])

            # Partitionnement pour lecture parallèle
            if all(k in self.config for k in ["partition_column", "lower_bound", "upper_bound", "num_partitions"]):
                jdbc_options["partitionColumn"] = self.config["partition_column"]
                jdbc_options["lowerBound"] = str(self.config["lower_bound"])
                jdbc_options["upperBound"] = str(self.config["upper_bound"])
                jdbc_options["numPartitions"] = str(self.config["num_partitions"])

            reader = self.spark.read.format("jdbc").options(**jdbc_options)

            if self._schema is not None:
                reader = reader.schema(self._schema)

            df = reader.load()

            logger.info(
                "Lecture JDBC réussie",
                table=self.config["table"],
                partitions=df.rdd.getNumPartitions(),
            )

            return df

        except Exception as e:
            raise DataSourceError(
                f"Erreur lors de la lecture JDBC: {e}",
                details={"table": self.config["table"], "url": self.config["url"]},
            ) from e

    def read_query(self, query: str) -> DataFrame:
        """
        Exécute une requête SQL personnalisée.

        Args:
            query: Requête SQL

        Returns:
            DataFrame avec les résultats
        """
        config_with_query = {**self.config, "table": f"({query}) as subquery"}
        return JDBCReader(self.spark, config_with_query).read()
