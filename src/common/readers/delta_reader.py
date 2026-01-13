"""Reader pour les tables Delta Lake."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pyspark.sql import DataFrame

from src.common.readers.base_reader import BaseReader
from src.core.exceptions import DataSourceError
from src.core.logger import get_logger

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = get_logger(__name__)


class DeltaReader(BaseReader):
    """Lecteur pour les tables Delta Lake."""

    def __init__(self, spark: SparkSession, config: dict[str, Any]):
        """
        Initialise le Delta reader.

        Config attendue:
            - path: Chemin vers la table Delta
            - version: (optionnel) Version spécifique à lire
            - timestamp: (optionnel) Timestamp pour time travel
        """
        super().__init__(spark, config)
        self._validate_config(["path"])

    def read(self) -> DataFrame:
        """
        Lit les données depuis la table Delta.

        Returns:
            DataFrame avec les données

        Raises:
            DataSourceError: En cas d'erreur de lecture
        """
        try:
            path = self.config["path"]
            reader = self.spark.read.format("delta")

            # Time travel par version
            if "version" in self.config:
                reader = reader.option("versionAsOf", self.config["version"])
                logger.info(f"Lecture Delta version {self.config['version']}")

            # Time travel par timestamp
            elif "timestamp" in self.config:
                reader = reader.option("timestampAsOf", self.config["timestamp"])
                logger.info(f"Lecture Delta au timestamp {self.config['timestamp']}")

            df = reader.load(path)

            logger.info(
                "Lecture Delta réussie",
                path=path,
                partitions=df.rdd.getNumPartitions(),
            )

            return df

        except Exception as e:
            raise DataSourceError(
                f"Erreur lors de la lecture Delta: {e}",
                details={"path": self.config["path"]},
            ) from e

    def read_changes(
        self,
        starting_version: int | None = None,
        starting_timestamp: str | None = None,
        ending_version: int | None = None,
        ending_timestamp: str | None = None,
    ) -> DataFrame:
        """
        Lit les changements (CDC) depuis une table Delta.

        Args:
            starting_version: Version de départ
            starting_timestamp: Timestamp de départ
            ending_version: Version de fin
            ending_timestamp: Timestamp de fin

        Returns:
            DataFrame avec les changements
        """
        try:
            reader = (
                self.spark.read
                .format("delta")
                .option("readChangeFeed", "true")
            )

            if starting_version is not None:
                reader = reader.option("startingVersion", starting_version)
            elif starting_timestamp is not None:
                reader = reader.option("startingTimestamp", starting_timestamp)

            if ending_version is not None:
                reader = reader.option("endingVersion", ending_version)
            elif ending_timestamp is not None:
                reader = reader.option("endingTimestamp", ending_timestamp)

            return reader.load(self.config["path"])

        except Exception as e:
            raise DataSourceError(
                f"Erreur lors de la lecture CDC Delta: {e}",
                details={"path": self.config["path"]},
            ) from e

    def get_history(self, limit: int | None = None) -> DataFrame:
        """
        Récupère l'historique de la table Delta.

        Args:
            limit: Nombre maximum d'entrées

        Returns:
            DataFrame avec l'historique
        """
        from delta.tables import DeltaTable

        delta_table = DeltaTable.forPath(self.spark, self.config["path"])
        history = delta_table.history()

        if limit:
            history = history.limit(limit)

        return history
