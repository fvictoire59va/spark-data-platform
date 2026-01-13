"""Reader pour les fichiers Parquet."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pyspark.sql import DataFrame

from src.common.readers.base_reader import BaseReader
from src.core.exceptions import DataSourceError
from src.core.logger import get_logger

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = get_logger(__name__)


class ParquetReader(BaseReader):
    """Lecteur pour les fichiers Parquet."""

    def __init__(self, spark: SparkSession, config: dict[str, Any]):
        """
        Initialise le Parquet reader.

        Config attendue:
            - path: Chemin vers les fichiers Parquet
            - merge_schema: (optionnel) Fusionner les schémas
            - path_glob_filter: (optionnel) Filtre glob pour les fichiers
        """
        super().__init__(spark, config)
        self._validate_config(["path"])

    def read(self) -> DataFrame:
        """
        Lit les données depuis les fichiers Parquet.

        Returns:
            DataFrame avec les données

        Raises:
            DataSourceError: En cas d'erreur de lecture
        """
        try:
            path = self.config["path"]
            
            reader = self.spark.read.format("parquet")
            
            # Options
            if self.config.get("merge_schema", False):
                reader = reader.option("mergeSchema", "true")
            
            if "path_glob_filter" in self.config:
                reader = reader.option("pathGlobFilter", self.config["path_glob_filter"])

            if self._schema is not None:
                reader = reader.schema(self._schema)

            df = reader.load(path)

            logger.info(
                "Lecture Parquet réussie",
                path=path,
                partitions=df.rdd.getNumPartitions(),
            )

            return df

        except Exception as e:
            raise DataSourceError(
                f"Erreur lors de la lecture Parquet: {e}",
                details={"path": self.config["path"]},
            ) from e
