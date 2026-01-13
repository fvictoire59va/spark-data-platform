"""Writer pour les fichiers Parquet."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pyspark.sql import DataFrame

from src.common.writers.base_writer import BaseWriter, WriteMode
from src.core.exceptions import WriteError
from src.core.logger import get_logger

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = get_logger(__name__)


class ParquetWriter(BaseWriter):
    """Writer pour les fichiers Parquet."""

    def __init__(self, spark: SparkSession, config: dict[str, Any]):
        """
        Initialise le Parquet writer.

        Config attendue:
            - path: Chemin de destination
            - compression: (optionnel) snappy, gzip, lz4, zstd
            - partition_by: (optionnel) Colonnes de partition
            - max_records_per_file: (optionnel) Records max par fichier
        """
        super().__init__(spark, config)
        self._validate_config(["path"])
        
        if "partition_by" in config:
            self.with_partitions(config["partition_by"])

    def write(self, df: DataFrame) -> None:
        """
        Écrit les données en format Parquet.

        Args:
            df: DataFrame à écrire

        Raises:
            WriteError: En cas d'erreur d'écriture
        """
        try:
            path = self.config["path"]
            compression = self.config.get("compression", "snappy")

            writer = (
                df.write
                .format("parquet")
                .option("compression", compression)
            )

            # Max records par fichier
            if "max_records_per_file" in self.config:
                writer = writer.option(
                    "maxRecordsPerFile",
                    self.config["max_records_per_file"]
                )

            # Partitionnement
            if self._partition_by:
                writer = writer.partitionBy(*self._partition_by)

            # Mode
            writer = writer.mode(self._mode.value)

            writer.save(path)

            logger.info(
                "Écriture Parquet réussie",
                path=path,
                mode=self._mode.value,
                compression=compression,
                partitions=self._partition_by or "none",
            )

        except Exception as e:
            raise WriteError(
                f"Erreur lors de l'écriture Parquet: {e}",
                details={"path": self.config["path"]},
            ) from e

    def write_partitioned_by_date(
        self,
        df: DataFrame,
        date_column: str,
        granularity: str = "day",
    ) -> None:
        """
        Écrit avec partitionnement par date automatique.

        Args:
            df: DataFrame à écrire
            date_column: Colonne de date
            granularity: 'day', 'month', 'year'
        """
        from pyspark.sql import functions as F

        # Ajouter les colonnes de partition
        if granularity == "day":
            df = (
                df.withColumn("year", F.year(date_column))
                .withColumn("month", F.month(date_column))
                .withColumn("day", F.dayofmonth(date_column))
            )
            self._partition_by = ["year", "month", "day"]
        elif granularity == "month":
            df = (
                df.withColumn("year", F.year(date_column))
                .withColumn("month", F.month(date_column))
            )
            self._partition_by = ["year", "month"]
        else:  # year
            df = df.withColumn("year", F.year(date_column))
            self._partition_by = ["year"]

        self.write(df)
