"""Reader pour les streams Kafka."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal

from pyspark.sql import DataFrame

from src.common.readers.base_reader import BaseReader
from src.core.exceptions import DataSourceError
from src.core.logger import get_logger

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = get_logger(__name__)


class KafkaReader(BaseReader):
    """Lecteur pour Apache Kafka (batch et streaming)."""

    def __init__(self, spark: SparkSession, config: dict[str, Any]):
        """
        Initialise le Kafka reader.

        Config attendue:
            - bootstrap_servers: Serveurs Kafka
            - topic: Topic(s) à lire
            - starting_offsets: (optionnel) "earliest", "latest" ou JSON
            - mode: (optionnel) "batch" ou "stream"
            - group_id: (optionnel) Consumer group ID
            - max_offsets_per_trigger: (optionnel) Pour streaming
        """
        super().__init__(spark, config)
        self._validate_config(["bootstrap_servers", "topic"])

    def read(self, mode: Literal["batch", "stream"] = "batch") -> DataFrame:
        """
        Lit les données depuis Kafka.

        Args:
            mode: Mode de lecture (batch ou stream)

        Returns:
            DataFrame avec les données

        Raises:
            DataSourceError: En cas d'erreur de lecture
        """
        try:
            kafka_options = {
                "kafka.bootstrap.servers": self.config["bootstrap_servers"],
                "subscribe": self.config["topic"],
            }

            # Options optionnelles
            starting_offsets = self.config.get("starting_offsets", "latest")
            kafka_options["startingOffsets"] = starting_offsets

            if "group_id" in self.config:
                kafka_options["kafka.group.id"] = self.config["group_id"]

            if "max_offsets_per_trigger" in self.config:
                kafka_options["maxOffsetsPerTrigger"] = str(self.config["max_offsets_per_trigger"])

            # Choix batch ou streaming
            if mode == "stream":
                df = self.spark.readStream.format("kafka").options(**kafka_options).load()
            else:
                df = self.spark.read.format("kafka").options(**kafka_options).load()

            logger.info(
                "Lecture Kafka configurée",
                topic=self.config["topic"],
                mode=mode,
                starting_offsets=starting_offsets,
            )

            return df

        except Exception as e:
            raise DataSourceError(
                f"Erreur lors de la lecture Kafka: {e}",
                details={
                    "topic": self.config["topic"],
                    "bootstrap_servers": self.config["bootstrap_servers"],
                },
            ) from e

    def read_with_schema(
        self, value_schema: str, mode: Literal["batch", "stream"] = "batch"
    ) -> DataFrame:
        """
        Lit et parse les messages avec un schéma JSON.

        Args:
            value_schema: Schéma JSON pour parser la valeur
            mode: Mode de lecture

        Returns:
            DataFrame avec les données parsées
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType

        df = self.read(mode)

        # Parse la valeur comme JSON
        df = df.select(
            F.col("key").cast(StringType()).alias("key"),
            F.from_json(F.col("value").cast(StringType()), value_schema).alias("value"),
            F.col("timestamp"),
            F.col("partition"),
            F.col("offset"),
        ).select("key", "value.*", "timestamp", "partition", "offset")

        return df
