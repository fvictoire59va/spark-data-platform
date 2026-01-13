"""Writer pour Apache Kafka."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from pyspark.sql import DataFrame, functions as F

from src.common.writers.base_writer import BaseWriter
from src.core.exceptions import WriteError
from src.core.logger import get_logger

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.streaming import StreamingQuery

logger = get_logger(__name__)


class KafkaWriter(BaseWriter):
    """Writer pour Apache Kafka (batch et streaming)."""

    def __init__(self, spark: SparkSession, config: dict[str, Any]):
        """
        Initialise le Kafka writer.

        Config attendue:
            - bootstrap_servers: Serveurs Kafka
            - topic: Topic de destination
            - key_column: (optionnel) Colonne pour la clé
            - value_columns: (optionnel) Colonnes pour la valeur (toutes si non spécifié)
            - checkpoint_location: (requis pour streaming)
        """
        super().__init__(spark, config)
        self._validate_config(["bootstrap_servers", "topic"])

    def write(self, df: DataFrame) -> None:
        """
        Écrit les données dans Kafka (batch).

        Args:
            df: DataFrame à écrire. Doit avoir 'key' et 'value' ou sera converti.

        Raises:
            WriteError: En cas d'erreur d'écriture
        """
        try:
            df_kafka = self._prepare_dataframe(df)

            (
                df_kafka.write
                .format("kafka")
                .option("kafka.bootstrap.servers", self.config["bootstrap_servers"])
                .option("topic", self.config["topic"])
                .save()
            )

            logger.info(
                "Écriture Kafka batch réussie",
                topic=self.config["topic"],
                rows=df.count(),
            )

        except Exception as e:
            raise WriteError(
                f"Erreur lors de l'écriture Kafka: {e}",
                details={"topic": self.config["topic"]},
            ) from e

    def write_stream(
        self,
        df: DataFrame,
        output_mode: str = "append",
        trigger_interval: str | None = None,
        trigger_once: bool = False,
    ) -> StreamingQuery:
        """
        Écrit un stream dans Kafka.

        Args:
            df: Streaming DataFrame
            output_mode: 'append', 'complete', 'update'
            trigger_interval: Intervalle (ex: '10 seconds')
            trigger_once: Exécuter une seule fois

        Returns:
            StreamingQuery
        """
        if "checkpoint_location" not in self.config:
            raise WriteError(
                "checkpoint_location requis pour le streaming",
                details={"topic": self.config["topic"]},
            )

        df_kafka = self._prepare_dataframe(df)

        writer = (
            df_kafka.writeStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self.config["bootstrap_servers"])
            .option("topic", self.config["topic"])
            .option("checkpointLocation", self.config["checkpoint_location"])
            .outputMode(output_mode)
        )

        # Configuration du trigger
        if trigger_once:
            writer = writer.trigger(once=True)
        elif trigger_interval:
            writer = writer.trigger(processingTime=trigger_interval)

        query = writer.start()

        logger.info(
            "Stream Kafka démarré",
            topic=self.config["topic"],
            output_mode=output_mode,
            query_id=query.id,
        )

        return query

    def _prepare_dataframe(self, df: DataFrame) -> DataFrame:
        """
        Prépare le DataFrame pour Kafka (colonnes key/value).

        Args:
            df: DataFrame source

        Returns:
            DataFrame avec colonnes key et value
        """
        # Si déjà au bon format
        if "key" in df.columns and "value" in df.columns:
            return df.select(
                F.col("key").cast("string"),
                F.col("value").cast("string"),
            )

        # Construire key
        key_column = self.config.get("key_column")
        if key_column and key_column in df.columns:
            key_expr = F.col(key_column).cast("string")
        else:
            key_expr = F.lit(None).cast("string")

        # Construire value (JSON de toutes les colonnes ou sélection)
        value_columns = self.config.get("value_columns", df.columns)
        value_columns = [c for c in value_columns if c in df.columns]
        
        value_expr = F.to_json(F.struct(*[F.col(c) for c in value_columns]))

        return df.select(
            key_expr.alias("key"),
            value_expr.alias("value"),
        )
