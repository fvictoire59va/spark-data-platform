"""Writer pour les tables Delta Lake."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from delta.tables import DeltaTable
from pyspark.sql import DataFrame

from src.common.writers.base_writer import BaseWriter, WriteMode
from src.core.exceptions import WriteError
from src.core.logger import get_logger

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = get_logger(__name__)


class DeltaWriter(BaseWriter):
    """Writer pour les tables Delta Lake."""

    def __init__(self, spark: SparkSession, config: dict[str, Any]):
        """
        Initialise le Delta writer.

        Config attendue:
            - path: Chemin vers la table Delta
            - mode: (optionnel) Mode d'écriture
            - partition_by: (optionnel) Colonnes de partition
            - merge_keys: (optionnel) Clés pour le merge
            - merge_condition: (optionnel) Condition personnalisée de merge
        """
        super().__init__(spark, config)
        self._validate_config(["path"])
        
        # Appliquer la config
        if "mode" in config:
            self.with_mode(config["mode"])
        if "partition_by" in config:
            self.with_partitions(config["partition_by"])

    def write(self, df: DataFrame) -> None:
        """
        Écrit les données dans la table Delta.

        Args:
            df: DataFrame à écrire

        Raises:
            WriteError: En cas d'erreur d'écriture
        """
        try:
            path = self.config["path"]

            if self._mode == WriteMode.MERGE:
                self._merge(df)
            else:
                writer = df.write.format("delta").mode(self._mode.value)

                if self._partition_by:
                    writer = writer.partitionBy(*self._partition_by)

                writer.save(path)

                logger.info(
                    "Écriture Delta réussie",
                    path=path,
                    mode=self._mode.value,
                    partitions=self._partition_by,
                )

        except Exception as e:
            raise WriteError(
                f"Erreur lors de l'écriture Delta: {e}",
                details={"path": self.config["path"], "mode": self._mode.value},
            ) from e

    def _merge(self, df: DataFrame) -> None:
        """
        Exécute un merge (upsert) dans la table Delta.

        Args:
            df: DataFrame source pour le merge
        """
        path = self.config["path"]
        merge_keys = self.config.get("merge_keys", [])

        if not merge_keys:
            raise WriteError(
                "merge_keys requis pour le mode MERGE",
                details={"path": path},
            )

        # Vérifier si la table existe
        if not DeltaTable.isDeltaTable(self.spark, path):
            # Première écriture
            logger.info(f"Table Delta inexistante, création: {path}")
            self.with_mode(WriteMode.OVERWRITE).write(df)
            return

        delta_table = DeltaTable.forPath(self.spark, path)

        # Construire la condition de merge
        merge_condition = self.config.get("merge_condition")
        if not merge_condition:
            merge_condition = " AND ".join(
                [f"target.{key} = source.{key}" for key in merge_keys]
            )

        # Exécuter le merge
        (
            delta_table.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        logger.info(
            "Merge Delta réussi",
            path=path,
            merge_keys=merge_keys,
        )

    def optimize(self, z_order_columns: list[str] | None = None) -> None:
        """
        Optimise la table Delta.

        Args:
            z_order_columns: Colonnes pour le Z-ordering
        """
        path = self.config["path"]

        if not DeltaTable.isDeltaTable(self.spark, path):
            logger.warning(f"Table Delta inexistante, optimisation ignorée: {path}")
            return

        delta_table = DeltaTable.forPath(self.spark, path)

        if z_order_columns:
            delta_table.optimize().executeZOrderBy(*z_order_columns)
            logger.info(f"Optimisation Z-order terminée: {z_order_columns}")
        else:
            delta_table.optimize().executeCompaction()
            logger.info("Compaction terminée")

    def vacuum(self, retention_hours: int = 168) -> None:
        """
        Nettoie les anciens fichiers de la table Delta.

        Args:
            retention_hours: Durée de rétention en heures (défaut: 7 jours)
        """
        path = self.config["path"]

        if not DeltaTable.isDeltaTable(self.spark, path):
            logger.warning(f"Table Delta inexistante, vacuum ignoré: {path}")
            return

        delta_table = DeltaTable.forPath(self.spark, path)
        delta_table.vacuum(retention_hours)

        logger.info(f"Vacuum terminé avec rétention de {retention_hours}h")
