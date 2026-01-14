"""Transformer avancé de déduplication."""
from __future__ import annotations

from typing import Literal

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.common.transformers.base_transformer import BaseTransformer
from src.core.logger import get_logger

logger = get_logger(__name__)


class DeduplicationTransformer(BaseTransformer):
    """Déduplication avancée avec stratégie de sélection."""

    def __init__(
        self,
        key_columns: list[str],
        order_column: str,
        order_direction: Literal["asc", "desc"] = "desc",
        keep: Literal["first", "last"] = "first",
    ):
        """
        Args:
            key_columns: Colonnes définissant l'unicité
            order_column: Colonne pour ordonner les doublons
            order_direction: Direction du tri ('asc' ou 'desc')
            keep: 'first' ou 'last' après tri
        """
        self.key_columns = key_columns
        self.order_column = order_column
        self.order_direction = order_direction
        self.keep = keep

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Déduplique en gardant le premier/dernier selon le tri.

        Returns:
            DataFrame dédupliqué
        """
        # Créer la window
        window = Window.partitionBy(*self.key_columns)

        # Appliquer le tri
        if self.order_direction == "desc":
            window = window.orderBy(F.col(self.order_column).desc())
        else:
            window = window.orderBy(F.col(self.order_column).asc())

        # Ajouter le numéro de ligne
        df_with_row = df.withColumn("_row_num", F.row_number().over(window))

        # Filtrer selon keep
        if self.keep == "first":
            df_deduped = df_with_row.filter(F.col("_row_num") == 1)
        else:
            # Pour 'last', on inverse la logique
            max_row = df_with_row.groupBy(*self.key_columns).agg(
                F.max("_row_num").alias("_max_row")
            )
            df_deduped = (
                df_with_row.join(max_row, self.key_columns)
                .filter(F.col("_row_num") == F.col("_max_row"))
                .drop("_max_row")
            )

        # Supprimer la colonne temporaire
        result = df_deduped.drop("_row_num")

        logger.info(
            "Déduplication terminée",
            key_columns=self.key_columns,
            order_column=self.order_column,
            strategy=f"{self.keep} by {self.order_direction}",
        )

        return result
