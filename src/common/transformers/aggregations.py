# Classe
# Description

# AggregationTransformer - Agrégations groupées (SUM, AVG, COUNT...)

# WindowAggregationTransformer - Window functions (running total, rank...)

# PivotTransformer - Opérations de pivot

# UnpivotTransformer - Opérations d'unpivot/melt

# RollupTransformer - Sous-totaux hiérarchiques

# CubeTransformer - Toutes combinaisons de sous-totaux

# src/common/transformers/aggregations.py
"""Transformers pour les agrégations de données."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import Any

from common.logging import get_logger
from common.transformers.base_transformer import BaseTransformer
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.column import Column

logger = get_logger(__name__)


class AggregationType(Enum):
    """Types d'agrégations supportées."""

    SUM = "sum"
    COUNT = "count"
    AVG = "avg"
    MEAN = "mean"
    MIN = "min"
    MAX = "max"
    FIRST = "first"
    LAST = "last"
    COLLECT_LIST = "collect_list"
    COLLECT_SET = "collect_set"
    COUNT_DISTINCT = "count_distinct"
    STDDEV = "stddev"
    VARIANCE = "variance"
    MEDIAN = "median"
    PERCENTILE = "percentile"


@dataclass
class AggregationSpec:
    """Spécification d'une agrégation."""

    column: str
    agg_type: AggregationType | str
    alias: str | None = None
    percentile_value: float = 0.5  # Pour PERCENTILE

    def __post_init__(self) -> None:
        """Convertit le type si nécessaire."""
        if isinstance(self.agg_type, str):
            self.agg_type = AggregationType(self.agg_type.lower())

        if not self.alias:
            self.alias = f"{self.column}_{self.agg_type.value}"


class AggregationTransformer(BaseTransformer):
    """
    Transformer pour les agrégations groupées.

    Supporte les agrégations simples et multiples avec
    différentes fonctions d'agrégation.

    Example:
        >>> transformer = AggregationTransformer(
        ...     group_by=["category", "region"],
        ...     aggregations=[
        ...         AggregationSpec("amount", AggregationType.SUM, "total_amount"),
        ...         AggregationSpec("quantity", AggregationType.AVG, "avg_quantity"),
        ...         AggregationSpec("order_id", AggregationType.COUNT, "order_count"),
        ...     ]
        ... )
        >>> result_df = transformer.transform(df)
    """

    def __init__(
        self,
        group_by: list[str],
        aggregations: list[AggregationSpec],
        having: Column | None = None,
        order_by: list[str] | None = None,
        ascending: bool | list[bool] = True,
    ):
        """
        Initialise l'AggregationTransformer.

        Args:
            group_by: Colonnes de groupement
            aggregations: Liste des spécifications d'agrégation
            having: Condition de filtrage post-agrégation
            order_by: Colonnes de tri du résultat
            ascending: Ordre de tri (True = ascendant)
        """
        self.group_by = group_by
        self.aggregations = aggregations
        self.having = having
        self.order_by = order_by
        self.ascending = ascending

        logger.debug(
            "AggregationTransformer initialisé",
            group_by=self.group_by,
            aggregations_count=len(self.aggregations),
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Applique les agrégations au DataFrame.

        Args:
            df: DataFrame source

        Returns:
            DataFrame agrégé
        """
        logger.info(
            "Application des agrégations",
            group_by=self.group_by,
            aggregations_count=len(self.aggregations),
        )

        # Construire les expressions d'agrégation
        agg_exprs = [self._build_agg_expr(spec) for spec in self.aggregations]

        # Appliquer le groupement et les agrégations
        result = df.groupBy(*self.group_by).agg(*agg_exprs)

        # Appliquer le filtre HAVING si présent
        if self.having is not None:
            result = result.filter(self.having)

        # Appliquer le tri si demandé
        if self.order_by:
            if isinstance(self.ascending, bool):
                ascending_list = [self.ascending] * len(self.order_by)
            else:
                ascending_list = self.ascending

            order_cols = [
                F.col(col).asc() if asc else F.col(col).desc()
                for col, asc in zip(self.order_by, ascending_list, strict=False)
            ]
            result = result.orderBy(*order_cols)

        logger.info(f"Agrégation terminée: {result.count()} groupes")

        return result

    def _build_agg_expr(self, spec: AggregationSpec) -> Column:
        """
        Construit l'expression d'agrégation Spark.

        Args:
            spec: Spécification d'agrégation

        Returns:
            Expression Column
        """
        col = F.col(spec.column)

        agg_map: dict[AggregationType, Callable[[], Column]] = {
            AggregationType.SUM: lambda: F.sum(col),
            AggregationType.COUNT: lambda: F.count(col),
            AggregationType.AVG: lambda: F.avg(col),
            AggregationType.MEAN: lambda: F.mean(col),
            AggregationType.MIN: lambda: F.min(col),
            AggregationType.MAX: lambda: F.max(col),
            AggregationType.FIRST: lambda: F.first(col),
            AggregationType.LAST: lambda: F.last(col),
            AggregationType.COLLECT_LIST: lambda: F.collect_list(col),
            AggregationType.COLLECT_SET: lambda: F.collect_set(col),
            AggregationType.COUNT_DISTINCT: lambda: F.countDistinct(col),
            AggregationType.STDDEV: lambda: F.stddev(col),
            AggregationType.VARIANCE: lambda: F.variance(col),
            AggregationType.MEDIAN: lambda: F.percentile_approx(col, 0.5),
            AggregationType.PERCENTILE: lambda: F.percentile_approx(col, spec.percentile_value),
        }

        agg_type = spec.agg_type if isinstance(spec.agg_type, AggregationType) else AggregationType(spec.agg_type)
        agg_func = agg_map.get(agg_type)
        if not agg_func:
            raise ValueError(f"Type d'agrégation non supporté: {agg_type}")

        alias = spec.alias if isinstance(spec.alias, str) else ""
        return agg_func().alias(alias)


class WindowAggregationTransformer(BaseTransformer):
    """
    Transformer pour les agrégations avec fenêtres (Window Functions).

    Example:
        >>> transformer = WindowAggregationTransformer(
        ...     partition_by=["customer_id"],
        ...     order_by=["order_date"],
        ...     aggregations=[
        ...         WindowAggSpec("amount", "sum", "running_total"),
        ...         WindowAggSpec("amount", "row_number", "order_rank"),
        ...     ]
        ... )
        >>> result_df = transformer.transform(df)
    """

    def __init__(
        self,
        partition_by: list[str],
        order_by: list[str] | None = None,
        aggregations: list[WindowAggSpec] | None = None,
        ascending: bool | list[bool] = True,
        rows_between: tuple[int | None, int | None] | None = None,
        range_between: tuple[int | None, int | None] | None = None,
    ):
        """
        Initialise le WindowAggregationTransformer.

        Args:
            partition_by: Colonnes de partitionnement
            order_by: Colonnes de tri dans la fenêtre
            aggregations: Liste des spécifications d'agrégation
            ascending: Ordre de tri
            rows_between: Bornes de la fenêtre en lignes (start, end)
            range_between: Bornes de la fenêtre en valeurs (start, end)
        """
        self.partition_by = partition_by
        self.order_by = order_by or []
        self.aggregations = aggregations or []
        self.ascending = ascending
        self.rows_between = rows_between
        self.range_between = range_between

        logger.debug(
            "WindowAggregationTransformer initialisé",
            partition_by=self.partition_by,
            order_by=self.order_by,
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Applique les agrégations de fenêtre au DataFrame.

        Args:
            df: DataFrame source

        Returns:
            DataFrame avec les colonnes de fenêtre ajoutées
        """
        logger.info(
            "Application des window functions",
            partition_by=self.partition_by,
            aggregations_count=len(self.aggregations),
        )

        # Construire la spécification de fenêtre
        window_spec = self._build_window_spec()

        # Appliquer chaque agrégation
        result = df
        for spec in self.aggregations:
            agg_expr = self._build_window_expr(spec, window_spec)
            result = result.withColumn(spec.alias, agg_expr)

        logger.info(f"Window functions appliquées: {len(self.aggregations)} colonnes ajoutées")

        return result

    def _build_window_spec(self) -> Any:
        """
        Construit la spécification de fenêtre.

        Returns:
            WindowSpec configurée
        """
        # Partitionnement
        spec = Window.partitionBy(*self.partition_by)

        # Tri
        if self.order_by:
            if isinstance(self.ascending, bool):
                ascending_list = [self.ascending] * len(self.order_by)
            else:
                ascending_list = self.ascending

            order_cols = [
                F.col(col).asc() if asc else F.col(col).desc()
                for col, asc in zip(self.order_by, ascending_list, strict=False)
            ]
            spec = spec.orderBy(*order_cols)

        # Bornes de fenêtre
        if self.rows_between:
            start, end = self.rows_between
            start = start if start is not None else Window.unboundedPreceding
            end = end if end is not None else Window.unboundedFollowing
            spec = spec.rowsBetween(start, end)
        elif self.range_between:
            start, end = self.range_between
            start = start if start is not None else Window.unboundedPreceding
            end = end if end is not None else Window.unboundedFollowing
            spec = spec.rangeBetween(start, end)

        return spec

    def _build_window_expr(self, spec: WindowAggSpec, window_spec: Any) -> Column:
        """
        Construit l'expression de fenêtre.

        Args:
            spec: Spécification d'agrégation
            window_spec: Spécification de fenêtre

        Returns:
            Expression Column
        """
        col = F.col(spec.column) if spec.column else None

        # Fonctions de fenêtre sans colonne
        if spec.agg_type == "row_number":
            return F.row_number().over(window_spec)
        elif spec.agg_type == "rank":
            return F.rank().over(window_spec)
        elif spec.agg_type == "dense_rank":
            return F.dense_rank().over(window_spec)
        elif spec.agg_type == "percent_rank":
            return F.percent_rank().over(window_spec)
        elif spec.agg_type == "ntile":
            return F.ntile(spec.n_tiles).over(window_spec)
        elif spec.agg_type == "cume_dist":
            return F.cume_dist().over(window_spec)

        # Fonctions de fenêtre avec colonne
        if col is None:
            raise ValueError(f"Colonne requise pour l'agrégation: {spec.agg_type}")

        window_funcs: dict[str, Callable[[], Column]] = {
            "sum": lambda: F.sum(col).over(window_spec),
            "avg": lambda: F.avg(col).over(window_spec),
            "mean": lambda: F.mean(col).over(window_spec),
            "min": lambda: F.min(col).over(window_spec),
            "max": lambda: F.max(col).over(window_spec),
            "count": lambda: F.count(col).over(window_spec),
            "first": lambda: F.first(col).over(window_spec),
            "last": lambda: F.last(col).over(window_spec),
            "lead": lambda: F.lead(col, spec.offset, spec.default_value).over(window_spec),
            "lag": lambda: F.lag(col, spec.offset, spec.default_value).over(window_spec),
            "stddev": lambda: F.stddev(col).over(window_spec),
            "variance": lambda: F.variance(col).over(window_spec),
        }

        func = window_funcs.get(spec.agg_type)
        if not func:
            raise ValueError(f"Type d'agrégation de fenêtre non supporté: {spec.agg_type}")

        return func()


@dataclass
class WindowAggSpec:
    """Spécification d'une agrégation de fenêtre."""

    column: str | None
    agg_type: str
    alias: str
    offset: int = 1  # Pour lead/lag
    default_value: Any = None  # Pour lead/lag
    n_tiles: int = 4  # Pour ntile


class PivotTransformer(BaseTransformer):
    """
    Transformer pour les opérations de pivot.

    Example:
        >>> transformer = PivotTransformer(
        ...     group_by=["year"],
        ...     pivot_column="month",
        ...     agg_column="sales",
        ...     agg_func="sum",
        ...     pivot_values=["Jan", "Feb", "Mar"],
        ... )
        >>> result_df = transformer.transform(df)
    """

    def __init__(
        self,
        group_by: list[str],
        pivot_column: str,
        agg_column: str,
        agg_func: str = "sum",
        pivot_values: list[Any] | None = None,
    ):
        """
        Initialise le PivotTransformer.

        Args:
            group_by: Colonnes de groupement
            pivot_column: Colonne à pivoter
            agg_column: Colonne à agréger
            agg_func: Fonction d'agrégation (sum, avg, count, etc.)
            pivot_values: Valeurs spécifiques à pivoter (optionnel)
        """
        self.group_by = group_by
        self.pivot_column = pivot_column
        self.agg_column = agg_column
        self.agg_func = agg_func
        self.pivot_values = pivot_values

        logger.debug(
            "PivotTransformer initialisé",
            pivot_column=self.pivot_column,
            agg_func=self.agg_func,
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Applique le pivot au DataFrame.

        Args:
            df: DataFrame source

        Returns:
            DataFrame pivoté
        """
        logger.info(
            "Application du pivot",
            pivot_column=self.pivot_column,
            agg_func=self.agg_func,
        )

        # Groupement
        grouped = df.groupBy(*self.group_by)

        # Pivot avec ou sans valeurs spécifiques
        if self.pivot_values:
            pivoted = grouped.pivot(self.pivot_column, self.pivot_values)
        else:
            pivoted = grouped.pivot(self.pivot_column)

        # Agrégation
        agg_expr = self._get_agg_expr()
        result = pivoted.agg(agg_expr)

        logger.info(f"Pivot terminé: {len(result.columns)} colonnes")

        return result

    def _get_agg_expr(self) -> Column:
        """Retourne l'expression d'agrégation."""
        col = F.col(self.agg_column)

        agg_funcs: dict[str, Callable[[], Column]] = {
            "sum": lambda: F.sum(col),
            "avg": lambda: F.avg(col),
            "mean": lambda: F.mean(col),
            "count": lambda: F.count(col),
            "min": lambda: F.min(col),
            "max": lambda: F.max(col),
            "first": lambda: F.first(col),
            "last": lambda: F.last(col),
        }

        func = agg_funcs.get(self.agg_func.lower())
        if not func:
            raise ValueError(f"Fonction d'agrégation non supportée: {self.agg_func}")

        return func()


class UnpivotTransformer(BaseTransformer):
    """
    Transformer pour les opérations d'unpivot (melt).

    Example:
        >>> transformer = UnpivotTransformer(
        ...     id_columns=["year", "product"],
        ...     value_columns=["Jan", "Feb", "Mar"],
        ...     variable_column="month",
        ...     value_column="sales",
        ... )
        >>> result_df = transformer.transform(df)
    """

    def __init__(
        self,
        id_columns: list[str],
        value_columns: list[str],
        variable_column: str = "variable",
        value_column: str = "value",
    ):
        """
        Initialise l'UnpivotTransformer.

        Args:
            id_columns: Colonnes à conserver (identifiants)
            value_columns: Colonnes à transformer en lignes
            variable_column: Nom de la colonne pour les noms de variables
            value_column: Nom de la colonne pour les valeurs
        """
        self.id_columns = id_columns
        self.value_columns = value_columns
        self.variable_column = variable_column
        self.value_column = value_column

        logger.debug(
            "UnpivotTransformer initialisé",
            id_columns=self.id_columns,
            value_columns=self.value_columns,
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Applique l'unpivot au DataFrame.

        Args:
            df: DataFrame source

        Returns:
            DataFrame dépivoté
        """
        logger.info(
            "Application de l'unpivot",
            value_columns=self.value_columns,
        )

        # Utiliser stack pour l'unpivot
        # Construire l'expression stack
        stack_expr = f"stack({len(self.value_columns)}"
        for col_name in self.value_columns:
            stack_expr += f", '{col_name}', `{col_name}`"
        stack_expr += f") as ({self.variable_column}, {self.value_column})"

        # Sélectionner les colonnes ID et appliquer stack
        result = df.select(
            *self.id_columns,
            F.expr(stack_expr),
        )

        logger.info(f"Unpivot terminé: {result.count()} lignes")

        return result


class RollupTransformer(BaseTransformer):
    """
    Transformer pour les opérations ROLLUP.

    Crée des sous-totaux hiérarchiques.

    Example:
        >>> transformer = RollupTransformer(
        ...     columns=["region", "country", "city"],
        ...     aggregations=[
        ...         AggregationSpec("sales", AggregationType.SUM, "total_sales"),
        ...     ]
        ... )
        >>> result_df = transformer.transform(df)
    """

    def __init__(
        self,
        columns: list[str],
        aggregations: list[AggregationSpec],
    ):
        """
        Initialise le RollupTransformer.

        Args:
            columns: Colonnes pour le rollup (ordre hiérarchique)
            aggregations: Agrégations à appliquer
        """
        self.columns = columns
        self.aggregations = aggregations

        logger.debug(
            "RollupTransformer initialisé",
            columns=self.columns,
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Applique le rollup au DataFrame.

        Args:
            df: DataFrame source

        Returns:
            DataFrame avec sous-totaux
        """
        logger.info("Application du rollup", columns=self.columns)

        # Construire les expressions d'agrégation
        agg_exprs = [self._build_agg_expr(spec) for spec in self.aggregations]

        # Appliquer rollup
        result = df.rollup(*self.columns).agg(*agg_exprs)

        logger.info(f"Rollup terminé: {result.count()} lignes")

        return result

    def _build_agg_expr(self, spec: AggregationSpec) -> Column:
        """Construit l'expression d'agrégation."""
        col = F.col(spec.column)

        agg_map = {
            AggregationType.SUM: F.sum(col),
            AggregationType.COUNT: F.count(col),
            AggregationType.AVG: F.avg(col),
            AggregationType.MIN: F.min(col),
            AggregationType.MAX: F.max(col),
        }

        agg_type = spec.agg_type if isinstance(spec.agg_type, AggregationType) else AggregationType(spec.agg_type)
        agg_expr = agg_map.get(agg_type)
        if not agg_expr:
            raise ValueError(f"Type non supporté pour rollup: {agg_type}")

        return agg_expr.alias(spec.alias)


class CubeTransformer(BaseTransformer):
    """
    Transformer pour les opérations CUBE.

    Crée toutes les combinaisons de sous-totaux possibles.

    Example:
        >>> transformer = CubeTransformer(
        ...     columns=["region", "product"],
        ...     aggregations=[
        ...         AggregationSpec("sales", AggregationType.SUM, "total_sales"),
        ...     ]
        ... )
        >>> result_df = transformer.transform(df)
    """

    def __init__(
        self,
        columns: list[str],
        aggregations: list[AggregationSpec],
    ):
        """
        Initialise le CubeTransformer.

        Args:
            columns: Colonnes pour le cube
            aggregations: Agrégations à appliquer
        """
        self.columns = columns
        self.aggregations = aggregations

        logger.debug("CubeTransformer initialisé", columns=self.columns)

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Applique le cube au DataFrame.

        Args:
            df: DataFrame source

        Returns:
            DataFrame avec toutes les combinaisons de sous-totaux
        """
        logger.info("Application du cube", columns=self.columns)

        # Construire les expressions d'agrégation
        agg_exprs = [self._build_agg_expr(spec) for spec in self.aggregations]

        # Appliquer cube
        result = df.cube(*self.columns).agg(*agg_exprs)

        logger.info(f"Cube terminé: {result.count()} lignes")

        return result

    def _build_agg_expr(self, spec: AggregationSpec) -> Column:
        """Construit l'expression d'agrégation."""
        col = F.col(spec.column)

        agg_map = {
            AggregationType.SUM: F.sum(col),
            AggregationType.COUNT: F.count(col),
            AggregationType.AVG: F.avg(col),
            AggregationType.MIN: F.min(col),
            AggregationType.MAX: F.max(col),
        }

        agg_type = spec.agg_type if isinstance(spec.agg_type, AggregationType) else AggregationType(spec.agg_type)
        agg_expr = agg_map.get(agg_type)
        if not agg_expr:
            raise ValueError(f"Type non supporté pour cube: {agg_type}")

        return agg_expr.alias(spec.alias)


# Fonctions utilitaires
def aggregate_by_group(
    df: DataFrame,
    group_by: list[str],
    agg_dict: dict[str, str],
) -> DataFrame:
    """
    Agrégation simplifiée avec dictionnaire.

    Args:
        df: DataFrame source
        group_by: Colonnes de groupement
        agg_dict: Dictionnaire {colonne: fonction_agg}

    Returns:
        DataFrame agrégé

    Example:
        >>> result = aggregate_by_group(
        ...     df,
        ...     group_by=["category"],
        ...     agg_dict={"amount": "sum", "quantity": "avg"}
        ... )
    """
    agg_exprs: list[Any] = []

    for col_name, agg_func in agg_dict.items():
        col = F.col(col_name)

        func_map = {
            "sum": F.sum(col),
            "avg": F.avg(col),
            "mean": F.mean(col),
            "count": F.count(col),
            "min": F.min(col),
            "max": F.max(col),
            "first": F.first(col),
            "last": F.last(col),
            "count_distinct": F.countDistinct(col),
        }

        expr = func_map.get(agg_func.lower())
        if expr:
            agg_exprs.append(expr.alias(f"{col_name}_{agg_func}"))

    return df.groupBy(*group_by).agg(*agg_exprs)


def add_running_total(
    df: DataFrame,
    partition_by: list[str],
    order_by: str,
    value_column: str,
    result_column: str = "running_total",
) -> DataFrame:
    """
    Ajoute une colonne de total cumulé.

    Args:
        df: DataFrame source
        partition_by: Colonnes de partitionnement
        order_by: Colonne de tri
        value_column: Colonne à cumuler
        result_column: Nom de la colonne résultat

    Returns:
        DataFrame avec total cumulé
    """
    window = (
        Window.partitionBy(*partition_by)
        .orderBy(order_by)
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    return df.withColumn(result_column, F.sum(F.col(value_column)).over(window))


def add_rank(
    df: DataFrame,
    partition_by: list[str],
    order_by: str,
    result_column: str = "rank",
    ascending: bool = True,
    dense: bool = False,
) -> DataFrame:
    """
    Ajoute une colonne de rang.

    Args:
        df: DataFrame source
        partition_by: Colonnes de partitionnement
        order_by: Colonne de tri
        result_column: Nom de la colonne résultat
        ascending: Ordre ascendant ou descendant
        dense: Si True, utilise dense_rank

    Returns:
        DataFrame avec rang
    """
    order_col = F.col(order_by).asc() if ascending else F.col(order_by).desc()
    window = Window.partitionBy(*partition_by).orderBy(order_col)

    rank_func = F.dense_rank() if dense else F.rank()

    return df.withColumn(result_column, rank_func.over(window))
