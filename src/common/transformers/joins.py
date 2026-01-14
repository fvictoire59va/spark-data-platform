# Classe
# Description

# JoinTransformer
# Jointures standard

# MultiJoinTransformer
# Jointures multiples en chaîne

# LookupJoinTransformer
# Jointures de dimension optimisées

# SelfJoinTransformer
# Auto-jointures

# ConditionalJoinTransformer
# Jointures avec conditions complexes

# src/common/transformers/joins.py
"""Transformers pour les jointures de DataFrames."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from common.logging import get_logger
from common.transformers.base_transformer import BaseTransformer
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column

logger = get_logger(__name__)


class JoinType(Enum):
    """Types de jointures supportées."""

    INNER = "inner"
    LEFT = "left"
    LEFT_OUTER = "left_outer"
    RIGHT = "right"
    RIGHT_OUTER = "right_outer"
    FULL = "full"
    FULL_OUTER = "full_outer"
    CROSS = "cross"
    SEMI = "left_semi"
    LEFT_SEMI = "left_semi"
    ANTI = "left_anti"
    LEFT_ANTI = "left_anti"


@dataclass
class JoinCondition:
    """Condition de jointure."""

    left_column: str
    right_column: str
    operator: str = "="  # =, <, >, <=, >=, !=

    def to_expression(self, left_df: DataFrame, right_df: DataFrame) -> Column:
        """
        Convertit en expression Column.

        Args:
            left_df: DataFrame gauche
            right_df: DataFrame droit

        Returns:
            Expression de jointure
        """
        left_col = left_df[self.left_column]
        right_col = right_df[self.right_column]

        operators = {
            "=": left_col == right_col,
            "==": left_col == right_col,
            "!=": left_col != right_col,
            "<>": left_col != right_col,
            "<": left_col < right_col,
            ">": left_col > right_col,
            "<=": left_col <= right_col,
            ">=": left_col >= right_col,
        }

        expr = operators.get(self.operator)
        if expr is None:
            raise ValueError(f"Opérateur non supporté: {self.operator}")

        return expr


@dataclass
class JoinSpec:
    """Spécification complète d'une jointure."""

    right_df: DataFrame
    join_type: JoinType | str = JoinType.INNER
    conditions: list[JoinCondition] = field(default_factory=list)
    on_columns: list[str] | None = None  # Colonnes communes pour jointure simple
    broadcast: bool = False  # Broadcast hint pour petites tables
    right_alias: str | None = None
    drop_duplicates: bool = True  # Supprime les colonnes dupliquées

    def __post_init__(self) -> None:
        """Convertit le type si nécessaire."""
        if isinstance(self.join_type, str):
            self.join_type = JoinType(self.join_type.lower())


class JoinTransformer(BaseTransformer):
    """
    Transformer pour les jointures entre DataFrames.

    Supporte tous les types de jointures Spark avec options avancées.

    Example:
        >>> # Jointure simple sur colonnes communes
        >>> transformer = JoinTransformer(
        ...     right_df=customers_df,
        ...     on_columns=["customer_id"],
        ...     join_type=JoinType.LEFT,
        ... )
        >>> result = transformer.transform(orders_df)

        >>> # Jointure avec conditions multiples
        >>> transformer = JoinTransformer(
        ...     right_df=products_df,
        ...     conditions=[
        ...         JoinCondition("product_id", "id"),
        ...         JoinCondition("region", "available_region"),
        ...     ],
        ...     join_type=JoinType.INNER,
        ... )
        >>> result = transformer.transform(orders_df)
    """

    def __init__(
        self,
        right_df: DataFrame,
        join_type: JoinType | str = JoinType.INNER,
        conditions: list[JoinCondition] | None = None,
        on_columns: list[str] | None = None,
        broadcast: bool = False,
        right_alias: str | None = None,
        drop_duplicate_columns: bool = True,
        suffixes: tuple[str, str] = ("_left", "_right"),
    ):
        """
        Initialise le JoinTransformer.

        Args:
            right_df: DataFrame à joindre (droite)
            join_type: Type de jointure
            conditions: Conditions de jointure explicites
            on_columns: Colonnes communes pour jointure simple
            broadcast: Utiliser broadcast hint
            right_alias: Alias pour le DataFrame droit
            drop_duplicate_columns: Supprimer les colonnes dupliquées
            suffixes: Suffixes pour les colonnes dupliquées
        """
        self.right_df = right_df
        self.join_type = JoinType(join_type) if isinstance(join_type, str) else join_type
        self.conditions = conditions or []
        self.on_columns = on_columns
        self.broadcast = broadcast
        self.right_alias = right_alias
        self.drop_duplicate_columns = drop_duplicate_columns
        self.suffixes = suffixes

        logger.debug(
            "JoinTransformer initialisé",
            join_type=self.join_type.value,
            broadcast=self.broadcast,
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Applique la jointure au DataFrame.

        Args:
            df: DataFrame gauche

        Returns:
            DataFrame joint
        """
        logger.info(
            "Application de la jointure",
            join_type=self.join_type.value,
            broadcast=self.broadcast,
        )

        left_df = df
        right_df = self.right_df

        # Appliquer l'alias si spécifié
        if self.right_alias:
            right_df = right_df.alias(self.right_alias)

        # Appliquer le broadcast hint si demandé
        if self.broadcast:
            right_df = F.broadcast(right_df)

        # Construire la condition de jointure
        join_condition = self._build_join_condition(left_df, right_df)

        # Exécuter la jointure
        if self.on_columns and not self.conditions:
            # Jointure simple sur colonnes communes
            result = left_df.join(right_df, on=self.on_columns, how=self.join_type.value)
        else:
            # Jointure avec condition explicite
            result = left_df.join(right_df, on=join_condition, how=self.join_type.value)

            # Gérer les colonnes dupliquées
            if self.drop_duplicate_columns:
                result = self._handle_duplicate_columns(result, left_df, right_df)

        logger.info(f"Jointure terminée: {result.count()} lignes")

        return result

    def _build_join_condition(self, left_df: DataFrame, right_df: DataFrame) -> Column | None:
        """
        Construit la condition de jointure.

        Args:
            left_df: DataFrame gauche
            right_df: DataFrame droit

        Returns:
            Expression de condition ou None
        """
        if not self.conditions:
            return None

        # Combiner toutes les conditions avec AND
        combined_condition = None

        for condition in self.conditions:
            expr = condition.to_expression(left_df, right_df)
            combined_condition = expr if combined_condition is None else combined_condition & expr

        return combined_condition

    def _handle_duplicate_columns(
        self,
        result: DataFrame,
        left_df: DataFrame,
        right_df: DataFrame,
    ) -> DataFrame:
        """
        Gère les colonnes dupliquées après jointure.

        Args:
            result: DataFrame résultat
            left_df: DataFrame gauche original
            right_df: DataFrame droit original

        Returns:
            DataFrame avec colonnes renommées ou supprimées
        """
        left_cols = set(left_df.columns)
        right_cols = set(right_df.columns)

        # Colonnes utilisées dans la jointure
        join_cols = set()
        for condition in self.conditions:
            join_cols.add(condition.right_column)

        # Supprimer les colonnes dupliquées du côté droit
        cols_to_drop = []
        for col in right_cols:
            if col in left_cols and col in join_cols:
                # Supprimer la colonne dupliquée du DataFrame droit
                cols_to_drop.append(right_df[col])

        if cols_to_drop:
            result = result.drop(*cols_to_drop)

        return result


class MultiJoinTransformer(BaseTransformer):
    """
    Transformer pour les jointures multiples en chaîne.

    Example:
        >>> transformer = MultiJoinTransformer(
        ...     joins=[
        ...         JoinSpec(customers_df, JoinType.LEFT, on_columns=["customer_id"]),
        ...         JoinSpec(products_df, JoinType.LEFT, on_columns=["product_id"]),
        ...         JoinSpec(regions_df, JoinType.LEFT, on_columns=["region_id"]),
        ...     ]
        ... )
        >>> result = transformer.transform(orders_df)
    """

    def __init__(self, joins: list[JoinSpec]):
        """
        Initialise le MultiJoinTransformer.

        Args:
            joins: Liste des spécifications de jointure
        """
        self.joins = joins

        logger.debug(
            "MultiJoinTransformer initialisé",
            join_count=len(self.joins),
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Applique les jointures en séquence.

        Args:
            df: DataFrame de base

        Returns:
            DataFrame avec toutes les jointures appliquées
        """
        logger.info(f"Application de {len(self.joins)} jointures")

        result = df

        for i, join_spec in enumerate(self.joins, 1):
            join_type_value = (
                join_spec.join_type.value
                if isinstance(join_spec.join_type, JoinType)
                else join_spec.join_type
            )
            logger.debug(f"Jointure {i}/{len(self.joins)}: {join_type_value}")

            transformer = JoinTransformer(
                right_df=join_spec.right_df,
                join_type=join_spec.join_type,
                conditions=join_spec.conditions,
                on_columns=join_spec.on_columns,
                broadcast=join_spec.broadcast,
                right_alias=join_spec.right_alias,
                drop_duplicate_columns=join_spec.drop_duplicates,
            )

            result = transformer.transform(result)

        logger.info(f"Jointures multiples terminées: {result.count()} lignes")

        return result


class LookupJoinTransformer(BaseTransformer):
    """
    Transformer pour les jointures de type lookup (dimension).

    Optimisé pour joindre une grande table de faits avec
    de petites tables de dimensions.

    Example:
        >>> transformer = LookupJoinTransformer(
        ...     lookup_df=dim_customers,
        ...     lookup_key="customer_id",
        ...     source_key="cust_id",
        ...     columns_to_add=["customer_name", "customer_segment"],
        ... )
        >>> result = transformer.transform(fact_orders)
    """

    def __init__(
        self,
        lookup_df: DataFrame,
        lookup_key: str,
        source_key: str,
        columns_to_add: list[str] | None = None,
        broadcast_lookup: bool = True,
        default_values: dict[str, Any] | None = None,
    ):
        """
        Initialise le LookupJoinTransformer.

        Args:
            lookup_df: Table de lookup/dimension
            lookup_key: Clé dans la table de lookup
            source_key: Clé dans la table source
            columns_to_add: Colonnes à ajouter (None = toutes)
            broadcast_lookup: Broadcast la table de lookup
            default_values: Valeurs par défaut si non trouvé
        """
        self.lookup_df = lookup_df
        self.lookup_key = lookup_key
        self.source_key = source_key
        self.columns_to_add = columns_to_add
        self.broadcast_lookup = broadcast_lookup
        self.default_values = default_values or {}

        logger.debug(
            "LookupJoinTransformer initialisé",
            lookup_key=self.lookup_key,
            source_key=self.source_key,
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Applique la jointure de lookup.

        Args:
            df: DataFrame source (table de faits)

        Returns:
            DataFrame enrichi avec les colonnes de lookup
        """
        logger.info(
            "Application du lookup join",
            lookup_key=self.lookup_key,
            source_key=self.source_key,
        )

        # Préparer la table de lookup
        lookup = self.lookup_df

        # Sélectionner uniquement les colonnes nécessaires
        if self.columns_to_add:
            cols_to_select = [self.lookup_key] + self.columns_to_add
            lookup = lookup.select(*cols_to_select)

        # Broadcast si demandé
        if self.broadcast_lookup:
            lookup = F.broadcast(lookup)

        # Condition de jointure
        join_condition = df[self.source_key] == lookup[self.lookup_key]

        # Exécuter la jointure
        result = df.join(lookup, on=join_condition, how="left")

        # Supprimer la clé dupliquée
        result = result.drop(lookup[self.lookup_key])

        # Appliquer les valeurs par défaut
        for col_name, default_value in self.default_values.items():
            if col_name in result.columns:
                result = result.withColumn(
                    col_name, F.coalesce(F.col(col_name), F.lit(default_value))
                )

        logger.info(f"Lookup join terminé: {result.count()} lignes")

        return result


class SelfJoinTransformer(BaseTransformer):
    """
    Transformer pour les auto-jointures.

    Example:
        >>> # Trouver les employés et leurs managers
        >>> transformer = SelfJoinTransformer(
        ...     left_alias="employee",
        ...     right_alias="manager",
        ...     join_condition=lambda l, r: l["manager_id"] == r["employee_id"],
        ...     right_columns={"name": "manager_name", "email": "manager_email"},
        ... )
        >>> result = transformer.transform(employees_df)
    """

    def __init__(
        self,
        left_alias: str,
        right_alias: str,
        join_condition: Callable[[DataFrame, DataFrame], Column],
        join_type: JoinType = JoinType.LEFT,
        right_columns: dict[str, str] | None = None,
    ):
        """
        Initialise le SelfJoinTransformer.

        Args:
            left_alias: Alias pour le DataFrame gauche
            right_alias: Alias pour le DataFrame droit
            join_condition: Fonction retournant la condition de jointure
            join_type: Type de jointure
            right_columns: Mapping {colonne_source: colonne_alias} pour le côté droit
        """
        self.left_alias = left_alias
        self.right_alias = right_alias
        self.join_condition = join_condition
        self.join_type = join_type
        self.right_columns = right_columns or {}

        logger.debug(
            "SelfJoinTransformer initialisé",
            left_alias=self.left_alias,
            right_alias=self.right_alias,
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Applique l'auto-jointure.

        Args:
            df: DataFrame source

        Returns:
            DataFrame avec auto-jointure appliquée
        """
        logger.info(
            "Application de l'auto-jointure",
            left_alias=self.left_alias,
            right_alias=self.right_alias,
        )

        # Créer les alias
        left_df = df.alias(self.left_alias)
        right_df = df.alias(self.right_alias)

        # Construire la condition
        condition = self.join_condition(left_df, right_df)

        # Exécuter la jointure
        result = left_df.join(right_df, on=condition, how=self.join_type.value)

        # Sélectionner les colonnes avec renommage
        select_cols = [F.col(f"{self.left_alias}.*")]

        for src_col, dest_col in self.right_columns.items():
            select_cols.append(F.col(f"{self.right_alias}.{src_col}").alias(dest_col))

        if self.right_columns:
            result = result.select(*select_cols)

        logger.info(f"Auto-jointure terminée: {result.count()} lignes")

        return result


class ConditionalJoinTransformer(BaseTransformer):
    """
    Transformer pour les jointures conditionnelles complexes.

    Permet des conditions de jointure avec logique OR ou plages de valeurs.

    Example:
        >>> transformer = ConditionalJoinTransformer(
        ...     right_df=price_ranges_df,
        ...     condition=lambda l, r: (
        ...         (l["product_id"] == r["product_id"]) &
        ...         (l["quantity"] >= r["min_qty"]) &
        ...         (l["quantity"] < r["max_qty"])
        ...     ),
        ... )
        >>> result = transformer.transform(orders_df)
    """

    def __init__(
        self,
        right_df: DataFrame,
        condition: Callable[[DataFrame, DataFrame], Column],
        join_type: JoinType = JoinType.LEFT,
        broadcast: bool = False,
        columns_to_add: list[str] | None = None,
    ):
        """
        Initialise le ConditionalJoinTransformer.

        Args:
            right_df: DataFrame droit
            condition: Fonction retournant la condition de jointure
            join_type: Type de jointure
            broadcast: Broadcast le DataFrame droit
            columns_to_add: Colonnes à ajouter du DataFrame droit
        """
        self.right_df = right_df
        self.condition = condition
        self.join_type = join_type
        self.broadcast = broadcast
        self.columns_to_add = columns_to_add

        logger.debug(
            "ConditionalJoinTransformer initialisé",
            join_type=self.join_type.value,
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Applique la jointure conditionnelle.

        Args:
            df: DataFrame gauche

        Returns:
            DataFrame joint
        """
        logger.info("Application de la jointure conditionnelle")

        right_df = self.right_df

        # Filtrer les colonnes si spécifié
        if self.columns_to_add:
            # Identifier la clé de jointure probable
            right_df = right_df.select(*self.columns_to_add)

        # Broadcast si demandé
        if self.broadcast:
            right_df = F.broadcast(right_df)

        # Construire et appliquer la condition
        join_condition = self.condition(df, right_df)

        result = df.join(right_df, on=join_condition, how=self.join_type.value)

        logger.info(f"Jointure conditionnelle terminée: {result.count()} lignes")

        return result


# Fonctions utilitaires
def inner_join(
    left_df: DataFrame,
    right_df: DataFrame,
    on: str | list[str],
    broadcast_right: bool = False,
) -> DataFrame:
    """
    Jointure interne simplifiée.

    Args:
        left_df: DataFrame gauche
        right_df: DataFrame droit
        on: Colonnes de jointure
        broadcast_right: Broadcast le DataFrame droit

    Returns:
        DataFrame joint
    """
    if broadcast_right:
        right_df = F.broadcast(right_df)

    return left_df.join(right_df, on=on, how="inner")


def left_join(
    left_df: DataFrame,
    right_df: DataFrame,
    on: str | list[str],
    broadcast_right: bool = False,
) -> DataFrame:
    """
    Jointure gauche simplifiée.

    Args:
        left_df: DataFrame gauche
        right_df: DataFrame droit
        on: Colonnes de jointure
        broadcast_right: Broadcast le DataFrame droit

    Returns:
        DataFrame joint
    """
    if broadcast_right:
        right_df = F.broadcast(right_df)

    return left_df.join(right_df, on=on, how="left")


def lookup(
    df: DataFrame,
    lookup_df: DataFrame,
    key: str,
    columns: list[str],
    broadcast_lookup: bool = True,
) -> DataFrame:
    """
    Fonction de lookup simplifiée.

    Args:
        df: DataFrame source
        lookup_df: Table de lookup
        key: Colonne clé
        columns: Colonnes à ajouter
        broadcast_lookup: Broadcast la table de lookup

    Returns:
        DataFrame enrichi
    """
    lookup_select = lookup_df.select(key, *columns)

    if broadcast_lookup:
        lookup_select = F.broadcast(lookup_select)

    return df.join(lookup_select, on=key, how="left")


def anti_join(
    left_df: DataFrame,
    right_df: DataFrame,
    on: str | list[str],
) -> DataFrame:
    """
    Anti-jointure (lignes de gauche non présentes à droite).

    Args:
        left_df: DataFrame gauche
        right_df: DataFrame droit
        on: Colonnes de jointure

    Returns:
        DataFrame avec lignes non matchées
    """
    return left_df.join(right_df, on=on, how="left_anti")


def semi_join(
    left_df: DataFrame,
    right_df: DataFrame,
    on: str | list[str],
) -> DataFrame:
    """
    Semi-jointure (lignes de gauche présentes à droite).

    Args:
        left_df: DataFrame gauche
        right_df: DataFrame droit
        on: Colonnes de jointure

    Returns:
        DataFrame avec lignes matchées (colonnes de gauche uniquement)
    """
    return left_df.join(right_df, on=on, how="left_semi")
