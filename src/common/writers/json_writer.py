# JSONWriter-Writer JSON Lines standard
# JSONWriterBuilder-Builder fluide pour configuration
# NestedJSONWriter-JSON avec structures imbriquées
# ArrayJSONWriter-JSON avec arrays (groupement)
# write_json()-Fonction utilitaire simple
# write_json_lines()-JSON Lines compressé
# write_nested_json()-JSON hiérarchique

# src/common/writers/json_writer.py
"""Writer pour les fichiers JSON."""

from __future__ import annotations

from typing import Any

from common.logging import get_logger
from common.writers.base_writer import BaseWriter, WriteMode
from pyspark.sql import DataFrame

logger = get_logger(__name__)


class JSONWriter(BaseWriter):
    """
    Writer pour les fichiers JSON.

    Supporte JSON Lines (un objet par ligne) et JSON multilignes.

    Example:
        >>> writer = JSONWriter(
        ...     path="data/output.json",
        ...     mode=WriteMode.OVERWRITE,
        ... )
        >>> writer.write(df)

        >>> # JSON Lines compressé
        >>> writer = JSONWriter(
        ...     path="data/output.jsonl",
        ...     compression="gzip",
        ... )
        >>> writer.write(df)
    """

    def __init__(
        self,
        path: str,
        mode: WriteMode | str = WriteMode.ERROR_IF_EXISTS,
        compression: str | None = None,
        date_format: str | None = None,
        timestamp_format: str | None = None,
        encoding: str = "utf-8",
        line_sep: str = "\n",
        ignore_null_fields: bool = True,
        partition_by: list[str] | None = None,
        num_partitions: int | None = None,
        coalesce_to_single_file: bool = False,
        pretty_print: bool = False,
        **kwargs: Any,
    ):
        """
        Initialise le JSONWriter.

        Args:
            path: Chemin de destination
            mode: Mode d'écriture
            compression: Compression (gzip, bzip2, lz4, snappy, deflate)
            date_format: Format des dates
            timestamp_format: Format des timestamps
            encoding: Encodage du fichier
            line_sep: Séparateur de lignes
            ignore_null_fields: Exclure les champs null du JSON
            partition_by: Colonnes de partitionnement
            num_partitions: Nombre de partitions
            coalesce_to_single_file: Fusionner en un seul fichier
            pretty_print: Formatage avec indentation (attention: non standard)
            **kwargs: Options supplémentaires
        """
        super().__init__(path=path, mode=mode)

        self.compression = compression
        self.date_format = date_format
        self.timestamp_format = timestamp_format
        self.encoding = encoding
        self.line_sep = line_sep
        self.ignore_null_fields = ignore_null_fields
        self.partition_by = partition_by
        self.num_partitions = num_partitions
        self.coalesce_to_single_file = coalesce_to_single_file
        self.pretty_print = pretty_print
        self.extra_options = kwargs

        logger.debug(
            "JSONWriter initialisé",
            path=self.path,
            mode=self._get_mode_string(),
        )

    def write(self, df: DataFrame) -> str:
        """
        Écrit le DataFrame en JSON.

        Args:
            df: DataFrame à écrire

        Returns:
            Chemin où les données ont été écrites
        """
        logger.info(f"Écriture JSON vers: {self.path}")

        # Ajuster le nombre de partitions si nécessaire
        output_df = df

        if self.coalesce_to_single_file:
            output_df = output_df.coalesce(1)
            logger.debug("DataFrame coalescé en une seule partition")
        elif self.num_partitions:
            output_df = output_df.repartition(self.num_partitions)
            logger.debug(f"DataFrame repartitionné en {self.num_partitions} partitions")

        # Construire le writer
        writer = output_df.write.mode(self._get_mode_string())

        # Appliquer le partitionnement
        if self.partition_by:
            writer = writer.partitionBy(*self.partition_by)
            logger.debug(f"Partitionnement par: {self.partition_by}")

        # Appliquer les options
        options = self._build_options()
        for key, value in options.items():
            writer = writer.option(key, value)

        # Écrire
        writer.json(self.path)

        logger.info(f"Écriture JSON terminée: {self.path}")

        return str(self.path)

    def _build_options(self) -> dict[str, Any]:
        """
        Construit le dictionnaire d'options pour Spark.

        Returns:
            Dictionnaire d'options
        """
        options = {
            "encoding": self.encoding,
            "lineSep": self.line_sep,
            "ignoreNullFields": str(self.ignore_null_fields).lower(),
        }

        # Options optionnelles
        if self.compression:
            options["compression"] = self.compression

        if self.date_format:
            options["dateFormat"] = self.date_format

        if self.timestamp_format:
            options["timestampFormat"] = self.timestamp_format

        # Note: pretty print n'est pas une option native Spark
        # mais on peut la passer pour compatibilité avec certains writers
        if self.pretty_print:
            options["pretty"] = "true"

        # Ajouter les options supplémentaires
        options.update(self.extra_options)

        return options

    def _get_mode_string(self) -> str:
        """
        Retourne le mode d'écriture sous forme de string.

        Returns:
            Mode d'écriture
        """
        if isinstance(self.mode, WriteMode):
            mode_value: str = self.mode.value
            return mode_value
        return str(self.mode)


class JSONWriterBuilder:
    """
    Builder pour JSONWriter avec interface fluide.

    Example:
        >>> writer = (
        ...     JSONWriterBuilder()
        ...     .path("data/output.json")
        ...     .mode_overwrite()
        ...     .with_compression("gzip")
        ...     .as_single_file()
        ...     .build()
        ... )
        >>> writer.write(df)
    """

    def __init__(self) -> None:
        """Initialise le builder."""
        self._path: str | None = None
        self._mode: WriteMode = WriteMode.ERROR_IF_EXISTS
        self._options: dict[str, Any] = {}

    def path(self, path: str) -> JSONWriterBuilder:
        """Définit le chemin de destination."""
        self._path = path
        return self

    def mode_overwrite(self) -> JSONWriterBuilder:
        """Mode écrasement."""
        self._mode = WriteMode.OVERWRITE
        return self

    def mode_append(self) -> JSONWriterBuilder:
        """Mode ajout."""
        self._mode = WriteMode.APPEND
        return self

    def mode_ignore(self) -> JSONWriterBuilder:
        """Mode ignore si existe."""
        self._mode = WriteMode.IGNORE
        return self

    def mode_error(self) -> JSONWriterBuilder:
        """Mode erreur si existe."""
        self._mode = WriteMode.ERROR_IF_EXISTS
        return self

    def with_compression(self, compression: str) -> JSONWriterBuilder:
        """
        Active la compression.

        Args:
            compression: Type (gzip, bzip2, lz4, snappy, deflate)
        """
        self._options["compression"] = compression
        return self

    def with_date_format(self, format: str) -> JSONWriterBuilder:
        """Définit le format des dates."""
        self._options["date_format"] = format
        return self

    def with_timestamp_format(self, format: str) -> JSONWriterBuilder:
        """Définit le format des timestamps."""
        self._options["timestamp_format"] = format
        return self

    def with_encoding(self, encoding: str) -> JSONWriterBuilder:
        """Définit l'encodage."""
        self._options["encoding"] = encoding
        return self

    def with_partition_by(self, *columns: str) -> JSONWriterBuilder:
        """Définit les colonnes de partitionnement."""
        self._options["partition_by"] = list(columns)
        return self

    def with_num_partitions(self, num: int) -> JSONWriterBuilder:
        """Définit le nombre de partitions."""
        self._options["num_partitions"] = num
        return self

    def as_single_file(self) -> JSONWriterBuilder:
        """Fusionne en un seul fichier."""
        self._options["coalesce_to_single_file"] = True
        return self

    def ignore_null_fields(self, ignore: bool = True) -> JSONWriterBuilder:
        """Configure l'exclusion des champs null."""
        self._options["ignore_null_fields"] = ignore
        return self

    def with_option(self, key: str, value: Any) -> JSONWriterBuilder:
        """Ajoute une option personnalisée."""
        self._options[key] = value
        return self

    def build(self) -> JSONWriter:
        """
        Construit le JSONWriter.

        Returns:
            Instance de JSONWriter configurée

        Raises:
            ValueError: Si le chemin n'est pas défini
        """
        if not self._path:
            raise ValueError("Le chemin de destination est requis")

        return JSONWriter(
            path=self._path,
            mode=self._mode,
            **self._options,
        )


class NestedJSONWriter(BaseWriter):
    """
    Writer pour créer des structures JSON imbriquées.

    Permet de restructurer un DataFrame plat en JSON hiérarchique.

    Example:
        >>> # Créer une structure imbriquée à partir d'un DataFrame plat
        >>> writer = NestedJSONWriter(
        ...     path="data/nested_output.json",
        ...     root_columns=["order_id", "order_date"],
        ...     nested_structures={
        ...         "customer": ["customer_id", "customer_name", "email"],
        ...         "shipping": ["ship_address", "ship_city", "ship_country"],
        ...     },
        ... )
        >>> writer.write(df)
    """

    def __init__(
        self,
        path: str,
        root_columns: list[str],
        nested_structures: dict[str, list[str]],
        mode: WriteMode | str = WriteMode.ERROR_IF_EXISTS,
        compression: str | None = None,
        coalesce_to_single_file: bool = False,
        **kwargs: Any,
    ):
        """
        Initialise le NestedJSONWriter.

        Args:
            path: Chemin de destination
            root_columns: Colonnes à garder au niveau racine
            nested_structures: Mapping {nom_structure: [colonnes]}
            mode: Mode d'écriture
            compression: Compression optionnelle
            coalesce_to_single_file: Fusionner en un seul fichier
            **kwargs: Options supplémentaires
        """
        super().__init__(path=path, mode=mode)

        self.root_columns = root_columns
        self.nested_structures = nested_structures
        self.compression = compression
        self.coalesce_to_single_file = coalesce_to_single_file
        self.extra_options = kwargs

        logger.debug(
            "NestedJSONWriter initialisé",
            path=self.path,
            nested_structures=list(self.nested_structures.keys()),
        )

    def write(self, df: DataFrame) -> str:
        """
        Écrit le DataFrame en JSON imbriqué.

        Args:
            df: DataFrame à écrire

        Returns:
            Chemin où les données ont été écrites
        """
        from pyspark.sql import functions as F

        logger.info(f"Écriture JSON imbriqué vers: {self.path}")

        # Construire les structures imbriquées
        select_exprs = [F.col(c) for c in self.root_columns]

        for struct_name, columns in self.nested_structures.items():
            struct_expr = F.struct(*[F.col(c) for c in columns])
            select_exprs.append(struct_expr.alias(struct_name))

        # Appliquer la transformation
        nested_df = df.select(*select_exprs)

        # Ajuster les partitions si nécessaire
        if self.coalesce_to_single_file:
            nested_df = nested_df.coalesce(1)

        # Construire le writer
        mode_str = self.mode.value if isinstance(self.mode, WriteMode) else self.mode
        writer = nested_df.write.mode(mode_str)

        if self.compression:
            writer = writer.option("compression", self.compression)

        writer.json(self.path)

        logger.info(f"Écriture JSON imbriqué terminée: {self.path}")

        return str(self.path)


class ArrayJSONWriter(BaseWriter):
    """
    Writer pour créer des structures JSON avec arrays.

    Regroupe les données et crée des arrays pour chaque groupe.

    Example:
        >>> # Créer un JSON avec orders groupés par customer
        >>> writer = ArrayJSONWriter(
        ...     path="data/customers_orders.json",
        ...     group_by=["customer_id", "customer_name"],
        ...     array_column="orders",
        ...     array_fields=["order_id", "order_date", "amount"],
        ... )
        >>> writer.write(df)
    """

    def __init__(
        self,
        path: str,
        group_by: list[str],
        array_column: str,
        array_fields: list[str],
        mode: WriteMode | str = WriteMode.ERROR_IF_EXISTS,
        compression: str | None = None,
        coalesce_to_single_file: bool = False,
        **kwargs: Any,
    ):
        """
        Initialise l'ArrayJSONWriter.

        Args:
            path: Chemin de destination
            group_by: Colonnes de groupement
            array_column: Nom de la colonne array dans le JSON
            array_fields: Champs à inclure dans chaque élément de l'array
            mode: Mode d'écriture
            compression: Compression optionnelle
            coalesce_to_single_file: Fusionner en un seul fichier
            **kwargs: Options supplémentaires
        """
        super().__init__(path=path, mode=mode)

        self.group_by = group_by
        self.array_column = array_column
        self.array_fields = array_fields
        self.compression = compression
        self.coalesce_to_single_file = coalesce_to_single_file
        self.extra_options = kwargs

        logger.debug(
            "ArrayJSONWriter initialisé",
            path=self.path,
            group_by=self.group_by,
            array_column=self.array_column,
        )

    def write(self, df: DataFrame) -> str:
        """
        Écrit le DataFrame en JSON avec arrays.

        Args:
            df: DataFrame à écrire

        Returns:
            Chemin où les données ont été écrites
        """
        from pyspark.sql import functions as F

        logger.info(f"Écriture JSON avec arrays vers: {self.path}")

        # Créer la structure pour l'array
        array_struct = F.struct(*[F.col(c) for c in self.array_fields])

        # Grouper et collecter
        array_df = df.groupBy(*self.group_by).agg(
            F.collect_list(array_struct).alias(self.array_column)
        )

        # Ajuster les partitions si nécessaire
        if self.coalesce_to_single_file:
            array_df = array_df.coalesce(1)

        # Construire le writer
        mode_str = self.mode.value if isinstance(self.mode, WriteMode) else self.mode
        writer = array_df.write.mode(mode_str)

        if self.compression:
            writer = writer.option("compression", self.compression)

        writer.json(self.path)

        logger.info(f"Écriture JSON avec arrays terminée: {self.path}")

        return str(self.path)


# Fonctions utilitaires
def write_json(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    single_file: bool = False,
    compression: str | None = None,
) -> str:
    """
    Fonction utilitaire pour écrire un JSON rapidement.

    Args:
        df: DataFrame à écrire
        path: Chemin de destination
        mode: Mode d'écriture
        single_file: Fusionner en un seul fichier
        compression: Compression optionnelle

    Returns:
        Chemin d'écriture
    """
    writer = JSONWriter(
        path=path,
        mode=mode,
        coalesce_to_single_file=single_file,
        compression=compression,
    )
    return writer.write(df)


def write_json_lines(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    compression: str | None = "gzip",
) -> str:
    """
    Écrit en format JSON Lines (NDJSON) compressé.

    Args:
        df: DataFrame à écrire
        path: Chemin de destination
        mode: Mode d'écriture
        compression: Compression (gzip par défaut)

    Returns:
        Chemin d'écriture
    """
    writer = JSONWriter(
        path=path,
        mode=mode,
        compression=compression,
    )
    return writer.write(df)


def write_json_partitioned(
    df: DataFrame,
    path: str,
    partition_by: list[str],
    mode: str = "overwrite",
    compression: str | None = None,
) -> str:
    """
    Fonction utilitaire pour écrire un JSON partitionné.

    Args:
        df: DataFrame à écrire
        path: Chemin de destination
        partition_by: Colonnes de partitionnement
        mode: Mode d'écriture
        compression: Compression optionnelle

    Returns:
        Chemin d'écriture
    """
    writer = JSONWriter(
        path=path,
        mode=mode,
        partition_by=partition_by,
        compression=compression,
    )
    return writer.write(df)


def write_nested_json(
    df: DataFrame,
    path: str,
    root_columns: list[str],
    nested_structures: dict[str, list[str]],
    mode: str = "overwrite",
    single_file: bool = True,
) -> str:
    """
    Fonction utilitaire pour écrire un JSON imbriqué.

    Args:
        df: DataFrame à écrire
        path: Chemin de destination
        root_columns: Colonnes au niveau racine
        nested_structures: Structures imbriquées
        mode: Mode d'écriture
        single_file: Fusionner en un seul fichier

    Returns:
        Chemin d'écriture
    """
    writer = NestedJSONWriter(
        path=path,
        root_columns=root_columns,
        nested_structures=nested_structures,
        mode=mode,
        coalesce_to_single_file=single_file,
    )
    return writer.write(df)
