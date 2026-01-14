# Classe/Fonction - Description

# CSVWriter-Writer CSV complet avec toutes les options
# CSVWriterBuilder-Builder fluide pour configuration
# write_csv()-Fonction utilitaire simple
# write_csv_partitioned()-Écriture partitionnée
# src/common/writers/csv_writer.py

"""Writer pour les fichiers CSV."""

from __future__ import annotations

from typing import Any

from common.logging import get_logger
from common.writers.base_writer import BaseWriter, WriteMode
from pyspark.sql import DataFrame

logger = get_logger(__name__)


class CSVWriter(BaseWriter):
    """
    Writer pour les fichiers CSV.

    Supporte toutes les options Spark CSV avec des valeurs
    par défaut sensées pour les cas d'usage courants.

    Example:
        >>> writer = CSVWriter(
        ...     path="data/output.csv",
        ...     header=True,
        ...     delimiter=";",
        ... )
        >>> writer.write(df)

        >>> # Avec partitionnement
        >>> writer = CSVWriter(
        ...     path="data/output",
        ...     partition_by=["year", "month"],
        ...     mode=WriteMode.OVERWRITE,
        ... )
        >>> writer.write(df)
    """

    def __init__(
        self,
        path: str,
        mode: WriteMode | str = WriteMode.ERROR_IF_EXISTS,
        header: bool = True,
        delimiter: str = ",",
        quote: str = '"',
        escape: str = "\\",
        encoding: str = "utf-8",
        null_value: str | None = None,
        empty_value: str | None = "",
        date_format: str | None = None,
        timestamp_format: str | None = None,
        quote_all: bool = False,
        compression: str | None = None,
        partition_by: list[str] | None = None,
        num_partitions: int | None = None,
        coalesce_to_single_file: bool = False,
        line_sep: str = "\n",
        char_to_escape_quote_escaping: str | None = None,
        **kwargs: Any,
    ):
        """
        Initialise le CSVWriter.

        Args:
            path: Chemin de destination
            mode: Mode d'écriture (overwrite, append, error, ignore)
            header: Inclure les noms de colonnes en en-tête
            delimiter: Séparateur de champs
            quote: Caractère de citation
            escape: Caractère d'échappement
            encoding: Encodage du fichier
            null_value: Représentation des valeurs nulles
            empty_value: Représentation des valeurs vides
            date_format: Format des dates
            timestamp_format: Format des timestamps
            quote_all: Mettre tous les champs entre guillemets
            compression: Compression (gzip, bzip2, lz4, snappy, deflate)
            partition_by: Colonnes de partitionnement
            num_partitions: Nombre de partitions en sortie
            coalesce_to_single_file: Fusionner en un seul fichier
            line_sep: Séparateur de lignes
            char_to_escape_quote_escaping: Caractère pour échapper le quote escaping
            **kwargs: Options supplémentaires
        """
        super().__init__(path=path, mode=mode)

        self.header = header
        self.delimiter = delimiter
        self.quote = quote
        self.escape = escape
        self.encoding = encoding
        self.null_value = null_value
        self.empty_value = empty_value
        self.date_format = date_format
        self.timestamp_format = timestamp_format
        self.quote_all = quote_all
        self.compression = compression
        self.partition_by = partition_by
        self.num_partitions = num_partitions
        self.coalesce_to_single_file = coalesce_to_single_file
        self.line_sep = line_sep
        self.char_to_escape_quote_escaping = char_to_escape_quote_escaping
        self.extra_options = kwargs

        logger.debug(
            "CSVWriter initialisé",
            path=self.path,
            mode=self.mode.value if isinstance(self.mode, WriteMode) else self.mode,
        )

    def write(self, df: DataFrame) -> str:
        """
        Écrit le DataFrame en CSV.

        Args:
            df: DataFrame à écrire

        Returns:
            Chemin où les données ont été écrites
        """
        logger.info(f"Écriture CSV vers: {self.path}")

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
        writer.csv(self.path)

        logger.info(f"Écriture CSV terminée: {self.path}")

        return self.path

    def _build_options(self) -> dict[str, Any]:
        """
        Construit le dictionnaire d'options pour Spark.

        Returns:
            Dictionnaire d'options
        """
        options = {
            "header": str(self.header).lower(),
            "delimiter": self.delimiter,
            "quote": self.quote,
            "escape": self.escape,
            "encoding": self.encoding,
            "quoteAll": str(self.quote_all).lower(),
            "lineSep": self.line_sep,
        }

        # Options optionnelles
        if self.null_value is not None:
            options["nullValue"] = self.null_value

        if self.empty_value is not None:
            options["emptyValue"] = self.empty_value

        if self.date_format:
            options["dateFormat"] = self.date_format

        if self.timestamp_format:
            options["timestampFormat"] = self.timestamp_format

        if self.compression:
            options["compression"] = self.compression

        if self.char_to_escape_quote_escaping:
            options["charToEscapeQuoteEscaping"] = self.char_to_escape_quote_escaping

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
            return self.mode.value
        return self.mode


class CSVWriterBuilder:
    """
    Builder pour CSVWriter avec interface fluide.

    Example:
        >>> writer = (
        ...     CSVWriterBuilder()
        ...     .path("data/output.csv")
        ...     .with_header()
        ...     .with_delimiter(";")
        ...     .with_compression("gzip")
        ...     .mode_overwrite()
        ...     .build()
        ... )
        >>> writer.write(df)
    """

    def __init__(self):
        """Initialise le builder."""
        self._path: str | None = None
        self._mode: WriteMode = WriteMode.ERROR_IF_EXISTS
        self._options: dict[str, Any] = {}

    def path(self, path: str) -> CSVWriterBuilder:
        """Définit le chemin de destination."""
        self._path = path
        return self

    def mode_overwrite(self) -> CSVWriterBuilder:
        """Mode écrasement."""
        self._mode = WriteMode.OVERWRITE
        return self

    def mode_append(self) -> CSVWriterBuilder:
        """Mode ajout."""
        self._mode = WriteMode.APPEND
        return self

    def mode_ignore(self) -> CSVWriterBuilder:
        """Mode ignore si existe."""
        self._mode = WriteMode.IGNORE
        return self

    def mode_error(self) -> CSVWriterBuilder:
        """Mode erreur si existe."""
        self._mode = WriteMode.ERROR_IF_EXISTS
        return self

    def with_header(self, header: bool = True) -> CSVWriterBuilder:
        """Active/désactive l'en-tête."""
        self._options["header"] = header
        return self

    def with_delimiter(self, delimiter: str) -> CSVWriterBuilder:
        """Définit le délimiteur."""
        self._options["delimiter"] = delimiter
        return self

    def with_quote(self, quote: str) -> CSVWriterBuilder:
        """Définit le caractère de citation."""
        self._options["quote"] = quote
        return self

    def with_escape(self, escape: str) -> CSVWriterBuilder:
        """Définit le caractère d'échappement."""
        self._options["escape"] = escape
        return self

    def with_encoding(self, encoding: str) -> CSVWriterBuilder:
        """Définit l'encodage."""
        self._options["encoding"] = encoding
        return self

    def with_null_value(self, null_value: str) -> CSVWriterBuilder:
        """Définit la représentation des nulls."""
        self._options["null_value"] = null_value
        return self

    def with_date_format(self, format: str) -> CSVWriterBuilder:
        """Définit le format des dates."""
        self._options["date_format"] = format
        return self

    def with_timestamp_format(self, format: str) -> CSVWriterBuilder:
        """Définit le format des timestamps."""
        self._options["timestamp_format"] = format
        return self

    def with_compression(self, compression: str) -> CSVWriterBuilder:
        """
        Active la compression.

        Args:
            compression: Type (gzip, bzip2, lz4, snappy, deflate)
        """
        self._options["compression"] = compression
        return self

    def with_partition_by(self, *columns: str) -> CSVWriterBuilder:
        """Définit les colonnes de partitionnement."""
        self._options["partition_by"] = list(columns)
        return self

    def with_num_partitions(self, num: int) -> CSVWriterBuilder:
        """Définit le nombre de partitions."""
        self._options["num_partitions"] = num
        return self

    def as_single_file(self) -> CSVWriterBuilder:
        """Fusionne en un seul fichier."""
        self._options["coalesce_to_single_file"] = True
        return self

    def quote_all(self) -> CSVWriterBuilder:
        """Met tous les champs entre guillemets."""
        self._options["quote_all"] = True
        return self

    def with_option(self, key: str, value: Any) -> CSVWriterBuilder:
        """Ajoute une option personnalisée."""
        self._options[key] = value
        return self

    def build(self) -> CSVWriter:
        """
        Construit le CSVWriter.

        Returns:
            Instance de CSVWriter configurée

        Raises:
            ValueError: Si le chemin n'est pas défini
        """
        if not self._path:
            raise ValueError("Le chemin de destination est requis")

        return CSVWriter(
            path=self._path,
            mode=self._mode,
            **self._options,
        )


def write_csv(
    df: DataFrame,
    path: str,
    mode: str = "overwrite",
    header: bool = True,
    delimiter: str = ",",
    single_file: bool = False,
    compression: str | None = None,
) -> str:
    """
    Fonction utilitaire pour écrire un CSV rapidement.

    Args:
        df: DataFrame à écrire
        path: Chemin de destination
        mode: Mode d'écriture
        header: Inclure l'en-tête
        delimiter: Séparateur
        single_file: Fusionner en un seul fichier
        compression: Compression optionnelle

    Returns:
        Chemin d'écriture
    """
    writer = CSVWriter(
        path=path,
        mode=mode,
        header=header,
        delimiter=delimiter,
        coalesce_to_single_file=single_file,
        compression=compression,
    )
    return writer.write(df)


def write_csv_partitioned(
    df: DataFrame,
    path: str,
    partition_by: list[str],
    mode: str = "overwrite",
    header: bool = True,
    compression: str | None = None,
) -> str:
    """
    Fonction utilitaire pour écrire un CSV partitionné.

    Args:
        df: DataFrame à écrire
        path: Chemin de destination
        partition_by: Colonnes de partitionnement
        mode: Mode d'écriture
        header: Inclure l'en-tête
        compression: Compression optionnelle

    Returns:
        Chemin d'écriture
    """
    writer = CSVWriter(
        path=path,
        mode=mode,
        header=header,
        partition_by=partition_by,
        compression=compression,
    )
    return writer.write(df)
