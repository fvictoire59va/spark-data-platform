# src/common/readers/csv_reader.py
"""Reader pour les fichiers CSV."""

from __future__ import annotations

from typing import Any

from common.logging import get_logger
from common.readers.base_reader import BaseReader
from pyspark.sql import DataFrame, SparkSession

logger = get_logger(__name__)


class CSVReader(BaseReader):
    """
    Reader pour les fichiers CSV.

    Supporte toutes les options Spark CSV avec des valeurs
    par défaut sensées pour les cas d'usage courants.

    Example:
        >>> reader = CSVReader(spark, path="data/input.csv")
        >>> df = reader.read()

        >>> # Avec options
        >>> reader = CSVReader(
        ...     spark,
        ...     path="data/input.csv",
        ...     header=True,
        ...     delimiter=";",
        ...     encoding="utf-8",
        ... )
        >>> df = reader.read()
    """

    def __init__(
        self,
        spark: SparkSession,
        path: str,
        header: bool = True,
        delimiter: str = ",",
        quote: str = '"',
        escape: str = "\\",
        encoding: str = "utf-8",
        infer_schema: bool = True,
        null_value: str | None = None,
        empty_value: str | None = None,
        date_format: str | None = None,
        timestamp_format: str | None = None,
        multiline: bool = False,
        ignore_leading_white_space: bool = True,
        ignore_trailing_white_space: bool = True,
        mode: str = "PERMISSIVE",
        column_name_of_corrupt_record: str | None = "_corrupt_record",
        schema: Any | None = None,
        **kwargs: Any,
    ):
        """
        Initialise le CSVReader.

        Args:
            spark: Session Spark
            path: Chemin vers le fichier ou dossier CSV
            header: Si True, la première ligne contient les noms de colonnes
            delimiter: Séparateur de champs
            quote: Caractère de citation
            escape: Caractère d'échappement
            encoding: Encodage du fichier
            infer_schema: Si True, infère automatiquement les types
            null_value: Chaîne représentant les valeurs nulles
            empty_value: Chaîne représentant les valeurs vides
            date_format: Format des dates (ex: "yyyy-MM-dd")
            timestamp_format: Format des timestamps
            multiline: Si True, autorise les champs multilignes
            ignore_leading_white_space: Ignorer les espaces en début
            ignore_trailing_white_space: Ignorer les espaces en fin
            mode: Mode de parsing (PERMISSIVE, DROPMALFORMED, FAILFAST)
            column_name_of_corrupt_record: Colonne pour les enregistrements corrompus
            schema: Schéma Spark explicite (optionnel)
            **kwargs: Options Spark supplémentaires
        """
        super().__init__(spark)

        self.path = path
        self.header = header
        self.delimiter = delimiter
        self.quote = quote
        self.escape = escape
        self.encoding = encoding
        self.infer_schema = infer_schema
        self.null_value = null_value
        self.empty_value = empty_value
        self.date_format = date_format
        self.timestamp_format = timestamp_format
        self.multiline = multiline
        self.ignore_leading_white_space = ignore_leading_white_space
        self.ignore_trailing_white_space = ignore_trailing_white_space
        self.mode = mode
        self.column_name_of_corrupt_record = column_name_of_corrupt_record
        self.schema = schema
        self.extra_options = kwargs

        logger.debug(
            "CSVReader initialisé",
            path=self.path,
            delimiter=self.delimiter,
            header=self.header,
        )

    def read(self) -> DataFrame:
        """
        Lit le fichier CSV et retourne un DataFrame.

        Returns:
            DataFrame Spark contenant les données CSV

        Raises:
            FileNotFoundError: Si le fichier n'existe pas
            Exception: Pour toute autre erreur de lecture
        """
        logger.info(f"Lecture CSV: {self.path}")

        try:
            # Construire les options
            options = self._build_options()

            # Créer le reader
            reader = self.spark.read.format("csv")

            # Appliquer le schéma si fourni
            if self.schema:
                reader = reader.schema(self.schema)

            # Appliquer les options
            for key, value in options.items():
                reader = reader.option(key, value)

            # Lire le fichier
            df = reader.load(self.path)

            row_count = df.count()
            logger.info(
                f"CSV lu avec succès: {self.path}",
                rows=row_count,
                columns=len(df.columns),
            )

            assert isinstance(df, DataFrame)
            return df

        except Exception as e:
            logger.error(f"Erreur lecture CSV: {self.path}", error=str(e))
            raise

    def _build_options(self) -> dict[str, Any]:
        """
        Construit le dictionnaire d'options Spark.

        Returns:
            Dictionnaire des options
        """
        options: dict[str, Any] = {
            "header": str(self.header).lower(),
            "delimiter": self.delimiter,
            "quote": self.quote,
            "escape": self.escape,
            "encoding": self.encoding,
            "inferSchema": str(self.infer_schema).lower(),
            "multiLine": str(self.multiline).lower(),
            "ignoreLeadingWhiteSpace": str(self.ignore_leading_white_space).lower(),
            "ignoreTrailingWhiteSpace": str(self.ignore_trailing_white_space).lower(),
            "mode": self.mode,
        }

        # Options conditionnelles
        if self.null_value is not None:
            options["nullValue"] = self.null_value

        if self.empty_value is not None:
            options["emptyValue"] = self.empty_value

        if self.date_format:
            options["dateFormat"] = self.date_format

        if self.timestamp_format:
            options["timestampFormat"] = self.timestamp_format

        if self.column_name_of_corrupt_record:
            options["columnNameOfCorruptRecord"] = self.column_name_of_corrupt_record

        # Ajouter les options supplémentaires
        options.update(self.extra_options)

        return options

    def read_with_schema(self, schema: Any) -> DataFrame:
        """
        Lit le CSV avec un schéma spécifique.

        Args:
            schema: Schéma Spark à appliquer

        Returns:
            DataFrame avec le schéma appliqué
        """
        self.schema = schema
        self.infer_schema = False
        return self.read()

    def preview(self, num_rows: int = 5) -> DataFrame:
        """
        Lit un aperçu du fichier CSV.

        Args:
            num_rows: Nombre de lignes à lire

        Returns:
            DataFrame avec les premières lignes
        """
        df = self.read()
        return df.limit(num_rows)

    def get_schema_info(self) -> dict[str, Any]:
        """
        Retourne les informations de schéma du CSV.

        Returns:
            Dictionnaire avec les informations de schéma
        """
        df = self.read()

        return {
            "columns": df.columns,
            "dtypes": df.dtypes,
            "schema": df.schema.json(),
            "row_count": df.count(),
        }


class CSVReaderBuilder:
    """
    Builder pattern pour construire un CSVReader.

    Example:
        >>> reader = (
        ...     CSVReaderBuilder(spark)
        ...     .path("data/input.csv")
        ...     .with_header()
        ...     .with_delimiter(";")
        ...     .with_encoding("latin-1")
        ...     .build()
        ... )
        >>> df = reader.read()
    """

    def __init__(self, spark: SparkSession):
        """Initialise le builder."""
        self._spark = spark
        self._path: str | None = None
        self._options: dict[str, Any] = {}

    def path(self, path: str) -> CSVReaderBuilder:
        """Définit le chemin du fichier."""
        self._path = path
        return self

    def with_header(self, header: bool = True) -> CSVReaderBuilder:
        """Configure l'option header."""
        self._options["header"] = header
        return self

    def with_delimiter(self, delimiter: str) -> CSVReaderBuilder:
        """Configure le délimiteur."""
        self._options["delimiter"] = delimiter
        return self

    def with_encoding(self, encoding: str) -> CSVReaderBuilder:
        """Configure l'encodage."""
        self._options["encoding"] = encoding
        return self

    def with_schema(self, schema: Any) -> CSVReaderBuilder:
        """Configure le schéma."""
        self._options["schema"] = schema
        self._options["infer_schema"] = False
        return self

    def with_null_value(self, null_value: str) -> CSVReaderBuilder:
        """Configure la représentation des nulls."""
        self._options["null_value"] = null_value
        return self

    def with_date_format(self, date_format: str) -> CSVReaderBuilder:
        """Configure le format des dates."""
        self._options["date_format"] = date_format
        return self

    def with_timestamp_format(self, timestamp_format: str) -> CSVReaderBuilder:
        """Configure le format des timestamps."""
        self._options["timestamp_format"] = timestamp_format
        return self

    def with_multiline(self, multiline: bool = True) -> CSVReaderBuilder:
        """Active le support multiline."""
        self._options["multiline"] = multiline
        return self

    def with_mode(self, mode: str) -> CSVReaderBuilder:
        """Configure le mode de parsing."""
        self._options["mode"] = mode
        return self

    def with_option(self, key: str, value: Any) -> CSVReaderBuilder:
        """Ajoute une option personnalisée."""
        self._options[key] = value
        return self

    def build(self) -> CSVReader:
        """
        Construit le CSVReader.

        Returns:
            Instance de CSVReader configurée

        Raises:
            ValueError: Si le chemin n'est pas défini
        """
        if not self._path:
            raise ValueError("Le chemin du fichier est requis")

        return CSVReader(
            spark=self._spark,
            path=self._path,
            **self._options,
        )
