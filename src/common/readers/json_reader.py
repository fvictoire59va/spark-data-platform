# src/common/readers/json_reader.py
"""Reader pour les fichiers JSON."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession

from common.readers.base_reader import BaseReader
from common.logging import get_logger

logger = get_logger(__name__)


class JSONReader(BaseReader):
    """
    Reader pour les fichiers JSON.
    
    Supporte les fichiers JSON classiques, JSON Lines (NDJSON),
    et les structures JSON imbriquées.
    
    Example:
        >>> reader = JSONReader(spark, path="data/input.json")
        >>> df = reader.read()
        
        >>> # JSON Lines (un objet JSON par ligne)
        >>> reader = JSONReader(
        ...     spark,
        ...     path="data/events.jsonl",
        ...     multiline=False,
        ... )
        >>> df = reader.read()
        
        >>> # JSON multiline avec schéma
        >>> reader = JSONReader(
        ...     spark,
        ...     path="data/complex.json",
        ...     multiline=True,
        ...     schema=my_schema,
        ... )
        >>> df = reader.read()
    """
    
    def __init__(
        self,
        spark: SparkSession,
        path: str,
        multiline: bool = False,
        primitives_as_string: bool = False,
        prefers_decimal: bool = False,
        allows_comments: bool = False,
        allows_unquoted_field_names: bool = False,
        allows_single_quotes: bool = False,
        allows_numeric_leading_zeros: bool = False,
        allows_backslash_escaping_any_character: bool = False,
        allows_unquoted_control_chars: bool = False,
        mode: str = "PERMISSIVE",
        column_name_of_corrupt_record: str | None = "_corrupt_record",
        date_format: str | None = None,
        timestamp_format: str | None = None,
        encoding: str = "utf-8",
        line_sep: str | None = None,
        sampling_ratio: float = 1.0,
        drop_field_if_all_null: bool = False,
        schema: Any | None = None,
        **kwargs: Any,
    ):
        """
        Initialise le JSONReader.
        
        Args:
            spark: Session Spark
            path: Chemin vers le fichier ou dossier JSON
            multiline: Si True, lit les fichiers JSON multi-lignes
            primitives_as_string: Si True, infère tous les primitifs comme strings
            prefers_decimal: Si True, préfère Decimal à Double
            allows_comments: Si True, autorise les commentaires Java/C++
            allows_unquoted_field_names: Si True, autorise les noms non quotés
            allows_single_quotes: Si True, autorise les guillemets simples
            allows_numeric_leading_zeros: Si True, autorise les zéros en tête
            allows_backslash_escaping_any_character: Échappement étendu
            allows_unquoted_control_chars: Autorise les caractères de contrôle
            mode: Mode de parsing (PERMISSIVE, DROPMALFORMED, FAILFAST)
            column_name_of_corrupt_record: Colonne pour les enregistrements corrompus
            date_format: Format des dates
            timestamp_format: Format des timestamps
            encoding: Encodage du fichier
            line_sep: Séparateur de lignes personnalisé
            sampling_ratio: Ratio d'échantillonnage pour l'inférence de schéma
            drop_field_if_all_null: Supprime les champs tous nuls
            schema: Schéma Spark explicite
            **kwargs: Options Spark supplémentaires
        """
        super().__init__(spark)
        
        self.path = path
        self.multiline = multiline
        self.primitives_as_string = primitives_as_string
        self.prefers_decimal = prefers_decimal
        self.allows_comments = allows_comments
        self.allows_unquoted_field_names = allows_unquoted_field_names
        self.allows_single_quotes = allows_single_quotes
        self.allows_numeric_leading_zeros = allows_numeric_leading_zeros
        self.allows_backslash_escaping_any_character = allows_backslash_escaping_any_character
        self.allows_unquoted_control_chars = allows_unquoted_control_chars
        self.mode = mode
        self.column_name_of_corrupt_record = column_name_of_corrupt_record
        self.date_format = date_format
        self.timestamp_format = timestamp_format
        self.encoding = encoding
        self.line_sep = line_sep
        self.sampling_ratio = sampling_ratio
        self.drop_field_if_all_null = drop_field_if_all_null
        self.schema = schema
        self.extra_options = kwargs
        
        logger.debug(
            "JSONReader initialisé",
            path=self.path,
            multiline=self.multiline,
        )
    
    def read(self) -> DataFrame:
        """
        Lit le fichier JSON et retourne un DataFrame.
        
        Returns:
            DataFrame Spark contenant les données JSON
            
        Raises:
            FileNotFoundError: Si le fichier n'existe pas
            Exception: Pour toute autre erreur de lecture
        """
        logger.info(f"Lecture JSON: {self.path}")
        
        try:
            # Construire les options
            options = self._build_options()
            
            # Créer le reader
            reader = self.spark.read.format("json")
            
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
                f"JSON lu avec succès: {self.path}",
                rows=row_count,
                columns=len(df.columns),
            )
            
            return df
            
        except Exception as e:
            logger.error(f"Erreur lecture JSON: {self.path}", error=str(e))
            raise
    
    def _build_options(self) -> dict[str, Any]:
        """
        Construit le dictionnaire d'options Spark.
        
        Returns:
            Dictionnaire des options
        """
        options: dict[str, Any] = {
            "multiLine": str(self.multiline).lower(),
            "primitivesAsString": str(self.primitives_as_string).lower(),
            "prefersDecimal": str(self.prefers_decimal).lower(),
            "allowComments": str(self.allows_comments).lower(),
            "allowUnquotedFieldNames": str(self.allows_unquoted_field_names).lower(),
            "allowSingleQuotes": str(self.allows_single_quotes).lower(),
            "allowNumericLeadingZeros": str(self.allows_numeric_leading_zeros).lower(),
            "allowBackslashEscapingAnyCharacter": str(self.allows_backslash_escaping_any_character).lower(),
            "allowUnquotedControlChars": str(self.allows_unquoted_control_chars).lower(),
            "mode": self.mode,
            "encoding": self.encoding,
            "samplingRatio": self.sampling_ratio,
            "dropFieldIfAllNull": str(self.drop_field_if_all_null).lower(),
        }
        
        # Options conditionnelles
        if self.column_name_of_corrupt_record:
            options["columnNameOfCorruptRecord"] = self.column_name_of_corrupt_record
        
        if self.date_format:
            options["dateFormat"] = self.date_format
        
        if self.timestamp_format:
            options["timestampFormat"] = self.timestamp_format
        
        if self.line_sep:
            options["lineSep"] = self.line_sep
        
        # Ajouter les options supplémentaires
        options.update(self.extra_options)
        
        return options
    
    def read_with_schema(self, schema: Any) -> DataFrame:
        """
        Lit le JSON avec un schéma spécifique.
        
        Args:
            schema: Schéma Spark à appliquer
            
        Returns:
            DataFrame avec le schéma appliqué
        """
        self.schema = schema
        return self.read()
    
    def preview(self, num_rows: int = 5) -> DataFrame:
        """
        Lit un aperçu du fichier JSON.
        
        Args:
            num_rows: Nombre de lignes à lire
            
        Returns:
            DataFrame avec les premières lignes
        """
        df = self.read()
        return df.limit(num_rows)
    
    def get_schema_info(self) -> dict[str, Any]:
        """
        Retourne les informations de schéma du JSON.
        
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
    
    def flatten(self, df: DataFrame | None = None) -> DataFrame:
        """
        Aplatit les structures JSON imbriquées.
        
        Args:
            df: DataFrame à aplatir (optionnel, lit le fichier sinon)
            
        Returns:
            DataFrame avec les colonnes aplaties
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import StructType, ArrayType
        
        if df is None:
            df = self.read()
        
        def _flatten_schema(schema: StructType, prefix: str = "") -> list[tuple[str, str]]:
            """Génère la liste des colonnes aplaties."""
            columns = []
            
            for field in schema.fields:
                col_name = f"{prefix}.{field.name}" if prefix else field.name
                
                if isinstance(field.dataType, StructType):
                    # Récursion pour les structures imbriquées
                    columns.extend(_flatten_schema(field.dataType, col_name))
                elif isinstance(field.dataType, ArrayType):
                    # Garder les arrays tels quels pour l'instant
                    columns.append((col_name, col_name.replace(".", "_")))
                else:
                    columns.append((col_name, col_name.replace(".", "_")))
            
            return columns
        
        # Générer les colonnes aplaties
        flat_columns = _flatten_schema(df.schema)
        
        # Sélectionner avec les nouveaux noms
        select_exprs = [
            F.col(orig).alias(new) for orig, new in flat_columns
        ]
        
        return df.select(*select_exprs)


class JSONReaderBuilder:
    """
    Builder pattern pour construire un JSONReader.
    
    Example:
        >>> reader = (
        ...     JSONReaderBuilder(spark)
        ...     .path("data/events.json")
        ...     .with_multiline()
        ...     .with_schema(my_schema)
        ...     .build()
        ... )
        >>> df = reader.read()
    """
    
    def __init__(self, spark: SparkSession):
        """Initialise le builder."""
        self._spark = spark
        self._path: str | None = None
        self._options: dict[str, Any] = {}
    
    def path(self, path: str) -> JSONReaderBuilder:
        """Définit le chemin du fichier."""
        self._path = path
        return self
    
    def with_multiline(self, multiline: bool = True) -> JSONReaderBuilder:
        """Active le support multiline."""
        self._options["multiline"] = multiline
        return self
    
    def with_schema(self, schema: Any) -> JSONReaderBuilder:
        """Configure le schéma."""
        self._options["schema"] = schema
        return self
    
    def with_encoding(self, encoding: str) -> JSONReaderBuilder:
        """Configure l'encodage."""
        self._options["encoding"] = encoding
        return self
    
    def with_date_format(self, date_format: str) -> JSONReaderBuilder:
        """Configure le format des dates."""
        self._options["date_format"] = date_format
        return self
    
    def with_timestamp_format(self, timestamp_format: str) -> JSONReaderBuilder:
        """Configure le format des timestamps."""
        self._options["timestamp_format"] = timestamp_format
        return self
    
    def with_mode(self, mode: str) -> JSONReaderBuilder:
        """Configure le mode de parsing."""
        self._options["mode"] = mode
        return self
    
    def with_comments(self, allow: bool = True) -> JSONReaderBuilder:
        """Autorise les commentaires."""
        self._options["allows_comments"] = allow
        return self
    
    def with_single_quotes(self, allow: bool = True) -> JSONReaderBuilder:
        """Autorise les guillemets simples."""
        self._options["allows_single_quotes"] = allow
        return self
    
    def with_primitives_as_string(self, value: bool = True) -> JSONReaderBuilder:
        """Lit tous les primitifs comme strings."""
        self._options["primitives_as_string"] = value
        return self
    
    def with_option(self, key: str, value: Any) -> JSONReaderBuilder:
        """Ajoute une option personnalisée."""
        self._options[key] = value
        return self
    
    def build(self) -> JSONReader:
        """
        Construit le JSONReader.
        
        Returns:
            Instance de JSONReader configurée
            
        Raises:
            ValueError: Si le chemin n'est pas défini
        """
        if not self._path:
            raise ValueError("Le chemin du fichier est requis")
        
        return JSONReader(
            spark=self._spark,
            path=self._path,
            **self._options,
        )


def read_json_lines(
    spark: SparkSession,
    path: str,
    schema: Any | None = None,
) -> DataFrame:
    """
    Fonction utilitaire pour lire des fichiers JSON Lines (NDJSON).
    
    Args:
        spark: Session Spark
        path: Chemin du fichier
        schema: Schéma optionnel
        
    Returns:
        DataFrame
    """
    reader = JSONReader(
        spark=spark,
        path=path,
        multiline=False,
        schema=schema,
    )
    return reader.read()


def read_json_multiline(
    spark: SparkSession,
    path: str,
    schema: Any | None = None,
) -> DataFrame:
    """
    Fonction utilitaire pour lire des fichiers JSON multilignes.
    
    Args:
        spark: Session Spark
        path: Chemin du fichier
        schema: Schéma optionnel
        
    Returns:
        DataFrame
    """
    reader = JSONReader(
        spark=spark,
        path=path,
        multiline=True,
        schema=schema,
    )
    return reader.read()
