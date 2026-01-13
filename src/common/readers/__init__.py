"""Readers pour différentes sources de données."""
from src.common.readers.base_reader import BaseReader
from src.common.readers.delta_reader import DeltaReader
from src.common.readers.jdbc_reader import JDBCReader
from src.common.readers.kafka_reader import KafkaReader
from src.common.readers.parquet_reader import ParquetReader

__all__ = [
    "BaseReader",
    "DeltaReader",
    "JDBCReader",
    "KafkaReader",
    "ParquetReader",
]
