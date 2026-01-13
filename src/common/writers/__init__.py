"""Writers pour différentes destinations."""
from src.common.writers.base_writer import BaseWriter
from src.common.writers.delta_writer import DeltaWriter
from src.common.writers.jdbc_writer import JDBCWriter
from src.common.writers.kafka_writer import KafkaWriter
from src.common.writers.parquet_writer import ParquetWriter

__all__ = [
    "BaseWriter",
    "DeltaWriter",
    "JDBCWriter",
    "KafkaWriter",
    "ParquetWriter",
]
