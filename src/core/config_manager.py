"""Gestionnaire de configuration centralisé."""
from __future__ import annotations

from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Environment(str, Enum):
    """Environnements disponibles."""

    LOCAL = "local"
    DEV = "dev"
    STAGING = "staging"
    PROD = "prod"


class Settings(BaseSettings):
    """Configuration globale de l'application."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # Environnement
    environment: Environment = Field(default=Environment.LOCAL)
    debug: bool = Field(default=False)

    # Spark
    spark_master: str = Field(default="local[*]")
    spark_shuffle_partitions: int = Field(default=200)
    spark_default_parallelism: int = Field(default=100)
    spark_log_level: str = Field(default="WARN")

    # Delta Lake
    delta_enabled: bool = Field(default=True)

    # Storage
    data_lake_path: str = Field(default="/data/lake")
    bronze_path: str = Field(default="/data/lake/bronze")
    silver_path: str = Field(default="/data/lake/silver")
    gold_path: str = Field(default="/data/lake/gold")

    # Database
    jdbc_url: str | None = Field(default=None)
    jdbc_user: str | None = Field(default=None)
    jdbc_password: str | None = Field(default=None)

    # Kafka
    kafka_bootstrap_servers: str = Field(default="localhost:9092")

    # Monitoring
    prometheus_port: int = Field(default=9090)
    enable_metrics: bool = Field(default=True)

    @field_validator("environment", mode="before")
    @classmethod
    def validate_environment(cls, v: str) -> Environment:
        """Valide et convertit l'environnement."""
        if isinstance(v, Environment):
            return v
        return Environment(v.lower())


class PipelineConfig:
    """Configuration spécifique à un pipeline."""

    def __init__(self, pipeline_name: str, domain: str, environment: Environment):
        self.pipeline_name = pipeline_name
        self.domain = domain
        self.environment = environment
        self._config: dict[str, Any] = {}
        self._load_config()

    def _load_config(self) -> None:
        """Charge la configuration depuis le fichier YAML."""
        config_path = (
            Path(__file__).parent.parent
            / "pipelines"
            / self.domain
            / "config"
            / f"{self.environment.value}.yaml"
        )

        if config_path.exists():
            with open(config_path) as f:
                self._config = yaml.safe_load(f) or {}
        else:
            raise FileNotFoundError(f"Configuration non trouvée: {config_path}")

    def get(self, key: str, default: Any = None) -> Any:
        """Récupère une valeur de configuration."""
        keys = key.split(".")
        value = self._config

        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return default

        return value if value is not None else default

    @property
    def source_config(self) -> dict[str, Any]:
        """Configuration des sources de données."""
        return self._config.get("sources", {})

    @property
    def target_config(self) -> dict[str, Any]:
        """Configuration des destinations."""
        return self._config.get("targets", {})

    @property
    def transformations(self) -> list[dict[str, Any]]:
        """Liste des transformations à appliquer."""
        return self._config.get("transformations", [])


@lru_cache
def get_settings() -> Settings:
    """Récupère les settings (singleton)."""
    return Settings()


def get_pipeline_config(
    pipeline_name: str,
    domain: str,
    environment: Environment | None = None,
) -> PipelineConfig:
    """Factory pour créer une configuration de pipeline."""
    env = environment or get_settings().environment
    return PipelineConfig(pipeline_name, domain, env)
