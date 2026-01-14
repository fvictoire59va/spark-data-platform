"""Configuration du logging structuré."""
from __future__ import annotations

import logging
import sys
from typing import Any

import structlog
from structlog.types import Processor

from src.core.config_manager import get_settings


def setup_logging() -> None:
    """Configure le logging structuré pour l'application."""
    settings = get_settings()

    # Processors communs
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.UnicodeDecoder(),
    ]

    if settings.environment.value in ("local", "dev"):
        # Format lisible pour le développement
        processors = shared_processors + [structlog.dev.ConsoleRenderer(colors=True)]
    else:
        # Format JSON pour la production
        processors = shared_processors + [
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.DEBUG if settings.debug else logging.INFO
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.BoundLogger:
    """
    Récupère un logger configuré.

    Args:
        name: Nom du module/composant

    Returns:
        Logger structuré
    """
    setup_logging()
    return structlog.get_logger(name)


class SparkJobLogger:
    """Logger spécialisé pour les jobs Spark."""

    def __init__(self, job_name: str, pipeline: str):
        self.logger = get_logger(job_name)
        self.job_name = job_name
        self.pipeline = pipeline
        self._bind_context()

    def _bind_context(self) -> None:
        """Ajoute le contexte du job au logger."""
        self.logger = self.logger.bind(
            job_name=self.job_name,
            pipeline=self.pipeline,
        )

    def job_started(self, **kwargs: Any) -> None:
        """Log le démarrage du job."""
        self.logger.info("Job démarré", status="STARTED", **kwargs)

    def job_completed(self, records_processed: int, duration_seconds: float, **kwargs: Any) -> None:
        """Log la fin du job avec succès."""
        self.logger.info(
            "Job terminé avec succès",
            status="COMPLETED",
            records_processed=records_processed,
            duration_seconds=round(duration_seconds, 2),
            **kwargs,
        )

    def job_failed(self, error: Exception, **kwargs: Any) -> None:
        """Log l'échec du job."""
        self.logger.error(
            "Job échoué",
            status="FAILED",
            error_type=type(error).__name__,
            error_message=str(error),
            **kwargs,
            exc_info=True,
        )

    def step_completed(self, step_name: str, **kwargs: Any) -> None:
        """Log la fin d'une étape."""
        self.logger.info(f"Étape '{step_name}' terminée", step=step_name, **kwargs)
