"""Métriques et monitoring pour les jobs Spark."""
from __future__ import annotations

import time
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from src.core.config_manager import get_settings
from src.core.logger import get_logger

logger = get_logger(__name__)

# Métriques Prometheus
JOB_DURATION = Histogram(
    "spark_job_duration_seconds",
    "Durée des jobs Spark",
    ["job_name", "domain", "environment"],
    buckets=[10, 30, 60, 120, 300, 600, 1800, 3600],
)

JOB_RECORDS_PROCESSED = Counter(
    "spark_job_records_processed_total",
    "Nombre total de records traités",
    ["job_name", "domain", "environment"],
)

JOB_STATUS = Counter(
    "spark_job_status_total",
    "Statut des jobs",
    ["job_name", "domain", "environment", "status"],
)

JOB_ERRORS = Counter(
    "spark_job_errors_total",
    "Nombre d'erreurs",
    ["job_name", "domain", "environment", "error_type"],
)

ACTIVE_JOBS = Gauge(
    "spark_active_jobs",
    "Nombre de jobs actifs",
    ["domain"],
)

DATA_QUALITY_CHECKS = Counter(
    "spark_data_quality_checks_total",
    "Résultats des contrôles qualité",
    ["job_name", "check_name", "result"],
)


@dataclass
class JobMetrics:
    """Collecteur de métriques pour un job."""

    job_name: str
    domain: str
    environment: str
    start_time: float = field(default_factory=time.time)
    records_processed: int = 0
    custom_metrics: dict[str, Any] = field(default_factory=dict)

    def record_processed(self, count: int) -> None:
        """Enregistre le nombre de records traités."""
        self.records_processed += count
        JOB_RECORDS_PROCESSED.labels(
            job_name=self.job_name,
            domain=self.domain,
            environment=self.environment,
        ).inc(count)

    def record_quality_check(self, check_name: str, passed: bool) -> None:
        """Enregistre le résultat d'un contrôle qualité."""
        DATA_QUALITY_CHECKS.labels(
            job_name=self.job_name,
            check_name=check_name,
            result="pass" if passed else "fail",
        ).inc()

    def add_custom_metric(self, name: str, value: Any) -> None:
        """Ajoute une métrique personnalisée."""
        self.custom_metrics[name] = value

    def complete(self, status: str = "success") -> dict[str, Any]:
        """
        Finalise les métriques du job.

        Args:
            status: Statut final (success, failed, cancelled)

        Returns:
            Dictionnaire des métriques
        """
        duration = time.time() - self.start_time

        # Enregistrer dans Prometheus
        JOB_DURATION.labels(
            job_name=self.job_name,
            domain=self.domain,
            environment=self.environment,
        ).observe(duration)

        JOB_STATUS.labels(
            job_name=self.job_name,
            domain=self.domain,
            environment=self.environment,
            status=status,
        ).inc()

        ACTIVE_JOBS.labels(domain=self.domain).dec()

        return {
            "job_name": self.job_name,
            "domain": self.domain,
            "environment": self.environment,
            "status": status,
            "duration_seconds": round(duration, 2),
            "records_processed": self.records_processed,
            **self.custom_metrics,
        }

    def error(self, error_type: str) -> None:
        """Enregistre une erreur."""
        JOB_ERRORS.labels(
            job_name=self.job_name,
            domain=self.domain,
            environment=self.environment,
            error_type=error_type,
        ).inc()


class MetricsServer:
    """Serveur de métriques Prometheus."""

    _started: bool = False

    @classmethod
    def start(cls, port: int | None = None) -> None:
        """Démarre le serveur de métriques."""
        if cls._started:
            return

        settings = get_settings()
        if not settings.enable_metrics:
            logger.info("Métriques désactivées")
            return

        port = port or settings.prometheus_port
        start_http_server(port)
        cls._started = True
        logger.info(f"Serveur de métriques démarré sur le port {port}")


@contextmanager
def track_job(
    job_name: str,
    domain: str,
    environment: str | None = None,
) -> Generator[JobMetrics, None, None]:
    """
    Context manager pour tracker les métriques d'un job.

    Usage:
        with track_job("my_job", "sales") as metrics:
            # ... exécution du job
            metrics.record_processed(1000)

    Args:
        job_name: Nom du job
        domain: Domaine métier
        environment: Environnement (optionnel, utilise settings sinon)

    Yields:
        JobMetrics instance
    """
    settings = get_settings()
    env = environment or settings.environment.value

    metrics = JobMetrics(
        job_name=job_name,
        domain=domain,
        environment=env,
    )

    ACTIVE_JOBS.labels(domain=domain).inc()

    try:
        yield metrics
        metrics.complete("success")
    except Exception as e:
        metrics.error(type(e).__name__)
        metrics.complete("failed")
        raise
