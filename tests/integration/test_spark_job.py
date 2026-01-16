"""Script de test pour exécuter le job d'ingestion dans Docker."""

import subprocess
import sys


def run_spark_job():
    """Exécute le job dans le container Spark Master."""
    cmd = [
        "docker",
        "exec",
        "spark-master",
        "spark-submit",
        "--master",
        "spark://spark-master:7077",
        "--deploy-mode",
        "client",
        "--driver-memory",
        "2g",
        "--executor-memory",
        "2g",
        "--num-executors",
        "2",
        "/opt/spark-apps/test_ingest.py",
        "--environment",
        "local",
    ]

    print("🚀 Exécution du job d'ingestion dans Spark...")
    print(f"Commande: {' '.join(cmd)}\n")

    result = subprocess.run(cmd, cwd=".")
    return result.returncode


if __name__ == "__main__":
    sys.exit(run_spark_job())
