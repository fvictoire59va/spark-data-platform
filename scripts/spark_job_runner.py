#!/usr/bin/env python3
"""
Script simple pour exécuter des jobs Spark dans le cluster
"""

import subprocess
import sys
from pathlib import Path


def run_spark_job(job_name: str, environment: str = "dev") -> int:
    """Exécute un job Spark dans le cluster."""

    print("\n" + "=" * 70)
    print(f"🚀 EXÉCUTION DU JOB SPARK: {job_name}")
    print("=" * 70)

    # Chemin du projet
    project_root = Path(__file__).parent.parent

    # Copier le code source dans le container
    print("\n📦 Copie du code source vers spark-master...")
    result = subprocess.run(
        ["docker", "cp", "src/.", "spark-master:/tmp/spark-code/src/"],
        cwd=project_root,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"❌ Erreur lors de la copie: {result.stderr}")
        return 1

    print("✅ Code copié avec succès")

    # Installer les dépendances Python dans le container
    print("\n📦 Installation des dépendances Python...")
    # Installer les dépendances critiques
    deps = [
        "PyYAML",
        "structlog",
        "prometheus-client",
        "boto3",
        "pyarrow",
        "pandas",
        "pydantic",
        "pydantic-settings",
        "delta-spark",
        "great-expectations",
    ]

    for dep in deps:
        subprocess.run(
            ["docker", "exec", "spark-master", "pip", "install", "-q", dep], capture_output=True
        )

    print("✅ Dépendances installées")

    # Préhistoire : initialiser PYTHONPATH dans le container
    init_cmd = [
        "docker",
        "exec",
        "spark-master",
        "bash",
        "-c",
        "export PYTHONPATH=/tmp/spark-code:$PYTHONPATH && echo 'PYTHONPATH configuré'",
    ]

    subprocess.run(init_cmd, capture_output=True)

    # Commande spark-submit
    job_path = f"/tmp/spark-code/src/pipelines/sales/jobs/{job_name}.py"

    cmd = [
        "docker",
        "exec",
        "spark-master",
        "bash",
        "-c",
        f"""
export PYTHONPATH=/tmp/spark-code:$PYTHONPATH
spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-memory 2g \
  --executor-memory 2g \
  --executor-cores 2 \
  --num-executors 2 \
  --packages org.postgresql:postgresql:42.6.0 \
  {job_path} \
  --environment {environment}
        """,
    ]

    print(f"\n▶️  Lancement du job: {job_name} ({environment})...")
    print("-" * 70)

    result = subprocess.run(cmd)

    print("-" * 70)
    if result.returncode == 0:
        print(f"\n✅ JOB {job_name} TERMINÉ AVEC SUCCÈS")
    else:
        print(f"\n❌ JOB {job_name} A ÉCHOUÉ (code: {result.returncode})")

    print("=" * 70 + "\n")

    return result.returncode


def main():
    """Interface principale."""

    jobs = [
        "ingest_orders",
        "enrich_orders_silver",
        "aggregate_sales",
    ]

    if len(sys.argv) < 2:
        print("Usage: python spark_job_runner.py <job_name> [environment]")
        print("\nJobs disponibles:")
        for job in jobs:
            print(f"  • {job}")
        return 1

    job_name = sys.argv[1]
    environment = sys.argv[2] if len(sys.argv) > 2 else "dev"

    if job_name not in jobs:
        print(f"❌ Job non trouvé: {job_name}")
        print(f"Jobs disponibles: {', '.join(jobs)}")
        return 1

    return run_spark_job(job_name, environment)


if __name__ == "__main__":
    sys.exit(main())
