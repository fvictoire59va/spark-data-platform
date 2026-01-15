"""Wrapper pour exécuter le job IngestOrdersJob dans Spark."""
import subprocess
import sys
import os


def run_ingest_orders_job(environment: str = "dev") -> int:
    """Exécute le job IngestOrdersJob dans le cluster Spark."""
    # Vérifier que l'environnement est valide
    valid_envs = ["local", "dev", "staging", "prod"]
    if environment not in valid_envs:
        print(f"❌ Environnement invalide: {environment}")
        print(f"   Environnements valides: {', '.join(valid_envs)}")
        return 1

    # Copier le répertoire src dans le container
    print("📦 Préparation du code...")
    subprocess.run(
        ["docker", "cp", ".", "spark-master:/opt/spark-apps/project"],
        cwd=os.path.dirname(os.path.abspath(__file__)),
        capture_output=True,
    )

    # Commande spark-submit
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
        "--executor-cores",
        "2",
        "--num-executors",
        "2",
        "--packages",
        "org.postgresql:postgresql:42.6.0",
        "--py-files",
        "/opt/spark-apps/project/src.zip",
        "/opt/spark-apps/project/src/pipelines/sales/jobs/ingest_orders.py",
        "--environment",
        environment,
    ]

    print(f"🚀 Exécution du job IngestOrdersJob (env: {environment})...")
    print(f"Commande: {' '.join(cmd)}\n")

    result = subprocess.run(cmd)
    return result.returncode


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Exécuter le job IngestOrdersJob")
    parser.add_argument(
        "--environment",
        type=str,
        default="dev",
        choices=["local", "dev", "staging", "prod"],
        help="Environnement d'exécution",
    )

    args = parser.parse_args()
    sys.exit(run_ingest_orders_job(args.environment))
