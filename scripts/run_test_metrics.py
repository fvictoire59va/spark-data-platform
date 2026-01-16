#!/usr/bin/env python3
"""
Wrapper pour lancer le test Spark metrics job
"""

import subprocess
import sys
from pathlib import Path


def main():
    project_root = Path(__file__).parent.parent

    print("\n" + "=" * 70)
    print("🚀 LANCEMENT DU JOB TEST SPARK (Générateur de Métriques)")
    print("=" * 70)

    # Copier le script test dans le container
    print("\n📦 Copie du script de test...")
    result = subprocess.run(
        ["docker", "cp", "scripts/test_spark_metrics.py", "spark-master:/tmp/"],
        cwd=project_root,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"❌ Erreur lors de la copie: {result.stderr}")
        return 1

    print("✅ Script copié")

    # Lancer le job
    print("\n▶️  Lancement du job Spark...")
    print("-" * 70)

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
        "--conf",
        "spark.eventLog.enabled=false",
        "/tmp/test_spark_metrics.py",
    ]

    result = subprocess.run(cmd)

    print("-" * 70)
    if result.returncode == 0:
        print("\n✅ JOB TERMINÉ AVEC SUCCÈS")
        print("\n📊 Les métriques Spark devraient maintenant être disponibles:")
        print("   • Visitez: http://localhost:3000")
        print("   • Dashboards: 'Spark Cluster Overview' et 'Spark Jobs Performance'")
    else:
        print(f"\n❌ JOB A ÉCHOUÉ (code: {result.returncode})")

    print("=" * 70 + "\n")

    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
