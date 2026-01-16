#!/usr/bin/env python3
"""
Script pour générer des données et métriques dans Prometheus et Grafana
"""

import sys
import time
from pathlib import Path

# Ajouter src au path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.pipelines.sales.jobs.aggregate_sales import AggregateSalesJob
from src.pipelines.sales.jobs.enrich_orders_silver import EnrichOrdersSilverJob
from src.pipelines.sales.jobs.ingest_orders import IngestOrdersJob


def generate_metrics(environment: str = "dev"):
    """Génère des données et métriques pour les dashboards."""

    print("\n" + "=" * 70)
    print("📊 GÉNÉRATEUR DE MÉTRIQUES POUR GRAFANA")
    print("=" * 70)

    jobs = [
        ("IngestOrdersJob", IngestOrdersJob),
        ("EnrichOrdersSilverJob", EnrichOrdersSilverJob),
        ("AggregateSalesJob", AggregateSalesJob),
    ]

    for job_name, JobClass in jobs:
        try:
            print(f"\n▶️  Exécution de {job_name} ({environment})...")
            job = JobClass(environment=environment)
            job.run()
            print(f"✅ {job_name} terminé avec succès")

            # Attendre un peu entre les jobs
            time.sleep(2)

        except Exception as e:
            print(f"⚠️  {job_name} a échoué: {str(e)}")
            continue

    print("\n" + "=" * 70)
    print("✅ GÉNÉRATION DE MÉTRIQUES TERMINÉE")
    print("=" * 70)
    print("\n📈 Les dashboards Grafana devraient maintenant contenir des données:")
    print("   • Spark Cluster Overview : Statut du cluster et jobs")
    print("   • Spark Jobs Performance : Exécution et performance des jobs")
    print("   • Infrastructure Containers : Ressources système")
    print("\n🌐 Accédez à Grafana : http://localhost:3000")
    print("   Identifiants: admin / spark123")


if __name__ == "__main__":
    env = sys.argv[1] if len(sys.argv) > 1 else "dev"
    generate_metrics(environment=env)
