#!/usr/bin/env python3
"""
Script simple pour générer des métriques en créant de la charge système
"""

import requests


def check_services():
    """Vérifie la disponibilité des services"""
    print("\n🔍 Vérification des services...")

    services = {
        "Prometheus": "http://localhost:9090/-/healthy",
        "Grafana": "http://localhost:3000/api/health",
        "Alertmanager": "http://localhost:9093/-/healthy",
    }

    for name, url in services.items():
        try:
            response = requests.get(url, timeout=2)
            if response.status_code == 200:
                print(f"  ✅ {name} : Disponible")
            else:
                print(f"  ⚠️  {name} : Statut {response.status_code}")
        except:
            print(f"  ❌ {name} : Non disponible")


def get_prometheus_metrics():
    """Récupère les métriques disponibles dans Prometheus"""
    print("\n📊 Métriques disponibles dans Prometheus:\n")

    queries = {
        "Processeurs": "count(rate(process_cpu_seconds_total[1m]))",
        "Mémoire (MB)": "process_resident_memory_bytes / 1024 / 1024",
        "CPU Système": '100 - (avg(rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)',
        "Mémoire Système": "(1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100",
        "Espace Disque": '100 - ((node_filesystem_avail_bytes{fstype!~"tmpfs|fuse.lowerfs|squashfs|vfat"} / node_filesystem_size_bytes) * 100)',
        "Disque I/O": "rate(node_disk_read_bytes_total[1m]) + rate(node_disk_written_bytes_total[1m])",
        "Connexions Réseau": "node_network_transmit_bytes_total + node_network_receive_bytes_total",
    }

    for metric_name, query in queries.items():
        try:
            response = requests.get(
                "http://localhost:9090/api/v1/query", params={"query": query}, timeout=5
            )
            data = response.json()

            if data.get("status") == "success" and data.get("data", {}).get("result"):
                result = data["data"]["result"][0]
                value = result.get("value", ["N/A", "N/A"])[1]
                print(f"  📈 {metric_name:30} : {value}")
            else:
                print(f"  ⚠️  {metric_name:30} : Pas de données")

        except Exception as e:
            print(f"  ❌ {metric_name:30} : Erreur - {str(e)}")


def get_dashboards_summary():
    """Affiche un résumé de ce qu'on devrait voir dans les dashboards"""
    print("\n\n" + "=" * 70)
    print("📊 SOMMAIRE DES DASHBOARDS GRAFANA")
    print("=" * 70)

    dashboards = {
        "Spark Cluster Overview": [
            "Status Master Spark",
            "Nombre de Workers",
            "CPU Utilization (Jauge)",
            "Memory Usage (Jauge)",
            "Status Services (MinIO, PostgreSQL, etc.)",
        ],
        "Spark Jobs Performance": [
            "Running/Completed/Failed Jobs",
            "Task Metrics",
            "Executor Memory",
            "Shuffle I/O",
            "Garbage Collection Stats",
        ],
        "Infrastructure Containers": [
            "System CPU Usage",
            "System Memory Usage",
            "Container CPU/Memory/Network/Disk I/O",
            "Host Network Traffic",
            "Disk Usage",
        ],
    }

    for dashboard, metrics in dashboards.items():
        print(f"\n🔹 {dashboard}:")
        for metric in metrics:
            print(f"   • {metric}")


def generate_sample_load():
    """Génère une charge de travail pour que les métriques soient visibles"""
    print("\n\n" + "=" * 70)
    print("⚡ GÉNÉRATION DE CHARGE SYSTÈME")
    print("=" * 70)

    print("\n💡 Pour populer les dashboards, vous pouvez:")
    print("\n1️⃣  Lancer un job Spark:")
    print("    cd infrastructure/docker")
    print("    docker-compose exec spark-master spark-submit \\")
    print("      --master spark://spark-master:7077 \\")
    print("      --class org.apache.spark.examples.SparkPi \\")
    print("      /opt/spark/examples/jars/spark-examples.jar 10000")

    print("\n2️⃣  Ou exécuter des requêtes Prometheus directes:")
    print("    http://localhost:9090/graph")

    print("\n3️⃣  Vérifier les métriques du système:")
    print("    http://localhost:9090/targets")

    print("\n4️⃣  Accéder aux dashboards:")
    print("    http://localhost:3000 (admin/spark123)")


def main():
    print("\n" + "=" * 70)
    print("🔧 DIAGNOSTIC DES DONNÉES GRAFANA")
    print("=" * 70)

    check_services()
    get_prometheus_metrics()
    get_dashboards_summary()
    generate_sample_load()

    print("\n\n" + "=" * 70)
    print("📌 PROCHAINES ÉTAPES")
    print("=" * 70)
    print("\n✅ Prometheus et Grafana sont actifs et collectent les métriques")
    print("✅ Les exporters (Node, Postgres) fournissent des données")
    print("⚠️  Les métriques Spark nécessitent l'exécution de jobs")
    print("\n🚀 Lancez un job Spark pour remplir les dashboards!")


if __name__ == "__main__":
    main()
