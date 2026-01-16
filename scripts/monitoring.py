#!/usr/bin/env python3
"""
Script utilitaire pour gérer la stack de monitoring
Utilisable sur Windows, macOS et Linux
"""

import subprocess
import sys
import time
from pathlib import Path
from typing import Optional


class MonitoringManager:
    def __init__(self):
        self.script_dir = Path(__file__).parent
        self.infra_path = self.script_dir / "infrastructure" / "docker"
        self.services = [
            "prometheus",
            "grafana",
            "alertmanager",
            "node-exporter",
            "postgres-exporter",
        ]

    def run_command(self, cmd: list, cwd: Optional[Path] = None) -> tuple:
        """Exécute une commande et retourne stdout et returncode"""
        try:
            result = subprocess.run(
                cmd, cwd=cwd or self.infra_path, capture_output=True, text=True, timeout=30
            )
            return result.stdout, result.returncode
        except subprocess.TimeoutExpired:
            return "Timeout", 1
        except Exception as e:
            return str(e), 1

    def check_docker(self) -> bool:
        """Vérifie que Docker fonctionne"""
        output, code = self.run_command(["docker", "ps"])
        return code == 0

    def start_stack(self, only_monitoring: bool = False) -> bool:
        """Démarre la stack"""
        if not self.check_docker():
            print("❌ Docker n'est pas disponible. Lancez Docker Desktop et réessayez.")
            return False

        print(
            "🚀 Démarrage de la stack..."
            if not only_monitoring
            else "🚀 Démarrage du monitoring..."
        )

        if only_monitoring:
            cmd = ["docker-compose", "up", "-d"] + self.services
        else:
            cmd = ["docker-compose", "up", "-d"]

        output, code = self.run_command(cmd)

        if code == 0:
            print("✅ Stack démarrée avec succès")
            print("\n⏳ Attente du démarrage des services (10 secondes)...")
            time.sleep(10)
            self.show_status()
            self.show_urls()
            return True
        else:
            print(f"❌ Erreur au démarrage: {output}")
            return False

    def stop_stack(self) -> bool:
        """Arrête la stack"""
        print("🛑 Arrêt de la stack...")
        cmd = ["docker-compose", "down"]
        output, code = self.run_command(cmd)

        if code == 0:
            print("✅ Stack arrêtée")
            return True
        else:
            print(f"❌ Erreur: {output}")
            return False

    def show_status(self) -> None:
        """Affiche l'état des services"""
        print("\n📊 État des services:")
        print("=" * 60)

        for service in self.services:
            output, _ = self.run_command(["docker", "inspect", "-f", "{{.State.Status}}", service])
            status = output.strip()

            if status == "running":
                print(f"  ✅ {service}: running")
            elif status:
                print(f"  ⚠️  {service}: {status}")
            else:
                print(f"  ❌ {service}: not found")

    def show_urls(self) -> None:
        """Affiche les URLs d'accès"""
        print("\n📍 URLs d'accès:")
        print("=" * 60)
        print("  🔵 Grafana:       http://localhost:3000  (admin/spark123)")
        print("  🟢 Prometheus:    http://localhost:9090")
        print("  🟡 Alertmanager:  http://localhost:9093")
        print("  ⚫ Node Exporter: http://localhost:9100/metrics")
        print()

    def show_logs(self) -> None:
        """Affiche les logs en direct"""
        print("📜 Logs (Ctrl+C pour arrêter)...")
        services_str = " ".join(["prometheus", "grafana", "alertmanager"])
        cmd = ["docker-compose", "logs", "-f"] + services_str.split()
        self.run_command(cmd)

    def test_health(self) -> None:
        """Teste la santé des services"""
        print("\n🔍 Test de santé des services:")
        print("=" * 60)

        endpoints = {
            "Prometheus": "http://localhost:9090/-/healthy",
            "Grafana": "http://localhost:3000/api/health",
            "Alertmanager": "http://localhost:9093/-/healthy",
        }

        try:
            import urllib.request

            for name, url in endpoints.items():
                try:
                    response = urllib.request.urlopen(url, timeout=5)
                    if response.status == 200:
                        print(f"  ✅ {name}: OK")
                    else:
                        print(f"  ⚠️  {name}: {response.status}")
                except Exception as e:
                    print(f"  ❌ {name}: Non disponible ({str(e)[:50]})")
        except ImportError:
            print("  (urllib3 non disponible, test ignoré)")


def main():
    mgr = MonitoringManager()

    if len(sys.argv) < 2:
        mgr.start_stack(only_monitoring=False)
        return

    cmd = sys.argv[1].lower()

    if cmd == "--help" or cmd == "-h":
        print(
            """
Usage: python monitoring.py [COMMAND]

Commands:
  (default)         Démarrer la stack complète
  --monitoring-only Démarrer uniquement le monitoring
  --stop           Arrêter la stack
  --status         Afficher l'état des services
  --logs           Afficher les logs en direct
  --health         Tester la santé des services
  --help           Afficher cette aide
        """
        )
    elif cmd == "--monitoring-only":
        mgr.start_stack(only_monitoring=True)
    elif cmd == "--stop":
        mgr.stop_stack()
    elif cmd == "--status":
        mgr.show_status()
        mgr.show_urls()
    elif cmd == "--logs":
        mgr.show_logs()
    elif cmd == "--health":
        mgr.test_health()
    else:
        print(f"Commande inconnue: {cmd}")
        print("Utilisez --help pour voir les commandes disponibles")


if __name__ == "__main__":
    main()
