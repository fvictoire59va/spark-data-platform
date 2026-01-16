#!/usr/bin/env python3
"""
Script de diagnostic pour les erreurs de connexion Docker
Détecte et propose des solutions aux problèmes courants
"""

import subprocess


class DockerDiagnostic:
    def __init__(self):
        self.issues = []
        self.warnings = []
        self.success = []

    def run_command(self, cmd: list) -> tuple[str, int]:
        """Exécute une commande et retourne stdout et code retour"""
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            return result.stdout, result.returncode
        except subprocess.TimeoutExpired:
            return "Timeout", 1
        except FileNotFoundError:
            return "Command not found", 1
        except Exception as e:
            return str(e), 1

    def check_docker_installed(self) -> bool:
        """Vérifie que Docker est installé"""
        print("🔍 Vérification de l'installation de Docker...")
        output, code = self.run_command(["docker", "--version"])

        if code == 0:
            self.success.append(f"Docker installé : {output.strip()}")
            print(f"  ✅ {output.strip()}")
            return True
        else:
            self.issues.append("Docker n'est pas installé ou pas dans PATH")
            print("  ❌ Docker n'est pas installé")
            return False

    def check_docker_running(self) -> bool:
        """Vérifie que le moteur Docker fonctionne"""
        print("\n🔍 Vérification du moteur Docker...")
        output, code = self.run_command(["docker", "ps"])

        if code == 0:
            self.success.append("Moteur Docker actif et réactif")
            print("  ✅ Moteur Docker est actif")
            return True
        else:
            self.issues.append("Moteur Docker n'est pas en cours d'exécution")
            print("  ❌ Moteur Docker n'est pas actif")
            if "dockerDesktopLinuxEngine" in output or "pipe" in output:
                self.warnings.append("Cause probable : Docker Desktop n'est pas lancé")
            return False

    def check_docker_compose(self) -> bool:
        """Vérifie que docker-compose est disponible"""
        print("\n🔍 Vérification de docker-compose...")
        output, code = self.run_command(["docker-compose", "--version"])

        if code == 0:
            self.success.append(f"docker-compose disponible : {output.strip()}")
            print(f"  ✅ {output.strip()}")
            return True
        else:
            self.issues.append("docker-compose n'est pas disponible")
            print("  ❌ docker-compose n'est pas disponible")
            return False

    def check_ports(self) -> dict:
        """Vérifie la disponibilité des ports"""
        print("\n🔍 Vérification des ports...")

        ports = {
            3000: "Grafana",
            9090: "Prometheus",
            9093: "Alertmanager",
            9100: "Node Exporter",
            9187: "Postgres Exporter",
        }

        available = {}

        for port, service in ports.items():
            # Essayer de faire une connexion TCP simple
            import socket

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(("127.0.0.1", port))
            sock.close()

            if result == 0:
                available[port] = False
                print(f"  ⚠️  Port {port} ({service}) : EN UTILISATION")
                self.warnings.append(f"Port {port} est déjà utilisé")
            else:
                available[port] = True
                print(f"  ✅ Port {port} ({service}) : Disponible")

        return available

    def check_containers(self) -> dict:
        """Vérifie l'état des conteneurs"""
        print("\n🔍 Vérification des conteneurs...")

        containers = {
            "prometheus": "Prometheus",
            "grafana": "Grafana",
            "alertmanager": "Alertmanager",
            "node-exporter": "Node Exporter",
            "postgres-exporter": "Postgres Exporter",
        }

        statuses = {}

        for container, name in containers.items():
            output, code = self.run_command(
                ["docker", "inspect", "-f", "{{.State.Status}}", container]
            )

            status = output.strip()
            statuses[container] = status

            if status == "running":
                print(f"  ✅ {name} : running")
                self.success.append(f"{name} est actif")
            elif status:
                print(f"  ⚠️  {name} : {status}")
                self.warnings.append(f"{name} n'est pas en cours d'exécution")
            else:
                print(f"  ❌ {name} : non trouvé")
                self.issues.append(f"{name} n'existe pas (le stack n'a pas démarré)")

        return statuses

    def check_endpoints(self) -> dict:
        """Vérifie la réactivité des endpoints"""
        print("\n🔍 Vérification des endpoints...")

        endpoints = {
            "Prometheus": "http://localhost:9090/-/healthy",
            "Grafana": "http://localhost:3000/api/health",
            "Alertmanager": "http://localhost:9093/-/healthy",
        }

        results = {}

        try:
            import urllib.request

            for name, url in endpoints.items():
                try:
                    response = urllib.request.urlopen(url, timeout=3)
                    if response.status == 200:
                        print(f"  ✅ {name} : OK")
                        self.success.append(f"{name} répond correctement")
                        results[name] = True
                    else:
                        print(f"  ⚠️  {name} : Statut {response.status}")
                        self.warnings.append(f"{name} retourne le statut {response.status}")
                        results[name] = False
                except Exception:
                    print(f"  ❌ {name} : Non disponible")
                    self.issues.append(f"{name} ne répond pas")
                    results[name] = False
        except ImportError:
            print("  ⚠️  urllib non disponible, test des endpoints ignoré")

        return results

    def print_summary(self):
        """Affiche le résumé du diagnostic"""
        print("\n" + "=" * 70)
        print("📊 RÉSUMÉ DU DIAGNOSTIC")
        print("=" * 70)

        if self.success:
            print(f"\n✅ SUCCÈS ({len(self.success)}):")
            for msg in self.success:
                print(f"   • {msg}")

        if self.warnings:
            print(f"\n⚠️  AVERTISSEMENTS ({len(self.warnings)}):")
            for msg in self.warnings:
                print(f"   • {msg}")

        if self.issues:
            print(f"\n❌ PROBLÈMES ({len(self.issues)}):")
            for msg in self.issues:
                print(f"   • {msg}")

        print("\n" + "=" * 70)
        print("💡 SOLUTIONS")
        print("=" * 70)

        if "Docker n'est pas installé" in self.issues:
            print(
                """
⚠️  Docker n'est pas installé
→ Téléchargez Docker Desktop : https://www.docker.com/products/docker-desktop
→ Installez-le et relancez ce diagnostic
            """
            )

        if "Moteur Docker n'est pas en cours d'exécution" in self.issues:
            print(
                """
⚠️  Docker Desktop n'est pas lancé
→ Menu Démarrer → Docker Desktop
→ Attendez 10-15 secondes que l'icône devienne verte
→ Puis réessayez
            """
            )

        if any("n'est pas disponible" in issue or "n'existe pas" in issue for issue in self.issues):
            print(
                """
⚠️  Les services ne sont pas démarrés
→ Assurez-vous que Docker Desktop fonctionne
→ Lancez : cd infrastructure/docker && docker-compose up -d
→ Attendez 10-15 secondes
→ Puis réessayez
            """
            )

        if "Port" in str(self.warnings):
            print(
                """
⚠️  Des ports sont déjà utilisés
→ Trouvez le processus : netstat -ano | findstr :3000
→ Tuez-le : taskkill /PID <PID> /F
→ Ou changez les ports dans docker-compose.yml
            """
            )

        if not self.issues and self.success:
            print("\n🎉 TOUT FONCTIONNE !")
            print(
                """
Accédez aux interfaces :
  • Grafana      : http://localhost:3000 (admin/spark123)
  • Prometheus   : http://localhost:9090
  • Alertmanager : http://localhost:9093
            """
            )

    def run_full_diagnostic(self):
        """Exécute le diagnostic complet"""
        print("\n" + "=" * 70)
        print("🔍 DIAGNOSTIC DOCKER - MONITORING SPARK")
        print("=" * 70)

        # Vérifications principales
        docker_installed = self.check_docker_installed()
        if not docker_installed:
            self.print_summary()
            return

        docker_running = self.check_docker_running()
        self.check_docker_compose()
        self.check_ports()
        self.check_containers()
        self.check_endpoints()

        # Afficher le résumé
        self.print_summary()


def main():
    diagnostic = DockerDiagnostic()
    diagnostic.run_full_diagnostic()


if __name__ == "__main__":
    main()
