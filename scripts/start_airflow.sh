#!/bin/bash
# Script pour initialiser et démarrer Airflow avec Spark

set -e

echo "🚀 Démarrage de l'infrastructure Airflow + Spark..."

# Répertoire du script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DOCKER_DIR="$SCRIPT_DIR/../infrastructure/docker"

cd "$DOCKER_DIR"

# Arrêter les containers existants
echo "⏹️  Arrêt des containers existants..."
docker-compose down 2>/dev/null || true

# Démarrer avec docker-compose-airflow.yml
echo "🔧 Démarrage des services..."
docker-compose -f docker-compose-airflow.yml up -d

# Attendre que les services soient prêts
echo "⏳ Attente de l'initialisation des services..."
sleep 30

# Afficher le statut
echo "✅ Services démarrés!"
echo ""
echo "📋 URLS D'ACCÈS:"
echo "  🎯 Airflow UI: http://localhost:8888 (admin/admin)"
echo "  ⚡ Spark Master: http://localhost:8080"
echo "  📊 Spark History: http://localhost:18080"
echo "  📓 Jupyter: http://localhost:8889 (token: spark123)"
echo ""
echo "📦 Logs:"
echo "  tail -f logs/airflow/webserver.log"
echo "  tail -f logs/airflow/scheduler.log"
