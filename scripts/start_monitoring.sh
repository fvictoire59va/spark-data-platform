#!/bin/bash
# =============================================================================
# Script pour démarrer la stack de monitoring Prometheus/Grafana
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_PATH="$SCRIPT_DIR/../infrastructure/docker"

# Couleurs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

show_banner() {
    echo -e "${CYAN}"
    echo "  ╔══════════════════════════════════════════════════════════════╗"
    echo "  ║     📊 SPARK DATA PLATFORM - MONITORING STACK               ║"
    echo "  ║         Prometheus | Grafana | Alertmanager                  ║"
    echo "  ╚══════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

show_urls() {
    echo -e "\n${YELLOW}📍 URLs d'accès:${NC}"
    echo -e "  ${GREEN}• Grafana:       http://localhost:3000  (admin/spark123)${NC}"
    echo -e "  ${GREEN}• Prometheus:    http://localhost:9090${NC}"
    echo -e "  ${GREEN}• Alertmanager:  http://localhost:9093${NC}"
    echo -e "  ${GREEN}• cAdvisor:      http://localhost:8085${NC}"
    echo -e "  ${GREEN}• Node Exporter: http://localhost:9100/metrics${NC}"
    echo ""
}

start_monitoring() {
    local only_monitoring=$1

    cd "$INFRA_PATH"

    if [ "$only_monitoring" = "true" ]; then
        echo -e "${YELLOW}🚀 Démarrage des services de monitoring uniquement...${NC}"
        docker-compose up -d prometheus grafana alertmanager node-exporter cadvisor postgres-exporter
    else
        echo -e "${YELLOW}🚀 Démarrage de la stack complète avec monitoring...${NC}"
        docker-compose up -d
    fi

    echo -e "\n${YELLOW}⏳ Attente du démarrage des services...${NC}"
    sleep 10

    # Vérification de l'état des services
    for service in prometheus grafana alertmanager; do
        status=$(docker inspect -f '{{.State.Status}}' $service 2>/dev/null || echo "not found")
        if [ "$status" = "running" ]; then
            echo -e "  ${GREEN}✅ $service : running${NC}"
        else
            echo -e "  ${RED}❌ $service : $status${NC}"
        fi
    done

    show_urls
}

stop_monitoring() {
    cd "$INFRA_PATH"
    echo -e "${YELLOW}🛑 Arrêt des services de monitoring...${NC}"
    docker-compose stop prometheus grafana alertmanager node-exporter postgres-exporter
    echo -e "${GREEN}✅ Services de monitoring arrêtés.${NC}"
}

show_status() {
    cd "$INFRA_PATH"
    echo -e "\n${YELLOW}📊 État des services de monitoring:${NC}"
    echo "════════════════════════════════════════════════════════"

    for service in prometheus grafana alertmanager node-exporter postgres-exporter; do
        status=$(docker inspect -f '{{.State.Status}}' $service 2>/dev/null || echo "not found")
        health=$(docker inspect -f '{{.State.Health.Status}}' $service 2>/dev/null || echo "")

        if [ "$status" = "running" ]; then
            health_str=""
            [ -n "$health" ] && health_str=" ($health)"
            echo -e "  ${GREEN}✅ $service : running$health_str${NC}"
        elif [ "$status" != "not found" ]; then
            echo -e "  ${YELLOW}⚠️  $service : $status${NC}"
        else
            echo -e "  ${RED}❌ $service : not found${NC}"
        fi
    done

    echo -e "\n${YELLOW}🔍 Vérification des endpoints:${NC}"
    echo "════════════════════════════════════════════════════════"

    check_endpoint() {
        local name=$1
        local url=$2
        if curl -s --connect-timeout 5 "$url" > /dev/null 2>&1; then
            echo -e "  ${GREEN}✅ $name : OK${NC}"
        else
            echo -e "  ${RED}❌ $name : Non disponible${NC}"
        fi
    }

    check_endpoint "Prometheus" "http://localhost:9090/-/healthy"
    check_endpoint "Grafana" "http://localhost:3000/api/health"
    check_endpoint "Alertmanager" "http://localhost:9093/-/healthy"

    show_urls
}

show_logs() {
    cd "$INFRA_PATH"
    echo -e "${YELLOW}📜 Logs des services de monitoring (Ctrl+C pour arrêter):${NC}"
    docker-compose logs -f prometheus grafana alertmanager
}

show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --only-monitoring  Démarrer uniquement les services de monitoring"
    echo "  --stop            Arrêter les services de monitoring"
    echo "  --status          Afficher l'état des services"
    echo "  --logs            Afficher les logs en continu"
    echo "  --help            Afficher cette aide"
    echo ""
}

# Point d'entrée principal
show_banner

case "$1" in
    --stop)
        stop_monitoring
        ;;
    --status)
        show_status
        ;;
    --logs)
        show_logs
        ;;
    --only-monitoring)
        start_monitoring true
        ;;
    --help)
        show_usage
        ;;
    *)
        start_monitoring false
        ;;
esac
