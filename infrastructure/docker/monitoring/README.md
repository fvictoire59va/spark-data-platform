# 📊 Monitoring - Prometheus & Grafana

## Fichiers de configuration

### Prometheus
- **prometheus.yml** - Configuration des targets (Spark, PostgreSQL, Node Exporter, etc.)
- **rules/spark_alerts.yml** - 20+ règles d'alertes pour Spark, infra et système

### Alertmanager
- **alertmanager.yml** - Configuration du routage des alertes (Email, Slack, Teams, webhook)

### Grafana
- **datasources/datasources.yml** - Configuration automatique de Prometheus comme datasource
- **dashboards/dashboards.yml** - Configuration du provisioning des dashboards
- **dashboards/spark-cluster-overview.json** - Dashboard d'aperçu du cluster
- **dashboards/spark-jobs-performance.json** - Dashboard de performance des jobs
- **dashboards/infrastructure-containers.json** - Dashboard d'infrastructure

## Démarrage rapide

```bash
# Depuis le répertoire docker
cd infrastructure/docker

# Démarrer tout
docker-compose up -d

# Ou seulement le monitoring
docker-compose up -d prometheus grafana alertmanager node-exporter postgres-exporter
```

## Accès

| Service | URL | Identifiants |
|---------|-----|--------------|
| **Grafana** | http://localhost:3000 | admin / spark123 |
| **Prometheus** | http://localhost:9090 | - |
| **Alertmanager** | http://localhost:9093 | - |

## Documentation

- [MONITORING_GUIDE.md](../../docs/MONITORING_GUIDE.md) - Guide complet
- [GETTING_STARTED_MONITORING.md](../../docs/GETTING_STARTED_MONITORING.md) - Démarrage rapide
- [INDEX_MONITORING.md](../../docs/INDEX_MONITORING.md) - Index complet

## Notes

- Prometheus scrape les métriques toutes les 15 secondes
- Les données sont conservées pendant 15 jours (configurable)
- Les dashboards se rafraîchissent toutes les 30 secondes
- cAdvisor est désactivé sous Windows (non compatible Docker Desktop)
