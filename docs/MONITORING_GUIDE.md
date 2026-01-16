# 📊 Monitoring Stack - Prometheus & Grafana

## Vue d'ensemble

La stack de monitoring intégrée fournit une observabilité complète de la plateforme Spark Data Platform avec :

- **Prometheus** - Collecte et stockage des métriques
- **Grafana** - Visualisation et dashboards
- **Alertmanager** - Gestion et routage des alertes
- **Node Exporter** - Métriques système de l'hôte
- **Postgres Exporter** - Métriques de la base PostgreSQL

> **Note Windows**: cAdvisor n'est pas compatible avec Docker Desktop sur Windows. Pour les environnements Linux, vous pouvez activer cAdvisor en décommentant le service dans `docker-compose.yml`.

## 🚀 Démarrage rapide

### Lancer la stack complète avec monitoring

```bash
cd infrastructure/docker
docker-compose up -d
```

### Accès aux interfaces

| Service | URL | Identifiants par défaut |
|---------|-----|------------------------|
| **Grafana** | http://localhost:3000 | admin / spark123 |
| **Prometheus** | http://localhost:9090 | - |
| **Alertmanager** | http://localhost:9093 | - |
| **Node Exporter** | http://localhost:9100/metrics | - |

## 📈 Dashboards Grafana

Trois dashboards préconfigurés sont disponibles :

### 1. Spark Cluster Overview
- Statut du cluster Spark (Master, Workers)
- Utilisation CPU/Mémoire des conteneurs Spark
- État de MinIO et PostgreSQL
- I/O réseau et disque des conteneurs

### 2. Spark Jobs & Performance
- Jobs en cours d'exécution
- Taux de complétion des tâches
- Utilisation mémoire des executors
- Métriques de shuffle I/O
- Temps de Garbage Collection (GC)

### 3. Infrastructure & Containers
- Ressources système de l'hôte (CPU, RAM, Disque)
- Métriques de tous les conteneurs
- Trafic réseau
- I/O disque par conteneur

## 🔔 Alertes configurées

### Alertes Spark
| Alerte | Seuil | Sévérité |
|--------|-------|----------|
| SparkMasterDown | 1 minute indisponible | 🔴 Critical |
| SparkWorkerDown | 1 minute indisponible | 🔴 Critical |
| NoActiveSparkWorkers | 0 workers pendant 2 min | 🔴 Critical |
| SparkMasterHighMemoryUsage | > 85% pendant 5 min | 🟡 Warning |
| SparkWorkerHighCPUUsage | > 90% pendant 10 min | 🟡 Warning |
| SparkJobFailed | Tout échec de job | 🟡 Warning |
| SparkJobLongRunning | > 1 heure | 🟡 Warning |
| SparkTaskFailureRateHigh | > 10% d'échecs | 🟡 Warning |

### Alertes Infrastructure
| Alerte | Seuil | Sévérité |
|--------|-------|----------|
| MinioDown | 1 minute indisponible | 🔴 Critical |
| PostgresDown | 1 minute indisponible | 🔴 Critical |
| HostDiskSpaceCritical | < 5% libre | 🔴 Critical |
| ContainerHighCPU | > 80% pendant 5 min | 🟡 Warning |
| ContainerHighMemory | > 85% pendant 5 min | 🟡 Warning |
| MinioDiskUsageHigh | < 15% libre | 🟡 Warning |
| PostgresHighConnections | > 80% max_connections | 🟡 Warning |
| HostHighCPU | > 80% pendant 10 min | 🟡 Warning |
| HostHighMemory | > 85% pendant 5 min | 🟡 Warning |
| HostDiskSpaceLow | < 15% libre | 🟡 Warning |

## ⚙️ Configuration

### Variables d'environnement

Ajoutez ces variables à votre fichier `.env` :

```env
# Grafana
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=spark123

# Rétention Prometheus (défaut: 15 jours)
PROMETHEUS_RETENTION_TIME=15d
```

### Configurer les notifications d'alertes

Éditez `monitoring/alertmanager/alertmanager.yml` pour configurer :

#### Email
```yaml
receivers:
  - name: 'email-receiver'
    email_configs:
      - to: 'votre-email@example.com'
        from: 'alertmanager@spark-platform.local'
        smarthost: 'smtp.example.com:587'
        auth_username: 'user'
        auth_password: 'password'
```

#### Slack
```yaml
global:
  slack_api_url: 'https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK'

receivers:
  - name: 'slack-receiver'
    slack_configs:
      - channel: '#spark-alerts'
        send_resolved: true
```

#### Microsoft Teams
```yaml
receivers:
  - name: 'teams-receiver'
    webhook_configs:
      - url: 'https://outlook.office.com/webhook/YOUR/TEAMS/WEBHOOK'
```

## 🔧 Commandes utiles

### Vérifier l'état des services
```bash
# Statut des conteneurs de monitoring
docker-compose ps prometheus grafana alertmanager node-exporter cadvisor postgres-exporter

# Logs Prometheus
docker-compose logs -f prometheus

# Logs Grafana
docker-compose logs -f grafana
```

### Recharger la configuration Prometheus
```bash
curl -X POST http://localhost:9090/-/reload
```

### Vérifier les alertes actives
```bash
curl http://localhost:9090/api/v1/alerts | jq
```

### Tester une alerte
```bash
# Simuler une alerte via Alertmanager API
curl -H "Content-Type: application/json" -d '[{
  "labels": {
    "alertname": "TestAlert",
    "severity": "warning"
  },
  "annotations": {
    "summary": "Test alert notification"
  }
}]' http://localhost:9093/api/v1/alerts
```

## 📊 Requêtes PromQL utiles

### Spark Cluster
```promql
# Workers actifs
count(up{job="spark-workers"} == 1)

# Utilisation CPU moyenne des conteneurs Spark
avg(rate(container_cpu_usage_seconds_total{name=~"spark.*"}[5m])) * 100

# Mémoire totale utilisée par Spark
sum(container_memory_usage_bytes{name=~"spark.*"})
```

### Infrastructure
```promql
# Utilisation CPU de l'hôte
100 - (avg(rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)

# Mémoire disponible
node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes * 100

# Espace disque libre
node_filesystem_avail_bytes{fstype!~"tmpfs|overlay"} / node_filesystem_size_bytes * 100
```

### Conteneurs
```promql
# Top 5 conteneurs par CPU
topk(5, rate(container_cpu_usage_seconds_total{name!=""}[5m]) * 100)

# Top 5 conteneurs par mémoire
topk(5, container_memory_usage_bytes{name!=""})
```

## 🗂️ Structure des fichiers

```
infrastructure/docker/monitoring/
├── prometheus/
│   ├── prometheus.yml          # Configuration principale
│   └── rules/
│       └── spark_alerts.yml    # Règles d'alertes
├── alertmanager/
│   └── alertmanager.yml        # Configuration des notifications
└── grafana/
    └── provisioning/
        ├── dashboards/
        │   ├── dashboards.yml
        │   ├── spark-cluster-overview.json
        │   ├── spark-jobs-performance.json
        │   └── infrastructure-containers.json
        └── datasources/
            └── datasources.yml
```

## 🔄 Mise à jour des dashboards

### Via l'interface Grafana
1. Modifiez le dashboard dans Grafana
2. Sauvegardez (Ctrl+S)
3. Exportez le JSON (Dashboard settings → JSON Model)
4. Mettez à jour le fichier dans `monitoring/grafana/provisioning/dashboards/`

### Ajouter un nouveau dashboard
1. Créez le dashboard dans Grafana
2. Exportez le JSON
3. Ajoutez le fichier `.json` dans `monitoring/grafana/provisioning/dashboards/`
4. Le dashboard sera automatiquement importé au redémarrage

## 🐛 Dépannage

### Prometheus ne collecte pas les métriques
```bash
# Vérifier les targets
curl http://localhost:9090/api/v1/targets | jq

# Vérifier la config
docker-compose exec prometheus promtool check config /etc/prometheus/prometheus.yml
```

### Grafana ne se connecte pas à Prometheus
```bash
# Tester la connexion depuis le conteneur Grafana
docker-compose exec grafana wget -qO- http://prometheus:9090/api/v1/status/config
```

### Les alertes ne sont pas envoyées
```bash
# Vérifier la config Alertmanager
docker-compose exec alertmanager amtool check-config /etc/alertmanager/alertmanager.yml

# Voir les alertes en cours
curl http://localhost:9093/api/v2/alerts | jq
```

## 📚 Ressources

- [Documentation Prometheus](https://prometheus.io/docs/)
- [Documentation Grafana](https://grafana.com/docs/)
- [Spark Metrics](https://spark.apache.org/docs/latest/monitoring.html)
- [PromQL Tutorial](https://prometheus.io/docs/prometheus/latest/querying/basics/)
