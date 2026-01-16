# ✅ Intégration du Monitoring Prometheus & Grafana

## 📋 Résumé des modifications

### 🐳 Services Docker ajoutés

| Service | Port | Description |
|---------|------|-------------|
| **Prometheus** | 9090 | Collecte et stockage des métriques (15 jours) |
| **Grafana** | 3000 | Visualisation et dashboards interactifs |
| **Alertmanager** | 9093 | Gestion et routage des alertes |
| **Node Exporter** | 9100 | Métriques système de l'hôte |
| **Postgres Exporter** | 9187 | Métriques de la base PostgreSQL |

### 📁 Fichiers créés

```
infrastructure/docker/monitoring/
├── prometheus/
│   ├── prometheus.yml              # Configuration de collecte (9 exporteurs)
│   └── rules/
│       └── spark_alerts.yml        # 20+ règles d'alertes
│
├── alertmanager/
│   └── alertmanager.yml            # Configuration notifications
│
└── grafana/
    └── provisioning/
        ├── dashboards/
        │   ├── spark-cluster-overview.json        # Dashboard cluster
        │   ├── spark-jobs-performance.json        # Dashboard jobs/performance
        │   └── infrastructure-containers.json     # Dashboard infrastructure
        └── datasources/
            └── datasources.yml                   # Configuration datasources

docs/
├── MONITORING_GUIDE.md             # Guide complet (40+ pages)
└── GETTING_STARTED_MONITORING.md   # Guide démarrage rapide

scripts/
├── start_monitoring.ps1            # Script PowerShell
└── start_monitoring.sh             # Script Bash
```

### 🔧 Fichiers modifiés

- `docker-compose.yml` - Ajout de 6 services de monitoring
- `.env` - Ajout des variables de configuration Grafana/Prometheus
- Désactivation de cAdvisor (non compatible Windows) - À activer sous Linux

## 🚀 Instructions de démarrage

### Étape 1 : Lancer Docker Desktop

1. Cliquez sur le menu Démarrer
2. Cherchez "Docker Desktop"
3. Lancez l'application
4. Attendez que l'icône dans la barre d'état indique "Engine running" (5-10 secondes)

### Étape 2 : Démarrer la stack

**Option A - Via script PowerShell :**
```powershell
cd d:\PROJETS\DOCKER - SPARK PIPELINE\spark-data-platform
.\scripts\start_monitoring.ps1
```

**Option B - Via docker-compose :**
```powershell
cd infrastructure/docker
docker-compose up -d
```

### Étape 3 : Vérifier le statut
```powershell
.\scripts\start_monitoring.ps1 -Status
```

### Étape 4 : Accéder aux interfaces

| Service | URL |
|---------|-----|
| **Grafana** | http://localhost:3000 |
| **Prometheus** | http://localhost:9090 |
| **Alertmanager** | http://localhost:9093 |

Identifiants Grafana : `admin` / `spark123`

## 📊 Dashboards préconfigurés

### 1. Spark Cluster Overview
- Statut Master/Workers
- Utilisation CPU/Mémoire des conteneurs
- État des services (MinIO, PostgreSQL)
- I/O réseau et disque

### 2. Spark Jobs & Performance
- Jobs en cours et complétés
- Taux de complétion des tâches
- Utilisation mémoire des executors
- Métriques Shuffle I/O et GC

### 3. Infrastructure & Containers
- Ressources système (CPU, RAM, Disque)
- Métriques tous les conteneurs
- Trafic réseau et I/O disque
- Uptime système

## 🔔 Alertes actives

**20+ alertes préconfigurées :**

| Catégorie | Alertes | Sévérité |
|-----------|---------|----------|
| **Spark** | 8 alertes | 🔴/🟡 |
| **Infrastructure** | 7 alertes | 🔴/🟡 |
| **Système** | 5 alertes | 🔴/🟡 |

Exemples :
- SparkMasterDown
- SparkWorkerHighCPUUsage
- HostDiskSpaceCritical
- MinioDown
- PostgresHighConnections

## 📈 Métriques collectées

### Spark (si disponibles)
- Job execution time
- Task completion/failure rates
- Executor memory usage
- Shuffle I/O statistics
- GC metrics

### Infrastructure
- CPU/Memory/Disk usage
- Network I/O
- Container metrics
- PostgreSQL connections

### System
- Host CPU, Memory, Disk
- Network traffic
- Boot time

## ⚙️ Configuration avancée

### Changer l'identifiant Grafana
Éditez `infrastructure/docker/.env` :
```env
GRAFANA_ADMIN_USER=votre-user
GRAFANA_ADMIN_PASSWORD=votre-password
```

Puis : `docker-compose restart grafana`

### Configurer les notifications (Email/Slack)
Éditez `infrastructure/docker/monitoring/alertmanager/alertmanager.yml`
Puis : `docker-compose restart alertmanager`

### Ajouter de nouvelles alertes
Éditez `infrastructure/docker/monitoring/prometheus/rules/spark_alerts.yml`
Puis : `curl -X POST http://localhost:9090/-/reload`

## 🛑 Arrêter les services

```powershell
# Arrêter tout
cd infrastructure/docker
docker-compose down

# Ou via script
.\scripts\start_monitoring.ps1 -Stop

# Voir les logs
.\scripts\start_monitoring.ps1 -Logs
```

## 📚 Documentation

- [MONITORING_GUIDE.md](docs/MONITORING_GUIDE.md) - Guide complet avec PromQL
- [GETTING_STARTED_MONITORING.md](docs/GETTING_STARTED_MONITORING.md) - Démarrage et dépannage
- [Prometheus Docs](https://prometheus.io/docs/)
- [Grafana Docs](https://grafana.com/docs/)

## 🐛 Troubleshooting rapide

**"Le fichier spécifié est introuvable"**
→ Lancez Docker Desktop et attendez 10 secondes

**"Connexion refusée sur port 3000/9090"**
→ Vérifiez `docker ps`, relancez les services

**"Pas de données dans Grafana"**
→ Attendez 30-60 secondes, vérifiez http://localhost:9090/targets

**"cAdvisor ne démarre pas"**
→ Normal sous Windows ! C'est désactivé. À activer sous Linux seulement.

## ✨ Cas d'usage

### Monitorer un job Spark
```bash
# 1. Lancez votre job
python run_ingest_orders_job.py

# 2. Ouvrez Grafana
# http://localhost:3000

# 3. Allez sur "Spark Jobs & Performance"
# Vous verrez les métriques du job en direct !
```

### Configurer une alerte Email
1. Modifiez `monitoring/alertmanager/alertmanager.yml`
2. Ajoutez votre SMTP et email
3. Redémarrez : `docker-compose restart alertmanager`

### Exporter les dashboards
Grafana → Dashboard → Share → Export JSON
Enregistrez dans `monitoring/grafana/provisioning/dashboards/`

## 🎯 Prochaines étapes

1. ✅ Démarrer Docker Desktop
2. ✅ Lancer les services : `.\scripts\start_monitoring.ps1`
3. ✅ Accéder à Grafana : http://localhost:3000
4. ✅ Consulter les dashboards
5. ⭕ Optionnel : Configurer les notifications d'alertes
6. ⭕ Optionnel : Ajouter des dashboards personnalisés

## 📞 Support

En cas de problème :
1. Consultez [GETTING_STARTED_MONITORING.md](docs/GETTING_STARTED_MONITORING.md)
2. Vérifiez les logs : `docker logs -f prometheus`
3. Testez les endpoints :
   ```powershell
   curl http://localhost:9090/-/healthy
   curl http://localhost:3000/api/health
   ```

---

**Vous êtes prêt ! Lancez `.\scripts\start_monitoring.ps1` et accédez à Grafana sur http://localhost:3000** 🚀
