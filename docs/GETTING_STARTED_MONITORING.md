# 🚀 Guide de démarrage - Docker & Monitoring

## ⚠️ Prérequis

### 1. Docker Desktop doit être en cours d'exécution

**Symptôme du problème :**
```
error during connect: Get "http://%2F%2F.%2Fpipe%2FdockerDesktopLinuxEngine":
Le fichier spécifié est introuvable.
```

**Solution :**
1. Ouvrez Docker Desktop depuis le menu Démarrer
2. Attendez que l'application se charge complètement (icône dans la barre d'état)
3. Vérifiez que Docker fonctionne :
   ```powershell
   docker ps
   docker --version
   ```

### 2. Configuration initiale

**Vérifier que tout est prêt :**
```powershell
# Aller dans le dossier docker
cd infrastructure/docker

# Vérifier que les répertoires existent
Test-Path monitoring/prometheus
Test-Path monitoring/grafana
Test-Path monitoring/alertmanager

# Vérifier le fichier .env
Test-Path .env
```

## 🚀 Démarrage rapide

### Option 1 : Démarrer la stack complète

```powershell
cd infrastructure/docker
docker-compose up -d
```

**Ou utiliser le script :**
```powershell
.\scripts\start_monitoring.ps1
```

### Option 2 : Démarrer uniquement le monitoring

```powershell
cd infrastructure/docker
docker-compose up -d prometheus grafana alertmanager node-exporter postgres-exporter
```

**Ou utiliser le script :**
```powershell
.\scripts\start_monitoring.ps1 -OnlyMonitoring
```

## 📊 Vérifier que tout fonctionne

### Vérifier l'état des services
```powershell
# Afficher l'état
.\scripts\start_monitoring.ps1 -Status

# Ou directement
docker ps | Select-String -Pattern "prometheus|grafana|alertmanager|node-exporter"
```

### Tester les endpoints
```powershell
# Prometheus
Invoke-WebRequest -Uri "http://localhost:9090/-/healthy" -UseBasicParsing

# Grafana
Invoke-WebRequest -Uri "http://localhost:3000/api/health" -UseBasicParsing

# Alertmanager
Invoke-WebRequest -Uri "http://localhost:9093/-/healthy" -UseBasicParsing
```

## 🌐 Accéder aux interfaces

Ouvrez dans votre navigateur :

| Service | URL | Identifiants |
|---------|-----|--------------|
| 📊 **Grafana** | http://localhost:3000 | `admin` / `spark123` |
| 📈 **Prometheus** | http://localhost:9090 | - |
| 🔔 **Alertmanager** | http://localhost:9093 | - |

### Premiers pas avec Grafana
1. Connectez-vous avec `admin/spark123`
2. Accédez à "Dashboards" (menu de gauche)
3. Vous verrez les dashboards préconfigurés :
   - 📊 Spark Cluster Overview
   - ⚡ Spark Jobs & Performance
   - 🖥️ Infrastructure & Containers

## 🛑 Arrêter les services

```powershell
# Arrêter tout
cd infrastructure/docker
docker-compose down

# Ou utiliser le script
.\scripts\start_monitoring.ps1 -Stop

# Ou via docker directement
docker stop prometheus grafana alertmanager node-exporter postgres-exporter
```

## 🔧 Commandes utiles

### Voir les logs en direct
```powershell
# Tous les services
.\scripts\start_monitoring.ps1 -Logs

# Ou un service spécifique
docker logs -f prometheus
docker logs -f grafana
docker logs -f alertmanager
```

### Redémarrer un service
```powershell
docker-compose restart prometheus
docker-compose restart grafana
```

### Vérifier les volumes
```powershell
docker volume ls | Select-String -Pattern "prometheus|grafana|alertmanager"
```

### Nettoyer les données (⚠️ Supprime l'historique)
```powershell
# Supprimer les volumes de données
docker volume rm spark-data-platform_prometheus-data
docker volume rm spark-data-platform_grafana-data
docker volume rm spark-data-platform_alertmanager-data
```

## 📊 Cas d'usage

### Monitorer un job Spark
1. Lancez votre job Spark
2. Ouvrez Grafana → Dashboard "Spark Jobs & Performance"
3. Vous verrez les métriques du job en temps réel

### Configurer des alertes
1. Éditez `infrastructure/docker/monitoring/alertmanager/alertmanager.yml`
2. Configurez vos canaux (Email, Slack, Teams, etc.)
3. Redémarrez : `docker-compose restart alertmanager`

### Ajouter un nouveau dashboard
1. Créez le dashboard dans Grafana
2. Exportez le JSON (Dashboard settings → JSON Model)
3. Enregistrez dans `infrastructure/docker/monitoring/grafana/provisioning/dashboards/`
4. Redémarrez Grafana

## 🐛 Dépannage

### Docker Desktop ne se lance pas
```powershell
# Vérifier la status
Get-Service DockerDesktopService

# Redémarrer Docker
docker system prune -a
```

### Les services ne démarrent pas
```powershell
# Vérifier les logs
docker-compose logs prometheus
docker-compose logs grafana

# Vérifier la config
docker-compose config

# Vérifier les ports
netstat -ano | findstr ":9090"  # Prometheus
netstat -ano | findstr ":3000"  # Grafana
netstat -ano | findstr ":9093"  # Alertmanager
```

### Grafana ne trouve pas Prometheus
```powershell
# Depuis le conteneur Grafana
docker-compose exec grafana wget -qO- http://prometheus:9090/api/v1/status/config

# Ou redémarrez Grafana
docker-compose restart grafana
```

### Pas de données dans les dashboards
1. Attendez 30-60 secondes pour la collecte des premières métriques
2. Vérifiez que Prometheus scrape les targets :
   ```
   http://localhost:9090/targets
   ```
3. Vérifiez les logs : `docker logs prometheus`

## 📚 Ressources

- [Documentation Prometheus](https://prometheus.io/docs/)
- [Documentation Grafana](https://grafana.com/docs/)
- [Spark Monitoring](https://spark.apache.org/docs/latest/monitoring.html)
- [MONITORING_GUIDE.md](MONITORING_GUIDE.md) - Guide complet du monitoring

## 💡 Astuces

- Les dashboards se rechargent automatiquement chaque 30 secondes
- Les alertes actives s'affichent en haut à droite de Grafana
- Vous pouvez partager des dashboards via le bouton "Share" en haut à droite
- Prometheus conserve les métriques pendant 15 jours par défaut (configurable via `.env`)
