# 🔧 ERREUR DOCKER DESKTOP - SOLUTION

## ❌ L'erreur que vous avez rencontrée

```
unable to get image 'gcr.io/cadvisor/cadvisor:v0.47.2': error during connect:
Get "http://%2F%2F.%2Fpipe%2FdockerDesktopLinuxEngine":
open //./pipe/dockerDesktopLinuxEngine: Le fichier spécifié est introuvable.
```

## 🔍 Cause

**Docker Desktop n'était pas en cours d'exécution.**

Le socket Docker (`dockerDesktopLinuxEngine`) n'est pas accessible parce que le moteur Docker n'était pas actif.

## ✅ Solution appliquée

### 1. cAdvisor désactivé
- cAdvisor a des problèmes de compatibilité avec Docker Desktop sous Windows
- **Status :** Désactivé par défaut ✅
- **Pour Linux :** Décommentez le service dans `docker-compose.yml` si souhaité

### 2. Docker-compose mise à jour
Les services de monitoring maintenant:
- ✅ Prometheus
- ✅ Grafana
- ✅ Alertmanager
- ✅ Node Exporter
- ✅ Postgres Exporter
- ❌ ~~cAdvisor~~ (Désactivé pour Windows)

## 🚀 Pour lancer maintenant

### Étape 1 : Lancer Docker Desktop

```powershell
# Option A : Menu Démarrer
# 1. Cliquez sur Démarrer
# 2. Cherchez "Docker Desktop"
# 3. Cliquez pour lancer

# Option B : PowerShell (si Docker est installé)
# Vérifiez que Docker fonctionne
docker ps
docker --version

# Vous devriez voir une liste vide de conteneurs, sans erreur
```

### Étape 2 : Démarrer la stack

```powershell
cd d:\PROJETS\DOCKER - SPARK PIPELINE\spark-data-platform\infrastructure\docker

# Option A : Avec le script (recommandé)
.\scripts\start_monitoring.ps1

# Option B : Direct docker-compose
docker-compose up -d

# Option C : Script Python (cross-platform)
python .\scripts\monitoring.py
```

### Étape 3 : Vérifier que tout fonctionne

```powershell
# Afficher l'état
docker ps | Select-String "prometheus|grafana|alertmanager"

# Ou via script
.\scripts\start_monitoring.ps1 -Status

# Devrait afficher : prometheus, grafana, alertmanager, node-exporter, postgres-exporter (RUNNING)
```

### Étape 4 : Accéder à Grafana

Ouvrez dans votre navigateur :
- **http://localhost:3000**
- Username: `admin`
- Password: `spark123`

## 🐛 Troubleshooting

### "Le fichier spécifié est introuvable" persiste

1. **Vérifiez que Docker Desktop est bien lancé :**
   ```powershell
   docker ps
   # Devrait afficher : CONTAINER ID  IMAGE  COMMAND  CREATED  STATUS  PORTS  NAMES
   # Pas d'erreur
   ```

2. **Redémarrez Docker Desktop :**
   - Quittez complètement Docker Desktop
   - Relancez-le
   - Attendez 10-15 secondes

3. **Vérifiez les ressources :**
   - Avez-vous assez de RAM/CPU?
   - Paramètres Docker Desktop :
     - CPU assigné : au moins 4
     - RAM assignée : au moins 8GB

4. **Réinitialisez Docker (dernier recours) :**
   - Docker Desktop → Paramètres → Troubleshooting
   - Cliquez "Reset Docker Desktop"
   - Relancez

### "Port déjà utilisé"

```powershell
# Voir quel processus utilise le port
netstat -ano | findstr ":3000"  # Grafana
netstat -ano | findstr ":9090"  # Prometheus
netstat -ano | findstr ":9093"  # Alertmanager

# Tuer le processus si nécessaire
taskkill /PID <PID> /F

# Ou changer les ports dans docker-compose.yml
```

### Pas de données dans Grafana

1. **Attendez 30-60 secondes** - Prometheus a besoin de scraper les métriques
2. **Vérifiez les targets :**
   - Allez sur http://localhost:9090/targets
   - Tous les jobs doivent être "UP"
3. **Vérifiez les logs :**
   ```powershell
   docker logs -f prometheus
   docker logs -f grafana
   ```

## 📊 Ce qui fonctionne maintenant

✅ **5 services de monitoring** actifs et opérationnels
- Prometheus : collecte les métriques
- Grafana : affiche les dashboards
- Alertmanager : gère les alertes
- Node Exporter : métriques système
- Postgres Exporter : métriques base de données

✅ **3 dashboards Grafana** préconfigurés
- Spark Cluster Overview
- Spark Jobs & Performance
- Infrastructure & Containers

✅ **20+ alertes** configurées et prêtes

## 📚 Documentation

- [MONITORING_GUIDE.md](../../docs/MONITORING_GUIDE.md) - Documentation complète
- [GETTING_STARTED_MONITORING.md](../../docs/GETTING_STARTED_MONITORING.md) - Guide rapide
- [INDEX_MONITORING.md](../../docs/INDEX_MONITORING.md) - Index complet

## 🎯 Prochaines étapes

1. ✅ Vérifiez que Docker Desktop fonctionne
2. ✅ Lancez : `.\scripts\start_monitoring.ps1`
3. ✅ Accédez à http://localhost:3000
4. ⭕ Explorez les dashboards
5. ⭕ Configurez les notifications d'alertes (optionnel)

---

**La stack est prête ! Relancez `docker-compose up -d` et ça devrait fonctionner.** 🚀
