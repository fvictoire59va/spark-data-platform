# 🔧 DIAGNOSTIC - ERR_CONNECTION_REFUSED

## ❌ Problème détecté

**Status Docker Desktop :** 🔴 **NON EN COURS D'EXÉCUTION**

Erreur obtenue :
```
error during connect: open //./pipe/dockerDesktopLinuxEngine: Le fichier spécifié est introuvable.
```

## 🎯 Cause

Docker Desktop n'est pas lancé. Sans le moteur Docker, les services de monitoring (Prometheus, Grafana, Alertmanager, etc.) ne peuvent pas fonctionner.

## ✅ SOLUTION - Lancer Docker Desktop

### Étape 1 : Démarrer Docker Desktop

**Méthode A - Via le menu Démarrer (Recommandé)**
```
1. Cliquez sur le bouton Windows (menu Démarrer)
2. Tapez "Docker Desktop"
3. Cliquez sur "Docker Desktop" pour lancer
4. ⏳ Attendez 10-15 secondes (important !)
   - Vous verrez l'icône Docker dans la barre d'état
   - Elle deviendra verte/active quand prête
```

**Méthode B - Via PowerShell (si installé)**
```powershell
# Vérifier si Docker Desktop peut être lancé
Get-Service Docker | Start-Service -ErrorAction SilentlyContinue

# Ou directement
& "C:\Program Files\Docker\Docker\Docker Desktop.exe"
```

### Étape 2 : Vérifier que Docker fonctionne

Après avoir lancé Docker Desktop et attendu 10 secondes, testez :

```powershell
# Cette commande devrait fonctionner SANS erreur
docker ps

# Vous devriez voir :
# CONTAINER ID  IMAGE  COMMAND  CREATED  STATUS  PORTS  NAMES
# (liste vide ou des conteneurs existants)
```

### Étape 3 : Démarrer les services

Une fois Docker Desktop actif, lancez le monitoring :

```powershell
# Option A - Script PowerShell (Recommandé)
cd d:\PROJETS\DOCKER - SPARK PIPELINE\spark-data-platform
.\scripts\start_monitoring.ps1

# Option B - Docker-compose direct
cd infrastructure/docker
docker-compose up -d

# Option C - Juste un service de test
docker run --rm -it gcr.io/cadvisor/cadvisor echo "Test"
```

### Étape 4 : Vérifier les services

```powershell
# Voir les conteneurs en cours
docker ps

# Vous devriez voir :
# prometheus, grafana, alertmanager, node-exporter, postgres-exporter
```

## 📋 Checklist de diagnostic

```powershell
# ✓ Docker Desktop est lancé et "Engine running" apparaît
docker ps

# ✓ Les services ont démarré
docker ps | Select-String "prometheus|grafana"

# ✓ Prometheus répond
curl http://localhost:9090/-/healthy

# ✓ Grafana répond
curl http://localhost:3000/api/health

# ✓ Alertmanager répond
curl http://localhost:9093/-/healthy
```

## 🐛 Troubleshooting avancé

### Docker ne veut pas démarrer

**Symptôme :** "Error initializing driver...Error getting container..."

**Solutions :**
```powershell
# 1. Redémarrer l'ordinateur (radical mais efficace)
Restart-Computer

# 2. Désactiver/activer Hyper-V
# Paramètres Windows → Programmes → Fonctionnalités Windows
# Désactiver/activer "Hyper-V" et redémarrer

# 3. Vider le cache Docker (⚠️ Supprime les images)
docker system prune -a
```

### Ports déjà utilisés

**Symptôme :** "Port 3000 is already allocated"

```powershell
# Trouver le processus utilisant le port
netstat -ano | findstr ":3000"
# Résultat : TCP  127.0.0.1:3000  LISTENING  <PID>

# Tuer le processus
taskkill /PID <PID> /F

# Ou changer les ports dans docker-compose.yml
# "3000:3000" → "3001:3000"
```

### Les services démarrent mais ne répondent pas

**Symptôme :** Docker ps montre les conteneurs, mais http://localhost:3000 refuse

```powershell
# 1. Vérifier les logs
docker logs prometheus
docker logs grafana

# 2. Vérifier la santé
docker inspect --format='{{.State.Health.Status}}' prometheus
docker inspect --format='{{.State.Health.Status}}' grafana

# 3. Redémarrer les services
docker-compose down
docker-compose up -d
```

## 🚀 Prochaines étapes

1. **NOW** : Lancez Docker Desktop (Menu Démarrer → Docker Desktop)
2. **WAIT** : Attendez 10-15 secondes
3. **RUN** : `.\scripts\start_monitoring.ps1`
4. **OPEN** : http://localhost:3000
5. **LOGIN** : admin / spark123

## 📞 Support rapide

| Problème | Commande | Solution |
|----------|----------|----------|
| Docker ne démarre pas | Restart-Computer | Redémarrer Windows |
| Port utilisé | netstat -ano \| findstr :3000 | Tuer le processus |
| Service ne répond pas | docker logs grafana | Vérifier les logs |
| Tout cassé | docker system prune -a | Reset Docker (radical) |

---

**Relancez Docker Desktop puis réessayez ! Ça devrait fonctionner.** ✨
