# 🚀 Spark Data Platform

[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/)
[![Spark 3.5](https://img.shields.io/badge/spark-3.5-orange.svg)](https://spark.apache.org/)
[![Delta Lake 3.0](https://img.shields.io/badge/delta--lake-3.0-00ADD8.svg)](https://delta.io/)

Plateforme de traitement de données basée sur **Apache Spark** et **Delta Lake**, suivant l'architecture **Medallion** (Bronze → Silver → Gold). Elle permet d'ingérer, transformer et exposer des données de manière scalable et orchestrée.

---

## 📋 Prérequis

| Outil | Version |
|-------|---------|
| Python | 3.11+ |
| Docker & Docker Compose | Latest |
| Java | 11+ |

---

## 🚀 Démarrage rapide

```bash
# 1. Cloner le projet
git clone https://github.com/fvictoire59va/spark-data-platform.git
cd spark-data-platform

# 2. Installer les dépendances
make dev

# 3. Démarrer l'infrastructure (Spark, MinIO, PostgreSQL)
make spark-up

# 4. Lancer un job de test
make run-job-local JOB=sales.ingest_orders
```

---

## 🏗 Architecture Medallion

```
Sources (CSV, API, DB)
        │
        ▼
   ┌─────────┐
   │ BRONZE  │  → Données brutes, immutables
   └────┬────┘
        ▼
   ┌─────────┐
   │ SILVER  │  → Données nettoyées, dédupliquées, typées
   └────┬────┘
        ▼
   ┌─────────┐
   │  GOLD   │  → Agrégations métier, KPIs, tables BI
   └─────────┘
```

---

## 📁 Structure du projet

```
spark-data-platform/
├── src/
│   ├── core/           # Spark session, config, logging, métriques
│   ├── common/         # Readers, writers, transformers, quality checks
│   └── pipelines/      # Pipelines métier (ex: sales)
├── configs/            # Configuration par environnement (dev, staging, prod)
├── infrastructure/     # Docker Compose, Terraform
├── orchestration/      # DAGs Airflow
├── scripts/            # Scripts utilitaires
└── tests/              # Tests unitaires, intégration, e2e
```

---

## 🎯 Commandes principales

| Commande | Description |
|----------|-------------|
| `make dev` | Installer les dépendances |
| `make spark-up` | Démarrer le cluster Spark |
| `make spark-down` | Arrêter le cluster |
| `make run-job-local JOB=<job>` | Lancer un job localement |
| `make test` | Lancer tous les tests |
| `make lint` | Vérifier la qualité du code |
| `make docs-serve` | Documentation locale (http://localhost:8000) |

---

## 🌐 Interfaces Web

| Service | URL | Notes |
|---------|-----|-------|
| Spark Master | http://localhost:8080 | UI du cluster Spark |
| Jupyter Lab | http://localhost:8888 | Token: `spark123` |
| MinIO | http://localhost:9001 | Stockage S3-compatible |
| Airflow | http://localhost:8082 | Après `make airflow-up` |
| Grafana | http://localhost:3000 | Monitoring |

---

## 🔄 Lancer un pipeline

```bash
# En local (développement)
./scripts/submit_job.sh sales.ingest_orders dev --mode local

# Sur le cluster (production)
./scripts/submit_job.sh sales.transform_orders prod --date 2024-01-15
```

---

## 📚 Documentation

La documentation détaillée est disponible dans le dossier `docs/` :

- [Architecture](docs/architecture.md)
- [Getting Started](docs/getting-started.md)
- [Guide Silver/Gold](docs/SILVER_GOLD_GUIDE.md)
- [Monitoring](docs/MONITORING_GUIDE.md)

```bash
# Servir la documentation localement
make docs-serve
```

---

## 📄 Licence

MIT License - voir [LICENSE](LICENSE)
