# 🚀 Spark Data Platform

[![CI](https://github.com/company/spark-data-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/company/spark-data-platform/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/company/spark-data-platform/branch/main/graph/badge.svg)](https://codecov.io/gh/company/spark-data-platform)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/)
[![Spark 3.5](https://img.shields.io/badge/spark-3.5-orange.svg)](https://spark.apache.org/)
[![Delta Lake 3.0](https://img.shields.io/badge/delta--lake-3.0-00ADD8.svg)](https://delta.io/)

Plateforme de données basée sur **Apache Spark** et **Delta Lake** suivant l'architecture **Medallion** (Bronze → Silver → Gold).

## 📋 Table des matières

- [Architecture](#-architecture)
- [Prérequis](#-prérequis)
- [Installation](#-installation)
- [Documentation](#-documentation)
- [Utilisation](#-utilisation)
- [Structure du projet](#-structure-du-projet)
- [Développement](#-développement)
- [Déploiement](#-déploiement)
- [Monitoring](#-monitoring)
- [Contributing](#-contributing)

## 🏗 Architecture
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│   (APIs, Databases, Files, Kafka, S3)                          │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                     BRONZE LAYER (Raw)                          │
│   • Ingestion brute                                             │
│   • Données immutables                                          │
│   • Schema-on-read                                              │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SILVER LAYER (Cleaned)                       │
│   • Données nettoyées et validées                              │
│   • Déduplication                                               │
│   • Typage fort                                                 │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                     GOLD LAYER (Business)                       │
│   • Agrégations métier                                          │
│   • Tables optimisées pour la BI                               │
│   • KPIs et métriques                                          │
└─────────────────────────────────────────────────────────────────┘

## 📦 Prérequis

- **Python** 3.11+
- **Poetry** 1.7+
- **Docker** & Docker Compose
- **Java** 11+ (pour Spark)
- **AWS CLI** (pour déploiement cloud)

## 🚀 Installation

```bash
# Cloner le repository
git clone https://github.com/fvictoire59va/spark-data-platform.git
cd spark-data-platform

# Installer les dépendances
make dev

# Démarrer le cluster Spark local
make spark-up

# Vérifier l'installation
make test

## 📚 Documentation

La documentation complète du projet est générée avec **MkDocs** et disponible en ligne.

### Consulter la documentation localement

```bash
# Servir la documentation en développement
make docs-serve
# → http://localhost:8000

# Générer la documentation
make docs
```

### Documentation en ligne

- **Architecture** : Voir [docs/architecture.md](docs/architecture.md)
- **Guide d'intégration** : Voir [docs/guides/integration.md](docs/guides/integration.md)
- **Quick Start** : Voir [docs/guides/quickstart.md](docs/guides/quickstart.md)
- **Getting Started** : Voir [docs/getting-started.md](docs/getting-started.md)
- **GitHub Pages** : https://spark-data-platform.example.com (après déploiement)

### Structures de la documentation

```
docs/
├── index.md                    # Accueil
├── architecture.md             # Architecture complète
├── getting-started.md          # Quick start
├── api-reference.md            # Référence API
└── guides/                     # Guides détaillés
    ├── README.md               # Index des guides
    ├── integration.md          # Intégration documentation
    ├── quickstart.md           # Quick start guide
    ├── before-after.md         # Analyse avant/après
    ├── checklist.md            # Checklist finalisation
    └── summary.md              # Récapitulatif technique
```

## 🎯 Utilisation

### Lancer un job localement

```bash
# Via Make
make run-job-local JOB=sales.ingest_orders

# Via script
./scripts/submit_job.sh sales.ingest_orders dev --mode local
```

### Lancer un job sur le cluster

```bash
# Développement
./scripts/submit_job.sh sales.ingest_orders dev

# Production
./scripts/submit_job.sh sales.transform_orders prod --date 2024-01-15
```

### Interfaces Web

| Service | URL |
|---------|-----|
| Spark Master UI | http://localhost:8080 |
| Spark History | http://localhost:18080 |
| Jupyter Lab | http://localhost:8888 (token: spark123) |
| Airflow Web | http://localhost:8082 (après `make airflow-up`) |
| MinIO Console | http://localhost:9001 |

Structure du projet
spark-data-platform/
├── src/
│   ├── core/               # Composants centraux
│   │   ├── config.py       # Configuration Pydantic
│   │   ├── spark_session.py
│   │   ├── logger.py
│   │   └── exceptions.py
│   ├── common/             # Composants réutilisables
│   │   ├── readers/        # Lecteurs de données
│   │   ├── writers/        # Writers de données
│   │   ├── transformers/   # Transformations communes
│   │   └── quality/        # Contrôles qualité
│   └── pipelines/          # Pipelines par domaine
│       └── sales/
│           ├── jobs/       # Jobs Spark
│           ├── schemas/    # Schémas de données
│           └── tests/      # Tests du domaine
├── configs/                # Configuration par environnement
├── tests/                  # Tests globaux
├── infrastructure/         # IaC (Docker, Terraform)
├── orchestration/          # Airflow DAGs
└── scripts/                # Scripts utilitaires
🛠 Développement
Commandes utiles
# Lancer les tests
make test              # Tous les tests
make test-unit         # Tests unitaires
make test-cov          # Avec couverture

# Qualité de code
make lint              # Vérification
make format            # Formatage auto

# Documentation
make docs-serve        # Servir la doc localement

Workflow Git

Créer une branche feature: git checkout -b feature/ma-feature
Développer avec des commits conventionnels
Lancer les tests: make test
Créer une Pull Request

Conventions de commit
feat: nouvelle fonctionnalité
fix: correction de bug
docs: documentation
style: formatage
refactor: refactoring
test: ajout de tests
chore: maintenance

Déploiement
Environnements

Environnement Branch Déploiement
--------------------------------
Development develop Automatique
Staging staging Automatique
Production main Manuel


Déployer
# Développement
make deploy-dev

# Staging
make deploy-staging

# Production (avec confirmation)
make deploy-prod

Monitoring
Métriques disponibles

Job Duration: Temps d'exécution des jobs
Records Processed: Nombre d'enregistrements traités
Error Rate: Taux d'erreurs
Data Quality Score: Score de qualité des données

Dashboards

Grafana: http://localhost:3000
Prometheus: http://localhost:9090

🤝 Contributing

Fork le projet
Créer une branche (git checkout -b feature/amazing-feature)
Commit les changements (git commit -m 'feat: add amazing feature')
Push la branche (git push origin feature/amazing-feature)
Ouvrir une Pull Request

📄 License
MIT License - voir LICENSE
📞 Support

Documentation: docs/
Issues: GitHub Issues
Slack: #data-platform
