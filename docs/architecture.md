# 🏗️ Spark Data Platform - Architecture Complète

## 1. Vue d'Ensemble du Projet

Cette plateforme de données entreprise est basée sur **Apache Spark 3.5** et **Delta Lake 3.0**, implémentant l'architecture **Medallion** (Bronze → Silver → Gold). C'est une stack moderne et production-ready.

### Technologies Clés

| Composant | Technologie | Version |
|-----------|-------------|---------|
| Processing | Apache Spark | 3.5.0 |
| Storage Format | Delta Lake | 3.0.0 |
| Orchestration | Apache Airflow | 2.10+ |
| Langage | Python | 3.11 |
| Containerisation | Docker Compose | - |
| IaC Cloud | Terraform | 1.5+ |
| Package Manager | Poetry | 1.7+ |

---

## 2. Architecture Applicative (Medallion Pattern)

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│   (PostgreSQL, APIs, Kafka, S3, Fichiers CSV/JSON/Parquet)     │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                     🥉 BRONZE LAYER (Raw)                       │
│   • Ingestion brute avec métadonnées (_ingestion_timestamp)    │
│   • Données immutables (append-only)                           │
│   • Schéma flexible (schema-on-read)                           │
│   • Jobs: ingest_orders.py, ingest_customers.py                │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    🥈 SILVER LAYER (Cleaned)                    │
│   • Données nettoyées et validées                              │
│   • Enrichissement (joins clients/produits)                    │
│   • Calcul marges, déduplication                               │
│   • Jobs: enrich_orders_silver.py, transform_orders.py         │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                     🥇 GOLD LAYER (Business)                    │
│   • Agrégations métier (daily, par produit, par client)        │
│   • Tables optimisées pour la BI                               │
│   • KPIs et métriques calculés                                 │
│   • Jobs: aggregate_sales.py, aggregate_gold.py                │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Structure du Code Source

### 3.1 Module Core (`src/core/`)

Le cœur du framework avec des composants réutilisables :

| Fichier | Rôle |
|---------|------|
| `base_job.py` | Classe abstraite ETL avec pattern `Extract → Transform → Validate → Load` |
| `config_manager.py` | Gestion config via Pydantic + fichiers YAML par environnement |
| `spark_session.py` | Factory Singleton pour SparkSession avec Delta Lake |
| `logger.py` | Logging structuré avec structlog |
| `exceptions.py` | Hiérarchie d'exceptions métier |
| `metrics.py` | Métriques Prometheus |

### 3.2 Module Common (`src/common/`)

Composants génériques réutilisables :

#### Readers (`src/common/readers/`)
- `JDBCReader` - Bases de données (PostgreSQL, MySQL, etc.)
- `DeltaReader` - Tables Delta Lake
- `CSVReader`, `JSONReader`, `ParquetReader` - Fichiers
- `KafkaReader` - Streaming Kafka

#### Writers (`src/common/writers/`)
- `DeltaWriter` - Écriture Delta avec support merge/upsert
- `CSVWriter`, `JSONWriter`, `ParquetWriter`
- `JDBCWriter`, `KafkaWriter`

#### Transformers (`src/common/transformers/`)
- `silver_transformers.py` - Enrichissement, calcul marges, déduplication
- `gold_transformers.py` - Agrégations daily/produit/client

### 3.3 Pipelines Métier (`src/pipelines/`)

Organisation par **domaine métier** (ex: `sales`) :

```
src/pipelines/sales/
├── config/           # Configs YAML par environnement
│   ├── dev.yaml
│   ├── staging.yaml
│   └── prod.yaml
├── jobs/             # Jobs Spark exécutables
│   ├── ingest_orders.py        # Bronze
│   ├── enrich_orders_silver.py # Silver
│   └── aggregate_sales.py      # Gold
├── schemas/          # Schémas Spark (StructType)
└── tests/            # Tests spécifiques au domaine
```

---

## 4. Infrastructure Docker (Développement Local)

### 4.1 Architecture des Services

Le fichier `infrastructure/docker/docker-compose.yml` définit :

```
┌─────────────────────────────────────────────────────────────────┐
│                      CLUSTER SPARK LOCAL                        │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │ spark-master │  │spark-worker-1│  │spark-worker-2│          │
│  │   :8080 UI   │  │  4GB / 2cores│  │  4GB / 2cores│          │
│  │   :7077      │  │    :8081     │  │    :8082     │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │spark-history │  │    minio     │  │ metastore-db │          │
│  │   :18080     │  │  :9000 :9001 │  │    :5432     │          │
│  │  (logs UI)   │  │ (S3 compat)  │  │ (PostgreSQL) │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐                                               │
│  │   jupyter    │   Notebook interactif pour exploration       │
│  │    :8888     │   Token: spark123                            │
│  └──────────────┘                                               │
└─────────────────────────────────────────────────────────────────┘
```

### 4.2 Ports Exposés

| Service | Port | URL |
|---------|------|-----|
| Spark Master UI | 8080 | http://localhost:8080 |
| Spark Worker 1 | 8081 | http://localhost:8081 |
| Spark Worker 2 | 8082 | http://localhost:8082 |
| Spark History | 18080 | http://localhost:18080 |
| MinIO API | 9000 | http://localhost:9000 |
| MinIO Console | 9001 | http://localhost:9001 |
| PostgreSQL | 5432 | `postgresql://hive:hive123@localhost:5432/metastore` |
| Jupyter Lab | 8888 | http://localhost:8888 (token: spark123) |

### 4.3 Stack Airflow (`docker-compose-airflow.yml`)

Ajoute l'orchestration avec :
- `airflow-webserver` (port 8082)
- `airflow-scheduler`
- `airflow-db` (PostgreSQL port 5433)

---

## 5. Infrastructure Cloud (Production)

### 5.1 Terraform (`infrastructure/terraform/`)

L'infrastructure AWS est définie via **Terraform** avec des modules :

```
infrastructure/terraform/
├── main.tf              # Orchestration des modules
├── variables.tf         # Variables d'entrée
├── outputs.tf           # Outputs (ARN, endpoints)
├── providers.tf         # Provider AWS + tags
├── backend.tf           # State S3 + DynamoDB lock
└── modules/
    ├── networking/      # VPC, subnets, security groups
    ├── storage/         # Buckets S3 (bronze, silver, gold, logs)
    ├── spark-cluster/   # EMR ou EC2 avec Spark
    └── monitoring/      # CloudWatch, alertes SNS
```

### 5.2 Ressources Provisionnées

| Module | Ressources |
|--------|------------|
| **Networking** | VPC (10.0.0.0/16), 3 AZs, subnets privés/publics |
| **Storage** | 4 buckets S3 : bronze, silver, gold, logs avec lifecycle rules |
| **Spark Cluster** | Master (m5.xlarge), Workers (m5.2xlarge x3) |
| **Monitoring** | CloudWatch Logs, métriques, alertes email |

---

## 6. Environnements et Configuration

### 6.1 Les 4 Environnements

| Environnement | Usage | Spark Master | Storage |
|---------------|-------|--------------|---------|
| **LOCAL** | Dev machine | `local[*]` | Fichiers locaux |
| **DEV** | Tests équipe | Docker cluster | MinIO (S3 local) |
| **STAGING** | Pré-prod | AWS EMR small | S3 staging |
| **PROD** | Production | AWS EMR scaled | S3 prod |

### 6.2 Configuration par Environnement

**Fichiers** : `configs/dev/spark.conf`, `configs/staging/`, `configs/prod/`

```properties
# configs/dev/spark.conf
SPARK_MASTER="local[*]"
SPARK_DRIVER_MEMORY="2g"
SPARK_EXECUTOR_MEMORY="4g"
SPARK_EXECUTOR_CORES="2"

# Paths S3
S3_BUCKET="spark-data-platform-dev"
BRONZE_PATH="s3a://${S3_BUCKET}/bronze"
SILVER_PATH="s3a://${S3_BUCKET}/silver"
GOLD_PATH="s3a://${S3_BUCKET}/gold"
```

### 6.3 Gestion via Pydantic

La classe `Settings` dans `config_manager.py` :
- Charge les variables depuis `.env` et variables d'environnement
- Valide les types avec Pydantic
- Supporte les 4 environnements via `Environment` enum

---

## 7. Workflow de Développement

### 7.1 Installation Initiale

```powershell
# 1. Cloner le repo
git clone https://github.com/fvictoire59va/spark-data-platform.git
cd spark-data-platform

# 2. Installer les dépendances avec Poetry
poetry install       # Ou via Makefile: make dev

# 3. Activer l'environnement virtuel
.\.venv\Scripts\Activate.ps1

# 4. Installer les pre-commit hooks
pre-commit install
```

### 7.2 Développement Local (sans Docker)

```powershell
# Lancer les tests unitaires
make test-unit

# Lancer un job en mode local
python -m src.pipelines.sales.jobs.ingest_orders --environment local

# Jupyter pour exploration
make jupyter
```

### 7.3 Développement avec Docker

```powershell
# Démarrer le cluster Spark + MinIO + PostgreSQL
make spark-up
# Ou: docker-compose -f infrastructure/docker/docker-compose.yml up -d

# Vérifier les logs
make spark-logs

# Soumettre un job au cluster
./scripts/submit_job.sh sales.ingest_orders dev

# Arrêter
make spark-down
```

### 7.4 Commandes Make Principales

| Commande | Description |
|----------|-------------|
| `make dev` | Installe toutes les dépendances + pre-commit |
| `make test` | Lance tous les tests |
| `make test-unit` | Tests unitaires uniquement |
| `make test-cov` | Tests + rapport de couverture |
| `make lint` | Vérifie qualité (ruff + mypy + bandit) |
| `make format` | Formate le code (black + ruff fix) |
| `make spark-up` | Démarre le cluster Docker |
| `make spark-down` | Arrête le cluster |
| `make docs-serve` | Documentation MkDocs locale |

---

## 8. Workflow de Déploiement

### 8.1 Pipeline CI/CD Suggéré

```
┌─────────────────────────────────────────────────────────────────┐
│                         GIT WORKFLOW                            │
│                                                                 │
│  feature/xyz  ──► PR ──► main ──► tag v1.2.0 ──► deploy        │
│                    │                                            │
│              ┌─────┴─────┐                                      │
│              │   CI/CD   │                                      │
│              │  Actions  │                                      │
│              └─────┬─────┘                                      │
│                    │                                            │
│     ┌──────────────┼──────────────┐                             │
│     ▼              ▼              ▼                             │
│  ┌─────┐      ┌─────────┐    ┌──────┐                          │
│  │ DEV │      │ STAGING │    │ PROD │                          │
│  │Auto │      │  Manual │    │Manual│                          │
│  │     │      │ Approve │    │Approve│                         │
│  └─────┘      └─────────┘    └──────┘                          │
└─────────────────────────────────────────────────────────────────┘
```

### 8.2 Déploiement Manuel

```bash
# Déploiement DEV (automatique après merge)
./scripts/deploy.sh dev

# Déploiement STAGING (validation)
./scripts/deploy.sh staging --skip-tests  # Si tests CI OK

# Déploiement PRODUCTION (avec confirmation)
./scripts/deploy.sh prod --version v1.2.0
# ⚠️ Demande confirmation interactive
```

### 8.3 Infrastructure avec Terraform

```bash
# Initialisation
make tf-init

# Plan des changements
make tf-plan ENV=dev

# Appliquer
make tf-apply ENV=dev
```

---

## 9. Orchestration Airflow

### 9.1 Structure des DAGs

Les DAGs sont dans `orchestration/airflow/dags/` :

- `sales_pipeline_dag.py` - Pipeline complet Bronze→Silver→Gold
- `sales_pipeline_silver_gold_dag.py` - Silver+Gold uniquement

### 9.2 Exécution du Pipeline

```
DAG: sales_pipeline (quotidien à 6h)

start ─► bronze_layer ─► silver_layer ─► gold_layer ─► end
              │               │              │
              ▼               ▼              ▼
         ingest_orders   transform      aggregate
         ingest_customers orders        sales
```

### 9.3 Démarrer Airflow

```powershell
# Avec le script fourni
./scripts/start_airflow.ps1

# Ou manuellement
docker-compose -f infrastructure/docker/docker-compose-airflow.yml up -d

# UI Airflow: http://localhost:8082
```

---

## 10. Tests

### 10.1 Structure des Tests

```
tests/
├── conftest.py          # Fixtures pytest (SparkSession, sample data)
├── unit/                # Tests unitaires (sans Spark réel)
│   ├── test_config.py
│   ├── test_transformers.py
│   ├── test_silver_transformers.py
│   └── test_gold_transformers.py
├── integration/         # Tests avec Spark local
│   ├── test_delta_operations.py
│   ├── test_sales_pipeline.py
│   └── test_spark_job.py
└── e2e/                 # Tests end-to-end
    └── test_full_pipeline.py
```

### 10.2 Exécution

```powershell
# Tous les tests
make test

# Par catégorie
make test-unit          # Rapides, sans Spark
make test-integration   # Avec SparkSession locale
make test-e2e           # Pipeline complet

# Avec couverture
make test-cov           # Génère htmlcov/index.html
```

---

## 11. Bonnes Pratiques Implémentées

| Domaine | Pratique |
|---------|----------|
| **Code** | Type hints, docstrings, Black+Ruff formatting |
| **Architecture** | Pattern ETL abstrait (`BaseSparkJob`), DI via config |
| **Config** | Pydantic validation, YAML par environnement |
| **Tests** | 3 niveaux (unit/integration/e2e), fixtures réutilisables |
| **Logging** | Structlog JSON, métriques Prometheus |
| **Sécurité** | Bandit scans, secrets via variables d'env |
| **CI/CD** | Pre-commit hooks, tests automatisés |
| **IaC** | Terraform modulaire, state remote S3 |

---

## 12. Résumé des Commandes Essentielles

```powershell
# ========== DÉVELOPPEMENT ==========
poetry install                    # Installer dépendances
make dev                          # Setup complet dev
make test                         # Lancer tests
make lint && make format          # Qualité code

# ========== LOCAL DOCKER ==========
make spark-up                     # Démarrer cluster
make spark-logs                   # Voir logs
./scripts/submit_job.sh sales.ingest_orders dev
make spark-down                   # Arrêter

# ========== DÉPLOIEMENT ==========
./scripts/deploy.sh dev           # Deploy dev
./scripts/deploy.sh prod          # Deploy prod (avec confirm)

# ========== INFRASTRUCTURE ==========
make tf-plan ENV=staging          # Plan Terraform
make tf-apply ENV=staging         # Appliquer
```

---

## 13. Diagramme de Flux de Données Complet

```
                                    ┌─────────────────┐
                                    │   Sources       │
                                    │  Externes       │
                                    └────────┬────────┘
                                             │
        ┌────────────────────────────────────┼────────────────────────────────────┐
        │                                    │                                    │
        ▼                                    ▼                                    ▼
┌───────────────┐                  ┌───────────────┐                    ┌───────────────┐
│   PostgreSQL  │                  │     Kafka     │                    │   S3 / Files  │
│   (JDBC)      │                  │   (Streaming) │                    │  (CSV/JSON)   │
└───────┬───────┘                  └───────┬───────┘                    └───────┬───────┘
        │                                  │                                    │
        └──────────────────────────────────┼────────────────────────────────────┘
                                           │
                                           ▼
                              ┌────────────────────────┐
                              │     SPARK CLUSTER      │
                              │  ┌──────────────────┐  │
                              │  │   BRONZE JOBS    │  │
                              │  │  (Ingestion)     │  │
                              │  └────────┬─────────┘  │
                              │           │            │
                              │  ┌────────▼─────────┐  │
                              │  │   SILVER JOBS    │  │
                              │  │ (Transformation) │  │
                              │  └────────┬─────────┘  │
                              │           │            │
                              │  ┌────────▼─────────┐  │
                              │  │    GOLD JOBS     │  │
                              │  │  (Aggregation)   │  │
                              │  └────────┬─────────┘  │
                              └───────────┼────────────┘
                                          │
                     ┌────────────────────┼────────────────────┐
                     │                    │                    │
                     ▼                    ▼                    ▼
            ┌───────────────┐    ┌───────────────┐    ┌───────────────┐
            │  Delta Lake   │    │   Metastore   │    │   Metrics     │
            │   (S3/MinIO)  │    │  (PostgreSQL) │    │  (Prometheus) │
            └───────┬───────┘    └───────────────┘    └───────────────┘
                    │
                    ▼
            ┌───────────────┐
            │   BI Tools    │
            │ (Tableau, etc)│
            └───────────────┘
```

---

## 14. Contact et Support

- **Repository**: https://github.com/fvictoire59va/spark-data-platform
- **Documentation**: `make docs-serve` → http://localhost:8000
- **Issues**: GitHub Issues

---

*Documentation générée le 16 janvier 2026*
