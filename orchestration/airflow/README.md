# 🚀 Orchestration avec Apache Airflow

## Vue d'ensemble

Ce projet utilise Apache Airflow pour orchestrer le pipeline de données Spark. Le DAG `sales_pipeline` exécute une série de jobs dans un ordre défini :

```
Bronze (Ingestion) → Silver (Transformation) → Gold (Agrégation)
```

## 📋 Architecture

### Services Docker

1. **airflow-webserver** (Port 8888)
   - Interface Web Airflow
   - Monitoring des DAGs et tasks
   - Configuration des variables et connexions

2. **airflow-scheduler**
   - Planification des exécutions
   - Gestion des dépendances entre tasks
   - Retry automatique en cas d'erreur

3. **airflow-db** (PostgreSQL)
   - Backend de stockage Airflow
   - Historique des executions
   - Variables et connexions

4. **spark-master** + **spark-workers**
   - Cluster Spark pour l'exécution des jobs
   - Accessible via `spark://spark-master:7077`

## 🎯 DAG: sales_pipeline

### Configuration

```python
{
    "dag_id": "sales_pipeline",
    "schedule_interval": "0 6 * * *",  # Quotidien à 06h00
    "owner": "data-engineering",
    "retries": 1,
    "execution_timeout": "2 hours"
}
```

### Workflow

#### Bronze Layer
- **ingest_orders**: Lit depuis PostgreSQL (JDBC), écrit en Delta Lake
- **ingest_customers**: Lit depuis PostgreSQL, écrit en Delta Lake

#### Silver Layer
- **transform_orders**: Nettoie et enrichit les commandes

#### Gold Layer
- **aggregate_sales**: Crée les agrégations par jour/statut/client

## 🔌 Connexions Airflow

### spark_default (SparkSubmitOperator)

```
Type: Spark
Host: spark-master
Port: 7077
```

### postgres_metastore (pour JDBC)

```
Type: Postgres
Host: metastore-db
Port: 5432
User: hive
Password: hive123
Database: metastore
```

## 🚀 Démarrage

### Docker Compose

```bash
cd infrastructure/docker
docker-compose -f docker-compose-airflow.yml up -d
```

### Vérifier le statut

```bash
# Web UI
open http://localhost:8888

# CLI
docker-compose exec airflow-webserver airflow dags list
docker-compose exec airflow-webserver airflow dags test sales_pipeline 2026-01-15
```

## 🧪 Exécution Manuelle d'un DAG

```bash
# Depuis le container Airflow
docker exec airflow-webserver airflow dags test sales_pipeline 2026-01-15

# Ou via la Web UI
# Menu → DAGs → sales_pipeline → Trigger DAG
```

## 📊 Monitoring

### Logs en temps réel

```bash
# Scheduler
docker-compose logs -f airflow-scheduler

# Webserver
docker-compose logs -f airflow-webserver

# Spark Master
docker-compose logs -f spark-master
```

### Spark UI

- Master: http://localhost:8080
- Application: http://localhost:4040 (pendant l'exécution)
- History: http://localhost:18080

## 🔧 Maintenance

### Redémarrer les services

```bash
docker-compose -f docker-compose-airflow.yml restart
```

### Réinitialiser la base Airflow

```bash
docker-compose -f docker-compose-airflow.yml exec airflow-webserver airflow db reset
```

### Recréer l'utilisateur admin

```bash
docker-compose -f docker-compose-airflow.yml exec airflow-webserver \
  airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

## 📝 Créer un Nouveau DAG

1. Créer le fichier dans `orchestration/airflow/dags/`
2. Définir les tasks
3. Configurer les dépendances
4. Le scheduler détectera automatiquement le DAG

Exemple simple :

```python
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

with DAG(
    'my_pipeline',
    schedule_interval='0 6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    
    job1 = SparkSubmitOperator(
        task_id='job1',
        application='/path/to/job1.py',
        conn_id='spark_default',
    )
    
    job2 = SparkSubmitOperator(
        task_id='job2',
        application='/path/to/job2.py',
        conn_id='spark_default',
    )
    
    job1 >> job2
```

## 🐛 Dépannage Courant

### DAG n'apparaît pas

```bash
# Vérifier le chemin
ls -la orchestration/airflow/dags/

# Vérifier la syntaxe
python -m py_compile orchestration/airflow/dags/sales_pipeline_dag.py

# Redémarrer le scheduler
docker-compose -f docker-compose-airflow.yml restart airflow-scheduler
```

### Task échoue avec erreur Spark

1. Vérifier les logs: `docker-compose logs airflow-scheduler`
2. Vérifier la connexion Spark: `telnet spark-master 7077`
3. Vérifier le driver PostgreSQL: logs du task

### Airflow webserver ne démarre pas

```bash
# Vérifier la base de données
docker-compose -f docker-compose-airflow.yml exec airflow-db psql -U airflow -d airflow -c "SELECT 1"

# Réinitialiser
docker-compose -f docker-compose-airflow.yml down -v
docker-compose -f docker-compose-airflow.yml up -d
```

## 📚 Ressources

- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [Spark Provider](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/)
- [Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
