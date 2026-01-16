# Configuration Airflow + Spark Pipeline

## 🚀 Démarrage des Services

### Avec Docker Compose (Airflow inclus)

```bash
# Démarrer tous les services (Spark + Airflow)
cd scripts
./start_airflow.sh        # Linux/Mac
.\start_airflow.ps1       # Windows

# Ou directement avec docker-compose
cd infrastructure/docker
docker-compose -f docker-compose-airflow.yml up -d
```

### Accès aux Services

- **Airflow Web UI**: http://localhost:8888
  - Login: `admin` / `admin`
  - Port alternatif configuré pour éviter conflit avec Jupyter

- **Spark Master UI**: http://localhost:8080
  - Status des nœuds workers
  - Jobs soumis

- **Spark History Server**: http://localhost:18080
  - Historique des jobs exécutés

- **Jupyter Notebook**: http://localhost:8889
  - Token: `spark123`

## 📋 Configuration des Connexions Airflow

### Connexion Spark (spark_default)

```
Conn Id:   spark_default
Conn Type: Spark
Host:      spark-master
Port:      7077
```

### Connexion PostgreSQL (airflow_db)

```
Conn Id:   postgres_airflow
Conn Type: Postgres
Host:      airflow-db
Port:      5432
Login:     airflow
Password:  airflow
Database:  airflow
```

### Connexion PostgreSQL (metastore)

```
Conn Id:   postgres_metastore
Conn Type: Postgres
Host:      metastore-db
Port:      5432
Login:     hive
Password:  hive123
Database:  metastore
```

## 🎯 DAG: sales_pipeline

### Structure

```
start
  ├── bronze_layer (ingestion)
  │   ├── ingest_orders
  │   └── ingest_customers
  ├── silver_layer (transformation)
  │   └── transform_orders
  ├── gold_layer (agrégation)
  │   └── aggregate_sales
  └── end
```

### Configuration

- **Schedule**: Tous les jours à 06h00 UTC (`0 6 * * *`)
- **Retry Policy**: 1 tentative avec délai de 5 minutes
- **Timeout**: 2 heures
- **Max Active Runs**: 1 (pas d'exécutions parallèles)

### Variables Airflow à créer

```
environment: dev                    # local, dev, staging, prod
spark_executor_memory: 2g
spark_executor_cores: 2
spark_executor_instances: 2
```

## 🔧 Configuration des Connecteurs Airflow

### SparkSubmitOperator

L'opérateur est configuré avec :

```python
SparkSubmitOperator(
    application="local:///opt/spark-apps/ingest_orders.py",
    conf={
        "spark.executor.memory": "2g",
        "spark.executor.cores": "2",
        "spark.executor.instances": "2",
    },
    conn_id="spark_default",
    packages="org.postgresql:postgresql:42.6.0",
)
```

## 📊 Monitoring et Logs

### Logs Airflow

```bash
# Webserver
tail -f infrastructure/docker/logs/airflow/webserver.log

# Scheduler
tail -f infrastructure/docker/logs/airflow/scheduler.log

# Tasks
docker-compose logs airflow-scheduler
docker-compose logs airflow-webserver
```

### Logs Spark

```bash
# Container Master
docker logs -f spark-master

# Container Worker
docker logs -f spark-worker-1

# History Server
tail -f infrastructure/docker/logs/spark-history/*
```

## 🐛 Troubleshooting

### Airflow ne démarre pas

```bash
# Réinitialiser la base de données Airflow
docker-compose -f docker-compose-airflow.yml exec airflow-webserver airflow db reset

# Recréer le compte admin
docker-compose -f docker-compose-airflow.yml exec airflow-webserver \
  airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
```

### DAG ne s'affiche pas

1. Vérifier le chemin des DAGs: `/opt/airflow/dags/`
2. Vérifier la syntaxe Python: `python -m py_compile dags/sales_pipeline_dag.py`
3. Redémarrer le scheduler: `docker-compose restart airflow-scheduler`

### Spark jobs échouent

1. Vérifier la connexion Spark: `telnet spark-master 7077`
2. Vérifier les logs du Master: `docker logs spark-master`
3. Vérifier les variables Airflow pour les paramètres Spark

## 🚢 Déploiement en Production

### Sécurité

- Changer les mots de passe par défaut (admin/admin)
- Utiliser une base PostgreSQL externe
- Configurer un reverse proxy (Nginx/Apache)
- Activer LDAP/OAuth pour l'authentification

### Scaling

- Augmenter les ressources Spark (executor memory/cores)
- Configurer un Executor externe (Kubernetes, Yarn)
- Augmenter le nombre de DAG parses en parallèle

### High Availability

- Configurer plusieurs schedulers (requiert PostgreSQL HA)
- Utiliser un load balancer pour le webserver
- Configurer une queue persistante pour les tasks

## 📚 Ressources

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [SparkSubmitOperator](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/operators.html)
- [Spark on Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html)
