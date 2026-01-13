# Makefile pour Spark Data Platform
# Usage: make <target>

.PHONY: help install dev test lint format build docker deploy clean

# Variables
PYTHON_VERSION := 3.11
PROJECT_NAME := spark-data-platform
DOCKER_REGISTRY := ghcr.io/company

# Couleurs
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[1;33m
NC := \033[0m

# ============ HELP ============
help: ## Affiche cette aide
    @echo ""
    @echo "$(BLUE)$(PROJECT_NAME)$(NC) - Commandes disponibles:"
    @echo ""
    @grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
        awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
    @echo ""

# ============ INSTALLATION ============
install: ## Installe les dépendances de production
    @echo "$(BLUE)Installation des dépendances...$(NC)"
    poetry install --only main

dev: ## Installe toutes les dépendances (dev inclus)
    @echo "$(BLUE)Installation des dépendances de développement...$(NC)"
    poetry install
    pre-commit install
    pre-commit install --hook-type commit-msg

update: ## Met à jour les dépendances
    poetry update
    pre-commit autoupdate

# ============ QUALITÉ DE CODE ============
lint: ## Vérifie la qualité du code
    @echo "$(BLUE)Vérification du code...$(NC)"
    poetry run ruff check src/ tests/
    poetry run mypy src/
    poetry run bandit -r src/ -ll

format: ## Formate le code
    @echo "$(BLUE)Formatage du code...$(NC)"
    poetry run black src/ tests/
    poetry run ruff check --fix src/ tests/

check: lint ## Alias pour lint

# ============ TESTS ============
test: ## Lance tous les tests
    @echo "$(BLUE)Exécution des tests...$(NC)"
    poetry run pytest tests/ -v

test-unit: ## Lance les tests unitaires
    @echo "$(BLUE)Tests unitaires...$(NC)"
    poetry run pytest tests/unit -v

test-integration: ## Lance les tests d'intégration
    @echo "$(BLUE)Tests d'intégration...$(NC)"
    poetry run pytest tests/integration -v -m integration

test-e2e: ## Lance les tests end-to-end
    @echo "$(BLUE)Tests E2E...$(NC)"
    poetry run pytest tests/e2e -v -m e2e

test-cov: ## Lance les tests avec couverture
    @echo "$(BLUE)Tests avec couverture...$(NC)"
    poetry run pytest tests/ -v --cov=src --cov-report=html --cov-report=xml
    @echo "$(GREEN)Rapport: htmlcov/index.html$(NC)"

test-fast: ## Lance les tests rapides (sans slow)
    poetry run pytest tests/ -v -m "not slow"

# ============ BUILD ============
build: clean ## Construit le package Python
    @echo "$(BLUE)Construction du package...$(NC)"
    poetry build
    @echo "$(GREEN)Package créé dans dist/$(NC)"

docker-build: ## Construit l'image Docker
    @echo "$(BLUE)Construction de l'image Docker...$(NC)"
    docker build \
        -f infrastructure/docker/Dockerfile.spark-job \
        -t $(DOCKER_REGISTRY)/spark-job:latest \
        .

docker-push: docker-build ## Pousse l'image Docker
    @echo "$(BLUE)Push de l'image Docker...$(NC)"
    docker push $(DOCKER_REGISTRY)/spark-job:latest

# ============ DÉVELOPPEMENT LOCAL ============
spark-up: ## Démarre le cluster Spark local
    @echo "$(BLUE)Démarrage du cluster Spark...$(NC)"
    docker-compose -f infrastructure/docker/docker-compose.yml up -d

spark-down: ## Arrête le cluster Spark local
    @echo "$(BLUE)Arrêt du cluster Spark...$(NC)"
    docker-compose -f infrastructure/docker/docker-compose.yml down

spark-logs: ## Affiche les logs Spark
    docker-compose -f infrastructure/docker/docker-compose.yml logs -f

spark-ui: ## Ouvre l'UI Spark
    @echo "$(BLUE)Spark UI: http://localhost:8080$(NC)"
    open http://localhost:8080 || xdg-open http://localhost:8080

jupyter: ## Démarre Jupyter Lab
    @echo "$(BLUE)Démarrage de Jupyter...$(NC)"
    poetry run jupyter lab --notebook-dir=notebooks

# ============ JOBS ============
run-job: ## Lance un job (JOB=<nom> ENV=<env>)
    @echo "$(BLUE)Lancement du job $(JOB) en $(ENV)...$(NC)"
    ./scripts/submit_job.sh $(JOB) $(ENV)

run-job-local: ## Lance un job en local
    @echo "$(BLUE)Lancement du job $(JOB) en local...$(NC)"
    ./scripts/submit_job.sh $(JOB) dev --mode local

# ============ DÉPLOIEMENT ============
deploy-dev: ## Déploie en dev
    @echo "$(BLUE)Déploiement en dev...$(NC)"
    ./scripts/deploy.sh dev

deploy-staging: ## Déploie en staging
    @echo "$(BLUE)Déploiement en staging...$(NC)"
    ./scripts/deploy.sh staging

deploy-prod: ## Déploie en production
    @echo "$(YELLOW)⚠️  Déploiement en PRODUCTION$(NC)"
    ./scripts/deploy.sh prod

# ============ INFRASTRUCTURE ============
tf-init: ## Initialise Terraform
    cd infrastructure/terraform && terraform init

tf-plan: ## Plan Terraform (ENV=<env>)
    cd infrastructure/terraform/environments/$(ENV) && terraform plan

tf-apply: ## Applique Terraform (ENV=<env>)
    cd infrastructure/terraform/environments/$(ENV) && terraform apply

# ============ DOCUMENTATION ============
docs: ## Génère la documentation
    @echo "$(BLUE)Génération de la documentation...$(NC)"
    poetry run mkdocs build

docs-serve: ## Sert la documentation localement
    @echo "$(BLUE)Documentation: http://localhost:8000$(NC)"
    poetry run mkdocs serve

# ============ NETTOYAGE ============
clean: ## Nettoie les fichiers temporaires
    @echo "$(BLUE)Nettoyage...$(NC)"
    rm -rf dist/ build/ *.egg-info
    rm -rf .pytest_cache/ .mypy_cache/ .ruff_cache/
    rm -rf htmlcov/ .coverage coverage.xml
    find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    find . -type f -name "*.pyc" -delete

clean-all: clean ## Nettoyage complet (inclut .venv)
    rm -rf .venv/

# ============ UTILITAIRES ============
shell: ## Lance un shell Python
    poetry run ipython

check-deps: ## Vérifie les vulnérabilités des dépendances
    poetry run pip-audit

version: ## Affiche la version
    @poetry version -s
