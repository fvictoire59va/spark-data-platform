# PowerShell Make equivalent pour Windows
# Usage: .\make.ps1 <target>

param(
    [string]$Target = "help"
)

# Couleurs
$BLUE = "`e[0;34m"
$GREEN = "`e[0;32m"
$YELLOW = "`e[1;33m"
$NC = "`e[0m"

function Show-Help {
    Write-Host ""
    Write-Host "${BLUE}spark-data-platform${NC} - Commandes disponibles:"
    Write-Host ""
    Write-Host "  ${GREEN}install${NC}            Installe les dépendances de production"
    Write-Host "  ${GREEN}dev${NC}               Installe toutes les dépendances (dev inclus)"
    Write-Host "  ${GREEN}update${NC}            Met à jour les dépendances"
    Write-Host "  ${GREEN}lint${NC}              Vérifie la qualité du code"
    Write-Host "  ${GREEN}format${NC}            Formate le code"
    Write-Host "  ${GREEN}test${NC}              Lance tous les tests"
    Write-Host "  ${GREEN}test-unit${NC}         Lance les tests unitaires"
    Write-Host "  ${GREEN}test-integration${NC}  Lance les tests d'intégration"
    Write-Host "  ${GREEN}test-e2e${NC}          Lance les tests end-to-end"
    Write-Host "  ${GREEN}test-cov${NC}          Lance les tests avec couverture"
    Write-Host "  ${GREEN}build${NC}             Construit le package Python"
    Write-Host "  ${GREEN}docker-build${NC}      Construit l'image Docker"
    Write-Host "  ${GREEN}spark-up${NC}          Démarre le cluster Spark local"
    Write-Host "  ${GREEN}spark-down${NC}        Arrête le cluster Spark"
    Write-Host "  ${GREEN}clean${NC}             Nettoie les fichiers générés"
    Write-Host "  ${GREEN}help${NC}              Affiche cette aide"
    Write-Host ""
}

function Run-Target {
    param([string]$TargetName)

    switch ($TargetName) {
        "help" {
            Show-Help
        }
        
        "install" {
            Write-Host "${BLUE}Installation des dépendances...${NC}"
            poetry install --only main
        }
        
        "dev" {
            Write-Host "${BLUE}Installation des dépendances de développement...${NC}"
            poetry install
            Write-Host "${BLUE}Configuration des hooks pre-commit...${NC}"
            poetry run pre-commit install
            poetry run pre-commit install --hook-type commit-msg
        }
        
        "update" {
            Write-Host "${BLUE}Mise à jour des dépendances...${NC}"
            poetry update
            poetry run pre-commit autoupdate
        }
        
        "lint" {
            Write-Host "${BLUE}Vérification du code...${NC}"
            poetry run ruff check src/ tests/
            poetry run mypy src/ --ignore-missing-imports
        }
        
        "format" {
            Write-Host "${BLUE}Formatage du code...${NC}"
            poetry run ruff format src/ tests/
            poetry run ruff check --fix src/ tests/
        }
        
        "test" {
            Write-Host "${BLUE}Exécution des tests...${NC}"
            poetry run pytest tests/ -v
        }
        
        "test-unit" {
            Write-Host "${BLUE}Tests unitaires...${NC}"
            poetry run pytest tests/unit -v
        }
        
        "test-integration" {
            Write-Host "${BLUE}Tests d'intégration...${NC}"
            poetry run pytest tests/integration -v -m integration
        }
        
        "test-e2e" {
            Write-Host "${BLUE}Tests E2E...${NC}"
            poetry run pytest tests/e2e -v -m e2e
        }
        
        "test-cov" {
            Write-Host "${BLUE}Tests avec couverture...${NC}"
            poetry run pytest tests/ -v --cov=src --cov-report=html --cov-report=xml
            Write-Host "${GREEN}Rapport: htmlcov/index.html${NC}"
        }
        
        "build" {
            Write-Host "${BLUE}Nettoyage...${NC}"
            Remove-Item -Path "dist", "build", "*.egg-info" -Recurse -ErrorAction SilentlyContinue
            Write-Host "${BLUE}Construction du package...${NC}"
            poetry build
            Write-Host "${GREEN}Package créé dans dist/${NC}"
        }
        
        "docker-build" {
            Write-Host "${BLUE}Construction de l'image Docker...${NC}"
            docker build `
                -f infrastructure/docker/Dockerfile.spark-job `
                -t ghcr.io/company/spark-job:latest `
                .
        }
        
        "docker-push" {
            & $MyInvocation.MyCommand.Path "docker-build"
            Write-Host "${BLUE}Push de l'image Docker...${NC}"
            docker push ghcr.io/company/spark-job:latest
        }
        
        "spark-up" {
            Write-Host "${BLUE}Démarrage du cluster Spark...${NC}"
            docker-compose -f infrastructure/docker/docker-compose.yml up -d
            Write-Host "${GREEN}Cluster démarré!${NC}"
            Write-Host "  Spark Master UI: http://localhost:8080"
            Write-Host "  Spark History: http://localhost:18080"
        }
        
        "spark-down" {
            Write-Host "${BLUE}Arrêt du cluster Spark...${NC}"
            docker-compose -f infrastructure/docker/docker-compose.yml down
        }
        
        "clean" {
            Write-Host "${BLUE}Nettoyage des fichiers générés...${NC}"
            Remove-Item -Path "dist", "build", ".pytest_cache", "__pycache__", "*.egg-info", ".coverage" -Recurse -ErrorAction SilentlyContinue
            Write-Host "${GREEN}Nettoyage terminé${NC}"
        }
        
        default {
            Write-Host "${YELLOW}Cible inconnue: $TargetName${NC}"
            Show-Help
            exit 1
        }
    }
}

# Exécuter la cible
Run-Target $Target
