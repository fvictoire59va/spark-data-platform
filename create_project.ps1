#
# Script de creation de l'arborescence Spark Data Platform
# Usage: .\create_project.ps1 [-ProjectName "nom_du_projet"]
#

param(
    [string]$ProjectName = "spark-data-platform"
)

# Fonctions d'affichage
function Write-Info { param($Message) Write-Host "[OK] $Message" -ForegroundColor Green }
function Write-Section { param($Message) Write-Host "`n=== $Message ===`n" -ForegroundColor Blue }
function Write-Warn { param($Message) Write-Host "[!] $Message" -ForegroundColor Yellow }

# Banniere
Write-Host ""
Write-Host "================================================================" -ForegroundColor Blue
Write-Host "         SPARK DATA PLATFORM - PROJECT GENERATOR               " -ForegroundColor Blue
Write-Host "================================================================" -ForegroundColor Blue
Write-Host ""

# Verification du repertoire existant
if (Test-Path $ProjectName) {
    Write-Warn "Le repertoire '$ProjectName' existe deja."
    $confirm = Read-Host "Voulez-vous le supprimer et recreer? (yes/no)"
    if ($confirm -eq "yes") {
        Remove-Item -Recurse -Force $ProjectName
    } else {
        Write-Host "Annule."
        exit 0
    }
}

# Creation du repertoire racine
New-Item -ItemType Directory -Path $ProjectName | Out-Null
Set-Location $ProjectName
$ProjectRoot = Get-Location

Write-Section "Creation de l'arborescence"

# ============================================================
# STRUCTURE DES REPERTOIRES
# ============================================================

$directories = @(
    # Source code
    "src/core"
    "src/common/readers"
    "src/common/writers"
    "src/common/transformers"
    "src/common/quality"
    "src/pipelines/sales/jobs"
    "src/pipelines/sales/tests"
    
    # Tests
    "tests/unit"
    "tests/integration"
    "tests/e2e"
    "tests/fixtures"
    
    # Configuration
    "configs/dev"
    "configs/staging"
    "configs/prod"
    
    # Infrastructure
    "infrastructure/docker/conf"
    "infrastructure/terraform/modules/spark-cluster"
    "infrastructure/terraform/modules/storage"
    "infrastructure/terraform/environments/dev"
    "infrastructure/terraform/environments/staging"
    "infrastructure/terraform/environments/prod"
    
    # Orchestration
    "orchestration/airflow/dags"
    "orchestration/airflow/plugins"
    
    # CI/CD
    ".github/workflows"
    
    # Scripts
    "scripts"
    
    # Documentation
    "docs"
    
    # Data local
    "data/raw"
    "data/processed"
    "data/output"
    
    # Logs
    "logs"
)

foreach ($dir in $directories) {
    $dirPath = $dir -replace "/", "\"
    New-Item -ItemType Directory -Path $dirPath -Force | Out-Null
    Write-Info "Repertoire: $dirPath"
}

Write-Section "Creation des fichiers"

# ============================================================
# FICHIERS A CREER
# ============================================================

$files = @(
    # Racine
    ".env.example"
    ".gitignore"
    ".pre-commit-config.yaml"
    ".secrets.baseline"
    ".tflint.hcl"
    ".yamllint.yaml"
    "LICENSE"
    "Makefile"
    "README.md"
    "pyproject.toml"
    "poetry.lock"
    
    # src/core
    "src/__init__.py"
    "src/core/__init__.py"
    "src/core/config.py"
    "src/core/exceptions.py"
    "src/core/logger.py"
    "src/core/metrics.py"
    "src/core/spark_session.py"
    
    # src/common
    "src/common/__init__.py"
    "src/common/readers/__init__.py"
    "src/common/readers/base_reader.py"
    "src/common/readers/csv_reader.py"
    "src/common/readers/delta_reader.py"
    "src/common/readers/jdbc_reader.py"
    "src/common/readers/json_reader.py"
    "src/common/readers/kafka_reader.py"
    "src/common/readers/parquet_reader.py"
    
    "src/common/writers/__init__.py"
    "src/common/writers/base_writer.py"
    "src/common/writers/csv_writer.py"
    "src/common/writers/delta_writer.py"
    "src/common/writers/jdbc_writer.py"
    "src/common/writers/json_writer.py"
    "src/common/writers/kafka_writer.py"
    "src/common/writers/parquet_writer.py"
    
    "src/common/transformers/__init__.py"
    "src/common/transformers/base_transformer.py"
    "src/common/transformers/cleaning.py"
    "src/common/transformers/aggregations.py"
    "src/common/transformers/joins.py"
    "src/common/transformers/validation.py"
    
    "src/common/quality/__init__.py"
    "src/common/quality/data_quality.py"
    "src/common/quality/great_expectations_runner.py"
    "src/common/quality/rules.py"
    
    # src/pipelines/sales
    "src/pipelines/__init__.py"
    "src/pipelines/sales/__init__.py"
    "src/pipelines/sales/config.py"
    "src/pipelines/sales/schemas.py"
    "src/pipelines/sales/jobs/__init__.py"
    "src/pipelines/sales/jobs/ingest_orders.py"
    "src/pipelines/sales/jobs/ingest_customers.py"
    "src/pipelines/sales/jobs/transform_orders.py"
    "src/pipelines/sales/jobs/aggregate_sales.py"
    "src/pipelines/sales/jobs/quality_checks.py"
    "src/pipelines/sales/tests/__init__.py"
    "src/pipelines/sales/tests/test_ingest_orders.py"
    "src/pipelines/sales/tests/test_transform_orders.py"
    "src/pipelines/sales/tests/test_aggregate_sales.py"
    
    # tests
    "tests/__init__.py"
    "tests/conftest.py"
    "tests/unit/__init__.py"
    "tests/unit/test_config.py"
    "tests/unit/test_spark_session.py"
    "tests/unit/test_readers.py"
    "tests/unit/test_writers.py"
    "tests/unit/test_transformers.py"
    "tests/integration/__init__.py"
    "tests/integration/test_sales_pipeline.py"
    "tests/integration/test_delta_operations.py"
    "tests/e2e/__init__.py"
    "tests/e2e/test_full_pipeline.py"
    "tests/fixtures/__init__.py"
    "tests/fixtures/sample_data.py"
    
    # configs
    "configs/dev/.env"
    "configs/dev/spark.conf"
    "configs/staging/.env"
    "configs/staging/spark.conf"
    "configs/prod/.env"
    "configs/prod/spark.conf"
    
    # infrastructure/docker
    "infrastructure/docker/Dockerfile.spark-base"
    "infrastructure/docker/Dockerfile.spark-job"
    "infrastructure/docker/docker-compose.yml"
    "infrastructure/docker/conf/spark-defaults.conf"
    "infrastructure/docker/conf/log4j2.properties"
    
    # infrastructure/terraform
    "infrastructure/terraform/main.tf"
    "infrastructure/terraform/variables.tf"
    "infrastructure/terraform/outputs.tf"
    "infrastructure/terraform/backend.tf"
    "infrastructure/terraform/providers.tf"
    "infrastructure/terraform/modules/spark-cluster/main.tf"
    "infrastructure/terraform/modules/spark-cluster/variables.tf"
    "infrastructure/terraform/modules/spark-cluster/outputs.tf"
    "infrastructure/terraform/modules/storage/main.tf"
    "infrastructure/terraform/modules/storage/variables.tf"
    "infrastructure/terraform/modules/storage/outputs.tf"
    "infrastructure/terraform/environments/dev/main.tf"
    "infrastructure/terraform/environments/dev/terraform.tfvars"
    "infrastructure/terraform/environments/staging/main.tf"
    "infrastructure/terraform/environments/staging/terraform.tfvars"
    "infrastructure/terraform/environments/prod/main.tf"
    "infrastructure/terraform/environments/prod/terraform.tfvars"
    
    # orchestration/airflow
    "orchestration/airflow/dags/__init__.py"
    "orchestration/airflow/dags/sales_pipeline_dag.py"
    "orchestration/airflow/plugins/__init__.py"
    
    # .github
    ".github/workflows/ci.yml"
    ".github/workflows/cd-dev.yml"
    ".github/workflows/cd-prod.yml"
    ".github/CODEOWNERS"
    
    # scripts
    "scripts/submit_job.ps1"
    "scripts/submit_job.sh"
    "scripts/run_tests.ps1"
    "scripts/run_tests.sh"
    "scripts/deploy.ps1"
    "scripts/deploy.sh"
    
    # docs
    "docs/index.md"
    "docs/architecture.md"
    "docs/getting-started.md"
    "docs/api-reference.md"
    
    # data (fichiers .gitkeep)
    "data/raw/.gitkeep"
    "data/processed/.gitkeep"
    "data/output/.gitkeep"
    
    # logs
    "logs/.gitkeep"
)

foreach ($file in $files) {
    $filePath = $file -replace "/", "\"
    New-Item -ItemType File -Path $filePath -Force | Out-Null
    Write-Info "Fichier: $filePath"
}

# ============================================================
# RESUME
# ============================================================

Write-Host ""
Write-Host "================================================================" -ForegroundColor Green
Write-Host "                    CREATION TERMINEE                          " -ForegroundColor Green
Write-Host "================================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Projet: $ProjectName" -ForegroundColor Cyan
Write-Host "  Chemin: $ProjectRoot" -ForegroundColor Cyan
Write-Host ""

# Compter les elements crees
$dirCount = (Get-ChildItem -Recurse -Directory).Count
$fileCount = (Get-ChildItem -Recurse -File).Count

Write-Host "  Repertoires crees: $dirCount" -ForegroundColor Yellow
Write-Host "  Fichiers crees:    $fileCount" -ForegroundColor Yellow
Write-Host ""
Write-Host "================================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Prochaines etapes:" -ForegroundColor White
Write-Host "    1. cd $ProjectName" -ForegroundColor Gray
Write-Host "    2. git init" -ForegroundColor Gray
Write-Host "    3. poetry install" -ForegroundColor Gray
Write-Host "    4. code ." -ForegroundColor Gray
Write-Host ""
