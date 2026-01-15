# Script pour initialiser et démarrer Airflow avec Spark
param(
    [switch]$Build = $false,
    [switch]$Down = $false
)

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$dockerDir = Join-Path $scriptDir "..\infrastructure\docker"
$projectRoot = Split-Path -Parent $scriptDir

Write-Host "🚀 Démarrage de l'infrastructure Airflow + Spark..." -ForegroundColor Green

Push-Location $dockerDir

try {
    if ($Down) {
        Write-Host "⏹️  Arrêt des containers..." -ForegroundColor Yellow
        docker-compose down
        exit 0
    }

    # Arrêter les containers existants
    Write-Host "⏹️  Arrêt des containers existants..." -ForegroundColor Yellow
    docker-compose down 2>$null
    docker-compose -f docker-compose-airflow.yml down 2>$null

    # Copier les DAGs et plugins
    Write-Host "📁 Copie des DAGs..." -ForegroundColor Blue
    try {
        docker cp "$projectRoot\orchestration\airflow\dags" airflow-webserver:/opt/airflow/ 2>$null
    }
    catch {
        Write-Host "Note: DAGs seront syncronisés automatiquement" -ForegroundColor Gray
    }

    # Démarrer avec docker-compose-airflow.yml
    Write-Host "🔧 Démarrage des services..." -ForegroundColor Blue
    docker-compose -f docker-compose-airflow.yml up -d

    # Attendre que les services soient prêts
    Write-Host "⏳ Attente de l'initialisation des services..." -ForegroundColor Yellow
    Start-Sleep -Seconds 30

    # Afficher le statut
    Write-Host "✅ Services démarrés!" -ForegroundColor Green
    Write-Host ""
    Write-Host "📋 URLS D'ACCÈS:" -ForegroundColor Cyan
    Write-Host "  🎯 Airflow UI: http://localhost:8888 (admin/admin)" -ForegroundColor White
    Write-Host "  ⚡ Spark Master: http://localhost:8080" -ForegroundColor White
    Write-Host "  📊 Spark History: http://localhost:18080" -ForegroundColor White
    Write-Host "  📓 Jupyter: http://localhost:8889 (token: spark123)" -ForegroundColor White
    Write-Host ""
    Write-Host "📊 Vérifier les services:" -ForegroundColor Cyan
    docker-compose -f docker-compose-airflow.yml ps

}
finally {
    Pop-Location
}
