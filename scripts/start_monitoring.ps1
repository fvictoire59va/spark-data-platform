<#
.SYNOPSIS
    Script pour démarrer la stack de monitoring Prometheus/Grafana
.DESCRIPTION
    Lance la stack complète de monitoring avec Prometheus, Grafana, Alertmanager,
    Node Exporter, cAdvisor et Postgres Exporter
.EXAMPLE
    .\start_monitoring.ps1
    .\start_monitoring.ps1 -OnlyMonitoring
#>

param(
    [switch]$OnlyMonitoring,
    [switch]$Stop,
    [switch]$Status,
    [switch]$Logs
)

$ErrorActionPreference = "Stop"
$InfraPath = Join-Path $PSScriptRoot "..\infrastructure\docker"

# Couleurs pour l'affichage
function Write-ColorOutput {
    param(
        [string]$Message,
        [string]$Color = "White"
    )
    Write-Host $Message -ForegroundColor $Color
}

function Show-Banner {
    Write-ColorOutput @"

  ╔══════════════════════════════════════════════════════════════╗
  ║     📊 SPARK DATA PLATFORM - MONITORING STACK               ║
  ║         Prometheus | Grafana | Alertmanager                  ║
  ╚══════════════════════════════════════════════════════════════╝

"@ "Cyan"
}

function Show-URLs {
    Write-ColorOutput "`n📍 URLs d'accès:" "Yellow"
    Write-ColorOutput "  • Grafana:       http://localhost:3000  (admin/spark123)" "Green"
    Write-ColorOutput "  • Prometheus:    http://localhost:9090" "Green"
    Write-ColorOutput "  • Alertmanager:  http://localhost:9093" "Green"
    Write-ColorOutput "  • cAdvisor:      http://localhost:8085" "Green"
    Write-ColorOutput "  • Node Exporter: http://localhost:9100/metrics" "Green"
    Write-ColorOutput ""
}

function Start-MonitoringStack {
    param([bool]$OnlyMonitoring = $false)

    Set-Location $InfraPath

    if ($OnlyMonitoring) {
        Write-ColorOutput "🚀 Démarrage des services de monitoring uniquement..." "Yellow"
        docker-compose up -d prometheus grafana alertmanager node-exporter postgres-exporter
    } else {
        Write-ColorOutput "🚀 Démarrage de la stack complète avec monitoring..." "Yellow"
        docker-compose up -d
    }

    Write-ColorOutput "`n⏳ Attente du démarrage des services..." "Yellow"
    Start-Sleep -Seconds 10

    # Vérification de l'état des services
    $services = @("prometheus", "grafana", "alertmanager")
    foreach ($service in $services) {
        $status = docker inspect -f '{{.State.Status}}' $service 2>$null
        if ($status -eq "running") {
            Write-ColorOutput "  ✅ $service : running" "Green"
        } else {
            Write-ColorOutput "  ❌ $service : $status" "Red"
        }
    }

    Show-URLs
}

function Stop-MonitoringStack {
    Set-Location $InfraPath
    Write-ColorOutput "🛑 Arrêt des services de monitoring..." "Yellow"
    docker-compose stop prometheus grafana alertmanager node-exporter postgres-exporter
    Write-ColorOutput "✅ Services de monitoring arrêtés." "Green"
}

function Show-MonitoringStatus {
    Set-Location $InfraPath
    Write-ColorOutput "`n📊 État des services de monitoring:" "Yellow"
    Write-ColorOutput "════════════════════════════════════════════════════════" "Gray"

    $monitoringServices = @("prometheus", "grafana", "alertmanager", "node-exporter", "postgres-exporter")

    foreach ($service in $monitoringServices) {
        $status = docker inspect -f '{{.State.Status}}' $service 2>$null
        $health = docker inspect -f '{{.State.Health.Status}}' $service 2>$null

        if ($status -eq "running") {
            $healthStr = if ($health) { " ($health)" } else { "" }
            Write-ColorOutput "  ✅ $service : running$healthStr" "Green"
        } elseif ($status) {
            Write-ColorOutput "  ⚠️  $service : $status" "Yellow"
        } else {
            Write-ColorOutput "  ❌ $service : not found" "Red"
        }
    }

    # Vérification des endpoints
    Write-ColorOutput "`n🔍 Vérification des endpoints:" "Yellow"
    Write-ColorOutput "════════════════════════════════════════════════════════" "Gray"

    $endpoints = @{
        "Prometheus" = "http://localhost:9090/-/healthy"
        "Grafana" = "http://localhost:3000/api/health"
        "Alertmanager" = "http://localhost:9093/-/healthy"
    }

    foreach ($endpoint in $endpoints.GetEnumerator()) {
        try {
            $response = Invoke-WebRequest -Uri $endpoint.Value -TimeoutSec 5 -UseBasicParsing -ErrorAction SilentlyContinue
            if ($response.StatusCode -eq 200) {
                Write-ColorOutput "  ✅ $($endpoint.Key) : OK" "Green"
            }
        } catch {
            Write-ColorOutput "  ❌ $($endpoint.Key) : Non disponible" "Red"
        }
    }

    Show-URLs
}

function Show-MonitoringLogs {
    Set-Location $InfraPath
    Write-ColorOutput "📜 Logs des services de monitoring (Ctrl+C pour arrêter):" "Yellow"
    docker-compose logs -f prometheus grafana alertmanager
}

# Point d'entrée principal
Show-Banner

if ($Stop) {
    Stop-MonitoringStack
} elseif ($Status) {
    Show-MonitoringStatus
} elseif ($Logs) {
    Show-MonitoringLogs
} else {
    Start-MonitoringStack -OnlyMonitoring:$OnlyMonitoring.IsPresent
}
