#!/usr/bin/env powershell
#
# Script PowerShell pour valider la documentation
# Equivalent Windows du script validate_docs.sh
#

param(
    [switch]$Strict = $false,
    [switch]$Verbose = $false
)

$ErrorActionPreference = "Stop"

# Couleurs
$Green = "`e[32m"
$Red = "`e[31m"
$Yellow = "`e[33m"
$Blue = "`e[34m"
$Reset = "`e[0m"

function Log-Info {
    param([string]$Message)
    Write-Host "${Green}[INFO]${Reset} $Message"
}

function Log-Error {
    param([string]$Message)
    Write-Host "${Red}[ERROR]${Reset} $Message" -ForegroundColor Red
}

function Log-Warn {
    param([string]$Message)
    Write-Host "${Yellow}[WARN]${Reset} $Message" -ForegroundColor Yellow
}

function Log-Section {
    param([string]$Title)
    Write-Host ""
    Write-Host "${Blue}=== $Title ===${Reset}" -ForegroundColor Blue
    Write-Host ""
}

function Validate-Docs {
    Log-Section "Validation de la documentation"

    $ProjectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)

    # Vérifier mkdocs.yml
    if (-not (Test-Path "$ProjectRoot/mkdocs.yml")) {
        Log-Error "mkdocs.yml non trouvé"
        return $false
    }
    Log-Info "✓ mkdocs.yml trouvé"

    # Vérifier dossier docs
    if (-not (Test-Path "$ProjectRoot/docs")) {
        Log-Error "Dossier docs non trouvé"
        return $false
    }
    Log-Info "✓ Dossier docs trouvé"

    # Vérifier index.md
    if (-not (Test-Path "$ProjectRoot/docs/index.md")) {
        Log-Error "docs/index.md non trouvé"
        return $false
    }
    Log-Info "✓ docs/index.md trouvé"

    # Vérifier architecture.md
    if (-not (Test-Path "$ProjectRoot/docs/architecture.md")) {
        Log-Error "docs/architecture.md non trouvé"
        return $false
    }
    Log-Info "✓ docs/architecture.md trouvé"

    return $true
}

function Build-Docs {
    Log-Section "Construction de la documentation"

    $ProjectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)

    # Vérifier Poetry
    if (-not (Get-Command poetry -ErrorAction SilentlyContinue)) {
        Log-Error "Poetry n'est pas installé ou pas dans le PATH"
        return $false
    }

    Log-Info "Installation des dépendances..."
    & poetry install --with docs --quiet
    if ($LASTEXITCODE -ne 0) {
        Log-Error "Erreur lors de l'installation des dépendances"
        return $false
    }

    Log-Info "Construction stricte..."
    if ($Strict -or $true) {
        & poetry run mkdocs build --strict $(if ($Verbose) { '--verbose' })
    } else {
        & poetry run mkdocs build $(if ($Verbose) { '--verbose' })
    }

    if ($LASTEXITCODE -ne 0) {
        Log-Error "Erreur lors de la construction"
        return $false
    }

    Log-Info "✓ Documentation construite avec succès"
    return $true
}

function Cleanup {
    Log-Section "Nettoyage"

    $ProjectRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)

    if (Test-Path "$ProjectRoot/site") {
        Remove-Item -Recurse -Force "$ProjectRoot/site"
        Log-Info "✓ Dossier site nettoyé"
    }
}

function Main {
    Log-Info "Validation de la documentation Spark Data Platform"

    if (-not (Validate-Docs)) {
        Log-Error "Validation échouée"
        exit 1
    }

    if (-not (Build-Docs)) {
        Log-Error "Construction échouée"
        exit 1
    }

    Cleanup

    Log-Section "Succès"
    Log-Info "La documentation est valide et peut être construite"
    exit 0
}

Main
