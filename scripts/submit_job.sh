#!/usr/bin/env bash
#
# Script de soumission de jobs Spark
# Usage: ./submit_job.sh <job_name> <environment> [options]
#

set -euo pipefail

# ============ CONFIGURATION ============
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Couleurs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# ============ FONCTIONS ============

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

usage() {
    cat << EOF
Usage: $(basename "$0") <job_name> <environment> [options]

Arguments:
    job_name        Nom du job (ex: sales.ingest_orders)
    environment     Environnement (dev, staging, prod)

Options:
    -d, --date      Date de traitement (YYYY-MM-DD), défaut: aujourd'hui
    -m, --mode      Mode Spark (local, cluster), défaut: cluster
    -c, --config    Fichier de configuration custom
    --dry-run       Affiche la commande sans l'exécuter
    -h, --help      Affiche cette aide

Exemples:
    $(basename "$0") sales.ingest_orders dev
    $(basename "$0") sales.transform_orders prod --date 2024-01-15
    $(basename "$0") sales.aggregate_sales staging --mode local --dry-run
EOF
    exit 1
}

get_job_path() {
    local job_name=$1
    local domain=$(echo "$job_name" | cut -d'.' -f1)
    local job=$(echo "$job_name" | cut -d'.' -f2)
    echo "src/pipelines/${domain}/jobs/${job}.py"
}

validate_environment() {
    local env=$1
    if [[ ! "$env" =~ ^(dev|staging|prod)$ ]]; then
        log_error "Environnement invalide: $env"
        log_error "Valeurs acceptées: dev, staging, prod"
        exit 1
    fi
}

validate_job() {
    local job_path=$1
    local full_path="${PROJECT_ROOT}/${job_path}"
    
    if [[ ! -f "$full_path" ]]; then
        log_error "Job non trouvé: $full_path"
        exit 1
    fi
}

load_spark_config() {
    local env=$1
    local config_file="${PROJECT_ROOT}/configs/${env}/spark.conf"
    
    if [[ -f "$config_file" ]]; then
        source "$config_file"
        log_info "Configuration Spark chargée depuis: $config_file"
    else
        log_warn "Fichier de configuration non trouvé: $config_file"
        log_warn "Utilisation des valeurs par défaut"
        
        # Valeurs par défaut
        SPARK_MASTER="local[*]"
        SPARK_EXECUTOR_MEMORY="4g"
        SPARK_EXECUTOR_CORES="2"
        SPARK_DRIVER_MEMORY="2g"
    fi
}

build_spark_submit_cmd() {
    local job_path=$1
    local env=$2
    local date=$3
    local mode=$4
    local config_file=$5
    
    local spark_submit_cmd="spark-submit"
    
    # Configuration de base
    spark_submit_cmd+=" --master ${SPARK_MASTER:-spark://localhost:7077}"
    spark_submit_cmd+=" --deploy-mode ${mode}"
    spark_submit_cmd+=" --driver-memory ${SPARK_DRIVER_MEMORY:-2g}"
    spark_submit_cmd+=" --executor-memory ${SPARK_EXECUTOR_MEMORY:-4g}"
    spark_submit_cmd+=" --executor-cores ${SPARK_EXECUTOR_CORES:-2}"
    
    # Configuration Spark
    spark_submit_cmd+=" --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
    spark_submit_cmd+=" --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    spark_submit_cmd+=" --conf spark.sql.adaptive.enabled=true"
    spark_submit_cmd+=" --conf spark.serializer=org.apache.spark.serializer.KryoSerializer"
    
    # Packages
    spark_submit_cmd+=" --packages io.delta:delta-spark_2.12:3.0.0"
    
    # Fichiers Python
    spark_submit_cmd+=" --py-files ${PROJECT_ROOT}/dist/*.whl"
    
    # Application
    spark_submit_cmd+=" ${PROJECT_ROOT}/${job_path}"
    
    # Arguments
    spark_submit_cmd+=" --env ${env}"
    spark_submit_cmd+=" --date ${date}"
    
    if [[ -n "$config_file" ]]; then
        spark_submit_cmd+=" --config ${config_file}"
    fi
    
    echo "$spark_submit_cmd"
}

# ============ MAIN ============

main() {
    # Arguments par défaut
    local job_name=""
    local environment=""
    local processing_date=$(date +%Y-%m-%d)
    local deploy_mode="cluster"
    local custom_config=""
    local dry_run=false
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            -d|--date)
                processing_date="$2"
                shift 2
                ;;
            -m|--mode)
                deploy_mode="$2"
                shift 2
                ;;
            -c|--config)
                custom_config="$2"
                shift 2
                ;;
            --dry-run)
                dry_run=true
                shift
                ;;
            -h|--help)
                usage
                ;;
            *)
                if [[ -z "$job_name" ]]; then
                    job_name="$1"
                elif [[ -z "$environment" ]]; then
                    environment="$1"
                else
                    log_error "Argument inconnu: $1"
                    usage
                fi
                shift
                ;;
        esac
    done
    
    # Validation
    if [[ -z "$job_name" || -z "$environment" ]]; then
        log_error "job_name et environment sont requis"
        usage
    fi
    
    validate_environment "$environment"
    
    local job_path=$(get_job_path "$job_name")
    validate_job "$job_path"
    
    log_info "Job: $job_name"
    log_info "Environment: $environment"
    log_info "Date: $processing_date"
    log_info "Mode: $deploy_mode"
    
    # Charger la configuration
    load_spark_config "$environment"
    
    # Construire la commande
    local cmd=$(build_spark_submit_cmd \
        "$job_path" \
        "$environment" \
        "$processing_date" \
        "$deploy_mode" \
        "$custom_config"
    )
    
    echo ""
    log_info "Commande Spark:"
    echo "$cmd"
    echo ""
    
    if [[ "$dry_run" == true ]]; then
        log_warn "Mode dry-run: commande non exécutée"
        exit 0
    fi
    
    # Exécution
    log_info "Démarrage du job..."
    eval "$cmd"
    
    local exit_code=$?
    if [[ $exit_code -eq 0 ]]; then
        log_info "Job terminé avec succès"
    else
        log_error "Job terminé avec erreur (code: $exit_code)"
        exit $exit_code
    fi
}

main "$@"
