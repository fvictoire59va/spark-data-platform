#!/usr/bin/env bash
#
# Script de déploiement
# Usage: ./deploy.sh <environment> [options]
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Couleurs
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_section() { echo -e "\n${BLUE}=== $1 ===${NC}\n"; }

usage() {
    cat << EOF
Usage: $(basename "$0") <environment> [options]

Arguments:
    environment     Environnement cible (dev, staging, prod)

Options:
    --skip-tests    Ne pas exécuter les tests
    --skip-build    Ne pas reconstruire le package
    --dry-run       Affiche les commandes sans les exécuter
    --version       Version à déployer (défaut: git tag ou timestamp)
    -h, --help      Affiche cette aide

Exemples:
    $(basename "$0") dev
    $(basename "$0") staging --skip-tests
    $(basename "$0") prod --version v1.2.0
EOF
    exit 1
}

validate_environment() {
    local env=$1
    if [[ ! "$env" =~ ^(dev|staging|prod)$ ]]; then
        log_error "Environnement invalide: $env"
        exit 1
    fi
    
    if [[ "$env" == "prod" ]]; then
        log_warn "⚠️  ATTENTION: Déploiement en PRODUCTION"
        read -p "Confirmer le déploiement en production? (yes/no): " confirm
        if [[ "$confirm" != "yes" ]]; then
            log_info "Déploiement annulé"
            exit 0
        fi
    fi
}

load_env_config() {
    local env=$1
    local env_file="${PROJECT_ROOT}/configs/${env}/.env"
    
    if [[ -f "$env_file" ]]; then
        source "$env_file"
        log_info "Configuration chargée: $env_file"
    else
        log_warn "Fichier .env non trouvé: $env_file"
    fi
}

run_tests() {
    log_section "Exécution des tests"
    
    "${SCRIPT_DIR}/run_tests.sh" all --coverage --failfast
    
    if [[ $? -ne 0 ]]; then
        log_error "Les tests ont échoué. Déploiement annulé."
        exit 1
    fi
    
    log_info "Tests passés avec succès"
}

build_package() {
    log_section "Construction du package"
    
    cd "$PROJECT_ROOT"
    
    # Nettoyage
    rm -rf dist/ build/ *.egg-info
    
    # Build
    poetry build
    
    log_info "Package construit: $(ls dist/)"
}

build_docker_image() {
    local env=$1
    local version=$2
    
    log_section "Construction de l'image Docker"
    
    local image_tag="${DOCKER_REGISTRY:-ghcr.io/company}/spark-job:${version}"
    
    docker build \
        -f "${PROJECT_ROOT}/infrastructure/docker/Dockerfile.spark-job" \
        -t "$image_tag" \
        --build-arg ENV="$env" \
        "${PROJECT_ROOT}"
    
    log_info "Image construite: $image_tag"
    
    if [[ "$DRY_RUN" != true ]]; then
        docker push "$image_tag"
        log_info "Image poussée: $image_tag"
    fi
}

deploy_to_s3() {
    local env=$1
    local version=$2
    
    log_section "Déploiement vers S3"
    
    local s3_bucket="${S3_BUCKET:-spark-data-platform-${env}}"
    local s3_prefix="jobs/${version}"
    
    log_info "Bucket: s3://${s3_bucket}/${s3_prefix}"
    
    if [[ "$DRY_RUN" == true ]]; then
        log_warn "DRY-RUN: aws s3 sync src/ s3://${s3_bucket}/${s3_prefix}/src/"
        log_warn "DRY-RUN: aws s3 sync configs/${env}/ s3://${s3_bucket}/${s3_prefix}/configs/"
        log_warn "DRY-RUN: aws s3 cp dist/*.whl s3://${s3_bucket}/${s3_prefix}/dist/"
    else
        aws s3 sync src/ "s3://${s3_bucket}/${s3_prefix}/src/" --delete
        aws s3 sync "configs/${env}/" "s3://${s3_bucket}/${s3_prefix}/configs/"
        aws s3 cp dist/*.whl "s3://${s3_bucket}/${s3_prefix}/dist/"
    fi
    
    log_info "Déploiement S3 terminé"
}

deploy_terraform() {
    local env=$1
    
    log_section "Déploiement Infrastructure (Terraform)"
    
    local tf_dir="${PROJECT_ROOT}/infrastructure/terraform/environments/${env}"
    
    if [[ ! -d "$tf_dir" ]]; then
        log_warn "Répertoire Terraform non trouvé: $tf_dir"
        return 0
    fi
    
    cd "$tf_dir"
    
    if [[ "$DRY_RUN" == true ]]; then
        terraform plan
    else
        terraform init -upgrade
        terraform plan -out=tfplan
        terraform apply -auto-approve tfplan
        rm -f tfplan
    fi
    
    cd "$PROJECT_ROOT"
    log_info "Infrastructure déployée"
}

update_airflow_dags() {
    local env=$1
    local version=$2
    
    log_section "Mise à jour des DAGs Airflow"
    
    local airflow_bucket="${AIRFLOW_BUCKET:-airflow-${env}}"
    local dags_path="orchestration/airflow/dags/"
    
    if [[ "$DRY_RUN" == true ]]; then
        log_warn "DRY-RUN: aws s3 sync ${dags_path} s3://${airflow_bucket}/dags/"
    else
        aws s3 sync "$dags_path" "s3://${airflow_bucket}/dags/" --delete
    fi
    
    log_info "DAGs mis à jour"
}

create_deployment_record() {
    local env=$1
    local version=$2
    
    local record_file="/tmp/deployment-${env}-${version}.json"
    
    cat > "$record_file" << EOF
{
    "environment": "${env}",
    "version": "${version}",
    "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
    "git_commit": "$(git rev-parse HEAD)",
    "git_branch": "$(git branch --show-current)",
    "deployed_by": "${USER}"
}
EOF
    
    if [[ "$DRY_RUN" != true ]]; then
        local s3_bucket="${S3_BUCKET:-spark-data-platform-${env}}"
        aws s3 cp "$record_file" "s3://${s3_bucket}/deployments/"
    fi
    
    log_info "Record de déploiement créé"
    cat "$record_file"
}

notify_deployment() {
    local env=$1
    local version=$2
    local status=$3
    
    if [[ -n "${SLACK_WEBHOOK:-}" ]]; then
        local color="good"
        [[ "$status" == "failed" ]] && color="danger"
        
        curl -s -X POST "$SLACK_WEBHOOK" \
            -H "Content-Type: application/json" \
            -d "{
                \"attachments\": [{
                    \"color\": \"${color}\",
                    \"title\": \"Déploiement ${status}\",
                    \"fields\": [
                        {\"title\": \"Environment\", \"value\": \"${env}\", \"short\": true},
                        {\"title\": \"Version\", \"value\": \"${version}\", \"short\": true}
                    ]
                }]
            }"
    fi
}

# ============ MAIN ============

main() {
    local environment=""
    local skip_tests=false
    local skip_build=false
    local version=""
    DRY_RUN=false
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --skip-tests)
                skip_tests=true
                shift
                ;;
            --skip-build)
                skip_build=true
                shift
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --version)
                version="$2"
                shift 2
                ;;
            -h|--help)
                usage
                ;;
            *)
                if [[ -z "$environment" ]]; then
                    environment="$1"
                else
                    log_error "Argument inconnu: $1"
                    usage
                fi
                shift
                ;;
        esac
    done
    
    if [[ -z "$environment" ]]; then
        log_error "L'environnement est requis"
        usage
    fi
    
    # Validation
    validate_environment "$environment"
    
    # Version par défaut
    if [[ -z "$version" ]]; then
        version=$(git describe --tags --always 2>/dev/null || echo "$(date +%Y%m%d%H%M%S)")
    fi
    
    log_section "Déploiement Spark Data Platform"
    log_info "Environment: $environment"
    log_info "Version: $version"
    log_info "Dry-run: $DRY_RUN"
    
    cd "$PROJECT_ROOT"
    
    # Charger la configuration
    load_env_config "$environment"
    
    # Étapes de déploiement
    if [[ "$skip_tests" != true ]]; then
        run_tests
    else
        log_warn "Tests ignorés (--skip-tests)"
    fi
    
    if [[ "$skip_build" != true ]]; then
        build_package
    else
        log_warn "Build ignoré (--skip-build)"
    fi
    
    # Déploiement
    deploy_to_s3 "$environment" "$version"
    deploy_terraform "$environment"
    update_airflow_dags "$environment" "$version"
    create_deployment_record "$environment" "$version"
    
    # Notification
    notify_deployment "$environment" "$version" "success"
    
    log_section "Déploiement terminé avec succès"
    log_info "Version ${version} déployée en ${environment}"
}

main "$@"
