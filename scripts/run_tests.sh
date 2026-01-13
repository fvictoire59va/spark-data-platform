#!/usr/bin/env bash
#
# Script d'exécution des tests
# Usage: ./run_tests.sh [type] [options]
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
Usage: $(basename "$0") [type] [options]

Types de tests:
    unit            Tests unitaires uniquement
    integration     Tests d'intégration uniquement
    e2e             Tests end-to-end uniquement
    all             Tous les tests (défaut)

Options:
    -v, --verbose   Mode verbose
    -c, --coverage  Génère le rapport de couverture
    -f, --failfast  Arrête au premier échec
    -k <pattern>    Exécute les tests correspondant au pattern
    --parallel      Exécution parallèle
    -h, --help      Affiche cette aide

Exemples:
    $(basename "$0")                    # Tous les tests
    $(basename "$0") unit -c            # Tests unitaires avec couverture
    $(basename "$0") integration -v     # Tests d'intégration en verbose
    $(basename "$0") -k "test_transform" # Tests contenant "test_transform"
EOF
    exit 1
}

run_tests() {
    local test_type=$1
    shift
    local pytest_args=("$@")
    
    local test_path=""
    local markers=""
    
    case $test_type in
        unit)
            test_path="tests/unit"
            ;;
        integration)
            test_path="tests/integration"
            markers="-m integration"
            ;;
        e2e)
            test_path="tests/e2e"
            markers="-m e2e"
            ;;
        all)
            test_path="tests/"
            ;;
        *)
            log_error "Type de test inconnu: $test_type"
            usage
            ;;
    esac
    
    log_section "Exécution des tests: $test_type"
    
    local cmd="poetry run pytest ${test_path} ${markers} ${pytest_args[*]}"
    log_info "Commande: $cmd"
    
    eval "$cmd"
}

main() {
    cd "$PROJECT_ROOT"
    
    local test_type="all"
    local pytest_args=()
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            unit|integration|e2e|all)
                test_type="$1"
                shift
                ;;
            -v|--verbose)
                pytest_args+=("-v")
                shift
                ;;
            -c|--coverage)
                pytest_args+=("--cov=src" "--cov-report=html" "--cov-report=xml")
                shift
                ;;
            -f|--failfast)
                pytest_args+=("-x")
                shift
                ;;
            -k)
                pytest_args+=("-k" "$2")
                shift 2
                ;;
            --parallel)
                pytest_args+=("-n" "auto")
                shift
                ;;
            -h|--help)
                usage
                ;;
            *)
                pytest_args+=("$1")
                shift
                ;;
        esac
    done
    
    # Vérifier que poetry est installé
    if ! command -v poetry &> /dev/null; then
        log_error "Poetry n'est pas installé"
        exit 1
    fi
    
    # Installer les dépendances si nécessaire
    if [[ ! -d ".venv" ]]; then
        log_info "Installation des dépendances..."
        poetry install
    fi
    
    # Exécuter les tests
    run_tests "$test_type" "${pytest_args[@]}"
    
    log_info "Tests terminés"
}

main "$@"
