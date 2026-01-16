#!/usr/bin/env bash
#
# Script de validation de la documentation
# Vérifie que la documentation peut être compilée correctement
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

validate_docs() {
    log_section "Validation de la documentation"

    # Vérifier que mkdocs.yml existe
    if [[ ! -f "$PROJECT_ROOT/mkdocs.yml" ]]; then
        log_error "mkdocs.yml non trouvé"
        return 1
    fi
    log_info "✓ mkdocs.yml trouvé"

    # Vérifier que le dossier docs existe
    if [[ ! -d "$PROJECT_ROOT/docs" ]]; then
        log_error "Dossier docs non trouvé"
        return 1
    fi
    log_info "✓ Dossier docs trouvé"

    # Vérifier que index.md existe
    if [[ ! -f "$PROJECT_ROOT/docs/index.md" ]]; then
        log_error "docs/index.md non trouvé"
        return 1
    fi
    log_info "✓ docs/index.md trouvé"

    # Vérifier que architecture.md existe
    if [[ ! -f "$PROJECT_ROOT/docs/architecture.md" ]]; then
        log_error "docs/architecture.md non trouvé"
        return 1
    fi
    log_info "✓ docs/architecture.md trouvé"

    return 0
}

build_docs() {
    log_section "Construction de la documentation"

    cd "$PROJECT_ROOT"

    if ! command -v poetry &> /dev/null; then
        log_error "Poetry n'est pas installé"
        return 1
    fi

    log_info "Installation des dépendances..."
    poetry install --with docs --quiet

    log_info "Construction stricte..."
    if poetry run mkdocs build --strict; then
        log_info "✓ Documentation construite avec succès"
        return 0
    else
        log_error "Erreur lors de la construction"
        return 1
    fi
}

cleanup() {
    log_section "Nettoyage"
    rm -rf "$PROJECT_ROOT/site"
    log_info "✓ Dossier site nettoyé"
}

main() {
    log_info "Validation de la documentation Spark Data Platform"

    if ! validate_docs; then
        log_error "Validation échouée"
        exit 1
    fi

    if ! build_docs; then
        log_error "Construction échouée"
        exit 1
    fi

    cleanup

    log_section "Succès"
    log_info "La documentation est valide et peut être construite"
    exit 0
}

main
