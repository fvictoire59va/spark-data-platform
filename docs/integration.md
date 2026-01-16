# 📚 Guide d'Intégration de la Documentation

Ce guide explique comment la documentation est intégrée dans la stack technique du projet.

## 1. Vue d'Ensemble

La documentation du projet utilise **MkDocs** avec le thème **Material** pour générer un site statique professionnel. Elle est automatiquement déployée sur **GitHub Pages** à chaque commit sur la branche `main`.

### Architecture de la Documentation

```
┌─────────────────────────────────────────────────────────────────┐
│                    Fichiers Documentation                        │
│                  (Markdown dans docs/)                           │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                      MkDocs + Material                           │
│              (mkdocs.yml + Extensions Python)                    │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 ├─────────────────────┬──────────────────────┐
                 ▼                     ▼                      ▼
         ┌───────────────┐     ┌───────────────┐    ┌───────────────┐
         │ Local Dev     │     │ CI/CD Build   │    │ GitHub Pages  │
         │ make docs-    │     │ GitHub Action │    │ (Deployment)  │
         │ serve         │     │ (Validation)  │    │               │
         └───────────────┘     └───────────────┘    └───────────────┘
```

---

## 2. Dépendances

Les dépendances de documentation sont installées via **Poetry** dans le groupe `docs` :

```bash
# Installer uniquement les dépendances de doc
poetry install --with docs

# Installer toutes les dépendances incluant doc
poetry install
```

### Dépendances installées

| Package | Version | Rôle |
|---------|---------|------|
| `mkdocs` | ^1.5.0 | Générateur de site statique |
| `mkdocs-material` | ^9.5.0 | Thème Material Design |
| `mkdocstrings` | ^0.24.0 | Génération auto de l'API |
| `mkdocs-awesome-pages` | ^2.9.0 | Navigation avancée |
| `mkdocs-macros-plugin` | ^1.0.4 | Support des macros Jinja2 |
| `pymdown-extensions` | ^10.5 | Extensions Markdown (Mermaid, code, etc.) |
| `mkdocs-minify-plugin` | ^0.7.0 | Minification du HTML/CSS |

---

## 3. Configuration (mkdocs.yml)

Le fichier [mkdocs.yml](../mkdocs.yml) centralise :

- **Navigation** : Structure des pages et menus
- **Thème** : Couleurs, logos, fonctionnalités Material
- **Plugins** : Extensions pour la recherche, macros, etc.
- **Extensions Markdown** : Support Mermaid, KaTeX, code blocks, etc.

### Exemple de structure de navigation

```yaml
nav:
  - Home: index.md
  - Getting Started: getting-started.md
  - Architecture:
    - Overview: architecture.md
    - Medallion Pattern: architecture.md#section
    - Docker: architecture.md#docker
  - Deployment:
    - Airflow: deployment/airflow.md
```

---

## 4. Commandes Make

### Développement local

```bash
# Servir la documentation localement
make docs-serve
# → http://localhost:8000 (avec hot reload)

# Générer le site (sortie dans site/)
make docs
```

### Validation et build strict

```bash
# Générer en mode strict (erreurs = exitcode 1)
make docs-build-strict

# Valider complètement
make docs-validate
```

### Nettoyage

```bash
# Nettoyer les fichiers générés
make docs-clean
# Ou via le nettoyage global
make clean-all
```

---

## 5. Workflow GitHub Actions

Le fichier [.github/workflows/docs.yml](.github/workflows/docs.yml) automatise :

### 1. Validation (tous les PRs)

```yaml
Job: validate
- Checkout du code
- Setup Python 3.11
- Installation Poetry
- Installation des dépendances
- Build strict (mkdocs build --strict)
```

**Déclenché par :**
- Push sur `main` ou `develop`
- Pull requests sur `main` ou `develop`
- Changements dans `docs/`, `mkdocs.yml`, ou workflow

### 2. Build et Déploiement (main seulement)

```yaml
Job: build
- Checkout du code
- Setup Python 3.11
- Installation Poetry
- Installation des dépendances
- Build la documentation
- Deploy sur GitHub Pages via peaceiris/actions-gh-pages
```

**Déploie sur :**
- `gh-pages` branch automatiquement
- URL : `https://spark-data-platform.example.com` (configurable)

### 3. Upload artifact

La documentation est aussi sauvegardée comme artifact pendant 7 jours pour inspection.

### 4. Notification

Log de succès du déploiement.

---

## 6. Intégration avec Git

### Pre-commit Hooks

Ajoutez au `.pre-commit-config.yaml` (optionnel) :

```yaml
- repo: https://github.com/executablebooks/mdformat
  rev: 0.7.0
  hooks:
    - id: mdformat
      args: [--wrap=100, --number]
```

### Conventions de commits

Utilisez des prefixes clairs pour les commits de doc :

```bash
git commit -m "docs: Update architecture documentation"
git commit -m "docs: Add deployment guide"
git commit -m "docs: Fix typos in getting-started"
```

---

## 7. Structure des Fichiers de Doc

```
docs/
├── index.md                 # Page d'accueil
├── getting-started.md       # Quick start
├── architecture.md          # Documentation architecture
├── api-reference.md         # Référence API
├── guides/                  # Guides pratiques
│   ├── development.md
│   ├── silver-gold-layer.md
│   └── testing/
│       └── ingest-orders.md
├── deployment/              # Guides de déploiement
│   ├── airflow.md
│   ├── airflow-setup.md
│   └── terraform.md
└── assets/                  # Images, logos, CSS
    ├── logo.png
    ├── favicon.ico
    └── extra.css
```

### Règles de nommage

- **Fichiers** : `nom-en-minuscules-avec-tirets.md`
- **Sections** : Utiliser `#`, `##`, `###` hiérarchiquement
- **Liens internes** : `[Texte](../autre-fichier.md)` ou `[Texte](../fichier.md#section)`
- **Images** : `![Alt](../assets/image.png)`

---

## 8. Écrire de la Documentation

### Format Markdown étendu

Le projet supporte les extensions suivantes :

#### Code blocks avec highlighting

```python
# Le langage est détecté automatiquement
def hello():
    print("Hello, World!")
```

#### Admonitions

```markdown
!!! note
    Ceci est une note

!!! warning
    Ceci est un avertissement

!!! danger
    Ceci est une erreur
```

#### Mermaid Diagrams

```mermaid
graph LR
    A[Bronze] --> B[Silver] --> C[Gold]
```

#### Onglets (tabbed)

```markdown
=== "Python"
    ```python
    print("Hello")
    ```

=== "Bash"
    ```bash
    echo "Hello"
    ```
```

#### Math (LaTeX)

Inline : `$E = mc^2$`

Block :
```
$$
\int_0^\infty e^{-x^2} dx = \frac{\sqrt{\pi}}{2}
$$
```

### Bonnes pratiques

1. **Clarté** : Titres descriptifs, explications simples
2. **Code** : Exemples exécutables, copy button auto
3. **Images** : Diagrammes pour les architectures
4. **Liens** : Internes pour navigation, externes avec protocole
5. **Structure** : Max 2-3 niveaux de hiérarchie
6. **Longueur** : ~500 mots par page, sinon diviser

---

## 9. Déploiement Local

### Tester localement avant le push

```bash
# Installer les dépendances
make dev

# Servir localement
make docs-serve

# Ouvrir http://localhost:8000
# Éditez les fichiers, le site se recharge automatiquement
```

### Valider avant le commit

```bash
# Validation stricte (même en CI)
make docs-validate

# Si tout est OK:
git add docs/
git commit -m "docs: Update documentation"
git push
```

---

## 10. Déploiement sur GitHub Pages

### Configuration initiale

1. **Activer GitHub Pages** dans Settings du repo:
   - Source: `gh-pages` branch
   - Deploy from: root directory

2. **Custom domain** (optionnel):
   - Ajouter `cname: spark-data-platform.example.com` dans `mkdocs.yml`
   - Créer un CNAME record DNS pointant vers GitHub Pages

### Vérifier le déploiement

- Workflow `Documentation` s'exécute automatiquement
- Vérifier l'onglet Actions pour les logs
- Visiter l'URL de GitHub Pages

---

## 11. Troubleshooting

### La doc ne se build pas localement

```bash
# Réinstaller les dépendances
poetry install --with docs --force-reinstall

# Supprimer le cache
make docs-clean
rm -rf .pytest_cache/ .mypy_cache/

# Réessayer
make docs-serve
```

### Erreurs de build strict

```bash
# Voir les erreurs détaillées
poetry run mkdocs build --strict --verbose

# Erreurs communes:
# - Liens cassés: Vérifier les chemins
# - Markdown mal formé: Vérifier les espaces
# - Images manquantes: Vérifier le chemin relatif
```

### GitHub Actions ne déploie pas

- Vérifier que la branch `main` a le commit
- Vérifier l'onglet Actions pour les erreurs
- Vérifier que `GITHUB_TOKEN` est disponible (automatique)
- Vérifier la permission du workflow en Settings → Actions

---

## 12. Checklist pour PR avec Documentation

- [ ] Fichiers Markdown créés/modifiés
- [ ] `mkdocs.yml` mis à jour (si nouvelle page)
- [ ] Test local : `make docs-serve` fonctionnel
- [ ] Validation stricte : `make docs-validate` réussit
- [ ] Pas de liens cassés
- [ ] Images et assets inclus
- [ ] Spellcheck (optionnel)
- [ ] Pas de fichiers temporaires (`.DS_Store`, etc.)

---

## 13. Maintenance

### Mise à jour des dépendances

```bash
# Mettre à jour les versions
poetry update

# Tester la doc après mise à jour
make docs-validate
```

### Archivage de versions anciennes

```bash
# Les versions passées restent sur gh-pages branch
# Pour purger: git on gh-pages branch, git rm old versions
```

---

## 14. Ressources Utiles

- **MkDocs** : https://www.mkdocs.org/
- **Material Theme** : https://squidfunk.github.io/mkdocs-material/
- **Markdown Guide** : https://www.markdownguide.org/
- **Mermaid** : https://mermaid.js.org/
- **GitHub Pages** : https://pages.github.com/

---

*Documentation mise à jour le 16 janvier 2026*
