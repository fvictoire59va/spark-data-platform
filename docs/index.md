# 🚀 Spark Data Platform

**Enterprise-grade data platform** with Apache Spark, Delta Lake, and Medallion Architecture.

---

## 📊 What is Spark Data Platform?

A comprehensive, production-ready data engineering platform that enables:

- **Large-scale data processing** with Apache Spark 3.5
- **Data quality assurance** with Delta Lake 3.0
- **Medallion architecture** (Bronze → Silver → Gold)
- **Cloud-ready infrastructure** with Terraform
- **Orchestration** with Apache Airflow
- **Local development** with Docker Compose

---

## 🏗️ Architecture Layers

```
┌─────────────────────────────────────────────────┐
│              GOLD (Analytics)                   │
│       Aggregated, business-ready data           │
└─────────────────────────────────────────────────┘
                        ↑
┌─────────────────────────────────────────────────┐
│             SILVER (Transformation)             │
│     Clean, deduplicated, standardized data      │
└─────────────────────────────────────────────────┘
                        ↑
┌─────────────────────────────────────────────────┐
│              BRONZE (Ingestion)                 │
│        Raw data from various sources            │
└─────────────────────────────────────────────────┘
```

---

## 🎯 Quick Start

### Installation
```bash
# Clone the repository
git clone https://github.com/fvictoire59va/spark-data-platform.git
cd spark-data-platform

# Setup Python environment
poetry install --with docs

# Run local development
poetry run pytest
```

### Local Development
```bash
# Start Docker services
docker-compose -f infrastructure/docker/docker-compose.yml up -d

# Run a sample job
python scripts/run_ingest_orders_job.py --env dev

# View documentation
poetry run mkdocs serve
```

---

## 📚 Documentation

### For New Team Members
Start here → **[Getting Started](getting-started.md)**

### Understanding the Architecture
→ **[Architecture Overview](architecture.md)**

### Implementing Features
→ **[Silver & Gold Guide](SILVER_GOLD_GUIDE.md)**

### Documentation System
→ **[Guides Index](guides/README.md)**

---

## 🔧 Tech Stack

| Component | Version | Purpose |
|-----------|---------|---------|
| **Apache Spark** | 3.5.0 | Distributed processing engine |
| **Delta Lake** | 3.0.0 | ACID transactions & versioning |
| **Python** | 3.11+ | Development language |
| **Poetry** | 1.7+ | Dependency management |
| **Docker** | Latest | Local development environment |
| **Airflow** | 2.10+ | Workflow orchestration |
| **Terraform** | 1.5+ | Infrastructure as Code |
| **MkDocs** | 1.5.0 | Documentation generation |

---

## 📁 Project Structure

```
spark-data-platform/
├── src/                    # Source code
│   ├── core/              # Base classes and configuration
│   ├── common/            # Shared utilities
│   └── pipelines/         # Data pipeline implementations
├── tests/                 # Test suite
│   ├── unit/
│   ├── integration/
│   └── e2e/
├── docs/                  # Documentation (you are here!)
├── infrastructure/        # Docker & Terraform configs
├── scripts/               # Utility scripts
└── config/                # Environment configurations
```

---

## 🚀 Key Features

✅ **Medallion Architecture** - Bronze, Silver, Gold layers  
✅ **Data Quality** - Great Expectations integration  
✅ **Delta Lake** - ACID transactions, time travel  
✅ **Production Ready** - Error handling, logging, monitoring  
✅ **Local Development** - Docker Compose stack  
✅ **Cloud Ready** - AWS infrastructure templates  
✅ **Orchestration** - Apache Airflow DAGs  
✅ **Testing** - Comprehensive test coverage  
✅ **Documentation** - Auto-generated with MkDocs  

---

## 📖 Documentation Sections

- **[Architecture](architecture.md)** - Technical architecture and design patterns
- **[Getting Started](getting-started.md)** - Setup and configuration
- **[Silver & Gold Pipeline](SILVER_GOLD_GUIDE.md)** - Data transformation layer guide
- **[Deployment](AIRFLOW_DEPLOYMENT_SUMMARY.md)** - Production deployment steps
- **[API Reference](api-reference.md)** - Code API documentation
- **[Guides](guides/README.md)** - Detailed implementation guides

---

## 💡 Common Tasks

### Running Tests
```bash
poetry run pytest tests/
poetry run pytest tests/ --cov=src
```

### Running a Job Locally
```bash
poetry run python src/pipelines/sales/ingest_job.py
```

### Building Docker Image
```bash
docker build -f infrastructure/docker/Dockerfile.spark-job -t spark-data-platform .
```

### Deploying to Cloud
```bash
cd infrastructure/terraform
terraform init -backend-config=backend.tfvars
terraform plan
terraform apply
```

---

## 🔗 Resources

- **GitHub Repository** → [fvictoire59va/spark-data-platform](https://github.com/fvictoire59va/spark-data-platform)
- **Apache Spark** → [spark.apache.org](https://spark.apache.org)
- **Delta Lake** → [delta.io](https://delta.io)
- **MkDocs** → [mkdocs.org](https://mkdocs.org)

---

## 📞 Support

For questions or issues:
1. Check the [FAQ](guides/checklist.md)
2. Review [troubleshooting guide](guides/integration.md#troubleshooting)
3. Open an issue on GitHub

---

## 📄 License

MIT License - See [LICENSE](../LICENSE) file for details.

---

**Last Updated:** January 2026 | **Version:** 1.0.0
