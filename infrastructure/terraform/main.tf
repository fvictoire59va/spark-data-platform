# Infrastructure Terraform pour Spark Data Platform
terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    bucket         = "terraform-state-spark-platform"
    key            = "spark-platform/terraform.tfstate"
    region         = "eu-west-1"
    encrypt        = true
    dynamodb_table = "terraform-locks"
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "spark-data-platform"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

# ============ MODULES ============

module "networking" {
  source = "./modules/networking"

  environment     = var.environment
  vpc_cidr        = var.vpc_cidr
  azs             = var.availability_zones
  private_subnets = var.private_subnets
  public_subnets  = var.public_subnets
}

module "storage" {
  source = "./modules/storage"

  environment         = var.environment
  bronze_bucket_name  = "${var.project_name}-${var.environment}-bronze"
  silver_bucket_name  = "${var.project_name}-${var.environment}-silver"
  gold_bucket_name    = "${var.project_name}-${var.environment}-gold"
  logs_bucket_name    = "${var.project_name}-${var.environment}-logs"
  
  enable_versioning   = var.environment == "prod"
  lifecycle_rules     = var.storage_lifecycle_rules
}

module "spark_cluster" {
  source = "./modules/spark-cluster"

  environment        = var.environment
  cluster_name       = "${var.project_name}-${var.environment}"
  
  vpc_id             = module.networking.vpc_id
  subnet_ids         = module.networking.private_subnet_ids
  
  master_instance_type = var.spark_master_instance_type
  worker_instance_type = var.spark_worker_instance_type
  worker_count         = var.spark_worker_count
  
  s3_logs_bucket     = module.storage.logs_bucket_arn
  
  tags = var.tags
}

module "monitoring" {
  source = "./modules/monitoring"

  environment    = var.environment
  cluster_name   = module.spark_cluster.cluster_name
  
  enable_alerts  = var.enable_monitoring_alerts
  alert_email    = var.alert_email
  
  log_retention_days = var.log_retention_days
}
