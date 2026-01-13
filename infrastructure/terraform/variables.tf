# Variables pour l'infrastructure Spark

variable "aws_region" {
  description = "Région AWS"
  type        = string
  default     = "eu-west-1"
}

variable "environment" {
  description = "Environnement (dev, staging, prod)"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "project_name" {
  description = "Nom du projet"
  type        = string
  default     = "spark-data-platform"
}

variable "vpc_cidr" {
  description = "CIDR du VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "availability_zones" {
  description = "Zones de disponibilité"
  type        = list(string)
  default     = ["eu-west-1a", "eu-west-1b", "eu-west-1c"]
}

variable "private_subnets" {
  description = "CIDRs des subnets privés"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
}

variable "public_subnets" {
  description = "CIDRs des subnets publics"
  type        = list(string)
  default     = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]
}

variable "spark_master_instance_type" {
  description = "Type d'instance pour le master Spark"
  type        = string
  default     = "m5.xlarge"
}

variable "spark_worker_instance_type" {
  description = "Type d'instance pour les workers Spark"
  type        = string
  default     = "m5.2xlarge"
}

variable "spark_worker_count" {
  description = "Nombre de workers Spark"
  type        = number
  default     = 3
}

variable "storage_lifecycle_rules" {
  description = "Règles de lifecycle pour S3"
  type = map(object({
    transition_days          = number
    transition_storage_class = string
    expiration_days          = optional(number)
  }))
  default = {
    bronze = {
      transition_days          = 30
      transition_storage_class = "STANDARD_IA"
      expiration_days          = 365
    }
    silver = {
      transition_days          = 60
      transition_storage_class = "STANDARD_IA"
    }
    gold = {
      transition_days          = 90
      transition_storage_class = "STANDARD_IA"
    }
  }
}

variable "enable_monitoring_alerts" {
  description = "Activer les alertes de monitoring"
  type        = bool
  default     = true
}

variable "alert_email" {
  description = "Email pour les alertes"
  type        = string
  default     = ""
}

variable "log_retention_days" {
  description = "Rétention des logs en jours"
  type        = number
  default     = 30
}

variable "tags" {
  description = "Tags additionnels"
  type        = map(string)
  default     = {}
}
