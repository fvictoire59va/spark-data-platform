# Outputs de l'infrastructure

output "vpc_id" {
  description = "ID du VPC"
  value       = module.networking.vpc_id
}

output "spark_cluster_endpoint" {
  description = "Endpoint du cluster Spark"
  value       = module.spark_cluster.master_endpoint
}

output "spark_ui_url" {
  description = "URL de l'interface Spark"
  value       = module.spark_cluster.spark_ui_url
}

output "s3_buckets" {
  description = "Noms des buckets S3"
  value = {
    bronze = module.storage.bronze_bucket_name
    silver = module.storage.silver_bucket_name
    gold   = module.storage.gold_bucket_name
    logs   = module.storage.logs_bucket_name
  }
}

output "monitoring_dashboard_url" {
  description = "URL du dashboard de monitoring"
  value       = module.monitoring.dashboard_url
}
