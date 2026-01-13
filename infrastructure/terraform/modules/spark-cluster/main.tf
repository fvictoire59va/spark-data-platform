# Module Spark Cluster (EMR)

resource "aws_emr_cluster" "spark" {
  name          = var.cluster_name
  release_label = "emr-7.0.0"
  applications  = ["Spark", "Hadoop", "Hive", "JupyterEnterpriseGateway"]

  service_role = aws_iam_role.emr_service_role.arn

  ec2_attributes {
    instance_profile                  = aws_iam_instance_profile.emr_ec2_profile.arn
    subnet_id                         = var.subnet_ids[0]
    emr_managed_master_security_group = aws_security_group.emr_master.id
    emr_managed_slave_security_group  = aws_security_group.emr_slave.id
    key_name                          = var.key_pair_name
  }

  master_instance_group {
    instance_type  = var.master_instance_type
    instance_count = 1
    name           = "Master"

    ebs_config {
      size                 = 100
      type                 = "gp3"
      volumes_per_instance = 1
    }
  }

  core_instance_group {
    instance_type  = var.worker_instance_type
    instance_count = var.worker_count
    name           = "Core"

    ebs_config {
      size                 = 200
      type                 = "gp3"
      volumes_per_instance = 2
    }

    autoscaling_policy = jsonencode({
      Constraints = {
        MinCapacity = var.worker_count
        MaxCapacity = var.worker_count * 3
      }
      Rules = [
        {
          Name   = "ScaleOutMemory"
          Action = {
            SimpleScalingPolicyConfiguration = {
              AdjustmentType   = "CHANGE_IN_CAPACITY"
              ScalingAdjustment = 2
              CoolDown         = 300
            }
          }
          Trigger = {
            CloudWatchAlarmDefinition = {
              MetricName         = "YARNMemoryAvailablePercentage"
              ComparisonOperator = "LESS_THAN"
              Threshold          = 20
              Period             = 300
              EvaluationPeriods  = 1
              Namespace          = "AWS/ElasticMapReduce"
              Statistic          = "AVERAGE"
            }
          }
        },
        {
          Name   = "ScaleInMemory"
          Action = {
            SimpleScalingPolicyConfiguration = {
              AdjustmentType   = "CHANGE_IN_CAPACITY"
              ScalingAdjustment = -1
              CoolDown         = 300
            }
          }
          Trigger = {
            CloudWatchAlarmDefinition = {
              MetricName         = "YARNMemoryAvailablePercentage"
              ComparisonOperator = "GREATER_THAN"
              Threshold          = 75
              Period             = 300
              EvaluationPeriods  = 3
              Namespace          = "AWS/ElasticMapReduce"
              Statistic          = "AVERAGE"
            }
          }
        }
      ]
    })
  }

  log_uri = "s3://${var.s3_logs_bucket}/emr-logs/"

  configurations_json = jsonencode([
    {
      Classification = "spark-defaults"
      Properties = {
        "spark.sql.extensions"                   = "io.delta.sql.DeltaSparkSessionExtension"
        "spark.sql.catalog.spark_catalog"        = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        "spark.databricks.delta.retentionDurationCheck.enabled" = "false"
        "spark.sql.adaptive.enabled"             = "true"
        "spark.sql.adaptive.coalescePartitions.enabled" = "true"
        "spark.serializer"                       = "org.apache.spark.serializer.KryoSerializer"
        "spark.dynamicAllocation.enabled"        = "true"
        "spark.shuffle.service.enabled"          = "true"
      }
    },
    {
      Classification = "spark-hive-site"
      Properties = {
        "hive.metastore.client.factory.class" = "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
      }
    },
    {
      Classification = "hive-site"
      Properties = {
        "hive.metastore.client.factory.class" = "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
      }
    }
  ])

  bootstrap_action {
    name = "Install Python packages"
    path = "s3://${var.s3_logs_bucket}/bootstrap/install_packages.sh"
  }

  step_concurrency_level = 10

  tags = merge(var.tags, {
    Name = var.cluster_name
  })

  lifecycle {
    create_before_destroy = true
  }
}

# Security Groups
resource "aws_security_group" "emr_master" {
  name        = "${var.cluster_name}-master-sg"
  description = "Security group for EMR master"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.cluster_name}-master-sg"
  }
}

resource "aws_security_group" "emr_slave" {
  name        = "${var.cluster_name}-slave-sg"
  description = "Security group for EMR slaves"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.cluster_name}-slave-sg"
  }
}

# IAM Roles
resource "aws_iam_role" "emr_service_role" {
  name = "${var.cluster_name}-emr-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "elasticmapreduce.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "emr_service" {
  role       = aws_iam_role.emr_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

resource "aws_iam_role" "emr_ec2_role" {
  name = "${var.cluster_name}-emr-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "emr_ec2" {
  role       = aws_iam_role.emr_ec2_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
}

resource "aws_iam_instance_profile" "emr_ec2_profile" {
  name = "${var.cluster_name}-emr-ec2-profile"
  role = aws_iam_role.emr_ec2_role.name
}

# Outputs
output "cluster_id" {
  value = aws_emr_cluster.spark.id
}

output "master_endpoint" {
  value = aws_emr_cluster.spark.master_public_dns
}

output "spark_ui_url" {
  value = "http://${aws_emr_cluster.spark.master_public_dns}:18080"
}
