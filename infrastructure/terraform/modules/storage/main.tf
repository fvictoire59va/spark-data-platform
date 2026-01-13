# Module Storage S3

# Bronze Bucket
resource "aws_s3_bucket" "bronze" {
  bucket = var.bronze_bucket_name

  tags = {
    Layer = "bronze"
  }
}

resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Disabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    id     = "transition-to-ia"
    status = "Enabled"

    transition {
      days          = var.lifecycle_rules["bronze"].transition_days
      storage_class = var.lifecycle_rules["bronze"].transition_storage_class
    }

    dynamic "expiration" {
      for_each = var.lifecycle_rules["bronze"].expiration_days != null ? [1] : []
      content {
        days = var.lifecycle_rules["bronze"].expiration_days
      }
    }
  }
}

# Silver Bucket
resource "aws_s3_bucket" "silver" {
  bucket = var.silver_bucket_name

  tags = {
    Layer = "silver"
  }
}

resource "aws_s3_bucket_versioning" "silver" {
  bucket = aws_s3_bucket.silver.id
  versioning_configuration {
    status = var.enable_versioning ? "Enabled" : "Disabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id

  rule {
    id     = "transition-to-ia"
    status = "Enabled"

    transition {
      days          = var.lifecycle_rules["silver"].transition_days
      storage_class = var.lifecycle_rules["silver"].transition_storage_class
    }
  }
}

# Gold Bucket
resource "aws_s3_bucket" "gold" {
  bucket = var.gold_bucket_name

  tags = {
    Layer = "gold"
  }
}

resource "aws_s3_bucket_versioning" "gold" {
  bucket = aws_s3_bucket.gold.id
  versioning_configuration {
    status = "Enabled"  # Toujours versionné pour Gold
  }
}

# Logs Bucket
resource "aws_s3_bucket" "logs" {
  bucket = var.logs_bucket_name

  tags = {
    Purpose = "logs"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id

  rule {
    id     = "delete-old-logs"
    status = "Enabled"

    expiration {
      days = 90
    }
  }
}

# Server-side encryption for all buckets
resource "aws_s3_bucket_server_side_encryption_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "silver" {
  bucket = aws_s3_bucket.silver.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "gold" {
  bucket = aws_s3_bucket.gold.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
  }
}

# Outputs
output "bronze_bucket_name" {
  value = aws_s3_bucket.bronze.id
}

output "bronze_bucket_arn" {
  value = aws_s3_bucket.bronze.arn
}

output "silver_bucket_name" {
  value = aws_s3_bucket.silver.id
}

output "silver_bucket_arn" {
  value = aws_s3_bucket.silver.arn
}

output "gold_bucket_name" {
  value = aws_s3_bucket.gold.id
}

output "gold_bucket_arn" {
  value = aws_s3_bucket.gold.arn
}

output "logs_bucket_name" {
  value = aws_s3_bucket.logs.id
}

output "logs_bucket_arn" {
  value = aws_s3_bucket.logs.arn
}
