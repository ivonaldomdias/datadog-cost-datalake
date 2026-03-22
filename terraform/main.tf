terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket = "your-terraform-state-bucket"
    key    = "datadog-cost-datalake/terraform.tfstate"
    region = "us-east-1"
  }
}

provider "aws" {
  region = var.aws_region
}

# ── S3 Bucket — Data Lake ─────────────────────────────────────────────────────
resource "aws_s3_bucket" "datadog_datalake" {
  bucket = var.s3_bucket_name

  tags = {
    Name        = "datadog-cost-datalake"
    env         = var.environment
    team        = "platform"
    cost-center = var.cost_center
    managed-by  = "terraform"
    project     = "datadog-cost-datalake"
  }
}

resource "aws_s3_bucket_versioning" "datadog_datalake" {
  bucket = aws_s3_bucket.datadog_datalake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "datadog_datalake" {
  bucket = aws_s3_bucket.datadog_datalake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "datadog_datalake" {
  bucket = aws_s3_bucket.datadog_datalake.id

  rule {
    id     = "raw-parquet-retention"
    status = "Enabled"
    filter { prefix = "datadog-costs/raw/" }
    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }
    transition {
      days          = 365
      storage_class = "GLACIER"
    }
  }

  rule {
    id     = "delta-log-cleanup"
    status = "Enabled"
    filter { prefix = "datadog-costs/delta/" }
    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

resource "aws_s3_bucket_public_access_block" "datadog_datalake" {
  bucket                  = aws_s3_bucket.datadog_datalake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ── IAM — Pipeline Extractor ──────────────────────────────────────────────────
resource "aws_iam_user" "pipeline_extractor" {
  name = "datadog-cost-pipeline-extractor"
  path = "/finops/"
  tags = { managed-by = "terraform" }
}

resource "aws_iam_access_key" "pipeline_extractor" {
  user = aws_iam_user.pipeline_extractor.name
}

resource "aws_iam_policy" "pipeline_s3_write" {
  name        = "datadog-cost-pipeline-s3-write"
  description = "Permite escrita no S3 do Data Lake de custos Datadog"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:PutObjectTagging",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:GetBucketLocation",
        ]
        Resource = [
          aws_s3_bucket.datadog_datalake.arn,
          "${aws_s3_bucket.datadog_datalake.arn}/datadog-costs/raw/*",
        ]
      }
    ]
  })
}

resource "aws_iam_user_policy_attachment" "pipeline_extractor" {
  user       = aws_iam_user.pipeline_extractor.name
  policy_arn = aws_iam_policy.pipeline_s3_write.arn
}

# ── IAM — Databricks ──────────────────────────────────────────────────────────
resource "aws_iam_role" "databricks_s3_access" {
  name = "databricks-datadog-datalake-access"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
  tags = { managed-by = "terraform" }
}

resource "aws_iam_policy" "databricks_s3_read_write" {
  name        = "databricks-datadog-datalake-s3"
  description = "Acesso completo do Databricks ao bucket do Data Lake"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
          "s3:ListBucket", "s3:GetBucketLocation",
          "s3:GetObjectTagging", "s3:PutObjectTagging",
        ]
        Resource = [
          aws_s3_bucket.datadog_datalake.arn,
          "${aws_s3_bucket.datadog_datalake.arn}/*",
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "databricks_s3" {
  role       = aws_iam_role.databricks_s3_access.name
  policy_arn = aws_iam_policy.databricks_s3_read_write.arn
}
