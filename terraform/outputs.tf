output "s3_bucket_name" {
  description = "Nome do bucket S3 do Data Lake"
  value       = aws_s3_bucket.datadog_datalake.bucket
}

output "s3_bucket_arn" {
  description = "ARN do bucket S3"
  value       = aws_s3_bucket.datadog_datalake.arn
}

output "pipeline_extractor_access_key_id" {
  description = "AWS Access Key ID para o pipeline extractor"
  value       = aws_iam_access_key.pipeline_extractor.id
  sensitive   = true
}

output "pipeline_extractor_secret_access_key" {
  description = "AWS Secret Access Key para o pipeline extractor"
  value       = aws_iam_access_key.pipeline_extractor.secret
  sensitive   = true
}

output "databricks_iam_role_arn" {
  description = "ARN da IAM Role para o Databricks acessar o S3"
  value       = aws_iam_role.databricks_s3_access.arn
}
