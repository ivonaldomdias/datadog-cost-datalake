variable "aws_region" {
  description = "Região AWS onde o bucket S3 será criado"
  type        = string
  default     = "us-east-1"
}

variable "s3_bucket_name" {
  description = "Nome do bucket S3 para o Data Lake de custos Datadog"
  type        = string
}

variable "environment" {
  description = "Ambiente (production, staging, development)"
  type        = string
  default     = "production"
}

variable "cost_center" {
  description = "Centro de custo para tagging dos recursos"
  type        = string
  default     = "cc-finops"
}
