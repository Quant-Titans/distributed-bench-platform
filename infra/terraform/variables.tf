variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "eu-north-1"
}

variable "cluster_name" {
  description = "EKS cluster name"
  type        = string
  default     = "quant-titans"
}

variable "db_password" {
  description = "TimescaleDB master password"
  type        = string
  sensitive   = true
}
