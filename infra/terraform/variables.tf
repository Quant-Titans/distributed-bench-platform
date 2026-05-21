variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "eu-north-1"
}

variable "cluster_name" {
  description = "EKS cluster name"
  type        = string
  default     = "quant-titans"
}

variable "k8s_namespace" {
  description = "Kubernetes namespace for all platform workloads"
  type        = string
  default     = "quant-titans"
}

variable "image_tag" {
  description = "Docker image tag to deploy (matches the GitHub Actions build tag)"
  type        = string
  default     = "latest"
}

variable "ghcr_username" {
  description = "GitHub username for ghcr.io image pulls"
  type        = string
  default     = "emmanueladutwum123"
}

variable "ghcr_token" {
  description = "GitHub Personal Access Token with read:packages scope for ghcr.io"
  type        = string
  sensitive   = true
}

variable "db_password" {
  description = "TimescaleDB postgres password"
  type        = string
  sensitive   = true
  default     = "titans"
}
