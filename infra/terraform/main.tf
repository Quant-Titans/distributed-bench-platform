terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.30"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.14"
    }
  }
  # Remote state — uncomment for team use
  # backend "s3" {
  #   bucket = "quant-titans-tfstate"
  #   key    = "prod/terraform.tfstate"
  #   region = "eu-north-1"
  # }
}

provider "aws" {
  region = var.aws_region
}

# ── VPC ───────────────────────────────────────────────────────────────────────

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = "quant-titans-vpc"
  cidr = "10.0.0.0/16"

  azs             = ["${var.aws_region}a", "${var.aws_region}b", "${var.aws_region}c"]
  private_subnets = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  public_subnets  = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]

  enable_nat_gateway   = true
  single_nat_gateway   = true # cost-optimised for hackathon
  enable_dns_hostnames = true

  tags = local.common_tags
}

# ── EKS Cluster ───────────────────────────────────────────────────────────────

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = var.cluster_name
  cluster_version = "1.30"

  vpc_id                         = module.vpc.vpc_id
  subnet_ids                     = module.vpc.private_subnets
  cluster_endpoint_public_access = true

  eks_managed_node_groups = {
    # CPU-pinned worker nodes for sandbox execution
    sandbox = {
      instance_types = ["c6i.2xlarge"]
      min_size       = 2
      max_size       = 10
      desired_size   = 3
      labels         = { role = "sandbox" }
      taints = [{
        key    = "role"
        value  = "sandbox"
        effect = "NO_SCHEDULE"
      }]
    }

    # General purpose nodes for botfleet + telemetry
    general = {
      instance_types = ["m6i.xlarge"]
      min_size       = 2
      max_size       = 8
      desired_size   = 3
      labels         = { role = "general" }
    }
  }

  tags = local.common_tags
}

# ── TimescaleDB (RDS PostgreSQL with TimescaleDB extension) ──────────────────

resource "aws_db_subnet_group" "timescale" {
  name       = "quant-titans-timescale"
  subnet_ids = module.vpc.private_subnets
  tags       = local.common_tags
}

resource "aws_security_group" "timescale" {
  name   = "quant-titans-timescale"
  vpc_id = module.vpc.vpc_id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [module.vpc.vpc_cidr_block]
  }
  tags = local.common_tags
}

resource "aws_db_instance" "timescale" {
  identifier        = "quant-titans-timescale"
  engine            = "postgres"
  engine_version    = "16"
  instance_class    = "db.t3.medium"
  allocated_storage = 100
  storage_type      = "gp3"

  db_name  = "benchmarks"
  username = "benchuser"
  password = var.db_password

  db_subnet_group_name   = aws_db_subnet_group.timescale.name
  vpc_security_group_ids = [aws_security_group.timescale.id]
  skip_final_snapshot    = true # hackathon — skip final snapshot
  publicly_accessible    = false

  tags = local.common_tags
}

# ── Elasticache Redis ─────────────────────────────────────────────────────────

resource "aws_elasticache_subnet_group" "redis" {
  name       = "quant-titans-redis"
  subnet_ids = module.vpc.private_subnets
}

resource "aws_elasticache_cluster" "redis" {
  cluster_id           = "quant-titans-redis"
  engine               = "redis"
  node_type            = "cache.t3.micro"
  num_cache_nodes      = 1
  parameter_group_name = "default.redis7"
  subnet_group_name    = aws_elasticache_subnet_group.redis.name
  tags                 = local.common_tags
}

# ── Redpanda (self-managed on EKS, deployed via Helm) ────────────────────────
# Redpanda Helm chart is deployed in infra/helm/platform

locals {
  common_tags = {
    Project     = "quant-titans"
    Competition = "IICPC-2026"
    ManagedBy   = "terraform"
  }
}
