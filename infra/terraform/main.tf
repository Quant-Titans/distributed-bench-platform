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

  # Uncomment for team use (create the bucket first):
  # backend "s3" {
  #   bucket = "quant-titans-tfstate"
  #   key    = "prod/terraform.tfstate"
  #   region = "eu-north-1"
  # }
}

provider "aws" {
  region = var.aws_region
}

# ── Kubernetes + Helm providers — configured from EKS outputs ────────────────
# Terraform evaluates providers before creating resources, but data sources that
# depend on resources (EKS) are refreshed during the apply phase, so this
# pattern works in a single terraform apply run.

data "aws_eks_cluster" "this" {
  name       = module.eks.cluster_name
  depends_on = [module.eks]
}

data "aws_eks_cluster_auth" "this" {
  name       = module.eks.cluster_name
  depends_on = [module.eks]
}

provider "kubernetes" {
  host                   = data.aws_eks_cluster.this.endpoint
  cluster_ca_certificate = base64decode(data.aws_eks_cluster.this.certificate_authority[0].data)
  token                  = data.aws_eks_cluster_auth.this.token
}

provider "helm" {
  kubernetes {
    host                   = data.aws_eks_cluster.this.endpoint
    cluster_ca_certificate = base64decode(data.aws_eks_cluster.this.certificate_authority[0].data)
    token                  = data.aws_eks_cluster_auth.this.token
  }
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
  single_nat_gateway   = true  # cost-optimised for hackathon
  enable_dns_hostnames = true

  # Required tags for EKS to discover subnets for load balancers
  public_subnet_tags = {
    "kubernetes.io/role/elb" = "1"
  }
  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = "1"
  }

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

  # EBS CSI driver — required for PersistentVolumeClaims (TimescaleDB, Redis)
  cluster_addons = {
    aws-ebs-csi-driver = {
      most_recent = true
    }
    coredns = {
      most_recent = true
    }
    kube-proxy = {
      most_recent = true
    }
    vpc-cni = {
      most_recent = true
    }
  }

  eks_managed_node_groups = {
    # CPU-pinned worker nodes for sandbox execution (NET_ADMIN for eBPF TC hooks)
    sandbox = {
      instance_types = ["c6i.2xlarge"]
      min_size       = 1
      max_size       = 6
      desired_size   = 2
      labels         = { role = "sandbox" }
      taints = [{
        key    = "role"
        value  = "sandbox"
        effect = "NO_SCHEDULE"
      }]
    }

    # General purpose nodes for botfleet, telemetry, data stores, redpanda
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

# ── Kubernetes namespace ──────────────────────────────────────────────────────

resource "kubernetes_namespace" "quant_titans" {
  metadata {
    name = var.k8s_namespace
    labels = {
      "app.kubernetes.io/managed-by" = "terraform"
    }
  }

  depends_on = [module.eks]
}

# ── GHCR image pull secret ────────────────────────────────────────────────────
# Images are built by GitHub Actions and pushed to ghcr.io/quant-titans.
# This secret lets EKS pull them.

resource "kubernetes_secret" "ghcr_pull" {
  metadata {
    name      = "ghcr-pull-secret"
    namespace = kubernetes_namespace.quant_titans.metadata[0].name
  }
  type = "kubernetes.io/dockerconfigjson"
  data = {
    ".dockerconfigjson" = jsonencode({
      auths = {
        "ghcr.io" = {
          auth = base64encode("${var.ghcr_username}:${var.ghcr_token}")
        }
      }
    })
  }
}

# ── Platform Helm release ─────────────────────────────────────────────────────
# Deploys: Redpanda, TimescaleDB, Redis, Sandbox, BotFleet, Telemetry, Leaderboard
# A single terraform apply wires everything.

resource "helm_release" "platform" {
  name             = "quant-titans"
  chart            = "${path.module}/../../helm/platform"
  namespace        = kubernetes_namespace.quant_titans.metadata[0].name
  create_namespace = false
  wait             = true
  timeout          = 600 # 10 minutes — Redpanda StatefulSet takes time

  set {
    name  = "image.tag"
    value = var.image_tag
  }

  set {
    name  = "image.registry"
    value = "ghcr.io/quant-titans"
  }

  set_sensitive {
    name  = "timescaledb.password"
    value = var.db_password
  }

  depends_on = [kubernetes_namespace.quant_titans]
}

# ── Locals ────────────────────────────────────────────────────────────────────

locals {
  common_tags = {
    Project     = "quant-titans"
    Competition = "IICPC-2026"
    ManagedBy   = "terraform"
  }
}
