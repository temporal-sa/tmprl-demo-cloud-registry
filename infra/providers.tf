provider "aws" {
  # Pinned to var.region so the stack never follows the caller's ambient
  # AWS_REGION (which is us-west-2 by default on dev machines).
  region = var.region

  # Hard gate: error during provider config (including on plan) if pointed at
  # the wrong account.
  allowed_account_ids = [var.aws_account_id]

  default_tags {
    tags = {
      ManagedBy = "opentofu"
      Project   = "tmprl-demo-cloud"
      # The local checkout dir is cluster-gitops-config, but the GitHub repo (and
      # the CI role's OIDC trust) is temporal-sa/tmprl-demo-cloud-registry.
      Repo = "temporal-sa/tmprl-demo-cloud-registry"
    }
  }
}

# --- In-cluster providers for the Flux install (P3) -------------------------
#
# Terraform/OpenTofu forbid for_each on provider blocks, while module.cluster IS
# for_each'd over var.colors. So the Flux layer uses a STATIC per-color aliased
# provider set — one block per color (blue + green as of P6). Referencing
# module.cluster["<color>"] is safe because each color is always a key of
# var.colors. Adding another color = another block here (the one non-for_each edit).
#
# Auth is the same `aws eks get-token` exec plugin the emitted kubeconfig uses
# (clusters.tf) — no static kubeconfig/token on disk, re-fetched at apply time.

provider "kubernetes" {
  alias                  = "blue"
  host                   = module.cluster["blue"].cluster_endpoint
  cluster_ca_certificate = base64decode(module.cluster["blue"].cluster_ca)
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["--region", var.region, "eks", "get-token", "--cluster-name", module.cluster["blue"].cluster_name, "--output", "json"]
  }
}

provider "helm" {
  alias = "blue"
  kubernetes {
    host                   = module.cluster["blue"].cluster_endpoint
    cluster_ca_certificate = base64decode(module.cluster["blue"].cluster_ca)
    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["--region", var.region, "eks", "get-token", "--cluster-name", module.cluster["blue"].cluster_name, "--output", "json"]
    }
  }
}

provider "kubectl" {
  alias                  = "blue"
  host                   = module.cluster["blue"].cluster_endpoint
  cluster_ca_certificate = base64decode(module.cluster["blue"].cluster_ca)
  load_config_file       = false
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["--region", var.region, "eks", "get-token", "--cluster-name", module.cluster["blue"].cluster_name, "--output", "json"]
  }
}

# --- green (P6): the cutover sibling. Identical to the blue set above, pointed at
# module.cluster["green"]. Safe because "green" is a key of var.colors.

provider "kubernetes" {
  alias                  = "green"
  host                   = module.cluster["green"].cluster_endpoint
  cluster_ca_certificate = base64decode(module.cluster["green"].cluster_ca)
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["--region", var.region, "eks", "get-token", "--cluster-name", module.cluster["green"].cluster_name, "--output", "json"]
  }
}

provider "helm" {
  alias = "green"
  kubernetes {
    host                   = module.cluster["green"].cluster_endpoint
    cluster_ca_certificate = base64decode(module.cluster["green"].cluster_ca)
    exec {
      api_version = "client.authentication.k8s.io/v1beta1"
      command     = "aws"
      args        = ["--region", var.region, "eks", "get-token", "--cluster-name", module.cluster["green"].cluster_name, "--output", "json"]
    }
  }
}

provider "kubectl" {
  alias                  = "green"
  host                   = module.cluster["green"].cluster_endpoint
  cluster_ca_certificate = base64decode(module.cluster["green"].cluster_ca)
  load_config_file       = false
  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["--region", var.region, "eks", "get-token", "--cluster-name", module.cluster["green"].cluster_name, "--output", "json"]
  }
}
