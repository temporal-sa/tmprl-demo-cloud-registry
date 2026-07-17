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

# A green cutover sibling adds a matching kubernetes/helm/kubectl alias block here
# pointed at module.cluster["green"] (P6 validated this, then removed it with green).
# ORDERING: the alekc/kubectl provider cannot configure against a not-yet-created
# cluster's (unknown) endpoint, so a new color's CLUSTER must exist before these
# blocks can plan — bootstrap it with `tofu apply -target='module.cluster["<color>"]'`
# first, then a full apply installs its Flux. On teardown, destroy module.flux_<color>
# (needs these providers + the live cluster) BEFORE removing these blocks.
