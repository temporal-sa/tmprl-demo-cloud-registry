terraform {
  # 1.10+ for native S3 state locking (use_lockfile).
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    # OIDC provider thumbprint is derived from the issuer's TLS chain, not hardcoded.
    tls = {
      source  = "hashicorp/tls"
      version = "~> 4.0"
    }
    # Emits a ready-to-use kubeconfig per cluster (the P1 gate).
    local = {
      source  = "hashicorp/local"
      version = "~> 2.5"
    }
    # P3 Flux install: helm installs the flux2 controllers; kubernetes creates the
    # namespace + git-auth Secret; kubectl (alekc, the maintained fork) applies the
    # two CRD-typed root-sync objects (GitRepository + Kustomization) — kubectl_manifest
    # does no plan-time server dry-run, so it avoids the CRD-chicken/egg that
    # kubernetes_manifest hits on a cluster whose CRDs are installed in the same apply.
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.17"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.31"
    }
    kubectl = {
      source  = "alekc/kubectl"
      version = "~> 2.1"
    }
  }
}
