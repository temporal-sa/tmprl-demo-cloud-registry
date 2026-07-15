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
  }
}
