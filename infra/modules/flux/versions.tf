terraform {
  required_version = ">= 1.10"

  required_providers {
    # Configured providers are passed in by the caller (aliased per color),
    # because provider blocks cannot be for_each'd over var.colors.
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
