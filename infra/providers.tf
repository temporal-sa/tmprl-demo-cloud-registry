provider "aws" {
  # Pinned to var.region so the stack never follows the caller's ambient
  # AWS_REGION (which is us-west-2 by default on dev machines).
  region = var.region

  default_tags {
    tags = {
      ManagedBy = "opentofu"
      Project   = "tmprl-demo-cloud"
      Repo      = "cluster-gitops-config"
    }
  }
}
