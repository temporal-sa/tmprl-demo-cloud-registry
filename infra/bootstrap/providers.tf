provider "aws" {
  region = var.region

  # Hard gate: fail provider config (on plan too) if pointed at another account.
  # This module actually creates the state bucket, so guard it here.
  allowed_account_ids = [var.aws_account_id]

  default_tags {
    tags = {
      ManagedBy = "opentofu"
      Project   = "tmprl-demo-cloud"
      Repo      = "cluster-gitops-config"
      Component = "tofu-state-backend"
    }
  }
}
