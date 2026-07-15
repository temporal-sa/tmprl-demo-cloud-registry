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
      Repo      = "cluster-gitops-config"
    }
  }
}
