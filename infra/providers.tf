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
