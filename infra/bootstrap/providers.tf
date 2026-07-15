provider "aws" {
  region = var.region

  default_tags {
    tags = {
      ManagedBy = "opentofu"
      Project   = "tmprl-demo-cloud"
      Repo      = "cluster-gitops-config"
      Component = "tofu-state-backend"
    }
  }
}
