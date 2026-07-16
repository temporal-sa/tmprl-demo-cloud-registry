# Flux install for blue (P3). Tofu installs the Flux controllers and applies the
# initial root sync; Flux then self-manages everything under clusters/blue/.
#
# Providers cannot be for_each'd the way module.cluster is, so this is a static
# per-color module instance with an explicitly-passed aliased provider set.
# Adding green at P6 = a second provider-alias block (providers.tf) + a second
# module block here — the one place adding a color is not a pure var.colors edit.

data "aws_secretsmanager_secret_version" "github_pat" {
  secret_id = var.bootstrap_secret_names.github_readonly_pat
}

locals {
  # The read-only PAT container stores {"username","password"} JSON — exactly
  # Flux's HTTPS basic-auth shape. Values are populated out of band (never in
  # git); they transit Tofu state, which lives in the locked-down S3 bucket.
  github_creds = jsondecode(data.aws_secretsmanager_secret_version.github_pat.secret_string)
  flux_git_url = "https://github.com/${var.github_repo}.git"
}

module "flux_blue" {
  source = "./modules/flux"
  providers = {
    helm       = helm.blue
    kubernetes = kubernetes.blue
    kubectl    = kubectl.blue
  }

  flux_chart_version = var.flux_chart_version

  git_url                 = local.flux_git_url
  git_branch              = var.flux_git_branch
  git_username            = local.github_creds.username
  git_password            = local.github_creds.password
  root_kustomization_path = "./clusters/blue/flux-system"
}

# A green cutover sibling adds a second module.flux_<color> here (green providers,
# syncing ./clusters/<color>/). Same repo/branch/PAT — each cluster runs its own Flux
# against its own color path, no cross-cluster conflict. P6 validated this with green,
# then tore it down (destroy module.flux_green BEFORE removing the green providers).
