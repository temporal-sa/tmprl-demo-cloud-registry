# One EKS cluster per color, all sharing module.network. Adding "green" to
# var.colors later stands up the sibling with no change here and no state moves.
module "cluster" {
  source   = "./modules/cluster"
  for_each = toset(var.colors)

  name   = "${var.name_prefix}-${each.key}"
  color  = each.key
  region = var.region

  kubernetes_version = var.kubernetes_version

  vpc_id          = module.network.vpc_id
  private_subnets = module.network.private_subnets_by_az
  public_subnets  = module.network.public_subnets_by_az

  endpoint_public_access_cidrs = var.endpoint_public_access_cidrs
  cluster_admin_principal_arns = var.cluster_admin_principal_arns
  enable_secrets_encryption    = var.enable_secrets_encryption
}

# Emit a ready-to-use kubeconfig per color (the P1 gate). Auth is the standard
# `aws eks get-token` exec plugin, so no long-lived credential is written to disk.
# Files are gitignored (infra/kubeconfig-*.yaml).
locals {
  kubeconfig = {
    for color, c in module.cluster : color => yamlencode({
      apiVersion      = "v1"
      kind            = "Config"
      current-context = c.cluster_name
      clusters = [{
        name = c.cluster_name
        cluster = {
          server                     = c.cluster_endpoint
          certificate-authority-data = c.cluster_ca
        }
      }]
      contexts = [{
        name = c.cluster_name
        context = {
          cluster = c.cluster_name
          user    = c.cluster_name
        }
      }]
      users = [{
        name = c.cluster_name
        user = {
          exec = {
            apiVersion = "client.authentication.k8s.io/v1beta1"
            command    = "aws"
            args       = ["--region", var.region, "eks", "get-token", "--cluster-name", c.cluster_name, "--output", "json"]
          }
        }
      }]
    })
  }
}

resource "local_file" "kubeconfig" {
  for_each = local.kubeconfig

  filename        = "${path.module}/kubeconfig-${each.key}.yaml"
  content         = each.value
  file_permission = "0600"
}
