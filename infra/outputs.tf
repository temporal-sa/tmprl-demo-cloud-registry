output "aws_account_id" {
  description = "AWS account tofu is operating in (from caller identity)."
  value       = data.aws_caller_identity.current.account_id
}

output "region" {
  description = "AWS region for the platform."
  value       = var.region
}

output "name_prefix" {
  description = "Resource name prefix for platform-owned AWS resources."
  value       = var.name_prefix
}

output "domain" {
  description = "Apex domain for the demo environment."
  value       = var.domain
}

output "vpc_id" {
  description = "Shared platform VPC id."
  value       = module.network.vpc_id
}

output "private_subnet_ids" {
  description = "Private subnet ids (nodes/pods)."
  value       = module.network.private_subnet_ids
}

output "public_subnet_ids" {
  description = "Public subnet ids (NAT + internet-facing LBs)."
  value       = module.network.public_subnet_ids
}

output "clusters" {
  description = "Per-color cluster facts consumed by later phases (IRSA, routing) and by operators."
  value = {
    for color, c in module.cluster : color => {
      cluster_name      = c.cluster_name
      cluster_endpoint  = c.cluster_endpoint
      cluster_version   = c.cluster_version
      oidc_provider_arn = c.oidc_provider_arn
      oidc_provider_url = c.oidc_provider_url
      node_role_arn     = c.node_role_arn
      kubeconfig_path   = local_file.kubeconfig[color].filename
      update_kubeconfig = "aws eks update-kubeconfig --region ${var.region} --name ${c.cluster_name}"
    }
  }
}
