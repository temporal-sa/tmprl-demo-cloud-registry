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

output "irsa_role_arns" {
  description = "Shared blue/green IRSA role ARNs, for the Flux overlays' service-account annotations."
  value = {
    external_secrets        = aws_iam_role.external_secrets.arn
    cert_manager            = aws_iam_role.cert_manager.arn
    cluster_autoscaler      = aws_iam_role.cluster_autoscaler.arn
    traefik                 = aws_iam_role.traefik.arn
    external_dns            = aws_iam_role.external_dns.arn
    instruqt_litellm_broker = aws_iam_role.instruqt_litellm_broker.arn
    registry_operator       = aws_iam_role.registry_operator.arn
  }
}

output "ci_tofu_role_arn" {
  description = "GitHub Actions OIDC role for Tofu apply — the durable cluster admin and future CI identity."
  value       = aws_iam_role.ci_tofu.arn
}

output "rollout_diagnostics_bucket" {
  description = "Private S3 bucket containing short-lived failed-rollout diagnostics."
  value       = aws_s3_bucket.rollout_diagnostics.id
}

output "instruqt_secret_broker_delivery" {
  description = "Public image and direct-download locations for the Instruqt secret-broker CLI."
  value = {
    public_ecr_uri     = aws_ecrpublic_repository.instruqt_secret_broker.repository_uri
    cli_base_url       = "https://${aws_s3_bucket.instruqt_secret_broker.bucket_regional_domain_name}/${local.instruqt_secret_broker_prefix}"
    signing_key_arn    = aws_kms_key.instruqt_secret_broker_signing.arn
    signing_public_key = "https://${aws_s3_bucket.instruqt_secret_broker.bucket_regional_domain_name}/${local.instruqt_secret_broker_prefix}/trust/v1/signing-key.pem"
  }
}

output "acm_wildcard_certificate_arn" {
  description = "ARN of the reused *.<domain> ACM wildcard cert (P4 attaches it to the load balancer)."
  value       = data.aws_acm_certificate.wildcard.arn
}

output "flux" {
  description = "Flux install facts per color (P3)."
  value = {
    blue = {
      namespace     = module.flux_blue.namespace
      chart_version = var.flux_chart_version
      git_url       = local.flux_git_url
      git_branch    = var.flux_git_branch
    }
  }
}

output "dns" {
  description = "Routing records Tofu owns (P4). Per-demo <demo> -> live CNAMEs are written by external-dns, not here; the pet's * wildcard is intentionally absent (not Tofu-managed)."
  value = {
    active_color         = var.active_color
    live_fqdn            = aws_route53_record.live.fqdn
    live_target          = local.traefik_elb_hostnames[var.active_color]
    color_fqdns          = { for color, r in aws_route53_record.color : color => r.fqdn }
    traefik_elb_by_color = local.traefik_elb_hostnames
  }
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

output "litellm_db_endpoint" {
  description = "Endpoint (host:port) of the shared LiteLLM Postgres; null until the migration apply creates it. Repoint the SM DATABASE_URL host at this."
  value       = try(aws_db_instance.litellm[0].address, null)
}
