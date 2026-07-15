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
