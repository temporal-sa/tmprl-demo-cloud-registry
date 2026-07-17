# Platform root stack. Network + clusters live in network.tf / clusters.tf
# (P1). Later phases add: P2 IAM/ECR/CodeBuild/Secrets/ACM, P3 Flux, P4
# routing/DNS.

# The wrong-account guard is enforced by allowed_account_ids on the aws
# provider (providers.tf) — a hard error on plan/apply, not a soft warning.
# This data source just feeds the aws_account_id output.
data "aws_caller_identity" "current" {}

# Partition (aws) for building ARNs without hardcoding it.
data "aws_partition" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  partition  = data.aws_partition.current.partition

  # The operator, external-secrets, and CodeBuild all address platform AWS
  # resources under the shared, color-agnostic `<name_prefix>/*` namespace
  # (secrets) and `<name_prefix>-*` name prefix. Built once here and reused.
  secrets_prefix_arn = "arn:${local.partition}:secretsmanager:${var.region}:${local.account_id}:secret:${var.name_prefix}/*"
  ecr_prefix_arn     = "arn:${local.partition}:ecr:${var.region}:${local.account_id}:repository/${var.name_prefix}/*"
}
