# Platform root stack. Network + clusters live in network.tf / clusters.tf
# (P1). Later phases add: P2 IAM/ECR/CodeBuild/Secrets/ACM, P3 Flux, P4
# routing/DNS.

# The wrong-account guard is enforced by allowed_account_ids on the aws
# provider (providers.tf) — a hard error on plan/apply, not a soft warning.
# This data source just feeds the aws_account_id output.
data "aws_caller_identity" "current" {}
