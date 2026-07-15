# Phase 0 — foundations only. This stack reads caller identity to guard the
# target account and otherwise creates nothing (the gate is a clean plan with
# only the state bucket existing). Real resources arrive in later phases:
# P1 network + EKS, P2 IAM/ECR/CodeBuild/Secrets/ACM, P3 Flux, P4 routing/DNS.

# The wrong-account guard is enforced by allowed_account_ids on the aws
# provider (providers.tf) — a hard error on plan/apply, not a soft warning.
# This data source just feeds the aws_account_id output.
data "aws_caller_identity" "current" {}
