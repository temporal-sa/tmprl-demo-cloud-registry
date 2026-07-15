# Phase 0 — foundations only. This stack reads caller identity to guard the
# target account and otherwise creates nothing (the gate is a clean plan with
# only the state bucket existing). Real resources arrive in later phases:
# P1 network + EKS, P2 IAM/ECR/CodeBuild/Secrets/ACM, P3 Flux, P4 routing/DNS.

data "aws_caller_identity" "current" {}

# Surface a wrong-account mistake loudly without adding anything to the plan.
# (Resource-level preconditions take over as a hard gate once later phases add
# real resources.)
check "correct_account" {
  assert {
    condition     = data.aws_caller_identity.current.account_id == var.aws_account_id
    error_message = "Wrong AWS account: expected ${var.aws_account_id}, got ${data.aws_caller_identity.current.account_id}. Check your AWS credentials/profile."
  }
}
