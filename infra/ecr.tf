# The operator's own image repos (P2). These are created by the operator's own
# self-hosting pipeline (projects/demo/registry.yaml, finalization-policy: retain),
# so Tofu adopts them in place to break the from-nothing bootstrap chicken-and-egg
# (the operator image must exist before the operator can reconcile itself). The
# operator's create_or_verify path is tag/policy-blind and idempotent, so Tofu
# ownership won't fight it. Per-DEMO repos (<prefix>/<project>/*) stay operator-owned
# and are NOT managed here. Lifecycle/expiry policy is deferred (see migration plan).
locals {
  operator_ecr_builds = toset(["web", "worker", "temporal-cloud-proxy"])
}

resource "aws_ecr_repository" "operator" {
  for_each = local.operator_ecr_builds

  name                 = "${var.name_prefix}/registry-operator/${each.key}"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  # Match the operator's runtime tags so the import is clean and its tag-blind
  # verify branch stays a no-op.
  tags = {
    managed-by = "demo-registry"
    project    = "registry-operator"
    build      = each.key
  }

  lifecycle {
    prevent_destroy = true
  }
}

import {
  for_each = local.operator_ecr_builds

  to = aws_ecr_repository.operator[each.key]
  id = "${var.name_prefix}/registry-operator/${each.key}"
}
