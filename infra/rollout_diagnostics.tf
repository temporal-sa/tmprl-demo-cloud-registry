# Durable, short-lived evidence for failed Kubernetes candidate rollouts.
#
# The registry operator captures pod status, Events, and bounded current/previous
# container logs before it deletes a failed candidate namespace. The bucket is
# shared by blue/green; object keys carry the cluster color and project identity.

locals {
  rollout_diagnostics_bucket = "${var.name_prefix}-rollout-diagnostics-${local.account_id}-${var.region}"
}

resource "aws_s3_bucket" "rollout_diagnostics" {
  bucket = local.rollout_diagnostics_bucket

  # A first CI-driven apply must grant the Tofu role access to this exact bucket
  # before that same role attempts CreateBucket.
  depends_on = [aws_iam_role_policy.ci_tofu]
}

resource "aws_s3_bucket_ownership_controls" "rollout_diagnostics" {
  bucket = aws_s3_bucket.rollout_diagnostics.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "rollout_diagnostics" {
  bucket = aws_s3_bucket.rollout_diagnostics.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "rollout_diagnostics" {
  bucket                  = aws_s3_bucket.rollout_diagnostics.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_policy" "rollout_diagnostics" {
  bucket = aws_s3_bucket.rollout_diagnostics.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "DenyInsecureTransport"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource = [
        aws_s3_bucket.rollout_diagnostics.arn,
        "${aws_s3_bucket.rollout_diagnostics.arn}/*",
      ]
      Condition = {
        Bool = { "aws:SecureTransport" = "false" }
      }
    }]
  })
}

resource "aws_s3_bucket_lifecycle_configuration" "rollout_diagnostics" {
  bucket = aws_s3_bucket.rollout_diagnostics.id

  rule {
    id     = "expire-rollout-diagnostics"
    status = "Enabled"

    filter {}

    expiration {
      days = 30
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 1
    }
  }
}

resource "aws_iam_role_policy" "registry_operator_rollout_diagnostics" {
  name = "${var.name_prefix}-registry-operator-rollout-diagnostics"
  role = aws_iam_role.registry_operator.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid      = "WriteRolloutDiagnostics"
      Effect   = "Allow"
      Action   = "s3:PutObject"
      Resource = "${aws_s3_bucket.rollout_diagnostics.arn}/clusters/*"
      Condition = {
        StringEquals = {
          "s3:x-amz-server-side-encryption" = "AES256"
        }
      }
    }]
  })
}
