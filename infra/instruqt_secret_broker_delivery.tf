# Public delivery resources for the Instruqt secret-broker CLI. The binary and
# artifact image contain no credentials; callers provide the HMAC credential at
# runtime through Instruqt. ECR Public serves the multi-platform artifact image,
# while S3 provides checksum-verified binaries suitable for setup scripts.

locals {
  instruqt_secret_broker_ecr_name = "${var.name_prefix}/instruqt-secret-broker"
  instruqt_secret_broker_role     = "${var.name_prefix}-instruqt-secret-broker-github-actions"
  instruqt_secret_broker_bucket   = "${var.name_prefix}-secret-broker-${local.account_id}-${var.region}"
  instruqt_secret_broker_prefix   = "secret-broker"
}

resource "aws_ecrpublic_repository" "instruqt_secret_broker" {
  region          = "us-east-1"
  repository_name = local.instruqt_secret_broker_ecr_name

  catalog_data {
    architectures     = ["ARM 64", "x86-64"]
    description       = "Multi-platform static CLI for minting scoped Temporal workshop runtime credentials."
    operating_systems = ["Linux"]
    about_text        = "Artifact image for the Temporal Instruqt secret-broker CLI. The image contains only the static CLI binary and no credentials."
    usage_text        = "Copy `/secret-broker` from `public.ecr.aws/s1u1b8l5/${local.instruqt_secret_broker_ecr_name}:<version>` into a workshop image. Pin an immutable version or digest for reproducibility."
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket" "instruqt_secret_broker" {
  bucket        = local.instruqt_secret_broker_bucket
  force_destroy = false

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_ownership_controls" "instruqt_secret_broker" {
  bucket = aws_s3_bucket.instruqt_secret_broker.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

resource "aws_s3_bucket_public_access_block" "instruqt_secret_broker" {
  bucket = aws_s3_bucket.instruqt_secret_broker.id

  block_public_acls       = true
  ignore_public_acls      = true
  block_public_policy     = false
  restrict_public_buckets = false
}

resource "aws_s3_bucket_versioning" "instruqt_secret_broker" {
  bucket = aws_s3_bucket.instruqt_secret_broker.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "instruqt_secret_broker" {
  bucket = aws_s3_bucket.instruqt_secret_broker.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "instruqt_secret_broker" {
  bucket = aws_s3_bucket.instruqt_secret_broker.id

  rule {
    id     = "artifact-history"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }

    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }
}

resource "aws_s3_bucket_policy" "instruqt_secret_broker" {
  bucket = aws_s3_bucket.instruqt_secret_broker.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "PublicReadSecretBrokerArtifacts"
      Effect    = "Allow"
      Principal = "*"
      Action    = "s3:GetObject"
      Resource  = "${aws_s3_bucket.instruqt_secret_broker.arn}/${local.instruqt_secret_broker_prefix}/*"
    }]
  })

  depends_on = [aws_s3_bucket_public_access_block.instruqt_secret_broker]
}

resource "aws_iam_role" "instruqt_secret_broker_github_actions" {
  name                 = local.instruqt_secret_broker_role
  max_session_duration = 3600

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Federated = data.aws_iam_openid_connect_provider.github.arn }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
        }
        StringLike = {
          "token.actions.githubusercontent.com:sub" = "repo:${var.github_repo}:ref:refs/heads/main"
        }
      }
    }]
  })

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_iam_role_policy" "instruqt_secret_broker_github_actions" {
  name = "${var.name_prefix}-instruqt-secret-broker-publish"
  role = aws_iam_role.instruqt_secret_broker_github_actions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "PublicEcrAuth"
        Effect = "Allow"
        Action = [
          "ecr-public:GetAuthorizationToken",
          "sts:GetServiceBearerToken",
        ]
        Resource = "*"
      },
      {
        Sid    = "PublicEcrPush"
        Effect = "Allow"
        Action = [
          "ecr-public:BatchCheckLayerAvailability",
          "ecr-public:CompleteLayerUpload",
          "ecr-public:InitiateLayerUpload",
          "ecr-public:PutImage",
          "ecr-public:UploadLayerPart",
        ]
        Resource = aws_ecrpublic_repository.instruqt_secret_broker.arn
      },
      {
        Sid      = "ListCliArtifacts"
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = aws_s3_bucket.instruqt_secret_broker.arn
        Condition = {
          StringLike = {
            "s3:prefix" = "${local.instruqt_secret_broker_prefix}/*"
          }
        }
      },
      {
        Sid    = "PublishCliArtifacts"
        Effect = "Allow"
        Action = [
          "s3:AbortMultipartUpload",
          "s3:PutObject",
        ]
        Resource = "${aws_s3_bucket.instruqt_secret_broker.arn}/${local.instruqt_secret_broker_prefix}/*"
      },
    ]
  })
}
