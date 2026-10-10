# Read existing cluster identities without depending on cluster resources.
# This keeps a data-store deployment from applying unrelated cluster drift.
data "aws_eks_cluster" "advanced_ai_agent" {
  for_each = toset(var.colors)
  name     = "${var.name_prefix}-${each.key}"
}

# Shared data for API, workers, and crashable clones. Temporal histories retain
# S3 claims, so project runtime teardown must not remove these stores.
locals {
  advanced_ai_agent_bucket = "${var.name_prefix}-advanced-ai-agent-data-${local.account_id}-${var.region}"
  advanced_ai_agent_subjects = flatten([
    for component in ["advanced-ai-agent-api", "advanced-ai-agent-worker"] : [
      "system:serviceaccount:${var.name_prefix}-advanced-ai-agent:${component}",
      "system:serviceaccount:${var.name_prefix}-advanced-ai-agent-*:${component}",
      "system:serviceaccount:${var.name_prefix}-crash-*:${component}",
    ]
  ])
}

resource "aws_s3_bucket" "advanced_ai_agent" {
  bucket = local.advanced_ai_agent_bucket
  lifecycle { prevent_destroy = true }
}

resource "aws_s3_bucket_public_access_block" "advanced_ai_agent" {
  bucket                  = aws_s3_bucket.advanced_ai_agent.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "advanced_ai_agent" {
  bucket = aws_s3_bucket.advanced_ai_agent.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_policy" "advanced_ai_agent" {
  bucket = aws_s3_bucket.advanced_ai_agent.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid       = "RequireTLS"
      Effect    = "Deny"
      Principal = "*"
      Action    = "s3:*"
      Resource  = [aws_s3_bucket.advanced_ai_agent.arn, "${aws_s3_bucket.advanced_ai_agent.arn}/*"]
      Condition = { Bool = { "aws:SecureTransport" = "false" } }
    }]
  })
}

resource "aws_dynamodb_table" "advanced_ai_agent_oauth" {
  name         = "${var.name_prefix}-advanced-ai-agent-oauth"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "user_id"
  range_key    = "provider"
  attribute {
    name = "user_id"
    type = "S"
  }
  attribute {
    name = "provider"
    type = "S"
  }
  attribute {
    name = "connection_id"
    type = "S"
  }
  global_secondary_index {
    name            = "connection-id-index"
    hash_key        = "connection_id"
    projection_type = "ALL"
  }
  global_secondary_index {
    name            = "provider-index"
    hash_key        = "provider"
    projection_type = "ALL"
  }
  point_in_time_recovery { enabled = true }
  lifecycle { prevent_destroy = true }
}

resource "aws_dynamodb_table" "advanced_ai_agent_artifacts" {
  name         = "${var.name_prefix}-advanced-ai-agent-artifacts"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "user_id"
  range_key    = "artifact_id"
  attribute {
    name = "user_id"
    type = "S"
  }
  attribute {
    name = "artifact_id"
    type = "S"
  }
  attribute {
    name = "workflow_id"
    type = "S"
  }
  global_secondary_index {
    name            = "workflow-index"
    hash_key        = "workflow_id"
    projection_type = "ALL"
  }
  point_in_time_recovery { enabled = true }
  lifecycle { prevent_destroy = true }
}

resource "aws_iam_role" "advanced_ai_agent" {
  name = "${var.name_prefix}-advanced-ai-agent-data"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [for color, cluster in data.aws_eks_cluster.advanced_ai_agent : {
      Effect    = "Allow"
      Principal = { Federated = "arn:aws:iam::${local.account_id}:oidc-provider/${trimprefix(cluster.identity[0].oidc[0].issuer, "https://")}" }
      Action    = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = { "${trimprefix(cluster.identity[0].oidc[0].issuer, "https://")}:aud" = "sts.amazonaws.com" }
        StringLike   = { "${trimprefix(cluster.identity[0].oidc[0].issuer, "https://")}:sub" = local.advanced_ai_agent_subjects }
      }
    }]
  })
}

resource "aws_iam_role_policy" "advanced_ai_agent" {
  name = "advanced-ai-agent-data"
  role = aws_iam_role.advanced_ai_agent.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucket"]
        Resource = aws_s3_bucket.advanced_ai_agent.arn
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = "${aws_s3_bucket.advanced_ai_agent.arn}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["dynamodb:DescribeTable", "dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DeleteItem", "dynamodb:Query"]
        Resource = flatten([for table in [aws_dynamodb_table.advanced_ai_agent_oauth, aws_dynamodb_table.advanced_ai_agent_artifacts] : [table.arn, "${table.arn}/index/*"]])
      },
    ]
  })
}
