# Stable delivery and state resources for the Instruqt LiteLLM broker. These
# resources predate this stack and are adopted in place. Application records in
# DynamoDB remain broker-owned; Tofu owns only the table and its lifecycle.

locals {
  instruqt_litellm_broker_ecr_name = "temporal-instruqt-litellm-broker"
  instruqt_litellm_broker_iam_name = "${var.name_prefix}-instruqt-broker-github-actions"
  instruqt_litellm_broker_table    = "temporal-instruqt-litellm-broker"

  instruqt_litellm_broker_github_subject = "repo:${var.github_repo}:ref:refs/heads/main"
}

resource "aws_ecr_repository" "instruqt_litellm_broker" {
  name                 = local.instruqt_litellm_broker_ecr_name
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = false
  }

  encryption_configuration {
    encryption_type = "AES256"
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_dynamodb_table" "instruqt_litellm_broker" {
  name         = local.instruqt_litellm_broker_table
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "pk"

  attribute {
    name = "pk"
    type = "S"
  }

  ttl {
    attribute_name = "expires_at_epoch"
    enabled        = true
  }

  point_in_time_recovery {
    enabled = false
  }

  deletion_protection_enabled = false

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_iam_role" "instruqt_litellm_broker_github_actions" {
  name                 = local.instruqt_litellm_broker_iam_name
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
          "token.actions.githubusercontent.com:sub" = local.instruqt_litellm_broker_github_subject
        }
      }
    }]
  })

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_iam_role_policy" "instruqt_litellm_broker_github_actions" {
  name = "${var.name_prefix}-instruqt-broker-ecr-push"
  role = aws_iam_role.instruqt_litellm_broker_github_actions.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "EcrAuth"
        Effect   = "Allow"
        Action   = "ecr:GetAuthorizationToken"
        Resource = "*"
      },
      {
        Sid    = "BrokerEcrPush"
        Effect = "Allow"
        Action = [
          "ecr:BatchCheckLayerAvailability",
          "ecr:BatchGetImage",
          "ecr:CompleteLayerUpload",
          "ecr:DescribeImages",
          "ecr:DescribeRepositories",
          "ecr:GetDownloadUrlForLayer",
          "ecr:InitiateLayerUpload",
          "ecr:ListImages",
          "ecr:PutImage",
          "ecr:UploadLayerPart",
        ]
        Resource = aws_ecr_repository.instruqt_litellm_broker.arn
      },
    ]
  })
}

import {
  to = aws_ecr_repository.instruqt_litellm_broker
  id = local.instruqt_litellm_broker_ecr_name
}

import {
  to = aws_dynamodb_table.instruqt_litellm_broker
  id = local.instruqt_litellm_broker_table
}

import {
  to = aws_iam_role.instruqt_litellm_broker_github_actions
  id = local.instruqt_litellm_broker_iam_name
}

import {
  to = aws_iam_role_policy.instruqt_litellm_broker_github_actions
  id = "${local.instruqt_litellm_broker_iam_name}:${var.name_prefix}-instruqt-broker-ecr-push"
}
