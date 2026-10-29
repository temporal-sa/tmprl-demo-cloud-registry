# CodeBuild — the single shared project the operator invokes (StartBuild) to build
# every project's images, plus its service role. Adopted in place: both are
# hand-built platform prerequisites the operator references by fixed name, not
# per-project and not operator-created. Bodies reproduced verbatim; prevent_destroy
# guards the live build path.

resource "aws_iam_role" "codebuild" {
  name        = "${var.name_prefix}-codebuild"
  description = "CodeBuild service role for tmprl-demo.cloud builds (ECR push + logs)"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "codebuild.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_iam_role_policy" "codebuild_perms" {
  name = "${var.name_prefix}-codebuild-perms"
  role = aws_iam_role.codebuild.id

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
        Sid    = "EcrPush"
        Effect = "Allow"
        Action = [
          "ecr:BatchCheckLayerAvailability",
          "ecr:DescribeRepositories",
          "ecr:InitiateLayerUpload",
          "ecr:UploadLayerPart",
          "ecr:CompleteLayerUpload",
          "ecr:PutImage",
        ]
        Resource = local.ecr_prefix_arn
      },
      {
        Sid    = "Logs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents",
        ]
        Resource = "arn:${local.partition}:logs:${var.region}:${local.account_id}:log-group:/aws/codebuild/${var.name_prefix}-build:*"
      },
      {
        # Per-project source-auth secrets live one level deeper (<prefix>/<project>/*).
        Sid      = "ProjectOwnedSourceSecrets"
        Effect   = "Allow"
        Action   = "secretsmanager:GetSecretValue"
        Resource = "arn:${local.partition}:secretsmanager:${var.region}:${local.account_id}:secret:${var.name_prefix}/*/*"
      },
      {
        # Docker Hub login for base-image pulls (DEMO_REGISTRY_DOCKERHUB_SECRET_PATH).
        Sid      = "DockerHubLogin"
        Effect   = "Allow"
        Action   = "secretsmanager:GetSecretValue"
        Resource = "arn:${local.partition}:secretsmanager:${var.region}:${local.account_id}:secret:${var.name_prefix}/platform/dockerhub-*"
      },
    ]
  })
}

resource "aws_iam_role_policy" "codebuild_public_ecr_push" {
  name = "${var.name_prefix}-codebuild-public-ecr-push"
  role = aws_iam_role.codebuild.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "PublicEcrAuth"
        Effect   = "Allow"
        Action   = "ecr-public:GetAuthorizationToken"
        Resource = "*"
      },
      {
        Sid      = "PublicEcrServiceBearerToken"
        Effect   = "Allow"
        Action   = "sts:GetServiceBearerToken"
        Resource = "*"
      },
      {
        Sid    = "ManagedInstruqtPublicEcrPush"
        Effect = "Allow"
        Action = [
          "ecr-public:BatchCheckLayerAvailability",
          "ecr-public:InitiateLayerUpload",
          "ecr-public:UploadLayerPart",
          "ecr-public:CompleteLayerUpload",
          "ecr-public:PutImage",
          "ecr-public:DescribeRepositories",
        ]
        Resource = "arn:${local.partition}:ecr-public::${local.account_id}:repository/${var.name_prefix}/*"
      },
    ]
  })
}

# The operator overrides the buildspec per build at StartBuild time; the project
# body is a NO_SOURCE shell. Privileged mode is required for the in-container
# docker builds.
resource "aws_codebuild_project" "build" {
  name         = "${var.name_prefix}-build"
  description  = "Shared tmprl-demo.cloud build runner. The registry operator uses StartBuild source/buildspec overrides per DemoProject."
  service_role = aws_iam_role.codebuild.arn

  source {
    type      = "NO_SOURCE"
    buildspec = "version: 0.2\nphases:\n  build:\n    commands:\n      - echo overridden-by-operator\n"
  }

  artifacts {
    type = "NO_ARTIFACTS"
  }

  environment {
    type                        = "LINUX_CONTAINER"
    image                       = "aws/codebuild/standard:7.0"
    compute_type                = "BUILD_GENERAL1_SMALL"
    privileged_mode             = true
    image_pull_credentials_type = "CODEBUILD"
  }

  build_timeout = 60

  lifecycle {
    prevent_destroy = true
  }
}

import {
  to = aws_iam_role.codebuild
  id = "${var.name_prefix}-codebuild"
}

import {
  to = aws_iam_role_policy.codebuild_perms
  id = "${var.name_prefix}-codebuild:${var.name_prefix}-codebuild-perms"
}

import {
  to = aws_iam_role_policy.codebuild_public_ecr_push
  id = "${var.name_prefix}-codebuild:${var.name_prefix}-codebuild-public-ecr-push"
}

import {
  to = aws_codebuild_project.build
  id = "arn:${local.partition}:codebuild:${var.region}:${local.account_id}:project/${var.name_prefix}-build"
}
