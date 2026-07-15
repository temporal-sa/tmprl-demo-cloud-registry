# Permissions boundary the registry-operator attaches to every per-project
# serverless-worker role it mints at runtime (its iam:CreateRole is CONDITIONED on
# exactly this policy ARN). Adopted in place: the operator only references it, it
# never creates it, so it is a hard platform prerequisite. Body reproduced verbatim
# from the live v2. prevent_destroy because deleting it silently breaks the
# operator's ability to provision serverless-worker demos.
resource "aws_iam_policy" "serverless_worker_boundary" {
  name        = "${var.name_prefix}-serverless-worker-boundary"
  description = "Permissions boundary for tmprl-demo.cloud serverless worker IAM roles"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "LambdaBasicLogGroups"
        Effect   = "Allow"
        Action   = "logs:CreateLogGroup"
        Resource = "arn:${local.partition}:logs:${var.region}:${local.account_id}:log-group:/aws/lambda/${var.name_prefix}-*"
      },
      {
        Sid      = "LambdaBasicLogStreams"
        Effect   = "Allow"
        Action   = ["logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "arn:${local.partition}:logs:${var.region}:${local.account_id}:log-group:/aws/lambda/${var.name_prefix}-*:*"
      },
      {
        Sid    = "TemporalInvokeManagedLambdas"
        Effect = "Allow"
        Action = ["lambda:GetFunction", "lambda:InvokeFunction"]
        Resource = [
          "arn:${local.partition}:lambda:${var.region}:${local.account_id}:function:${var.name_prefix}-*",
          "arn:${local.partition}:lambda:${var.region}:${local.account_id}:function:${var.name_prefix}-*:*",
        ]
      },
      {
        Sid      = "ReadManagedProjectSecrets"
        Effect   = "Allow"
        Action   = ["secretsmanager:DescribeSecret", "secretsmanager:GetSecretValue"]
        Resource = local.secrets_prefix_arn
      },
    ]
  })

  lifecycle {
    prevent_destroy = true
  }
}

import {
  to = aws_iam_policy.serverless_worker_boundary
  id = "arn:${local.partition}:iam::${local.account_id}:policy/${var.name_prefix}-serverless-worker-boundary"
}
