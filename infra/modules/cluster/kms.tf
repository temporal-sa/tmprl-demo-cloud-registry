# Customer-managed CMK for EKS Secret envelope encryption (referenced by the
# cluster's encryption_config in main.tf). The key policy must let the cluster
# IAM role use the key or CreateCluster fails during encryption setup. On
# destroy the key goes to PendingDeletion (no charge, no manual cleanup); the
# alias is name-scoped per color so re-create never collides.
data "aws_caller_identity" "current" {}

resource "aws_kms_key" "secrets" {
  count = var.enable_secrets_encryption ? 1 : 0

  description             = "Envelope encryption for ${var.name} Kubernetes secrets"
  enable_key_rotation     = true
  deletion_window_in_days = 7

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "AllowAccountAdmin"
        Effect    = "Allow"
        Principal = { AWS = "arn:${data.aws_partition.current.partition}:iam::${data.aws_caller_identity.current.account_id}:root" }
        Action    = "kms:*"
        Resource  = "*"
      },
      {
        Sid       = "AllowEKSClusterUse"
        Effect    = "Allow"
        Principal = { AWS = aws_iam_role.cluster.arn }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ListGrants",
          "kms:DescribeKey",
          "kms:CreateGrant",
        ]
        Resource = "*"
      },
    ]
  })
}

resource "aws_kms_alias" "secrets" {
  count = var.enable_secrets_encryption ? 1 : 0

  name          = "alias/${var.name}-eks-secrets"
  target_key_id = aws_kms_key.secrets[0].key_id
}
