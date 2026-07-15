# GitHub Actions OIDC CI role (P2). This is the durable Tofu apply identity for
# the intended end state (Tofu running in a GitHub Action that assumes into this
# account), and — because its ARN is stable, unlike the churning SSO admin — it is
# also registered as a cluster admin on every color (see clusters.tf), resolving
# the P1 review finding. Today `tofu` still runs locally under SSO; this role is
# built now so the CI cutover is a config change, not new infra.

locals {
  ci_tofu_role_name = "${var.name_prefix}-ci-tofu"
  # The P0 state backend bucket (infra/bootstrap): <prefix>-tofu-state-<acct>-<region>.
  tofu_state_bucket = "${var.name_prefix}-tofu-state-${local.account_id}-${var.region}"
}

# The account already federates GitHub Actions OIDC (a per-URL singleton), so read
# it — never declare it as a resource (that would EntityAlreadyExists-collide).
data "aws_iam_openid_connect_provider" "github" {
  url = "https://token.actions.githubusercontent.com"
}

resource "aws_iam_role" "ci_tofu" {
  name = local.ci_tofu_role_name

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
          "token.actions.githubusercontent.com:sub" = [
            for ref in var.ci_branch_refs : "repo:${var.github_repo}:${startswith(ref, "refs/") ? "ref:${ref}" : ref}"
          ]
        }
      }
    }]
  })
}

# Apply-surface permissions. Broad on the pure-infra services (EC2/EKS/ASG/ELB/
# logs), tightly scoped on the sensitive ones: IAM to <prefix>-* names, Secrets
# Manager to container ops only (never GetSecretValue — values stay out of band),
# Route53 to the one zone, ACM read-only, and S3 to the Tofu state bucket.
resource "aws_iam_role_policy" "ci_tofu" {
  name = "${var.name_prefix}-ci-tofu"
  role = aws_iam_role.ci_tofu.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ProvisionInfraServices"
        Effect = "Allow"
        Action = [
          "ec2:*",
          "eks:*",
          "autoscaling:*",
          "elasticloadbalancing:*",
          "logs:*",
        ]
        Resource = "*"
      },
      {
        Sid    = "ManageEksSecretsCmk"
        Effect = "Allow"
        Action = [
          "kms:CreateKey",
          "kms:CreateAlias",
          "kms:DeleteAlias",
          "kms:DescribeKey",
          "kms:ScheduleKeyDeletion",
          "kms:EnableKeyRotation",
          "kms:GetKeyPolicy",
          "kms:GetKeyRotationStatus",
          "kms:PutKeyPolicy",
          "kms:ListAliases",
          "kms:ListKeys",
          "kms:ListResourceTags",
          "kms:TagResource",
          "kms:UntagResource",
          "kms:CreateGrant",
          "kms:ListGrants",
          "kms:RevokeGrant",
        ]
        Resource = "*"
      },
      {
        Sid    = "ManagePlatformRolesAndPolicies"
        Effect = "Allow"
        Action = [
          "iam:CreateRole",
          "iam:DeleteRole",
          "iam:GetRole",
          "iam:UpdateRole",
          "iam:UpdateAssumeRolePolicy",
          "iam:TagRole",
          "iam:UntagRole",
          "iam:PutRolePolicy",
          "iam:DeleteRolePolicy",
          "iam:GetRolePolicy",
          "iam:ListRolePolicies",
          "iam:AttachRolePolicy",
          "iam:DetachRolePolicy",
          "iam:ListAttachedRolePolicies",
          "iam:ListInstanceProfilesForRole",
          "iam:CreatePolicy",
          "iam:DeletePolicy",
          "iam:GetPolicy",
          "iam:GetPolicyVersion",
          "iam:CreatePolicyVersion",
          "iam:DeletePolicyVersion",
          "iam:ListPolicyVersions",
        ]
        Resource = [
          "arn:${local.partition}:iam::${local.account_id}:role/${var.name_prefix}-*",
          "arn:${local.partition}:iam::${local.account_id}:policy/${var.name_prefix}-*",
        ]
      },
      {
        Sid    = "ManageIrsaOidcProviders"
        Effect = "Allow"
        Action = [
          "iam:CreateOpenIDConnectProvider",
          "iam:DeleteOpenIDConnectProvider",
          "iam:GetOpenIDConnectProvider",
          "iam:TagOpenIDConnectProvider",
          "iam:UpdateOpenIDConnectProviderThumbprint",
        ]
        Resource = "arn:${local.partition}:iam::${local.account_id}:oidc-provider/*"
      },
      {
        # Read-only IAM lookups and the service-linked roles EKS/ASG create.
        Sid    = "IamReadAndServiceLinked"
        Effect = "Allow"
        Action = [
          "iam:ListRoles",
          "iam:ListPolicies",
          "iam:ListOpenIDConnectProviders",
          "iam:GetOpenIDConnectProvider",
          "iam:CreateServiceLinkedRole",
        ]
        Resource = "*"
      },
      {
        Sid      = "PassInfraRoles"
        Effect   = "Allow"
        Action   = "iam:PassRole"
        Resource = "arn:${local.partition}:iam::${local.account_id}:role/${var.name_prefix}-*"
        Condition = {
          StringEquals = {
            "iam:PassedToService" = [
              "eks.amazonaws.com",
              "ec2.amazonaws.com",
              "codebuild.amazonaws.com",
            ]
          }
        }
      },
      {
        Sid      = "EcrAuth"
        Effect   = "Allow"
        Action   = "ecr:GetAuthorizationToken"
        Resource = "*"
      },
      {
        Sid    = "ManagePlatformEcr"
        Effect = "Allow"
        Action = [
          "ecr:CreateRepository",
          "ecr:DeleteRepository",
          "ecr:DescribeRepositories",
          "ecr:ListTagsForResource",
          "ecr:TagResource",
          "ecr:UntagResource",
          "ecr:PutLifecyclePolicy",
          "ecr:GetLifecyclePolicy",
          "ecr:DeleteLifecyclePolicy",
          "ecr:GetRepositoryPolicy",
          "ecr:SetRepositoryPolicy",
          "ecr:PutImageScanningConfiguration",
        ]
        Resource = local.ecr_prefix_arn
      },
      {
        Sid    = "ManageBuildProject"
        Effect = "Allow"
        Action = [
          "codebuild:CreateProject",
          "codebuild:UpdateProject",
          "codebuild:DeleteProject",
          "codebuild:BatchGetProjects",
          "codebuild:ListProjects",
        ]
        Resource = "arn:${local.partition}:codebuild:${var.region}:${local.account_id}:project/${var.name_prefix}-*"
      },
      {
        # Container lifecycle + tags only. Deliberately NO GetSecretValue /
        # PutSecretValue: secret values are populated out of band, never by Tofu.
        Sid    = "ManageSecretContainers"
        Effect = "Allow"
        Action = [
          "secretsmanager:CreateSecret",
          "secretsmanager:DeleteSecret",
          "secretsmanager:DescribeSecret",
          "secretsmanager:UpdateSecret",
          "secretsmanager:TagResource",
          "secretsmanager:UntagResource",
          "secretsmanager:GetResourcePolicy",
          "secretsmanager:ListSecretVersionIds",
        ]
        Resource = local.secrets_prefix_arn
      },
      {
        Sid      = "ListSecrets"
        Effect   = "Allow"
        Action   = "secretsmanager:ListSecrets"
        Resource = "*"
      },
      {
        Sid    = "ManageZoneRecords"
        Effect = "Allow"
        Action = [
          "route53:ChangeResourceRecordSets",
          "route53:ListResourceRecordSets",
          "route53:GetHostedZone",
        ]
        Resource = "arn:${local.partition}:route53:::hostedzone/${data.aws_route53_zone.this.zone_id}"
      },
      {
        Sid    = "Route53ReadAndChangeStatus"
        Effect = "Allow"
        Action = [
          "route53:GetChange",
          "route53:ListHostedZones",
          "route53:ListHostedZonesByName",
        ]
        Resource = "*"
      },
      {
        Sid    = "AcmRead"
        Effect = "Allow"
        Action = [
          "acm:DescribeCertificate",
          "acm:ListCertificates",
          "acm:ListTagsForCertificate",
        ]
        Resource = "*"
      },
      {
        Sid    = "TofuStateBucket"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketVersioning",
          "s3:GetBucketLocation",
        ]
        Resource = "arn:${local.partition}:s3:::${local.tofu_state_bucket}"
      },
      {
        Sid    = "TofuStateObjects"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
        ]
        Resource = "arn:${local.partition}:s3:::${local.tofu_state_bucket}/*"
      },
    ]
  })
}

# Register the CI role as cluster admin on every color. Done here (not via the
# module's cluster_admin_principal_arns var) so the entry takes an implicit
# dependency on the role attribute — guaranteeing the role exists first — without
# a module-level depends_on (which would defer the module's data sources and force
# spurious replacements). Keyed by color, so keys are known at plan.
resource "aws_eks_access_entry" "ci_tofu" {
  for_each = module.cluster

  cluster_name  = each.value.cluster_name
  principal_arn = aws_iam_role.ci_tofu.arn
  type          = "STANDARD"
}

resource "aws_eks_access_policy_association" "ci_tofu" {
  for_each = module.cluster

  cluster_name  = each.value.cluster_name
  principal_arn = aws_iam_role.ci_tofu.arn
  policy_arn    = "arn:${local.partition}:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy"

  access_scope {
    type = "cluster"
  }

  depends_on = [aws_eks_access_entry.ci_tofu]
}
