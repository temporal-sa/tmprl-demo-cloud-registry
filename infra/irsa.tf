# Shared blue/green IRSA roles (P2). These are built GREENFIELD — the hand-built
# pet roles are left untouched. Each role's trust federates EVERY color's OIDC
# provider (blue today, green when added to var.colors) so a single role ARN
# serves all colors and the Flux overlays annotate one stable ARN. Trust is
# derived from the P1 cluster module's OIDC outputs, never a literal id (the id
# rotates on cluster re-create).
#
# The registry-operator role IS here (P5): blue's operator must actually run to
# reconcile demos (the e2e gate + cutover prep). The clean name
# `<name_prefix>-registry-operator` is still held by the live pet until it retires
# (P7), so this greenfield shared role takes a `-bg` (blue/green) suffix; the SA
# annotation points at this ARN. Policy is reproduced verbatim from the pet role.

locals {
  # Per-color IRSA OIDC facts from the cluster module.
  irsa_oidc = {
    for color, c in module.cluster : color => {
      arn = c.oidc_provider_arn
      url = c.oidc_provider_url
    }
  }

  # The Kubernetes service account (ns/name) each shared role is bound to. The
  # blue/green Flux overlays annotate these SAs with the matching role ARN (P3).
  irsa_service_accounts = {
    external-secrets        = "system:serviceaccount:external-secrets:external-secrets"
    cert-manager            = "system:serviceaccount:cert-manager:cert-manager"
    cluster-autoscaler      = "system:serviceaccount:kube-system:cluster-autoscaler"
    traefik                 = "system:serviceaccount:traefik:traefik"
    instruqt-litellm-broker = "system:serviceaccount:${var.name_prefix}-instruqt-litellm:instruqt-litellm-broker"
    registry-operator       = "system:serviceaccount:${var.name_prefix}-registry-operator:registry-operator"
  }

  # One assume-role-with-web-identity statement per color for a given SA. Adding a
  # color to var.colors extends every role's trust automatically, no edits here.
  irsa_assume_role_policies = {
    for key, sa in local.irsa_service_accounts : key => jsonencode({
      Version = "2012-10-17"
      Statement = [
        for color, o in local.irsa_oidc : {
          Effect    = "Allow"
          Principal = { Federated = o.arn }
          Action    = "sts:AssumeRoleWithWebIdentity"
          Condition = {
            StringEquals = {
              "${o.url}:sub" = sa
              "${o.url}:aud" = "sts.amazonaws.com"
            }
          }
        }
      ]
    })
  }
}

# --- external-secrets -------------------------------------------------------
# Reads platform secrets under <name_prefix>/* (read-only). Runs on every color.
resource "aws_iam_role" "external_secrets" {
  name               = "${var.name_prefix}-external-secrets"
  assume_role_policy = local.irsa_assume_role_policies["external-secrets"]
}

resource "aws_iam_role_policy" "external_secrets" {
  name = "${var.name_prefix}-secrets-read"
  role = aws_iam_role.external_secrets.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ReadPlatformSecrets"
        Effect = "Allow"
        Action = [
          "secretsmanager:DescribeSecret",
          "secretsmanager:GetResourcePolicy",
          "secretsmanager:GetSecretValue",
          "secretsmanager:ListSecretVersionIds",
        ]
        Resource = local.secrets_prefix_arn
      },
      {
        # The external-secrets AWS provider requires ListSecrets, which cannot be
        # resource-scoped.
        Sid      = "ListSecretsForProvider"
        Effect   = "Allow"
        Action   = ["secretsmanager:ListSecrets"]
        Resource = "*"
      },
    ]
  })
}

# --- cert-manager -----------------------------------------------------------
# DNS-01 (Route53) ACME solver. Scoped tighter than the pet's role (the one zone,
# not hostedzone/*). Runs on every color so a standby green can pre-warm TLS.
resource "aws_iam_role" "cert_manager" {
  name               = "${var.name_prefix}-cert-manager"
  assume_role_policy = local.irsa_assume_role_policies["cert-manager"]
}

resource "aws_iam_role_policy" "cert_manager" {
  name = "${var.name_prefix}-cert-manager-route53"
  role = aws_iam_role.cert_manager.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "route53:GetChange"
        Resource = "arn:${local.partition}:route53:::change/*"
      },
      {
        Effect   = "Allow"
        Action   = ["route53:ChangeResourceRecordSets", "route53:ListResourceRecordSets"]
        Resource = "arn:${local.partition}:route53:::hostedzone/${data.aws_route53_zone.this.zone_id}"
      },
      {
        Effect   = "Allow"
        Action   = "route53:ListHostedZonesByName"
        Resource = "*"
      },
    ]
  })
}

# --- cluster-autoscaler -----------------------------------------------------
# Read-only discovery on *, plus a per-color tag-gated scaling statement matching
# the ASG discovery tags the P1 cluster module stamps
# (k8s.io/cluster-autoscaler/<cluster_name>=owned). One statement per color.
resource "aws_iam_role" "cluster_autoscaler" {
  name               = "${var.name_prefix}-cluster-autoscaler"
  assume_role_policy = local.irsa_assume_role_policies["cluster-autoscaler"]
}

resource "aws_iam_role_policy" "cluster_autoscaler" {
  name = "${var.name_prefix}-cluster-autoscaler"
  role = aws_iam_role.cluster_autoscaler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = concat(
      [{
        Sid    = "AutoscalerReadOnly"
        Effect = "Allow"
        Action = [
          "autoscaling:DescribeAutoScalingGroups",
          "autoscaling:DescribeAutoScalingInstances",
          "autoscaling:DescribeLaunchConfigurations",
          "autoscaling:DescribeScalingActivities",
          "autoscaling:DescribeTags",
          "ec2:DescribeImages",
          "ec2:DescribeInstanceTypes",
          "ec2:DescribeLaunchTemplateVersions",
          "ec2:GetInstanceTypesFromInstanceRequirements",
          "eks:DescribeNodegroup",
        ]
        Resource = "*"
      }],
      [
        for color, c in module.cluster : {
          Sid      = "AutoscalerScaling${title(color)}"
          Effect   = "Allow"
          Action   = ["autoscaling:SetDesiredCapacity", "autoscaling:TerminateInstanceInAutoScalingGroup"]
          Resource = "*"
          Condition = {
            StringEquals = {
              "aws:ResourceTag/k8s.io/cluster-autoscaler/enabled"           = "true"
              "aws:ResourceTag/k8s.io/cluster-autoscaler/${c.cluster_name}" = "owned"
            }
          }
        }
      ],
    )
  })
}

# --- traefik ----------------------------------------------------------------
# Trust-only: Traefik's Classic ELB is provisioned by the in-tree cloud provider
# via the node role, so the ingress SA needs no AWS permissions. The role exists
# only to satisfy the SA's role-arn annotation; P3 may drop the annotation.
resource "aws_iam_role" "traefik" {
  name               = "${var.name_prefix}-traefik"
  assume_role_policy = local.irsa_assume_role_policies["traefik"]
}

# --- instruqt-litellm broker ------------------------------------------------
# DynamoDB CRUD on the broker table. Trusts only the current namespace's SA (the
# pet role's legacy second namespace is not carried forward).
resource "aws_iam_role" "instruqt_litellm_broker" {
  name               = "${var.name_prefix}-instruqt-litellm-broker"
  assume_role_policy = local.irsa_assume_role_policies["instruqt-litellm-broker"]
}

resource "aws_iam_role_policy" "instruqt_litellm_broker" {
  name = "${var.name_prefix}-instruqt-litellm-broker-dynamodb"
  role = aws_iam_role.instruqt_litellm_broker.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "dynamodb:DeleteItem",
        "dynamodb:GetItem",
        "dynamodb:PutItem",
        "dynamodb:UpdateItem",
      ]
      Resource = aws_dynamodb_table.instruqt_litellm_broker.arn
    }]
  })
}

# --- registry-operator ------------------------------------------------------
# Blue/green demo provisioner. Reconciles demos onto its color's cluster and
# manages the shared external plane (ECR, CodeBuild, Secrets Manager, serverless
# Lambda workers, Temporal Cloud namespace tags). Policy reproduced verbatim from
# the pet's tmprl-dem-cld-registry-operator role, split into the same four inline
# policies. Standby model still holds: only the active color's operator polls
# Temporal, so a shared identity across colors is safe.
resource "aws_iam_role" "registry_operator" {
  name               = "${var.name_prefix}-registry-operator-bg"
  assume_role_policy = local.irsa_assume_role_policies["registry-operator"]
}

resource "aws_iam_role_policy" "registry_operator_perms" {
  name = "${var.name_prefix}-registry-operator-perms"
  role = aws_iam_role.registry_operator.id

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
        Sid    = "EcrProjectRepos"
        Effect = "Allow"
        Action = [
          "ecr:CreateRepository",
          "ecr:DescribeRepositories",
          "ecr:GetRepositoryPolicy",
          "ecr:SetRepositoryPolicy",
          "ecr:TagResource",
          "ecr:BatchCheckLayerAvailability",
          "ecr:BatchGetImage",
          "ecr:GetDownloadUrlForLayer",
          "ecr:InitiateLayerUpload",
          "ecr:UploadLayerPart",
          "ecr:CompleteLayerUpload",
          "ecr:PutImage",
          "ecr:DeleteRepository",
        ]
        Resource = "arn:${local.partition}:ecr:${var.region}:${local.account_id}:repository/${var.name_prefix}/*"
      },
      {
        Sid    = "SecretsManagerProjectAndPlatform"
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret",
          "secretsmanager:CreateSecret",
          "secretsmanager:PutSecretValue",
          "secretsmanager:TagResource",
          "secretsmanager:DeleteSecret",
        ]
        Resource = "arn:${local.partition}:secretsmanager:${var.region}:${local.account_id}:secret:${var.name_prefix}/*"
      },
    ]
  })
}

resource "aws_iam_role_policy" "registry_operator_codebuild" {
  name = "${var.name_prefix}-registry-operator-codebuild"
  role = aws_iam_role.registry_operator.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["codebuild:StartBuild", "codebuild:BatchGetBuilds"]
      Resource = "arn:${local.partition}:codebuild:${var.region}:${local.account_id}:project/${var.name_prefix}-build"
    }]
  })
}

resource "aws_iam_role_policy" "registry_operator_instruqt_public_ecr" {
  name = "${var.name_prefix}-registry-operator-instruqt-public-ecr"
  role = aws_iam_role.registry_operator.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "ManagedInstruqtPublicEcrRepositories"
      Effect = "Allow"
      Action = [
        "ecr-public:DescribeRepositories",
        "ecr-public:CreateRepository",
        "ecr-public:DeleteRepository",
        "ecr-public:TagResource",
      ]
      Resource = "arn:${local.partition}:ecr-public::${local.account_id}:repository/${var.name_prefix}/*"
    }]
  })
}

resource "aws_iam_role_policy" "registry_operator_serverless" {
  name = "${var.name_prefix}-registry-operator-serverless"
  role = aws_iam_role.registry_operator.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ManageServerlessLambdaFunctions"
        Effect = "Allow"
        Action = [
          "lambda:CreateFunction",
          "lambda:GetFunction",
          "lambda:GetFunctionConfiguration",
          "lambda:UpdateFunctionConfiguration",
          "lambda:UpdateFunctionCode",
          "lambda:PublishVersion",
          "lambda:ListVersionsByFunction",
          "lambda:DeleteFunction",
          "lambda:TagResource",
        ]
        Resource = [
          "arn:${local.partition}:lambda:${var.region}:${local.account_id}:function:${var.name_prefix}-*",
          "arn:${local.partition}:lambda:${var.region}:${local.account_id}:function:${var.name_prefix}-*:*",
        ]
      },
      {
        Sid    = "CreateBoundedServerlessRoles"
        Effect = "Allow"
        Action = "iam:CreateRole"
        Resource = [
          "arn:${local.partition}:iam::${local.account_id}:role/${var.name_prefix}-*-lambda",
          "arn:${local.partition}:iam::${local.account_id}:role/${var.name_prefix}-*-temporal",
        ]
        Condition = {
          StringEquals = {
            "iam:PermissionsBoundary" = "arn:${local.partition}:iam::${local.account_id}:policy/${var.name_prefix}-serverless-worker-boundary"
          }
        }
      },
      {
        Sid    = "ManageServerlessRoles"
        Effect = "Allow"
        Action = [
          "iam:GetRole",
          "iam:UpdateAssumeRolePolicy",
          "iam:TagRole",
          "iam:GetRolePolicy",
          "iam:PutRolePolicy",
          "iam:ListRolePolicies",
          "iam:DeleteRolePolicy",
          "iam:DeleteRole",
        ]
        Resource = [
          "arn:${local.partition}:iam::${local.account_id}:role/${var.name_prefix}-*-lambda",
          "arn:${local.partition}:iam::${local.account_id}:role/${var.name_prefix}-*-temporal",
        ]
      },
      {
        Sid      = "PassServerlessLambdaExecutionRoles"
        Effect   = "Allow"
        Action   = "iam:PassRole"
        Resource = "arn:${local.partition}:iam::${local.account_id}:role/${var.name_prefix}-*-lambda"
        Condition = {
          StringEquals = {
            "iam:PassedToService" = "lambda.amazonaws.com"
          }
        }
      },
    ]
  })
}
