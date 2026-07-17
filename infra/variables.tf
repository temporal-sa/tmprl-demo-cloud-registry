# Inputs for the platform stack. Every value defaults to the live
# tmprl-demo.cloud environment, so a fresh checkout plans without a tfvars file.
# A blue/green sibling cluster overrides only what differs (added in later
# phases: cluster color, node sizing, active_color).

variable "region" {
  description = "AWS region for all platform resources. Reuses the existing us-west-1 ACM cert, Route53 zone, Secrets Manager, and CodeBuild."
  type        = string
  default     = "us-west-1"
}

variable "colors" {
  description = "Cluster colors to stand up on the shared VPC. Blue only in steady state; a green sibling is added for a blue/green cutover (P6 validated the full swing + rollback with an ephemeral green, then tore it down). Adding a color = this list + a providers.tf alias block + a flux.tf module block + a dns.tf data source/map entry (the four non-for_each edits), applied in the documented order."
  type        = list(string)
  default     = ["blue"]
}

variable "active_color" {
  description = "The color the live.<domain> swing record points at. Managed demos all CNAME `live`, so flipping this and re-applying cuts every demo over to the other color (P6). Must be a member of var.colors."
  type        = string
  default     = "blue"

  validation {
    condition     = contains(var.colors, var.active_color)
    error_message = "active_color must be one of the colors in var.colors."
  }
}

variable "kubernetes_version" {
  description = "EKS control-plane version for all colors. Defaults to the latest GA — a fresh stack is the upgrade vehicle (stand up latest, validate, roll to it)."
  type        = string
  default     = "1.36"
}

variable "flux_chart_version" {
  description = "fluxcd-community/flux2 Helm chart version installed on each cluster (P3). Latest-GA per the upgrade-vehicle doctrine (appVersion Flux 2.9.1); pin exactly, never float."
  type        = string
  default     = "2.19.0"
}

variable "flux_git_branch" {
  description = "Branch of var.github_repo each cluster's Flux root sync tracks. Flipped to main after the P7 squash-merge (#13); the feature branch is retired."
  type        = string
  default     = "main"
}

variable "single_nat_gateway" {
  description = "One shared NAT gateway instead of one per AZ. Defaults to per-AZ HA (matches the pet); set true to halve NAT baseline cost on the shared demo VPC."
  type        = bool
  default     = false
}

variable "endpoint_public_access_cidrs" {
  description = "CIDRs allowed to reach the clusters' public API endpoints. Open by default for frictionless bring-up (auth is still IAM/access-entry gated); tighten to an operator allowlist via tfvars as a hardening step."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "cluster_admin_principal_arns" {
  description = "IAM principals granted durable cluster-admin on every color, on top of the bootstrap cluster-creator. Set a stable break-glass/CI role ARN here so kubectl admin survives SSO permission-set re-provisioning (the SSO role ARN churns)."
  type        = list(string)
  default     = []
}

variable "enable_secrets_encryption" {
  description = "Envelope-encrypt Kubernetes Secrets with a customer-managed KMS CMK per cluster. On by default; escape hatch if KMS setup ever blocks bring-up."
  type        = bool
  default     = true
}

variable "aws_account_id" {
  description = "AWS account the platform lives in. Guards against applying to the wrong account via allowed_account_ids on the aws provider (providers.tf), which errors at provider-config time."
  type        = string
  default     = "429214323166"
}

variable "name_prefix" {
  description = "Prefix for platform-owned AWS resources (Secrets Manager, ECR, CodeBuild, IAM). NB: the domain uses the longer 'tmprl-demo.cloud' spelling; resources use the abbreviated 'tmprl-dem-cld'."
  type        = string
  default     = "tmprl-dem-cld"
}

variable "domain" {
  description = "Apex domain for the demo environment. The zone itself is registered out of band (see migration plan)."
  type        = string
  default     = "tmprl-demo.cloud"
}

variable "route53_zone_id" {
  description = "Existing Route53 hosted zone id for var.domain. Tofu writes records into it but does not create the zone."
  type        = string
  default     = "Z08792403SH7ADUAH7X5X"
}

variable "github_repo" {
  description = "GitHub repo slug (org/name) the CI Tofu apply role trusts via GitHub OIDC. NB: this is the real GitHub repo, which differs from the local checkout dir name."
  type        = string
  default     = "temporal-sa/tmprl-demo-cloud-registry"
}

variable "ci_branch_refs" {
  description = "Git refs on var.github_repo allowed to assume the CI Tofu role. Least-privilege: exact branch subs, not a refs/heads/* wildcard."
  type        = list(string)
  default     = ["refs/heads/main", "refs/heads/feat/full-e2e-iac"]
}

variable "bootstrap_secret_names" {
  description = "Names of the pre-existing AWS Secrets Manager entries the operator reads. Tofu manages the containers (P2); the sensitive values are populated out of band."
  type = object({
    temporal_cloud_bootstrap = string
    github_readonly_pat      = string
    instruqt_cli             = string
  })
  default = {
    temporal_cloud_bootstrap = "tmprl-dem-cld/platform/temporal-cloud-bootstrap"
    github_readonly_pat      = "tmprl-dem-cld/platform/github-temporal-sa-readonly"
    instruqt_cli             = "tmprl-dem-cld/platform/instruqt-cli"
  }
}

variable "litellm_db_snapshot_identifier" {
  description = "RDS snapshot the shared LiteLLM Postgres was restored from (the P7 migration seed from the legacy temporal-instruqt-litellm DB). Created from it once; ignore_changes then keeps the instance stable and deletion_protection prevents replacement. Kept as the default so a routine `tofu apply` does NOT drop the live RDS (empty would set count=0)."
  type        = string
  default     = "litellm-migrate-blue-20260717"
}
