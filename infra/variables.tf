# Inputs for the platform stack. Every value defaults to the live
# tmprl-demo.cloud environment, so a fresh checkout plans without a tfvars file.
# A blue/green sibling cluster overrides only what differs (added in later
# phases: cluster color, node sizing, active_color).

variable "region" {
  description = "AWS region for all platform resources. Reuses the existing us-west-1 ACM cert, Route53 zone, Secrets Manager, and CodeBuild."
  type        = string
  default     = "us-west-1"
}

variable "aws_account_id" {
  description = "AWS account the platform lives in. Guards against applying to the wrong account (see the check block in main.tf)."
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
