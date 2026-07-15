variable "region" {
  description = "AWS region for the OpenTofu state bucket. Must match the platform region (us-west-1)."
  type        = string
  default     = "us-west-1"
}

variable "aws_account_id" {
  description = "AWS account the state bucket must live in. Guards against a wrong-account apply (see providers.tf)."
  type        = string
  default     = "429214323166"
}

variable "state_bucket_name" {
  description = "Globally-unique S3 bucket that stores all OpenTofu state for this platform."
  type        = string
  default     = "tmprl-dem-cld-tofu-state-429214323166-us-west-1"
}
