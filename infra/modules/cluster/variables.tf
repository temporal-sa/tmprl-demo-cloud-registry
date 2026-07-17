# One EKS cluster of a given color, on the shared platform VPC. Instantiated
# once per color by the root stack (blue today, green added at cutover). Every
# AWS name is derived from var.name so two colors never collide.

variable "name" {
  description = "Full cluster name, e.g. tmprl-dem-cld-blue. All cluster-owned resources derive their names from this."
  type        = string
}

variable "color" {
  description = "Cluster color (blue/green). Recorded as a tag; routing/standby logic keys off it in later phases."
  type        = string
}

variable "region" {
  description = "AWS region (used to render the emitted kubeconfig's get-token call in the root stack)."
  type        = string
}

variable "kubernetes_version" {
  description = "EKS control-plane minor version. Defaults to the latest GA: a fresh stack is the upgrade vehicle (stand up latest, validate, roll to it), never an in-place bump of the pet."
  type        = string
  default     = "1.36"
}

variable "vpc_id" {
  description = "Shared VPC id from the network module."
  type        = string
}

variable "private_subnets" {
  description = "Private subnets as an AZ-keyed map (az => id). Nodes run here; the control plane also gets ENIs here. AZ-keyed so per-subnet tags can for_each on static keys."
  type        = map(string)
}

variable "public_subnets" {
  description = "Public subnets as an AZ-keyed map (az => id). Tagged for this cluster so internet-facing LB auto-discovery works; the control plane also gets ENIs here for AZ reach."
  type        = map(string)
}

variable "endpoint_public_access_cidrs" {
  description = "CIDRs allowed to reach the public API endpoint. Defaults open (auth is still IAM/access-entry gated); tighten to an operator allowlist as a hardening step."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "enabled_cluster_log_types" {
  description = "Control-plane log types shipped to CloudWatch. Improves on the pet (which had logging off)."
  type        = list(string)
  default     = ["api", "audit", "authenticator"]
}

variable "log_retention_days" {
  description = "Retention for the control-plane CloudWatch log group. Tofu owns the group so cluster teardown removes it and re-create never name-collides."
  type        = number
  default     = 30
}

variable "cluster_admin_principal_arns" {
  description = "IAM principals granted cluster-admin via EKS access entries, on top of the bootstrap cluster-creator. Empty for P1 (the SSO admin that runs apply already gets creator admin); add a durable break-glass/CI principal here so admin survives SSO permission-set re-provisioning (the SSO role ARN churns)."
  type        = list(string)
  default     = []
}

variable "enable_secrets_encryption" {
  description = "Envelope-encrypt Kubernetes Secrets with a customer-managed KMS CMK. On by default (best done at creation); an escape hatch if KMS setup ever blocks bring-up."
  type        = bool
  default     = true
}

variable "node_instance_types" {
  description = "Managed node group instance types."
  type        = list(string)
  default     = ["t3.large"]
}

variable "node_min_size" {
  description = "Minimum managed node count (matches the pet's floor)."
  type        = number
  default     = 3
}

variable "node_desired_size" {
  description = "Desired managed node count at creation (matches the pet's steady state)."
  type        = number
  default     = 4
}

variable "node_max_size" {
  description = "Maximum managed node count (cluster-autoscaler ceiling; matches the pet)."
  type        = number
  default     = 10
}

variable "node_disk_size" {
  description = "Managed node root volume size in GiB."
  type        = number
  default     = 60
}

variable "node_ami_type" {
  description = "Managed node AMI type. AL2023 (the pet is on end-of-life AL2)."
  type        = string
  default     = "AL2023_x86_64_STANDARD"
}
