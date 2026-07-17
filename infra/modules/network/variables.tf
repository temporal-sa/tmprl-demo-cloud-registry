# Shared VPC for the platform. One network hosts every cluster color
# (blue/green), so this module is instantiated exactly once by the root stack.
# It creates nothing cluster-specific — per-cluster subnet ownership tags live
# in the cluster module (they differ per color and must not fight over one
# subnet's tag set).

variable "name_prefix" {
  description = "Prefix for network resource names (e.g. tmprl-dem-cld -> tmprl-dem-cld-vpc)."
  type        = string
}

variable "region" {
  description = "AWS region, used to build the S3 gateway endpoint service name."
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR for the platform VPC. Deliberately distinct from the hand-built pet VPC (10.0.0.0/16) so both can coexist during the P7 migration."
  type        = string
  default     = "10.20.0.0/16"
}

variable "availability_zones" {
  description = "AZs to spread subnets across. us-west-1 only exposes us-west-1b/us-west-1c to this account, so that is the default (and the minimum EKS needs)."
  type        = list(string)
  default     = ["us-west-1b", "us-west-1c"]

  validation {
    condition     = length(var.availability_zones) >= 2
    error_message = "EKS requires subnets in at least two availability zones."
  }
}

variable "single_nat_gateway" {
  description = "If true, one shared NAT gateway serves all private subnets (cheaper, single AZ of failure). Default false = one NAT per AZ, matching the pet's HA topology."
  type        = bool
  default     = false
}
