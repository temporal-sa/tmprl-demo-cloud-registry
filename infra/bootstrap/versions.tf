terraform {
  # 1.10+ is required for native S3 state locking (use_lockfile), which lets us
  # avoid a separate DynamoDB lock table.
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}
