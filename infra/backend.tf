terraform {
  # State bucket is created by infra/bootstrap/. Locking is native S3 lockfile
  # (no DynamoDB table).
  backend "s3" {
    bucket       = "tmprl-dem-cld-tofu-state-429214323166-us-west-1"
    key          = "platform/terraform.tfstate"
    region       = "us-west-1"
    encrypt      = true
    use_lockfile = true
  }
}
