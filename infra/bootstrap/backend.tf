terraform {
  # This module's own state lives in the bucket it created. Added after the
  # first (local-state) apply, then: tofu init -migrate-state
  backend "s3" {
    bucket       = "tmprl-dem-cld-tofu-state-429214323166-us-west-1"
    key          = "bootstrap/terraform.tfstate"
    region       = "us-west-1"
    encrypt      = true
    use_lockfile = true
  }
}
