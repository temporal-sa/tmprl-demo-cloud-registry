# One-time bootstrap: creates the S3 bucket that holds OpenTofu state for the
# whole platform. Run this once (see README.md). Everything else under infra/
# uses this bucket as its S3 backend; this module stores its own state here too
# (backend.tf, added after the first apply).

resource "aws_s3_bucket" "state" {
  bucket = var.state_bucket_name

  # This bucket holds the state for every other stack. A stray `tofu destroy`
  # here would orphan all of it, so refuse to delete the bucket.
  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_versioning" "state" {
  bucket = aws_s3_bucket.state.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "state" {
  bucket = aws_s3_bucket.state.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "state" {
  bucket                  = aws_s3_bucket.state.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "state" {
  # Noncurrent-version rules only make sense once versioning is on.
  depends_on = [aws_s3_bucket_versioning.state]

  bucket = aws_s3_bucket.state.id

  rule {
    id     = "expire-noncurrent-state-versions"
    status = "Enabled"

    filter {} # apply to all objects

    noncurrent_version_expiration {
      noncurrent_days = 90
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

output "state_bucket" {
  description = "Name of the S3 bucket that stores OpenTofu state."
  value       = aws_s3_bucket.state.id
}
