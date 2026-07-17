# infra/bootstrap — OpenTofu state backend

One-time module that creates the single S3 bucket holding **all** OpenTofu
state for this platform:

- `tmprl-dem-cld-tofu-state-429214323166-us-west-1` (us-west-1)
- versioning **enabled**, SSE-S3 encryption, public access fully blocked,
  TLS-only bucket policy, noncurrent versions expired after 90 days.

Locking uses OpenTofu's native S3 lockfile (`use_lockfile = true` in each
stack's `backend "s3"` block), so there is **no DynamoDB table**.

**The live environment is already bootstrapped** — the bucket exists and this
module's own state already lives in it (`bootstrap/terraform.tfstate`). You only
run the brand-new-environment steps below for a new account or a
disaster-recovery rebuild.

## Standing up a brand-new environment

Chicken-and-egg: the bucket must exist before it can be a backend, but the
committed `backend.tf` points at that not-yet-existing bucket, so a plain `tofu
init` would fail ("S3 bucket does not exist"). Create the bucket with local
state first, then migrate this module's state into it:

```sh
cd infra/bootstrap

# 1. Create the bucket with LOCAL state (set the S3 backend aside first).
mv backend.tf backend.tf.tmp
tofu init
tofu apply                       # creates the bucket

# 2. Migrate this module's state into the bucket it just created.
mv backend.tf.tmp backend.tf
tofu init -migrate-state
```

Afterwards the root platform stack in `infra/` inits normally — the bucket
exists by then, so no dance is needed there.

## Day-to-day (bucket already exists)

```sh
tofu -chdir=infra/bootstrap init     # configures the S3 backend
tofu -chdir=infra/bootstrap plan
```

## Notes

- The bucket has `prevent_destroy = true`; it cannot be removed by `tofu
  destroy`. Deleting it would orphan every stack's state.
- Re-running `apply` here is safe and idempotent.
- If this module's state is lost but the bucket still exists, re-adopt it with
  `backend.tf` in place (the bucket exists, so the S3 backend inits fine):
  `tofu -chdir=infra/bootstrap init` then `tofu -chdir=infra/bootstrap import
  aws_s3_bucket.state tmprl-dem-cld-tofu-state-429214323166-us-west-1` (import
  the other sub-resources similarly).
