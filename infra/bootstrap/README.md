# infra/bootstrap — OpenTofu state backend

One-time module that creates the single S3 bucket holding **all** OpenTofu
state for this platform:

- `tmprl-dem-cld-tofu-state-429214323166-us-west-1` (us-west-1)
- versioning **enabled**, SSE-S3 encryption, public access fully blocked,
  noncurrent versions expired after 90 days.

Locking uses OpenTofu's native S3 lockfile (`use_lockfile = true` in each
stack's `backend "s3"` block), so there is **no DynamoDB table**.

This is a chicken-and-egg step: the bucket must exist before any stack can use
it as a backend. So this module is applied **first with local state**, then its
own state is migrated into the bucket it just created.

## First-time run

```sh
tofu -chdir=infra/bootstrap init
tofu -chdir=infra/bootstrap apply      # creates the bucket (local state)
```

Then migrate this module's state into the new bucket:

```sh
# backend.tf (committed) points at the bucket under key bootstrap/terraform.tfstate
tofu -chdir=infra/bootstrap init -migrate-state
```

After that, `infra/` (the root platform stack) can `tofu init` against the same
bucket.

## Notes

- The bucket has `prevent_destroy = true`; it cannot be removed by `tofu
  destroy`. That is deliberate — deleting it would orphan every stack's state.
- Re-running `apply` here is safe and idempotent. If the local state is ever
  lost before migration, re-import with
  `tofu import aws_s3_bucket.state tmprl-dem-cld-tofu-state-429214323166-us-west-1`.
