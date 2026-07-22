# tmprl-demo-cloud-instruqt-broker

LiteLLM broker for Instruqt workshop token minting.

Any caller with a valid broker HMAC credential may mint a learner-scoped
LiteLLM virtual key. Tracks do not need to be registered with the broker.

## Minting Contract

`POST /v1/token` requires a signed JSON body containing:

```json
{
  "track_id": "my-workshop",
  "participant_id": "sandbox-or-participant-id",
  "max_budget": 5.0,
  "duration": "2h"
}
```

`max_budget` must be positive. `duration` must be a positive integer followed
by `s`, `m`, `h`, or `d`. Both fields are required.

`models` is optional. When it is omitted, the broker omits LiteLLM's model
restriction and the key can use any model exposed by the gateway. A caller can
restrict the key by providing a non-empty list:

```json
{
  "track_id": "my-workshop",
  "participant_id": "sandbox-or-participant-id",
  "max_budget": 5.0,
  "duration": "2h",
  "models": ["anthropic/*", "openai/*"],
  "default_model": "anthropic/claude-sonnet-4-5"
}
```

Optional per-key controls are `budget_duration`, `rpm_limit`, `tpm_limit`,
and `max_parallel_requests`. `track_id` remains audit and idempotency context;
it is not an authorization allowlist.

The gateway's server-side `upperbound_key_generate_params` remain the final
safety ceiling even though callers choose their requested budget and duration.

Grant identity includes the authenticated HMAC key id, track id, and participant
id. Repeating a request returns the existing key only when the requested policy
matches the stored policy; a conflicting policy returns HTTP 409.

## Deployment Flow

The broker source and Kubernetes manifests live together in
`apps/instruqt-litellm`. Merges to `main` that touch broker source or build
files build and push an ECR image under both a moving `main` tag and an
immutable audit tag:

```text
main-YYYYMMDDHHMMSS-<git-sha>
```

The same GitHub Actions job resolves the pushed image digest and commits
`main@sha256:...` to `deploy/broker.yaml`. Flux remains read-only: it notices the
Git commit and reconciles the digest-pinned deployment. Changes limited to the
deployment manifest do not retrigger the image build.

## AWS Auth

GitHub Actions uses OIDC, not stored AWS credentials.

Role:

```text
arn:aws:iam::429214323166:role/tmprl-dem-cld-instruqt-broker-github-actions
```

ECR repository:

```text
429214323166.dkr.ecr.us-west-1.amazonaws.com/temporal-instruqt-litellm-broker
```

The role, ECR repository, and broker DynamoDB table are adopted and managed by
OpenTofu in `infra/litellm_delivery.tf`.
