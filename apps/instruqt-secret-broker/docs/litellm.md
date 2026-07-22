# How to Generate a LiteLLM Secret

## Required Instruqt Secret

### Add this secret to the Instruqt track configuration:

| Instruqt secret | Required | Purpose |
| --- | --- | --- |
| `TEMPORAL_LITELLM_BROKER_SECRET` | Yes | HMAC shared secret used to authenticate token mint requests to the broker |


### Example `instruqt/config.yml` entry:

```yaml
secrets:
  - name: TEMPORAL_LITELLM_BROKER_SECRET
```

The secret value is injected by Instruqt during the setup lifecycle. It is only accessible during setup and should not be written permanently at any point.

## Required Track Identity

The CLI requires a track identity from one of these environment variables:

| Environment variable | Purpose |
| --- | --- |
| `INSTRUQT_LITELLM_TRACK_ID` | Optional explicit override for the broker track ID |
| `INSTRUQT_TRACK_SLUG` | Instruqt-provided track slug, used when no explicit override is set |

Instruqt injects `INSTRUQT_TRACK_SLUG` into lifecycle scripts automatically, so
the override is only needed when a different audit and grant identity is
desired. The command fails without making a broker request if neither value is
set.

## Available Flags

Use these flags when minting a LiteLLM secret:

```bash
secret-broker litellm --duration=1d --budget=5
```

| Flag | Required | Purpose |
| --- | --- | --- |
| `--duration` | Yes | LiteLLM key lifetime as a positive integer followed by `s`, `m`, `h`, or `d`, for example `1d`. |
| `--budget` | Yes | Positive maximum spend budget for the minted LiteLLM virtual key. |
| `--model` | No | Allowed model or pattern. Repeat the flag for multiple values. If omitted, the key may use any model exposed by the gateway. |
| `--default-model` | No | Default model returned to workshop clients. |
| `--budget-duration` | No | Optional budget reset duration using the same format as `--duration`. |
| `--rpm-limit` | No | Positive requests-per-minute limit. |
| `--tpm-limit` | No | Positive tokens-per-minute limit. |
| `--max-parallel-requests` | No | Positive concurrent request limit. |

For example, to restrict a key to Anthropic and OpenAI models:

```bash
secret-broker litellm \
  --duration=1d \
  --budget=5 \
  --model='anthropic/*' \
  --model='openai/*' \
  --default-model='anthropic/claude-sonnet-4-5'
```

## Environment Variables Written

The LiteLLM provider writes this standardized set:

| Environment variable | Value |
| --- | --- |
| `OPENAI_API_KEY` | Minted LiteLLM virtual key |
| `OPENAI_BASE_URL` | Broker response `litellm_base_url`, trimmed of trailing slash |
| `LITELLM_BASE_URL` | Same value as `OPENAI_BASE_URL` |
| `OPENAI_MODEL` | `OPENAI_MODEL` input if set; otherwise broker `default_model` without provider prefix; otherwise `gpt-4o` |
| `OPENAI_AGENTS_DISABLE_TRACING` | `1` |
| `SPRING_AI_OPENAI_BASE_URL` | Same value as `OPENAI_BASE_URL` |
| `SPRING_AI_OPENAI_CHAT_BASE_URL` | Same value as `OPENAI_BASE_URL` |

## Dockerfile Integration

The recommended integration copies the static binary from a versioned artifact
image into the workshop image:

```dockerfile
FROM public.ecr.aws/s1u1b8l5/tmprl-dem-cld/instruqt-secret-broker:<version> AS secret-broker

FROM ubuntu:24.04

COPY --from=secret-broker /secret-broker /usr/local/bin/secret-broker
```

See [Distributing the Secret Broker to Instruqt Tracks](./instruqt-distribution.md)
for building the artifact layer, compiling directly from this repository during
a workshop-image build, and the tradeoffs of setup-script installation and
sandbox presets.

## Instruqt Integration

In your Instruqt setup script, add this code:

```bash
secret-broker litellm --duration=1d --budget=5

# optionally source the environment variables into the setup script for immediate use
. /root/.litellm-env
```

> secret-broker persists the values to /root/.litellm-env, updates /etc/environment, and ensures future root shells source /root/.litellm-env from /root/.bashrc. Already-running processes, and later commands in this same setup script, will not receive those variables unless the env file is sourced.
