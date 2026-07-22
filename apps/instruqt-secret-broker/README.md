# Instruqt secret-broker CLI

Small static CLI for minting workshop runtime secrets from the Instruqt secret
broker.

## Usage

```bash
secret-broker {secretProvider} {flags}
```

Example:

```bash
secret-broker litellm --duration=1d --budget=5
```

The CLI reads the lifecycle secrets injected by Instruqt as environment variables, and then converts these into the requested standardized (scoped) secrets.
These secrets are written to the environment automatically.

Example, litellm writes to:

- `/root/.litellm-env`
- `/etc/environment`
- `/root/.bashrc`, by adding a source block for `/root/.litellm-env`

## Standardized Secrets

| Provider | Created secret bundle | Primary use | How to Use |
| --- | --- | --- | --- |
| `litellm` | LiteLLM virtual key, LiteLLM base URL, default model | OpenAI or Anthropic SDK access through the workshop LiteLLM gateway | [litellm.md](./docs/litellm.md) |

## Instruqt Distribution

See [instruqt-distribution.md](./docs/instruqt-distribution.md) for the
public multi-platform container image, checksum-verified Linux CLI downloads,
direct source-build option, and Instruqt sandbox preset tradeoffs.

A complete course-image example is available in
[example/](./example/). It copies the CLI from the public artifact image and
invokes it from an Instruqt setup lifecycle script.

The published artifacts contain no credential material. Linux binaries are
signed by a Tofu-managed asymmetric AWS KMS key and immediately verified by the
publishing workflow. The artifact image packages that same target-platform
binary without recompiling or downloading it. The HMAC credential is read only
at runtime from `TEMPORAL_LITELLM_BROKER_SECRET`; the binary cannot mint a token
without a valid credential.
