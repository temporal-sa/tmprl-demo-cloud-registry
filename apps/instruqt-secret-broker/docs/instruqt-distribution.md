# Distributing the Secret Broker to Instruqt Tracks

The monorepo publishes the secret-broker as both a multi-platform OCI image and
standalone Linux binaries. Both forms contain only the CLI; callers provide the
broker credential at runtime through Instruqt.

| Artifact | Location | Architectures |
| --- | --- | --- |
| Container artifact | `public.ecr.aws/s1u1b8l5/tmprl-dem-cld/instruqt-secret-broker` | `linux/amd64`, `linux/arm64` |
| Native CLI | `https://tmprl-dem-cld-secret-broker-429214323166-us-west-1.s3.us-west-1.amazonaws.com/secret-broker` | Linux `amd64`, Linux `arm64` |

Every `main` build publishes a moving `main` reference and an immutable
`main-YYYYMMDDHHMMSS-<git-sha>` version. Pin the immutable version or image
digest in durable workshop configuration. The moving reference is useful for
testing and deliberate rolling upgrades.

Each Linux binary has an adjacent `.sig` file produced by an asymmetric AWS KMS
key using RSA-PSS with SHA-256. The stable public key is published at
`trust/v1/signing-key.pem`; consumers must pin its SPKI SHA-256 fingerprint,
`cf0310e7827f2b444a876b06768e8e11bef5b2ad9d5f58f93e45c27674b2b594`,
rather than trusting an arbitrary key downloaded from the artifact bucket.

Instruqt injects `INSTRUQT_TRACK_ID`, `INSTRUQT_TRACK_SLUG`, and
`INSTRUQT_PARTICIPANT_ID` into lifecycle scripts. Calling `secret-broker` from
the track setup script therefore supplies the required track and participant
identity without putting those values into an image.

## Option 1: Copy from the Public Artifact Image

Use the public image as a build stage and copy its static binary into a workshop
image:

```dockerfile
FROM public.ecr.aws/s1u1b8l5/tmprl-dem-cld/instruqt-secret-broker:<version> AS secret-broker

FROM ubuntu:24.04

COPY --from=secret-broker /secret-broker /usr/local/bin/secret-broker
```

Docker automatically selects the `amd64` or `arm64` image from the manifest
list for the target platform. The workflow builds each target binary once,
signs it, verifies it, and passes that same file into the artifact-image build.
The image does not compile the CLI again or download it back through S3.

## Option 2: Install from a Setup Script

This path adds the CLI to an existing sandbox without changing its container
image. The installer detects `amd64` or `arm64`, downloads the matching binary,
verifies its SHA-256 checksum and KMS signature against the pinned public key,
and installs it at
`/usr/local/bin/secret-broker` by default.

For a convenient moving install:

```bash
curl --fail --silent --show-error --location \
  https://tmprl-dem-cld-secret-broker-429214323166-us-west-1.s3.us-west-1.amazonaws.com/secret-broker/main/install.sh \
  --output /tmp/install-secret-broker.sh
sh /tmp/install-secret-broker.sh
```

For reproducible workshops, pin the same immutable version in both places:

```bash
version='main-YYYYMMDDHHMMSS-<git-sha>'
base='https://tmprl-dem-cld-secret-broker-429214323166-us-west-1.s3.us-west-1.amazonaws.com/secret-broker'

curl --fail --silent --show-error --location \
  "${base}/${version}/install.sh" \
  --output /tmp/install-secret-broker.sh
SECRET_BROKER_VERSION="${version}" sh /tmp/install-secret-broker.sh
```

The host needs `curl`, `openssl`, `sha256sum`, `awk`, and `install`, which are
present in the standard Ubuntu-based Instruqt images.

## Option 3: Build Directly from the Monorepo

An existing workshop-image pipeline can compile the CLI from this internal
repository. Pin a full commit SHA and provide a GitHub token with repository
read access as a BuildKit secret.

```dockerfile
# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.26.5

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-bookworm AS secret-broker-build

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src
COPY --from=secret-broker-source apps/instruqt-secret-broker/go.mod ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY --from=secret-broker-source apps/instruqt-secret-broker/ ./
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/secret-broker .

FROM ubuntu:24.04
COPY --from=secret-broker-build /out/secret-broker /usr/local/bin/secret-broker
```

```bash
GIT_AUTH_TOKEN="$(gh auth token)" \
docker buildx build \
  --platform linux/amd64 \
  --secret id=GIT_AUTH_TOKEN \
  --build-context secret-broker-source='https://github.com/temporal-sa/tmprl-demo-cloud-registry.git#<full-commit-sha>' \
  --tag <registry>/<workshop-image>:<version> \
  --push \
  .
```

## Security Boundary

Publishing the CLI does not publish a usable credential. It embeds the public
broker URL and a credential key identifier, but reads the HMAC secret only from
`TEMPORAL_LITELLM_BROKER_SECRET` at runtime. The broker must continue to treat
the client and signing protocol as public information and authorize every mint
request from the supplied credential.

The KMS signing key protects consumers from a binary or public-key substitution
in the artifact bucket: the private key never leaves KMS, and the fingerprint
trusted by the publishing workflow and installer is stored in source control
rather than accepted from S3. A signature proves that the artifact passed
through the authorized signing identity; it does not prevent rollback to an
older signed release or a malicious source change that the authorized workflow
signs.

After a successful mint, the CLI intentionally handles and persists the
short-lived LiteLLM key in the sandbox environment files documented in
[litellm.md](./litellm.md). Protect the running sandbox accordingly. Prefer
immutable artifact versions, retain checksum verification, and never add a
credential through Go linker flags, Docker build arguments, or image layers.

## Sandbox Presets

An Instruqt sandbox preset can share hosts, scripts, and secrets across tracks,
but presets are whole-sandbox configurations rather than additive dependencies.
Publishing the native CLI avoids coupling unrelated tracks to one preset solely
for tool distribution.
