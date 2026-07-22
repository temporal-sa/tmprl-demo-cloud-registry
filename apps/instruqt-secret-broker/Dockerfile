# syntax=docker/dockerfile:1.7

ARG ALPINE_VERSION=3.22

FROM alpine:${ALPINE_VERSION} AS verify

ARG TARGETARCH
ARG VERSION=main
ARG ARTIFACT_BASE_URL=https://tmprl-dem-cld-secret-broker-429214323166-us-west-1.s3.us-west-1.amazonaws.com/secret-broker
ARG SIGNING_KEY_SHA256=cf0310e7827f2b444a876b06768e8e11bef5b2ad9d5f58f93e45c27674b2b594

RUN apk add --no-cache ca-certificates curl openssl

# The CLI is compiled and KMS-signed before this image build starts. Download
# the target-platform artifact, anchor the public key to the fingerprint stored
# in this Dockerfile, and only copy a successfully verified binary into /out.
RUN set -eu; \
    case "${TARGETARCH}" in \
      amd64 | arm64) ;; \
      *) echo "Unsupported secret-broker architecture: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    artifact="secret-broker-linux-${TARGETARCH}"; \
    temporary_directory="$(mktemp -d)"; \
    download() { \
      curl --fail --silent --show-error --location \
        --retry 10 \
        --retry-all-errors \
        --retry-delay 2 \
        --retry-max-time 60 \
        --connect-timeout 10 \
        "$1" \
        --output "$2"; \
    }; \
    download \
      "${ARTIFACT_BASE_URL}/${VERSION}/${artifact}" \
      "${temporary_directory}/${artifact}"; \
    download \
      "${ARTIFACT_BASE_URL}/${VERSION}/${artifact}.sig" \
      "${temporary_directory}/${artifact}.sig"; \
    download \
      "${ARTIFACT_BASE_URL}/trust/v1/signing-key.pem" \
      "${temporary_directory}/signing-key.pem"; \
    actual_key_sha256="$( \
      openssl pkey \
        -pubin \
        -in "${temporary_directory}/signing-key.pem" \
        -outform DER \
        | sha256sum \
        | awk '{print $1}' \
    )"; \
    if [ "${actual_key_sha256}" != "${SIGNING_KEY_SHA256}" ]; then \
      echo "Secret-broker signing public key fingerprint mismatch" >&2; \
      exit 1; \
    fi; \
    openssl dgst -sha256 \
      -verify "${temporary_directory}/signing-key.pem" \
      -signature "${temporary_directory}/${artifact}.sig" \
      -sigopt rsa_padding_mode:pss \
      -sigopt rsa_pss_saltlen:32 \
      "${temporary_directory}/${artifact}"; \
    mkdir -p /out; \
    cp "${temporary_directory}/${artifact}" /out/secret-broker; \
    chmod 0755 /out/secret-broker; \
    rm -rf "${temporary_directory}"

# This image is an artifact layer for COPY --from. It is intentionally not a
# runnable image: the workshop image supplies CA certificates, /root, and the
# rest of the operating-system filesystem used by secret-broker.
FROM scratch AS artifact

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

LABEL org.opencontainers.image.title="Temporal Instruqt secret broker" \
      org.opencontainers.image.description="KMS-verified static workshop CLI for minting scoped runtime credentials" \
      org.opencontainers.image.source="https://github.com/temporal-sa/tmprl-demo-cloud-registry/tree/main/apps/instruqt-secret-broker" \
      org.opencontainers.image.version=${VERSION} \
      org.opencontainers.image.revision=${COMMIT} \
      org.opencontainers.image.created=${BUILD_DATE}

COPY --from=verify /out/secret-broker /secret-broker
