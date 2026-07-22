# syntax=docker/dockerfile:1.7

# The workflow builds each target binary once, signs and verifies it, then makes
# dist/ available as this build context. The image packages that exact verified
# binary rather than compiling it again or downloading it through S3.
FROM scratch AS artifact

ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

LABEL org.opencontainers.image.title="Temporal Instruqt secret broker" \
      org.opencontainers.image.description="Static workshop CLI for minting scoped runtime credentials" \
      org.opencontainers.image.source="https://github.com/temporal-sa/tmprl-demo-cloud-registry/tree/main/apps/instruqt-secret-broker" \
      org.opencontainers.image.version=${VERSION} \
      org.opencontainers.image.revision=${COMMIT} \
      org.opencontainers.image.created=${BUILD_DATE}

COPY --chmod=0755 dist/secret-broker-linux-${TARGETARCH} /secret-broker
