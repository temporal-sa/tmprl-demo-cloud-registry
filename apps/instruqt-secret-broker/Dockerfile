# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.26.5

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-bookworm AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

WORKDIR /src

COPY go.mod ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY main.go ./
COPY internal ./internal

RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath \
      -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.buildDate=${BUILD_DATE}" \
      -o /out/secret-broker .

# This image is an artifact layer for COPY --from. It is intentionally not a
# runnable image: the workshop image supplies CA certificates, /root, and the
# rest of the operating-system filesystem used by secret-broker.
FROM scratch AS artifact

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

LABEL org.opencontainers.image.title="Temporal Instruqt secret broker" \
      org.opencontainers.image.description="Static workshop CLI for minting scoped runtime credentials" \
      org.opencontainers.image.source="https://github.com/temporal-sa/tmprl-demo-cloud-registry/tree/main/apps/instruqt-secret-broker" \
      org.opencontainers.image.version=${VERSION} \
      org.opencontainers.image.revision=${COMMIT} \
      org.opencontainers.image.created=${BUILD_DATE}

COPY --from=build /out/secret-broker /secret-broker
