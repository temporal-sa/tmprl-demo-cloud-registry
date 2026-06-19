FROM golang:1.25-alpine AS build

WORKDIR /src

COPY registry/temporal-cloud-proxy/go.mod registry/temporal-cloud-proxy/go.sum ./
RUN go mod download

COPY registry/temporal-cloud-proxy/ ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/temporal-cloud-proxy ./cmd/temporal-cloud-proxy

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/temporal-cloud-proxy /temporal-cloud-proxy

USER nonroot:nonroot
ENTRYPOINT ["/temporal-cloud-proxy"]
