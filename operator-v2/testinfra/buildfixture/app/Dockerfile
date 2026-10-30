# Trusted preload binds this immutable config ID to the reviewed official
# Go1.27.3 linux/arm64 manifest. Native FROM-config-ID acceptance is a gate.
FROM sha256:dba766c2797d15d2b6e35651bd5acb02764b27611e9b89bba1100a54a0167bf7 AS compile
WORKDIR /src
COPY main.go /src/main.go
RUN CGO_ENABLED=0 GOOS=linux GOARCH=arm64 GOMAXPROCS=2 GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local /usr/local/go/bin/go build -p=1 -trimpath -buildvcs=false -o /out/app /src/main.go
FROM scratch
COPY --from=compile /out/app /app
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/app"]
