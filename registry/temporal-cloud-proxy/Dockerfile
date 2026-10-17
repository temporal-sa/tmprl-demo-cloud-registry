# Official Temporal Proxy (https://github.com/temporalio/temporal-proxy) with
# the sidecar config baked in. Upgrade by bumping the tag below.
FROM temporalio/temporal-proxy:v1.0.0

COPY registry/temporal-cloud-proxy/config.yaml /etc/proxy/config.yaml

# Upstream runs as the named user "proxy"; a numeric UID lets runAsNonRoot verify it.
USER 1000:1000

CMD ["serve", "--config", "/etc/proxy/config.yaml"]
