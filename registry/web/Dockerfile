FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    REGISTRY_DATA_PATH=/app/data/registry.json \
    CATALOG_STATUS_DIR=/app/status \
    PORT=8080

WORKDIR /app

COPY registry/web/server.py /app/server.py
COPY registry/web/static /app/static
# The catalog is published at runtime by the registry operator into a ConfigMap
# mounted at /app/data. Ship an empty fallback so the image builds and serves
# before the first catalog publish (or in local runs without the mount).
RUN mkdir -p /app/data && printf '{"baseDomain":"tmprl-demo.cloud","projects":[]}' > /app/data/registry.json

EXPOSE 8080

CMD ["python", "/app/server.py"]
