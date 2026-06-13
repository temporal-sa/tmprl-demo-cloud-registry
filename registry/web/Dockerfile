FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    REGISTRY_DATA_PATH=/app/data/registry.json \
    CATALOG_STATUS_DIR=/app/status \
    PORT=8080

WORKDIR /app

COPY registry/web/server.py /app/server.py
COPY registry/web/static /app/static
COPY clusters/sa-demo/generated/registry.json /app/data/registry.json

EXPOSE 8080

CMD ["python", "/app/server.py"]
