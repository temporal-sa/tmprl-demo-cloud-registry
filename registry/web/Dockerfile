# Stage 1: compile Tailwind CSS for the SPA shell.
FROM node:20-alpine AS web-build
WORKDIR /work
COPY registry/web/package.json ./package.json
RUN npm install --no-audit --no-fund --silent
COPY registry/web/src ./src
COPY registry/web/static ./static
RUN npx --no-install @tailwindcss/cli -i src/styles.css -o static/app.css --minify

# Stage 2: Python runtime serving the SPA + JSON catalog.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    REGISTRY_DATA_PATH=/app/data/registry.json \
    CATALOG_STATUS_DIR=/app/status \
    PORT=8080

WORKDIR /app

COPY registry/web/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY registry/web/server.py /app/server.py
COPY --from=web-build /work/static /app/static
# The catalog is published at runtime by the registry operator into a ConfigMap
# mounted at /app/data. Ship an empty fallback so the image builds and serves
# before the first catalog publish (or in local runs without the mount).
RUN mkdir -p /app/data && printf '{"baseDomain":"tmprl-demo.cloud","projects":[]}' > /app/data/registry.json

EXPOSE 8080

CMD ["python", "/app/server.py"]
