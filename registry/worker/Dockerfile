FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl git unzip \
    && curl -fsSL https://github.com/instruqt/cli/releases/latest/download/instruqt-linux.zip -o /tmp/instruqt.zip \
    && unzip /tmp/instruqt.zip -d /tmp/instruqt-cli \
    && install -m 0755 /tmp/instruqt-cli/instruqt /usr/local/bin/instruqt \
    && rm -rf /var/lib/apt/lists/* /tmp/instruqt.zip /tmp/instruqt-cli

COPY registry/worker/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY registry/worker/src /app/src

CMD ["python", "-m", "demo_registry_operator.worker"]
