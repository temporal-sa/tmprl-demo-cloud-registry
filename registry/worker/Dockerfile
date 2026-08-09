FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

# Pin the CLI so release updates invalidate the Docker layer predictably.
ARG INSTRUQT_CLI_VERSION=2380-865ec91
ARG INSTRUQT_CLI_SHA256=6b0f67a39fac4b202782ce3730fd6c2aab34dd8bf165adb44436febfdbb19942

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl git unzip \
    && curl -fsSL "https://github.com/instruqt/cli/releases/download/${INSTRUQT_CLI_VERSION}/instruqt-linux.zip" -o /tmp/instruqt.zip \
    && echo "${INSTRUQT_CLI_SHA256}  /tmp/instruqt.zip" | sha256sum -c - \
    && unzip /tmp/instruqt.zip -d /tmp/instruqt-cli \
    && install -m 0755 /tmp/instruqt-cli/instruqt /usr/local/bin/instruqt \
    && rm -rf /var/lib/apt/lists/* /tmp/instruqt.zip /tmp/instruqt-cli

COPY registry/worker/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY registry/worker/src /app/src

CMD ["python", "-m", "demo_registry_operator.worker"]
