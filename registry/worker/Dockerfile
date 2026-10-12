FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

# Pin the CLI so release updates invalidate the Docker layer predictably. The
# worker self-updates a copy at runtime if Instruqt later requires a newer CLI.
ARG INSTRUQT_CLI_VERSION=2427-422d0f4
ARG INSTRUQT_CLI_SHA256=6bcef7c37b82fce83653c620d1eb1d11577e0d5b962fcc34ebb4358e61220735

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
# The onboarding workflow validates drafts with the same validator and schemas
# CI runs, at the operator's own commit. The validator resolves schemas from its
# parent directory, so it lands in /app/src and the schemas in /app/schemas.
COPY schemas /app/schemas
COPY scripts/validate_projects.py /app/src/validate_projects.py
# The onboarding agent's grounding also includes the projects README.
COPY projects/README.md /app/projects/README.md

CMD ["python", "-m", "demo_registry_operator.worker"]
