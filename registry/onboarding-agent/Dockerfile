# Catalog onboarding agent. Built as the registry-operator project's
# onboarding-agent build; the operator runs it as one Kubernetes Job per agent
# request (activities/onboarding_agent.py). The same image runs the pod's
# firewall init container, so it carries iptables.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash ca-certificates git iptables \
    && rm -rf /var/lib/apt/lists/*

COPY registry/onboarding-agent/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /agent
COPY registry/onboarding-agent/agent.py /agent/agent.py
COPY registry/onboarding-agent/context /agent/context

USER 65532:65532
CMD ["python", "/agent/agent.py"]
