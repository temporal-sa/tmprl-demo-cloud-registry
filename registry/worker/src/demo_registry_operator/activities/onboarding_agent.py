"""Runs the catalog onboarding agent as one Kubernetes Job per request.

``run_onboarding_agent`` is the only activity. For a new run it:

1. mints a LiteLLM key through the Instruqt broker: $2, one hour, Anthropic
   models only. The operator signs with the broker's existing ``v1``
   credential, read from the ``onboarding-litellm-broker`` secret (only the
   ``BROKER_HMAC_SECRETS_JSON`` property of the broker's secret).
2. creates the Job in its own namespace, and a Secret owned by the Job that
   carries the key and the request: the draft, plus the catalog contract
   (schema, validator, projects/README.md, example projects) from this
   operator's own image and the cluster.
3. follows the pod's log for ``@@progress`` lines, reports them as heartbeat
   details, and reads the ``@@result`` lines when the Job succeeds. If the
   Job still can't create its pod after a minute (a quota or admission
   rejection), the run fails with the Job's ``FailedCreate`` reason.

The key never leaves this activity except into the Secret, so it is not in
workflow history. The Job is named after the run, so a retried attempt
reattaches to a Job that is still running. Cancelling deletes the Job; the
Secret goes with it. Keys are revoked when the run ends.

The pod is the sandbox: no service-account token, no cloud or Temporal
credentials, non-root, read-only root filesystem, and the isolated-executor
firewall that drops traffic to private networks.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import yaml
from temporalio import activity
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError, CancelledError

from demo_registry_operator import (
    errors,
    isolated_executor,
    kube,
    project_validation,
    resources,
)
from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.workflows.catalog_onboarding_messages import (
    AgentField,
    AgentFinding,
    AgentRunRequest,
    AgentRunResult,
)

AGENT_IMAGE_ENV = resources.ONBOARDING_AGENT_IMAGE_ENV
AGENT_NAMESPACE_ENV = "DEMO_REGISTRY_ONBOARDING_AGENT_NAMESPACE"
DEFAULT_AGENT_NAMESPACE = "tmprl-dem-cld-onboarding-agents"
BROKER_URL_ENV = "DEMO_REGISTRY_LITELLM_BROKER_URL"
DEFAULT_BROKER_URL = "https://litellm-broker-instruqt.tmprl-demo.cloud"
BROKER_SECRET_NAME = "onboarding-litellm-broker"
BROKER_SECRET_KEY = "BROKER_HMAC_SECRETS_JSON"
BROKER_KEY_ID = "v1"
BROKER_TRACK_ID = "registry-onboarding"
KEY_POLICY = {
    "max_budget": 2.0,
    "duration": "1h",
    # The SDK picks its own models and sends bare claude-* names; the gateway
    # routes those to anthropic/claude-*.
    "models": ["claude-*", "anthropic/*"],
    "max_parallel_requests": 4,
}
JOB_DEADLINE_SECONDS = 25 * 60
JOB_TTL_SECONDS = 10 * 60
POLL_SECONDS = 5
# The Job controller reports a rejected pod (quota, admission) within seconds
# and then keeps retrying; give it this long before failing the run on it.
POD_CREATE_GRACE_SECONDS = 60
AGENT_CONTAINER = "agent"
# Deployed projects the agent copies patterns from, one per feature.
EXAMPLE_PROJECTS = (
    "banking-money-transfer",
    "pizza-worker-versioning",
    "canonical-ai-demo",
    "no-roads-serverless",
    "agent-gateway",
)
MAX_PATCH_CHARS = 200_000
RUN_LABEL = "registry.tmprl-demo.cloud/onboarding-run"
PARTICIPANT_ANNOTATION = "registry.tmprl-demo.cloud/broker-participant"

# Agent runs cost money and take minutes; retry only to recover from a lost
# worker (which reattaches to the running Job), not to rerun a failed agent.
AGENT_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    maximum_attempts=2,
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)


@activity.defn
def run_onboarding_agent(request: AgentRunRequest) -> AgentRunResult:
    image = os.environ.get(AGENT_IMAGE_ENV, "").strip()
    if not image:
        raise errors.non_retryable_error(
            "The onboarding agent isn't configured on this operator.",
            error_type=errors.CONFIGURATION_ERROR,
        )
    namespace = os.environ.get(AGENT_NAMESPACE_ENV, "").strip() or DEFAULT_AGENT_NAMESPACE
    name = job_name(request.run_id)

    job = kube.read_job(namespace, name)
    if job is None:
        participant = f"{request.run_id}/{activity.info().attempt}"
        key = mint_key(participant)
        job = kube.create_job(job_manifest(name, namespace, image, request.run_id, participant))
        kube.create_secret(
            secret_manifest(name, namespace, job, key, request_bundle(request))
        )
    participant = (job.get("metadata", {}).get("annotations") or {}).get(
        PARTICIPANT_ANNOTATION, ""
    )

    try:
        result = follow_job(namespace, name)
    except (CancelledError, ApplicationError):
        finish(namespace, name, participant)
        raise
    # Anything else (a lost API call, worker shutdown) leaves the Job running for
    # the retry to reattach to; its deadline and the key's hour bound it.
    finish(namespace, name, participant)
    return result


def follow_job(namespace: str, name: str) -> AgentRunResult:
    progress = "Starting"
    while True:
        job = kube.read_job(namespace, name)
        if job is None:
            raise errors.non_retryable_error(f"Agent job {name} disappeared.")
        pods = kube.list_pods(namespace, label_selector=f"job-name={name}")
        if not pods and (reason := pod_create_failure(namespace, name, job)):
            raise errors.non_retryable_error("The agent pod couldn't start: " + reason)
        log = (
            kube.read_pod_log(namespace, pods[0]["name"], container=AGENT_CONTAINER)
            if pods
            else ""
        )
        progress = last_progress(log) or progress
        activity.heartbeat({"progress": progress, "job": name})
        status = job.get("status") or {}
        if status.get("succeeded"):
            return parse_result(log)
        if status.get("failed") or job_failed(status):
            raise errors.non_retryable_error(
                "The agent failed: " + (last_error(log) or failure_reason(status))
            )
        activity.wait_for_cancelled_sync(timeout=POLL_SECONDS)
        if activity.is_cancelled():
            raise CancelledError("Agent run cancelled")
        if activity.is_worker_shutdown():
            # Leave the Job running; the retry reattaches to it.
            raise RuntimeError("Worker shutting down while following the agent")


def finish(namespace: str, name: str, participant: str) -> None:
    kube.delete_job(namespace, name)
    if participant:
        try:
            revoke_key(participant)
        except Exception:  # noqa: BLE001 - the key expires within the hour anyway
            activity.logger.warning("Could not revoke agent key %s", participant)


def job_name(run_id: str) -> str:
    return "onboarding-agent-" + hashlib.sha256(run_id.encode()).hexdigest()[:16]


def job_manifest(
    name: str, namespace: str, image: str, run_id: str, participant: str
) -> dict[str, Any]:
    labels = {
        "app.kubernetes.io/name": "onboarding-agent",
        "app.kubernetes.io/managed-by": "registry-operator",
        RUN_LABEL: name,
    }
    secret_env = [
        {
            "name": "ANTHROPIC_AUTH_TOKEN",
            "valueFrom": {"secretKeyRef": {"name": name, "key": "token"}},
        },
        {
            "name": "ANTHROPIC_BASE_URL",
            "valueFrom": {"secretKeyRef": {"name": name, "key": "base_url"}},
        },
    ]
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": labels,
            "annotations": {
                PARTICIPANT_ANNOTATION: participant,
                "registry.tmprl-demo.cloud/onboarding-run-id": run_id[:250],
            },
        },
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": JOB_DEADLINE_SECONDS,
            "ttlSecondsAfterFinished": JOB_TTL_SECONDS,
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "restartPolicy": "Never",
                    "automountServiceAccountToken": False,
                    "enableServiceLinks": False,
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": 65532,
                        "runAsGroup": 65532,
                        "fsGroup": 65532,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "initContainers": [
                        {
                            # Same firewall as isolated executors: DNS and public
                            # HTTP(S) only; private networks and cloud metadata
                            # are dropped.
                            "name": "firewall",
                            "image": image,
                            "command": ["/bin/bash", "-ec", isolated_executor.FIREWALL_COMMAND],
                            "securityContext": {
                                "runAsNonRoot": False,
                                "runAsUser": 0,
                                "runAsGroup": 0,
                                "allowPrivilegeEscalation": False,
                                "readOnlyRootFilesystem": True,
                                "capabilities": {"drop": ["ALL"], "add": ["NET_ADMIN"]},
                            },
                            # The namespace quota rejects pods whose containers
                            # don't all set requests and limits.
                            "resources": {
                                "requests": {"cpu": "50m", "memory": "32Mi"},
                                "limits": {"cpu": "200m", "memory": "64Mi"},
                            },
                        }
                    ],
                    "containers": [
                        {
                            "name": AGENT_CONTAINER,
                            "image": image,
                            "env": [
                                *secret_env,
                                {"name": "ONBOARDING_AGENT_REQUEST", "value": "/request/request.json"},
                                {"name": "ONBOARDING_AGENT_WORKSPACE", "value": "/workspace"},
                                {"name": "HOME", "value": "/workspace/home"},
                                {"name": "TMPDIR", "value": "/tmp"},
                            ],
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "readOnlyRootFilesystem": True,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "resources": {
                                "requests": {"cpu": "500m", "memory": "1Gi"},
                                "limits": {"cpu": "2", "memory": "3Gi"},
                            },
                            "volumeMounts": [
                                {"name": "request", "mountPath": "/request", "readOnly": True},
                                {"name": "workspace", "mountPath": "/workspace"},
                                {"name": "tmp", "mountPath": "/tmp"},
                            ],
                        }
                    ],
                    "volumes": [
                        {
                            "name": "request",
                            "secret": {
                                "secretName": name,
                                "items": [{"key": "request.json", "path": "request.json"}],
                            },
                        },
                        {"name": "workspace", "emptyDir": {"sizeLimit": "4Gi"}},
                        {"name": "tmp", "emptyDir": {"sizeLimit": "1Gi"}},
                    ],
                },
            },
        },
    }


def secret_manifest(
    name: str,
    namespace: str,
    job: dict[str, Any],
    key: dict[str, Any],
    bundle: dict[str, Any],
) -> dict[str, Any]:
    metadata = job.get("metadata") or {}
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "type": "Opaque",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {RUN_LABEL: name},
            # Owned by the Job, so deleting the Job deletes the key with it.
            "ownerReferences": [
                {
                    "apiVersion": "batch/v1",
                    "kind": "Job",
                    "name": name,
                    "uid": metadata.get("uid"),
                    "blockOwnerDeletion": False,
                }
            ],
        },
        "stringData": {
            "token": key["api_key"],
            "base_url": key["litellm_base_url"],
            "request.json": json.dumps(bundle),
        },
    }


def request_bundle(request: AgentRunRequest) -> dict[str, Any]:
    """The request plus the catalog contract, as of this operator's commit."""

    validator = project_validation.validator_module()
    root = Path(validator.__file__).resolve().parents[1]
    readme = root / "projects" / "README.md"
    examples = {}
    for project in kube.list_demo_projects():
        project_name = (project.get("metadata") or {}).get("name")
        if project_name in EXAMPLE_PROJECTS:
            examples[project_name] = yaml.safe_dump(
                {
                    "apiVersion": project.get("apiVersion"),
                    "kind": "DemoProject",
                    "metadata": {"name": project_name},
                    "spec": project.get("spec") or {},
                },
                sort_keys=False,
            )
    return {
        "mode": request.mode,
        "repo": {"url": request.repo.url, "commit_sha": request.repo.commit_sha},
        "project_name": request.project_name,
        "document": request.document,
        "errors": request.errors,
        "pointer": request.pointer,
        "question": request.question,
        "context": {
            # Every schema the validator loads, by file name.
            "schemas": {
                Path(path).name: Path(path).read_text()
                for path in validator.DEFAULT_SCHEMAS.values()
            },
            "validator": Path(validator.__file__).read_text(),
            "projects_readme": readme.read_text() if readme.exists() else "",
            "examples": examples,
        },
    }


def pod_create_failure(namespace: str, name: str, job: dict[str, Any]) -> str | None:
    """Why the Job can't create its pod, once it has had time to try."""

    created = (job.get("metadata") or {}).get("creationTimestamp")
    if not created:
        return None
    age = datetime.now(timezone.utc) - datetime.fromisoformat(str(created))
    if age.total_seconds() < POD_CREATE_GRACE_SECONDS:
        return None
    for event in reversed(kube.list_object_events(namespace, "Job", name)):
        if event.get("type") == "Warning" and event.get("reason") == "FailedCreate":
            return str(event.get("message") or "FailedCreate")
    return None


def tagged_lines(log: str, tag: str) -> list[str]:
    prefix = f"@@{tag} "
    return [line[len(prefix) :] for line in log.splitlines() if line.startswith(prefix)]


def last_progress(log: str) -> str | None:
    for line in reversed(tagged_lines(log, "progress")):
        try:
            return str(json.loads(line).get("text") or "") or None
        except (ValueError, AttributeError):
            continue
    return None


def last_error(log: str) -> str | None:
    for line in reversed(tagged_lines(log, "error")):
        try:
            return str(json.loads(line).get("message") or "") or None
        except (ValueError, AttributeError):
            continue
    return None


def job_failed(status: dict[str, Any]) -> bool:
    return any(
        condition.get("type") == "Failed" and condition.get("status") == "True"
        for condition in status.get("conditions") or []
    )


def failure_reason(status: dict[str, Any]) -> str:
    for condition in status.get("conditions") or []:
        if condition.get("type") == "Failed":
            return str(condition.get("message") or condition.get("reason") or "unknown")
    return "the job failed without a reason"


def parse_result(log: str) -> AgentRunResult:
    chunks: dict[int, str] = {}
    count = 0
    for line in tagged_lines(log, "result"):
        parts = line.split(" ", 2)
        if len(parts) == 3 and parts[0].isdigit() and parts[1].isdigit():
            chunks[int(parts[0])] = parts[2]
            count = int(parts[1])
    if not count or len(chunks) != count:
        raise errors.non_retryable_error("The agent finished without a complete result.")
    data = json.loads(base64.b64decode("".join(chunks[i] for i in range(count))))
    patch = str(data.get("patch") or "")
    if len(patch) > MAX_PATCH_CHARS:
        patch = patch[:MAX_PATCH_CHARS] + "\n# … patch truncated\n"
    findings = [
        AgentFinding(
            severity=str(item.get("severity") or "suggestion"),
            title=str(item.get("title") or ""),
            detail=str(item.get("detail") or ""),
            location=item.get("location"),
        )
        for item in data.get("findings") or []
        if isinstance(item, dict)
    ]
    document = data.get("document")
    return AgentRunResult(
        summary=str(data.get("summary") or ""),
        fields=[
            AgentField(
                pointer=str(item.get("pointer") or ""),
                reason=str(item.get("reason") or ""),
                evidence=[str(e) for e in item.get("evidence") or []],
            )
            for item in data.get("fields") or []
            if isinstance(item, dict)
        ],
        findings=findings,
        document=document if isinstance(document, dict) else None,
        patch=patch,
        prompt=claude_code_prompt(findings, bool(patch)),
        cost_usd=float(data.get("cost_usd") or 0.0),
        num_turns=int(data.get("num_turns") or 0),
    )


def claude_code_prompt(findings: list[AgentFinding], has_patch: bool) -> str:
    """A prompt the user can give Claude Code in their checkout of the demo."""

    actionable = [f for f in findings if f.severity in {"blocker", "warning"}]
    if not actionable and not has_patch:
        return ""
    lines = [
        "Make this repo ready for the tmprl-demo.cloud demo catalog. The catalog "
        "onboarding review found:",
        "",
    ]
    for finding in actionable:
        where = f" ({finding.location})" if finding.location else ""
        lines.append(f"- [{finding.severity}] {finding.title}{where}: {finding.detail}")
    if has_patch:
        lines += [
            "",
            "A suggested patch is in onboarding.patch. Review it, apply what fits "
            "(git apply onboarding.patch), and fix anything it misses.",
        ]
    lines += [
        "",
        "Keep changes small. Read the task queue and every Temporal or service "
        "address from environment variables, and keep the demo working locally.",
    ]
    return "\n".join(lines)


def broker_hmac_secret() -> str:
    config = WorkerConfig.from_env()
    data = kube.read_secret_data(config.source_watch_namespace, BROKER_SECRET_NAME) or {}
    try:
        secret = json.loads(data.get(BROKER_SECRET_KEY) or "{}").get(BROKER_KEY_ID)
    except (ValueError, AttributeError):
        secret = None
    if not secret:
        raise errors.non_retryable_error(
            "The operator has no LiteLLM broker credential.",
            error_type=errors.CONFIGURATION_ERROR,
        )
    return str(secret)


def broker_post(path: str, payload: dict[str, Any]) -> dict[str, Any]:
    """POST to the broker, signed the way the Instruqt CLI signs."""

    body = json.dumps(payload).encode()
    timestamp = str(int(time.time()))
    nonce = secrets.token_hex(16)
    digest = hmac.new(
        broker_hmac_secret().encode(),
        timestamp.encode() + b"." + nonce.encode() + b"." + body,
        hashlib.sha256,
    ).hexdigest()
    base_url = os.environ.get(BROKER_URL_ENV, "").strip() or DEFAULT_BROKER_URL
    request = Request(
        base_url.rstrip("/") + path,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Instruqt-Key-Id": BROKER_KEY_ID,
            "X-Instruqt-Timestamp": timestamp,
            "X-Instruqt-Nonce": nonce,
            "X-Instruqt-Signature": f"sha256={digest}",
        },
    )
    try:
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode())
    except HTTPError as error:
        detail = error.read().decode(errors="replace")[:500]
        # 410: the broker already revoked this participant and never re-issues it.
        if error.code in {400, 401, 403, 409, 410, 422}:
            raise errors.non_retryable_error(
                f"The LiteLLM broker refused {path} with HTTP {error.code}: {detail}",
                error_type=errors.CONFIGURATION_ERROR,
            ) from error
        raise RuntimeError(f"LiteLLM broker {path} failed with HTTP {error.code}.") from error
    except (OSError, URLError) as error:
        raise RuntimeError(f"LiteLLM broker {path} failed: {error}") from error


def mint_key(participant: str) -> dict[str, Any]:
    key = broker_post(
        "/v1/token",
        {"track_id": BROKER_TRACK_ID, "participant_id": participant, **KEY_POLICY},
    )
    if not key.get("api_key") or not key.get("litellm_base_url"):
        raise RuntimeError("The LiteLLM broker returned no key.")
    return key


def revoke_key(participant: str) -> None:
    broker_post(
        "/v1/token/revoke",
        {"track_id": BROKER_TRACK_ID, "participant_id": participant},
    )
