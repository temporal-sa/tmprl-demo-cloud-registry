from __future__ import annotations

import base64
import hashlib
import hmac
import io
import json
from types import SimpleNamespace

import pytest
from temporalio.exceptions import ApplicationError, CancelledError

from demo_registry_operator.activities import onboarding_agent as agent
from demo_registry_operator.workflows.catalog_onboarding_messages import (
    AgentFinding,
    AgentRunRequest,
    OnboardingRepo,
)

REPO = OnboardingRepo(
    url="https://github.com/temporalio/demo",
    owner="temporalio",
    name="demo",
    default_branch="main",
    commit_sha="c" * 40,
)


def result_lines(payload: dict, chunk: int = 10) -> str:
    encoded = base64.b64encode(json.dumps(payload).encode()).decode()
    chunks = [encoded[i : i + chunk] for i in range(0, len(encoded), chunk)]
    return "\n".join(
        f"@@result {index} {len(chunks)} {part}" for index, part in enumerate(chunks)
    )


def test_job_runs_without_credentials_behind_the_firewall():
    job = agent.job_manifest("onboarding-agent-x", "agents", "img:1", "run#1", "run#1/1")
    pod = job["spec"]["template"]["spec"]

    assert job["spec"]["backoffLimit"] == 0
    assert job["spec"]["activeDeadlineSeconds"] == agent.JOB_DEADLINE_SECONDS
    assert pod["automountServiceAccountToken"] is False
    assert pod["securityContext"]["runAsNonRoot"] is True
    [firewall] = pod["initContainers"]
    assert firewall["securityContext"]["capabilities"] == {
        "drop": ["ALL"],
        "add": ["NET_ADMIN"],
    }
    assert "iptables -P OUTPUT DROP" in firewall["command"][-1]
    # The namespace ResourceQuota rejects any pod with a container missing these.
    for each in pod["initContainers"] + pod["containers"]:
        for kind in ("requests", "limits"):
            assert set(each["resources"][kind]) == {"cpu", "memory"}, each["name"]
    [container] = pod["containers"]
    assert container["securityContext"] == {
        "allowPrivilegeEscalation": False,
        "readOnlyRootFilesystem": True,
        "capabilities": {"drop": ["ALL"]},
    }
    # The only credential is the model key, from the run's own Secret.
    secret_refs = {
        item["name"]: item["valueFrom"]["secretKeyRef"]
        for item in container["env"]
        if "valueFrom" in item
    }
    assert secret_refs == {
        "ANTHROPIC_AUTH_TOKEN": {"name": "onboarding-agent-x", "key": "token"},
        "ANTHROPIC_BASE_URL": {"name": "onboarding-agent-x", "key": "base_url"},
    }
    assert not any(
        item["name"].startswith(("AWS_", "TEMPORAL_")) for item in container["env"]
    )


def test_secret_is_owned_by_the_job_so_deleting_the_job_deletes_the_key():
    job = {"metadata": {"name": "onboarding-agent-x", "uid": "uid-1"}}
    secret = agent.secret_manifest(
        "onboarding-agent-x",
        "agents",
        job,
        {"api_key": "sk-key", "litellm_base_url": "https://gateway"},
        {"mode": "draft"},
    )

    assert secret["metadata"]["ownerReferences"] == [
        {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "name": "onboarding-agent-x",
            "uid": "uid-1",
            "blockOwnerDeletion": False,
        }
    ]
    assert secret["stringData"]["token"] == "sk-key"
    assert json.loads(secret["stringData"]["request.json"]) == {"mode": "draft"}


def test_job_names_are_stable_per_run_and_dns_safe():
    name = agent.job_name("catalog-onboarding/blue/ada@temporal.io#3")

    assert name == agent.job_name("catalog-onboarding/blue/ada@temporal.io#3")
    assert name != agent.job_name("catalog-onboarding/blue/ada@temporal.io#4")
    assert len(name) <= 63 and name.replace("-", "").isalnum()


def test_broker_requests_are_signed_the_way_the_broker_verifies(monkeypatch):
    sent = {}

    class Response(io.BytesIO):
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def fake_urlopen(request, timeout):
        sent["request"] = request
        return Response(json.dumps({"api_key": "sk", "litellm_base_url": "https://g"}).encode())

    monkeypatch.setattr(agent, "broker_hmac_secret", lambda: "shh")
    monkeypatch.setattr(agent, "urlopen", fake_urlopen)

    key = agent.mint_key("run#1/1")

    request = sent["request"]
    headers = {name.lower(): value for name, value in request.header_items()}
    body = request.data
    # The broker's _signature: sha256 HMAC over "<timestamp>.<nonce>.<body>".
    expected = hmac.new(
        b"shh",
        headers["x-instruqt-timestamp"].encode()
        + b"."
        + headers["x-instruqt-nonce"].encode()
        + b"."
        + body,
        hashlib.sha256,
    ).hexdigest()
    assert headers["x-instruqt-signature"] == f"sha256={expected}"
    assert headers["x-instruqt-key-id"] == "v1"
    assert request.full_url == "https://litellm-broker-instruqt.tmprl-demo.cloud/v1/token"
    assert json.loads(body) == {
        "track_id": "registry-onboarding",
        "participant_id": "run#1/1",
        "max_budget": 2.0,
        "duration": "1h",
        "models": ["claude-*", "anthropic/*"],
        "max_parallel_requests": 4,
    }
    assert key["api_key"] == "sk"


def test_revoked_participant_is_not_retried(monkeypatch):
    def fake_urlopen(request, timeout):
        raise agent.HTTPError(
            request.full_url, 410, "Gone", {}, io.BytesIO(b'{"detail":"Token was revoked"}')
        )

    monkeypatch.setattr(agent, "broker_hmac_secret", lambda: "shh")
    monkeypatch.setattr(agent, "urlopen", fake_urlopen)

    with pytest.raises(ApplicationError, match="HTTP 410.*revoked") as raised:
        agent.mint_key("run#1/1")

    assert raised.value.non_retryable


def test_broker_credential_is_read_from_the_single_property(monkeypatch):
    monkeypatch.setattr(
        agent.kube,
        "read_secret_data",
        lambda namespace, name: {"BROKER_HMAC_SECRETS_JSON": json.dumps({"v1": "s1", "v2": "s2"})},
    )

    assert agent.broker_hmac_secret() == "s1"


def test_parse_result_reassembles_chunks_and_builds_a_prompt():
    log = "\n".join(
        [
            '@@progress {"text":"Reading main.go"}',
            result_lines(
                {
                    "summary": "Two components.",
                    "fields": [{"pointer": "/spec/components/worker/worker", "reason": "runs a worker", "evidence": ["worker/main.go:12"]}],
                    "findings": [{"severity": "blocker", "title": "Hardcoded task queue", "detail": "Read it from TASK_QUEUE.", "location": "worker/main.go:20"}],
                    "document": {"kind": "DemoProject"},
                    "patch": "diff --git a/x b/x\n",
                    "cost_usd": 0.42,
                    "num_turns": 17,
                }
            ),
        ]
    )

    result = agent.parse_result(log)

    assert result.summary == "Two components."
    assert result.fields[0].evidence == ["worker/main.go:12"]
    assert result.document == {"kind": "DemoProject"}
    assert result.cost_usd == 0.42
    assert "[blocker] Hardcoded task queue (worker/main.go:20)" in result.prompt
    assert "onboarding.patch" in result.prompt


def test_parse_result_rejects_a_partial_result():
    log = result_lines({"summary": "x" * 200}).splitlines()[0]

    with pytest.raises(ApplicationError, match="without a complete result"):
        agent.parse_result(log)


def test_progress_and_errors_come_from_the_last_tagged_line():
    log = '@@progress {"text":"a"}\nnoise\n@@progress {"text":"b"}\n@@error {"message":"boom"}'

    assert agent.last_progress(log) == "b"
    assert agent.last_error(log) == "boom"


def test_prompt_is_empty_when_nothing_needs_changing():
    assert agent.claude_code_prompt([AgentFinding("suggestion", "t", "d")], False) == ""


class FakeCluster:
    """Just enough of kube for run_onboarding_agent."""

    def __init__(
        self,
        *,
        log: str,
        status: dict,
        existing: bool = False,
        pods: list | None = None,
        created: str = "2026-01-01T00:00:00Z",
        events: list | None = None,
    ):
        self.log = log
        self.status = status
        self.pods = [{"name": "pod-1"}] if pods is None else pods
        self.created = created
        self.events = events or []
        self.jobs = {}
        self.secrets = []
        self.deleted = []
        if existing:
            self.jobs["existing"] = True

    def read_job(self, namespace, name):
        if name in self.deleted:
            return None
        if self.jobs:
            return {
                "metadata": {
                    "name": name,
                    "uid": "uid",
                    "creationTimestamp": self.created,
                    "annotations": {agent.PARTICIPANT_ANNOTATION: "run#1/1"},
                },
                "status": self.status,
            }
        return None

    def create_job(self, job):
        self.jobs[job["metadata"]["name"]] = job
        return {**job, "metadata": {**job["metadata"], "uid": "uid"}}

    def create_secret(self, secret):
        self.secrets.append(secret)
        return "created"

    def delete_job(self, namespace, name):
        self.deleted.append(name)
        return "deleting"

    def list_pods(self, namespace, label_selector):
        return self.pods

    def list_object_events(self, namespace, kind, name):
        return self.events

    def read_pod_log(self, namespace, name, container):
        return self.log


@pytest.fixture
def run(monkeypatch):
    def setup(cluster: FakeCluster):
        for attribute in ("read_job", "create_job", "create_secret", "delete_job", "list_pods", "read_pod_log", "list_object_events"):
            monkeypatch.setattr(agent.kube, attribute, getattr(cluster, attribute))
        minted, revoked, beats = [], [], []
        monkeypatch.setenv(agent.AGENT_IMAGE_ENV, "img:1")
        monkeypatch.setattr(agent, "mint_key", lambda participant: minted.append(participant) or {"api_key": "sk", "litellm_base_url": "https://g"})
        monkeypatch.setattr(agent, "revoke_key", revoked.append)
        monkeypatch.setattr(agent, "request_bundle", lambda request: {"mode": request.mode})
        monkeypatch.setattr(
            agent,
            "activity",
            SimpleNamespace(
                info=lambda: SimpleNamespace(attempt=1),
                heartbeat=beats.append,
                wait_for_cancelled_sync=lambda timeout: None,
                is_cancelled=lambda: False,
                is_worker_shutdown=lambda: False,
                logger=SimpleNamespace(warning=lambda *args: None),
            ),
        )
        return minted, revoked, beats

    return setup


REQUEST = AgentRunRequest(run_id="run#1", mode="draft", repo=REPO, project_name="demo")


def test_run_creates_job_and_secret_then_cleans_up(run):
    cluster = FakeCluster(
        log='@@progress {"text":"Reading main.go"}\n' + result_lines({"summary": "ok"}),
        status={"succeeded": 1},
    )
    minted, revoked, beats = run(cluster)

    result = agent.run_onboarding_agent(REQUEST)

    assert result.summary == "ok"
    assert minted == ["run#1/1"]
    assert len(cluster.secrets) == 1
    assert cluster.deleted == [agent.job_name("run#1")]
    assert revoked == ["run#1/1"]
    assert beats[-1]["progress"] == "Reading main.go"


def test_retry_reattaches_to_the_running_job_without_minting(run):
    cluster = FakeCluster(log=result_lines({"summary": "ok"}), status={"succeeded": 1}, existing=True)
    minted, revoked, _ = run(cluster)

    agent.run_onboarding_agent(REQUEST)

    assert minted == []
    assert cluster.secrets == []
    assert revoked == ["run#1/1"]


def test_failed_agent_is_not_retried_and_reports_its_error(run):
    cluster = FakeCluster(log='@@error {"message":"401 from gateway"}', status={"failed": 1})
    _, revoked, _ = run(cluster)

    with pytest.raises(ApplicationError, match="401 from gateway") as raised:
        agent.run_onboarding_agent(REQUEST)

    assert raised.value.non_retryable
    assert revoked == ["run#1/1"]


QUOTA_EVENT = {
    "type": "Warning",
    "reason": "FailedCreate",
    "message": 'pods "x" is forbidden: failed quota: onboarding-agents: must specify limits.cpu for: firewall',
}


def test_unschedulable_pod_fails_fast_with_the_job_event(run):
    cluster = FakeCluster(log="", status={}, pods=[], events=[QUOTA_EVENT])
    _, revoked, _ = run(cluster)

    with pytest.raises(ApplicationError, match="couldn't start: .*failed quota") as raised:
        agent.run_onboarding_agent(REQUEST)

    assert raised.value.non_retryable
    assert cluster.deleted == [agent.job_name("run#1")]
    assert revoked == ["run#1/1"]


def test_new_job_gets_a_grace_period_before_failing_on_events(run, monkeypatch):
    # A Job created just now: the rejection may still clear, so keep polling.
    now = agent.datetime.now(agent.timezone.utc).isoformat()
    cluster = FakeCluster(log="", status={}, pods=[], created=now, events=[QUOTA_EVENT])
    run(cluster)
    monkeypatch.setattr(agent.activity, "is_cancelled", lambda: True)

    with pytest.raises(CancelledError):
        agent.run_onboarding_agent(REQUEST)


def test_cancel_deletes_the_job(run, monkeypatch):
    cluster = FakeCluster(log="", status={})
    _, revoked, _ = run(cluster)
    monkeypatch.setattr(agent.activity, "is_cancelled", lambda: True)

    with pytest.raises(CancelledError):
        agent.run_onboarding_agent(REQUEST)

    assert cluster.deleted == [agent.job_name("run#1")]
    assert revoked == ["run#1/1"]


def test_worker_shutdown_leaves_the_job_for_the_retry(run, monkeypatch):
    cluster = FakeCluster(log="", status={})
    _, revoked, _ = run(cluster)
    monkeypatch.setattr(agent.activity, "is_worker_shutdown", lambda: True)

    with pytest.raises(RuntimeError, match="shutting down"):
        agent.run_onboarding_agent(REQUEST)

    assert cluster.deleted == []
    assert revoked == []


def test_unconfigured_operator_fails_fast(run, monkeypatch):
    run(FakeCluster(log="", status={}))
    monkeypatch.delenv(agent.AGENT_IMAGE_ENV)

    with pytest.raises(ApplicationError, match="isn't configured"):
        agent.run_onboarding_agent(REQUEST)


def test_request_bundle_carries_the_contract_and_live_examples(monkeypatch):
    monkeypatch.setattr(
        agent.kube,
        "list_demo_projects",
        lambda: [
            {"apiVersion": "v", "metadata": {"name": "canonical-ai-demo", "uid": "x"}, "spec": {"source": {"repo": "r"}}},
            {"apiVersion": "v", "metadata": {"name": "not-an-example"}, "spec": {}},
        ],
    )

    bundle = agent.request_bundle(REQUEST)

    assert bundle["repo"] == {"url": REPO.url, "commit_sha": REPO.commit_sha}
    assert set(bundle["context"]["schemas"]) == {
        "project.schema.json",
        "instruqt-course.schema.json",
    }
    assert '"title": "DemoProject"' in bundle["context"]["schemas"]["project.schema.json"]
    assert "def validate_candidate" in bundle["context"]["validator"]
    assert "DemoProject" in bundle["context"]["projects_readme"]
    assert list(bundle["context"]["examples"]) == ["canonical-ai-demo"]
    assert "uid" not in bundle["context"]["examples"]["canonical-ai-demo"]
