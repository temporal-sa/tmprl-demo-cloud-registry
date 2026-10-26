from __future__ import annotations

import asyncio
import concurrent.futures
import os
import shutil
from datetime import datetime, timedelta
from types import SimpleNamespace

import pytest
from temporalio import activity
from temporalio.client import WithStartWorkflowOperation, WorkflowUpdateFailedError
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from demo_registry_operator import kube, resources
from demo_registry_operator.activities import catalog_onboarding
from demo_registry_operator.activities import onboarding_preview as activities
from demo_registry_operator.worker import create_workflow_runner
from demo_registry_operator.workflows import onboarding_preview as preview_workflow
from demo_registry_operator.workflows.onboarding_preview import (
    OnboardingPreviewWorkflow,
)
from demo_registry_operator.workflows.onboarding_preview_messages import (
    AppliedPreview,
    ApplyPreviewRequest,
    PreviewDeployRequest,
    PreviewHealth,
    PreviewNameRequest,
    ReadPreviewRequest,
    StartPreviewRequest,
)

OWNER = "ada@temporal.io"
SHA = "b" * 40


def draft(**spec) -> dict:
    return {
        "apiVersion": "registry.tmprl-demo.cloud/v1alpha1",
        "kind": "DemoProject",
        "metadata": {
            "name": "money-transfer",
            "annotations": {resources.SECRETS_FROM_ANNOTATION: "someone-else"},
            "labels": {"team": "sa"},
        },
        "spec": {
            "source": {"repo": "https://github.com/temporalio/money-transfer"},
            "secrets": {"stripe": {"properties": ["API_KEY"]}},
            "temporal": {"runtimeSecretPath": "tmprl-dem-cld/other/temporal"},
            "components": {"app": {"build": "app"}},
            "ingress": {"subdomain": "money-transfer", "temporalAuthRequired": False},
            **spec,
        },
    }


def test_preview_renders_under_its_own_name_with_rebuilt_metadata():
    document = draft()
    rendered = activities.render_preview_project(
        document, owner_email=OWNER, commit_sha=SHA
    )

    assert rendered["metadata"] == {
        "name": "pv-money-transfer",
        "labels": {resources.PREVIEW_LABEL: "true"},
        "annotations": {
            resources.SECRETS_FROM_ANNOTATION: "money-transfer",
            resources.PREVIEW_OWNER_ANNOTATION: OWNER,
            resources.SOURCE_COMMIT_ANNOTATION: SHA,
        },
    }
    assert rendered["spec"]["ingress"] == {
        "subdomain": "pv-money-transfer",
        "temporalAuthRequired": True,
    }
    assert "runtimeSecretPath" not in rendered["spec"]["temporal"]
    assert activities.preview_url(rendered) == "https://pv-money-transfer.tmprl-demo.cloud"
    # The draft itself is untouched.
    assert document["spec"]["ingress"]["subdomain"] == "money-transfer"


def test_preview_keeps_public_routes_while_forcing_sso_elsewhere():
    document = draft(
        components={"app": {"ports": [{"containerPort": 8080, "servicePort": 80}]}},
        ingress={
            "subdomain": "money-transfer",
            "temporalAuthRequired": False,
            "routes": [
                {
                    "pathPrefix": "/webhooks/github",
                    "component": "app",
                    "port": "http",
                    "public": True,
                },
                {"component": "app", "port": "http"},
            ],
        },
    )
    rendered = activities.render_preview_project(
        document, owner_email=OWNER, commit_sha=SHA
    )

    https = resources.ingress_resources(rendered, image_overrides={})[0]
    middlewares = {
        route["match"]: [middleware["name"] for middleware in route["middlewares"]]
        for route in https["spec"]["routes"]
    }
    assert middlewares == {
        "Host(`pv-money-transfer.tmprl-demo.cloud`) && PathPrefix(`/webhooks/github`)": [
            resources.AUTH_HEADER_STRIP_MIDDLEWARE_NAME,
        ],
        "Host(`pv-money-transfer.tmprl-demo.cloud`)": [
            resources.AUTH_HEADER_STRIP_MIDDLEWARE_NAME,
            resources.AUTH_MIDDLEWARE_NAME,
        ],
    }


def test_preview_name_stays_a_dns_label():
    name = activities.preview_name("x" * 63)
    assert len(name) <= 63
    assert name.startswith("pv-")


def test_preview_reads_user_secrets_from_the_real_project_only():
    rendered = activities.render_preview_project(
        draft(), owner_email=OWNER, commit_sha=SHA
    )

    assert resources.user_secret_remote_path(rendered, "stripe") == (
        "tmprl-dem-cld/money-transfer/stripe"
    )
    # The operator writes, and finalize deletes, the Temporal credentials: they
    # must stay under the preview's own prefix.
    assert resources.temporal_secret_remote_path(rendered) == (
        "tmprl-dem-cld/pv-money-transfer/temporal"
    )


def test_secrets_from_is_ignored_without_the_preview_label():
    document = draft()
    assert resources.user_secret_remote_path(document, "stripe") == (
        "tmprl-dem-cld/money-transfer/stripe"
    )


def test_catalog_and_onboarding_skip_previews(monkeypatch):
    real = draft()
    real["metadata"] = {"name": "money-transfer"}
    preview = activities.render_preview_project(
        draft(), owner_email=OWNER, commit_sha=SHA
    )

    payload = resources.catalog_payload([real, preview])
    assert [entry["name"] for entry in payload["projects"]] == ["money-transfer"]

    monkeypatch.setattr(catalog_onboarding.kube, "list_demo_projects", lambda: [preview])
    monkeypatch.setattr(catalog_onboarding.kube, "list_instruqt_courses", lambda: [])
    validator = SimpleNamespace(ProjectDocument=lambda **kwargs: kwargs)
    assert catalog_onboarding.existing_project_documents(validator) == []


def test_preview_refuses_nexus_endpoints():
    document = draft(temporal={"nexusEndpoints": {"payments": {"target": "primary"}}})
    assert activities.unsupported_preview_problem(document)
    assert activities.unsupported_preview_problem(draft()) is None


@pytest.mark.parametrize(
    ("status", "phase"),
    [
        (None, "Deploying"),
        ({"phase": "Building"}, "Deploying"),
        (
            {"phase": "Ready", "candidateRolloutStatus": {"sourceCommitSha": "c" * 40}},
            "Deploying",
        ),
        (
            {"phase": "Ready", "candidateRolloutStatus": {"sourceCommitSha": SHA}},
            "Ready",
        ),
        ({"phase": "Error", "message": "build failed"}, "Failed"),
    ],
)
def test_preview_health_follows_the_applied_commit(status, phase):
    health = activities.preview_health(status, SHA)
    assert health.phase == phase
    if phase == "Failed":
        assert health.message == "The test deploy failed: build failed"


@pytest.fixture
def cluster(monkeypatch):
    projects: dict[str, dict] = {}
    calls: list[str] = []

    def replace(resource):
        calls.append(f"replace:{resource['metadata']['name']}")
        projects[resource["metadata"]["name"]] = resource
        return "created"

    monkeypatch.setattr(activities.kube, "read_demo_project", projects.get)
    monkeypatch.setattr(activities.kube, "replace_demo_project", replace)
    monkeypatch.setattr(
        activities.kube,
        "ensure_demo_project_finalizer",
        lambda name: calls.append(f"finalizer:{name}") or True,
    )
    return SimpleNamespace(projects=projects, calls=calls)


def test_apply_creates_the_preview_and_adopts_it(cluster):
    applied = activities.apply_onboarding_preview(
        ApplyPreviewRequest(owner_email=OWNER, document=draft(), commit_sha=SHA)
    )

    assert applied == AppliedPreview(
        name="pv-money-transfer", url="https://pv-money-transfer.tmprl-demo.cloud"
    )
    assert cluster.calls == ["replace:pv-money-transfer", "finalizer:pv-money-transfer"]


def test_apply_refuses_a_name_another_user_is_previewing(cluster):
    cluster.projects["pv-money-transfer"] = activities.render_preview_project(
        draft(), owner_email="grace@temporal.io", commit_sha=SHA
    )

    with pytest.raises(Exception, match="Another test deploy"):
        activities.apply_onboarding_preview(
            ApplyPreviewRequest(owner_email=OWNER, document=draft(), commit_sha=SHA)
        )
    assert cluster.calls == []


def test_apply_waits_for_a_previous_preview_to_finish_deleting(cluster):
    existing = activities.render_preview_project(
        draft(), owner_email=OWNER, commit_sha=SHA
    )
    existing["metadata"]["deletionTimestamp"] = "2026-10-13T00:00:00Z"
    cluster.projects["pv-money-transfer"] = existing

    with pytest.raises(RuntimeError, match="still being deleted"):
        activities.apply_onboarding_preview(
            ApplyPreviewRequest(owner_email=OWNER, document=draft(), commit_sha=SHA)
        )


def test_source_commit_is_the_head_of_the_source_branch(monkeypatch):
    seen = []

    def github_get(path, token):
        seen.append(path)
        return {"commit": {"sha": SHA}} if path.endswith("/main") else None

    monkeypatch.setattr(activities, "platform_github_token", lambda: "token")
    monkeypatch.setattr(activities, "github_get", github_get)

    assert activities.resolve_source_commit(draft()) == SHA
    assert seen == ["/repos/temporalio/money-transfer/branches/main"]

    missing = draft(source={"repo": "https://github.com/temporalio/x", "branch": "dev"})
    with pytest.raises(Exception, match="Branch 'dev' was not found"):
        activities.resolve_source_commit(missing)

    pinned = draft(source={"repo": "https://github.com/temporalio/x", "branch": SHA})
    assert activities.resolve_source_commit(pinned) == SHA


def test_replace_keeps_finalizers_and_drops_removed_fields(monkeypatch):
    replaced = {}
    existing = {
        "metadata": {
            "name": "pv-money-transfer",
            "resourceVersion": "42",
            "finalizers": [kube.DEMO_PROJECT_FINALIZER],
        },
        "spec": {"old": True},
    }

    class Api:
        def get_cluster_custom_object(self, *args):
            return existing

        def replace_cluster_custom_object(self, group, version, plural, name, body):
            replaced.update(body)

    monkeypatch.setattr(kube, "_client_module", lambda: SimpleNamespace(CustomObjectsApi=Api))
    monkeypatch.setattr(kube, "load_kubernetes_config", lambda: None)

    action = kube.replace_demo_project(
        {"metadata": {"name": "pv-money-transfer"}, "spec": {"new": True}}
    )

    assert action == "replaced"
    assert replaced == {
        "metadata": {
            "name": "pv-money-transfer",
            "resourceVersion": "42",
            "finalizers": [kube.DEMO_PROJECT_FINALIZER],
        },
        "spec": {"new": True},
    }


@pytest.mark.skipif(
    not os.environ.get("DEMO_REGISTRY_TEST_TEMPORAL_HISTORY"),
    reason="Requires an explicitly enabled local Temporal server",
)
def test_preview_deploys_redeploys_and_tears_down_on_end():
    seen: dict[str, list] = {"applied": [], "read": [], "deleted": [], "cleaned": []}

    @activity.defn(name="apply_onboarding_preview")
    def apply(request: ApplyPreviewRequest) -> AppliedPreview:
        seen["applied"].append(request.commit_sha)
        return AppliedPreview(name="pv-money-transfer", url="https://pv.example")

    @activity.defn(name="read_onboarding_preview")
    def read(request: ReadPreviewRequest) -> PreviewHealth:
        seen["read"].append(request.commit_sha)
        return PreviewHealth(phase="Ready", step="Ready")

    @activity.defn(name="delete_onboarding_preview")
    def delete(request: PreviewNameRequest) -> str:
        seen["deleted"].append(request.name)
        return "deleting"

    @activity.defn(name="wait_onboarding_preview_cleanup")
    def cleanup(request: PreviewNameRequest) -> None:
        seen["cleaned"].append(request.name)

    def deploy(commit: str, project: str = "money-transfer") -> PreviewDeployRequest:
        return PreviewDeployRequest(
            project=project, document=draft(), revision=3, commit_sha=commit
        )

    async def scenario():
        async with await WorkflowEnvironment.start_local(
            dev_server_existing_path=shutil.which("temporal")
        ) as env:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                async with Worker(
                    env.client,
                    task_queue="onboarding-preview",
                    workflows=[OnboardingPreviewWorkflow],
                    activities=[apply, read, delete, cleanup],
                    activity_executor=executor,
                    workflow_runner=create_workflow_runner(),
                ):
                    status = await env.client.execute_update_with_start_workflow(
                        OnboardingPreviewWorkflow.deploy,
                        deploy(SHA),
                        start_workflow_operation=WithStartWorkflowOperation(
                            OnboardingPreviewWorkflow.run,
                            StartPreviewRequest(owner_email=OWNER),
                            id="onboarding-preview/ada@temporal.io",
                            task_queue="onboarding-preview",
                            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
                        ),
                    )
                    assert (status.phase, status.deploys) == ("Deploying", 1)
                    assert lifetime(status) == preview_workflow.PREVIEW_TTL
                    expires_at = status.expires_at
                    handle = env.client.get_workflow_handle(
                        "onboarding-preview/ada@temporal.io"
                    )

                    async def phase_is(phase: str) -> dict:
                        # Status is polled every DEPLOYING_POLL.
                        polls = preview_workflow.DEPLOYING_POLL.total_seconds() * 15
                        for _ in range(int(polls)):
                            current = await handle.query(OnboardingPreviewWorkflow.status)
                            if current["phase"] == phase:
                                return current
                            await asyncio.sleep(0.1)
                        raise AssertionError(f"never reached {phase}: {current}")

                    ready = await phase_is("Ready")
                    assert ready["url"] == "https://pv.example"

                    status = await handle.execute_update(
                        OnboardingPreviewWorkflow.deploy, deploy("c" * 40)
                    )
                    assert status.deploys == 2
                    assert status.expires_at == expires_at  # never extended
                    await phase_is("Ready")

                    with pytest.raises(WorkflowUpdateFailedError):
                        await handle.execute_update(
                            OnboardingPreviewWorkflow.deploy,
                            deploy(SHA, project="renamed"),
                        )

                    status = await handle.execute_update(OnboardingPreviewWorkflow.end)
                    assert status.phase == "Ending"
                    result = await handle.result()
                    assert result["phase"] == "Ended"
                    assert result["ended_reason"] == "Ended by user"
                    history = await handle.fetch_history()

            await Replayer(
                workflows=[OnboardingPreviewWorkflow],
                workflow_runner=create_workflow_runner(),
            ).replay_workflow(history)

    asyncio.run(scenario())
    assert seen["applied"] == [SHA, "c" * 40]
    assert seen["deleted"] == ["pv-money-transfer"]
    assert seen["cleaned"] == ["pv-money-transfer"]


def lifetime(status) -> timedelta:
    return datetime.fromisoformat(status.expires_at) - datetime.fromisoformat(
        status.created_at
    )


@pytest.mark.skipif(
    not os.environ.get("DEMO_REGISTRY_TEST_TEMPORAL_HISTORY"),
    reason="Requires an explicitly enabled local Temporal server",
)
def test_preview_started_with_a_ttl_expires_after_it():
    @activity.defn(name="apply_onboarding_preview")
    def apply(request: ApplyPreviewRequest) -> AppliedPreview:
        return AppliedPreview(name="pv-infra-demo", url="https://pv.example")

    @activity.defn(name="read_onboarding_preview")
    def read(request: ReadPreviewRequest) -> PreviewHealth:
        return PreviewHealth(phase="Ready", step="Ready")

    @activity.defn(name="delete_onboarding_preview")
    def delete(request: PreviewNameRequest) -> str:
        return "deleting"

    @activity.defn(name="wait_onboarding_preview_cleanup")
    def cleanup(request: PreviewNameRequest) -> None:
        return None

    async def scenario():
        async with await WorkflowEnvironment.start_local(
            dev_server_existing_path=shutil.which("temporal")
        ) as env:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                async with Worker(
                    env.client,
                    task_queue="onboarding-preview",
                    workflows=[OnboardingPreviewWorkflow],
                    activities=[apply, read, delete, cleanup],
                    activity_executor=executor,
                    workflow_runner=create_workflow_runner(),
                ):
                    status = await env.client.execute_update_with_start_workflow(
                        OnboardingPreviewWorkflow.deploy,
                        PreviewDeployRequest(
                            project="infra-demo",
                            document=draft(),
                            revision=1,
                            commit_sha=SHA,
                        ),
                        start_workflow_operation=WithStartWorkflowOperation(
                            OnboardingPreviewWorkflow.run,
                            StartPreviewRequest(owner_email=OWNER, ttl_seconds=7200),
                            id="demo-control/ada@temporal.io",
                            task_queue="onboarding-preview",
                            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
                        ),
                    )
                    assert lifetime(status) == timedelta(hours=2)
                    handle = env.client.get_workflow_handle(
                        "demo-control/ada@temporal.io"
                    )
                    await handle.execute_update(OnboardingPreviewWorkflow.end)
                    result = await handle.result()
                    assert result["phase"] == "Ended"

    asyncio.run(scenario())


def test_a_failure_from_an_earlier_deploy_is_not_this_deploys():
    stale = {"phase": "Error", "buildStatus": {"sourceCommitSha": "c" * 40}}
    assert activities.preview_health(stale, SHA).phase == "Deploying"

    current = {
        "phase": "Error",
        "message": "build failed",
        "buildStatus": {
            "sourceCommitSha": SHA,
            "codebuild": {"logs": {"deepLink": "https://logs.example/build"}},
        },
    }
    health = activities.preview_health(current, SHA)
    assert (health.phase, health.log_url) == ("Failed", "https://logs.example/build")


def test_status_without_a_phase_is_still_deploying():
    # The runtime child patches only candidateRevision before it applies.
    health = activities.preview_health({"candidateRevision": {"name": "x"}}, SHA)
    assert (health.phase, health.stage) == ("Deploying", "resources")


def pending_activity(name, heartbeat=None, attempt=1, failure=""):
    return SimpleNamespace(
        activity_type=SimpleNamespace(name=name),
        heartbeat_details=SimpleNamespace(payloads=[heartbeat] if heartbeat else []),
        attempt=attempt,
        last_failure=SimpleNamespace(message=failure),
    )


def fake_temporal(monkeypatch, workflows):
    """workflows: workflow id -> (pending children, pending activities)."""

    seen = []

    class Handle:
        def __init__(self, workflow_id):
            self.workflow_id = workflow_id

        async def describe(self):
            seen.append(self.workflow_id)
            if self.workflow_id not in workflows:
                raise activities.RPCError("missing", activities.RPCStatusCode.NOT_FOUND, b"")
            children, pending = workflows[self.workflow_id]
            return SimpleNamespace(
                raw_description=SimpleNamespace(
                    pending_children=children, pending_activities=pending
                )
            )

    async def decode(payloads):
        return list(payloads)

    client = SimpleNamespace(
        get_workflow_handle=lambda workflow_id, run_id=None: Handle(workflow_id),
        data_converter=SimpleNamespace(decode=decode),
    )

    async def connect(config):
        return client

    monkeypatch.setattr(activities, "connect", connect)
    monkeypatch.setenv("DEMO_REGISTRY_CLUSTER", "blue")
    return seen


def child(workflow_type, workflow_id):
    return SimpleNamespace(workflow_type_name=workflow_type, workflow_id=workflow_id, run_id="r")


def test_progress_reports_the_build_with_codebuild_phase_and_log(monkeypatch):
    parent = "demo-project/blue/pv-money-transfer"
    build = f"{parent}/reconcile-1/artifact-build"
    fake_temporal(
        monkeypatch,
        {
            parent: ([child("DemoProjectArtifactBuildWorkflow", build)], []),
            build: (
                [],
                [pending_activity("wait_for_project_image_build", {"codebuildBuildId": "cb:1"})],
            ),
        },
    )
    monkeypatch.setattr(
        activities,
        "codebuild_progress",
        lambda config, build_id: ("CodeBuild phase: Build", f"https://logs.example/{build_id}"),
    )

    progress = asyncio.run(activities.rollout_progress("pv-money-transfer"))

    assert progress == {
        "stage": "build",
        "step": "Building images",
        "detail": "CodeBuild phase: Build",
        "log_url": "https://logs.example/cb:1",
    }


@pytest.mark.parametrize(
    ("workflows", "expected"),
    [
        # The watcher hasn't started the project's workflow yet.
        ({}, {"stage": "resources", "step": "Waiting for the operator"}),
        (
            {"P": ([], [pending_activity("create_or_verify_temporal_cloud_namespace")])},
            {"stage": "resources", "step": "Creating the Temporal namespace", "detail": None, "log_url": None},
        ),
        (
            {"P": ([], [pending_activity("create_temporal_api_key_candidate_secret", attempt=3, failure="429")])},
            {
                "stage": "resources",
                "step": "Setting up Temporal credentials",
                "detail": "Retrying (attempt 3): 429",
                "log_url": None,
            },
        ),
        (
            {
                "P": ([child("DemoProjectKubernetesRuntimeWorkflow", "K")], []),
                "K": ([], [pending_activity("run_smoke_check", {"smokeCheck": "u", "attempt": 4, "lastStatus": 502})]),
            },
            {"stage": "rollout", "step": "Waiting for the health check", "detail": "Check 4, last response 502", "log_url": None},
        ),
        (
            {
                "P": ([child("DemoProjectKubernetesRuntimeWorkflow", "K")], []),
                "K": ([], [pending_activity("apply_demo_project_resources")]),
            },
            {"stage": "deploy", "step": "Applying Kubernetes resources", "detail": None, "log_url": None},
        ),
        # Between reconciles: nothing to report, so the CR status decides.
        ({"P": ([], [])}, None),
    ],
)
def test_progress_maps_what_the_reconcile_is_running(monkeypatch, workflows, expected):
    parent = "demo-project/blue/pv-money-transfer"
    workflows = {parent if key == "P" else key: value for key, value in workflows.items()}
    fake_temporal(monkeypatch, workflows)

    assert asyncio.run(activities.rollout_progress("pv-money-transfer")) == expected
