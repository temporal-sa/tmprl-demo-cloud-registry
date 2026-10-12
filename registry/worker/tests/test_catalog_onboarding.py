from __future__ import annotations

import asyncio
import base64
import concurrent.futures
import json
import os
import shutil
from datetime import timedelta

import pytest
import yaml
from temporalio import activity
from temporalio.client import WithStartWorkflowOperation, WorkflowUpdateFailedError
from temporalio.common import WorkflowIDConflictPolicy
from temporalio.exceptions import ApplicationError, CancelledError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from demo_registry_operator.activities import catalog_onboarding as activities
from demo_registry_operator.worker import create_workflow_runner
from demo_registry_operator.workflows.catalog_onboarding import (
    CatalogOnboardingWorkflow,
    project_name_from_repo,
    seed_document,
)
from demo_registry_operator.workflows.catalog_onboarding_messages import (
    AgentField,
    AgentRequest,
    AgentRunRequest,
    AgentRunResult,
    DraftError,
    DraftValidation,
    InspectRepoRequest,
    OnboardingRepo,
    PullRequestCheck,
    SetRepoRequest,
    StartOnboardingRequest,
    SubmitPullRequestRequest,
    UpdateDraftRequest,
    ValidateDraftRequest,
    VerifyPullRequestRequest,
)

REPO = OnboardingRepo(
    url="https://github.com/temporalio/money-transfer-demo",
    owner="temporalio",
    name="money-transfer-demo",
    default_branch="main",
    commit_sha="a" * 40,
    description="Move money durably.",
)


@pytest.fixture
def cluster(monkeypatch):
    """Projects already in the cluster; tests append CRs to these lists."""

    demo_projects: list[dict] = []
    monkeypatch.setattr(activities.kube, "list_demo_projects", lambda: demo_projects)
    monkeypatch.setattr(activities.kube, "list_instruqt_courses", lambda: [])
    return demo_projects


@pytest.fixture
def github(monkeypatch):
    """Canned GitHub API responses keyed by path; missing paths are 404s."""

    responses: dict[str, object] = {}
    monkeypatch.setattr(activities, "platform_github_token", lambda: "token")
    monkeypatch.setattr(
        activities, "github_get", lambda path, token: responses.get(path)
    )
    return responses


@pytest.mark.parametrize(
    "url",
    [
        "https://github.com/temporal-sa/demo",
        "https://github.com/temporalio/demo.git",
        "https://github.com/temporal-community/demo/",
    ],
)
def test_parse_repo_url_accepts_allowed_orgs(url):
    owner, name = activities.parse_repo_url(url)

    assert name == "demo"
    assert owner in activities.ALLOWED_REPO_OWNERS


@pytest.mark.parametrize(
    "url",
    [
        "https://github.com/example/demo",
        "http://github.com/temporal-sa/demo",
        "https://gitlab.com/temporal-sa/demo",
        "https://github.com/temporal-sa/demo/tree/main",
        "temporal-sa/demo",
    ],
)
def test_parse_repo_url_rejects_everything_else(url):
    with pytest.raises(ApplicationError) as raised:
        activities.parse_repo_url(url)

    assert raised.value.non_retryable


def test_inspect_pins_the_default_branch_commit_of_a_public_repo(github):
    github["/repos/temporalio/money-transfer-demo"] = {
        "private": False,
        "visibility": "public",
        "owner": {"login": "temporalio"},
        "name": "money-transfer-demo",
        "default_branch": "trunk",
        "description": "Move money durably.",
    }
    github["/repos/temporalio/money-transfer-demo/branches/trunk"] = {
        "commit": {"sha": "b" * 40}
    }

    repo = activities.inspect_onboarding_repo(
        InspectRepoRequest("https://github.com/temporalio/money-transfer-demo")
    )

    assert repo.url == "https://github.com/temporalio/money-transfer-demo"
    assert repo.default_branch == "trunk"
    assert repo.commit_sha == "b" * 40


@pytest.mark.parametrize("visibility", ["private", "internal"])
def test_inspect_rejects_repos_that_are_not_public(github, visibility):
    github["/repos/temporal-sa/demo"] = {
        "private": visibility == "private",
        "visibility": visibility,
        "default_branch": "main",
    }

    with pytest.raises(ApplicationError, match="not public"):
        activities.inspect_onboarding_repo(
            InspectRepoRequest("https://github.com/temporal-sa/demo")
        )


def test_inspect_treats_a_missing_repo_as_a_user_error(github):
    with pytest.raises(ApplicationError, match="was not found") as raised:
        activities.inspect_onboarding_repo(
            InspectRepoRequest("https://github.com/temporal-sa/demo")
        )

    assert raised.value.non_retryable


def test_seed_document_validates_and_links_to_the_new_file_page(cluster):
    result = activities.validate_onboarding_draft(
        ValidateDraftRequest(document=seed_document(REPO))
    )

    assert result.errors == []
    assert result.project_path == "projects/demo/money-transfer-demo.yaml"
    assert yaml.safe_load(result.yaml) == result.document
    assert result.new_file_url.startswith(
        "https://github.com/temporal-sa/tmprl-demo-cloud-registry/new/main"
        "?filename=projects/demo/money-transfer-demo.yaml&value="
    )


def test_render_yaml_uses_project_file_order_whatever_order_keys_arrive_in():
    document = seed_document(REPO)
    alphabetical = json.loads(json.dumps(document, sort_keys=True))

    rendered = activities.render_yaml(alphabetical)

    assert yaml.safe_load(rendered) == document
    assert list(yaml.safe_load(rendered)["spec"]) == [
        "registry",
        "source",
        "builds",
        "components",
        "ingress",
    ]
    assert "      ports:\n        - containerPort: 8080\n" in rendered


def test_validate_reports_collisions_with_projects_in_the_cluster(cluster):
    existing = seed_document(REPO)
    cluster.append(
        {**existing, "metadata": {"name": "money-transfer-demo", "uid": "x"}}
    )

    result = activities.validate_onboarding_draft(
        ValidateDraftRequest(document=seed_document(REPO))
    )

    assert {error.pointer for error in result.errors} == {
        "/metadata/name",
        "/spec/ingress/subdomain",
    }
    assert result.new_file_url is None


def test_validate_skips_operator_canaries_in_the_cluster(cluster):
    canary = seed_document(REPO)
    canary["metadata"] = {
        "name": "money-transfer-demo",
        "labels": {activities.resources.CANARY_LABEL: "true"},
    }
    cluster.append(canary)

    result = activities.validate_onboarding_draft(
        ValidateDraftRequest(document=seed_document(REPO))
    )

    assert result.errors == []


def test_validate_points_schema_errors_at_the_field(cluster):
    document = seed_document(REPO)
    document["spec"]["components"]["app"]["ports"][0]["containerPort"] = "eighty"

    result = activities.validate_onboarding_draft(
        ValidateDraftRequest(document=document)
    )

    assert [error.pointer for error in result.errors] == [
        "/spec/components/app/ports/0/containerPort"
    ]


def test_validate_keeps_unparseable_yaml_without_a_document(cluster):
    result = activities.validate_onboarding_draft(
        ValidateDraftRequest(yaml="spec: [unclosed")
    )

    assert result.document is None
    assert result.yaml == "spec: [unclosed"
    assert result.errors[0].message.startswith("YAML parse error")


def test_validate_only_accepts_demo_projects(cluster):
    document = seed_document(REPO)
    document["kind"] = "InstruqtCourse"

    result = activities.validate_onboarding_draft(
        ValidateDraftRequest(document=document)
    )

    assert [error.pointer for error in result.errors] == ["/kind"]


def pull_request(github, number: int, document: dict, *, files=None, state="open"):
    content = activities.render_yaml(document)
    github[f"/repos/{activities.CATALOG_REPO}/pulls/{number}"] = {
        "state": state,
        "base": {"ref": "main"},
        "user": {"login": "ada"},
    }
    github[f"/repos/{activities.CATALOG_REPO}/pulls/{number}/files?per_page=100"] = (
        files
        if files is not None
        else [
            {
                "filename": "projects/demo/money-transfer-demo.yaml",
                "status": "added",
                "sha": "blob",
            }
        ]
    )
    github[f"/repos/{activities.CATALOG_REPO}/git/blobs/blob"] = {
        "encoding": "base64",
        "content": base64.b64encode(content.encode()).decode(),
    }


def verify(number: int, document: dict):
    return activities.verify_onboarding_pull_request(
        VerifyPullRequestRequest(
            pull_request_url=(
                f"https://github.com/{activities.CATALOG_REPO}/pull/{number}/files"
            ),
            project_path="projects/demo/money-transfer-demo.yaml",
            document=document,
        )
    )


def test_verify_accepts_a_pull_request_adding_exactly_the_draft(github):
    document = seed_document(REPO)
    pull_request(github, 7, document)

    check = verify(7, document)

    assert check.problems == []
    assert check.url == f"https://github.com/{activities.CATALOG_REPO}/pull/7"
    assert check.author == "ada"


def test_verify_rejects_edited_content(github):
    document = seed_document(REPO)
    edited = seed_document(REPO)
    edited["spec"]["registry"]["title"] = "Edited on GitHub"
    pull_request(github, 7, edited)

    assert "differs from the validated draft" in " ".join(verify(7, document).problems)


def test_verify_rejects_closed_pull_requests_and_extra_files(github):
    document = seed_document(REPO)
    pull_request(
        github,
        7,
        document,
        state="closed",
        files=[
            {
                "filename": "projects/demo/money-transfer-demo.yaml",
                "status": "added",
                "sha": "blob",
            },
            {"filename": "README.md", "status": "modified", "sha": "other"},
        ],
    )

    problems = " ".join(verify(7, document).problems)

    assert "is not open" in problems
    assert "README.md" in problems


def test_verify_rejects_pull_requests_on_other_repos(github):
    with pytest.raises(ApplicationError, match="Paste the link"):
        activities.verify_onboarding_pull_request(
            VerifyPullRequestRequest(
                pull_request_url="https://github.com/temporal-sa/other/pull/1",
                project_path="projects/demo/x.yaml",
                document={},
            )
        )


@pytest.mark.parametrize(
    ("repo_name", "project_name"),
    [
        ("Money_Transfer.Demo", "money-transfer-demo"),
        ("--demo--", "demo"),
        ("___", "demo"),
    ],
)
def test_project_name_from_repo_is_a_dns_label(repo_name, project_name):
    assert project_name_from_repo(repo_name) == project_name


@pytest.mark.skipif(
    not os.environ.get("DEMO_REGISTRY_TEST_TEMPORAL_HISTORY"),
    reason="Requires an explicitly enabled local Temporal server",
)
def test_onboarding_runs_from_repo_to_hand_off_and_replays():
    seen: dict[str, list] = {"validated": [], "verified": []}

    @activity.defn(name="inspect_onboarding_repo")
    def inspect(request: InspectRepoRequest) -> OnboardingRepo:
        return REPO

    @activity.defn(name="validate_onboarding_draft")
    def validate(request: ValidateDraftRequest) -> DraftValidation:
        seen["validated"].append(request)
        if request.yaml is not None:
            document = yaml.safe_load(request.yaml)
        else:
            document = request.document
        title = document["spec"]["registry"]["title"]
        errors = (
            [DraftError("title is required", pointer="/spec/registry/title")]
            if not title
            else []
        )
        return DraftValidation(
            document=document,
            yaml=activities.render_yaml(document),
            errors=errors,
            project_path="projects/demo/money-transfer-demo.yaml",
            new_file_url=None if errors else "https://github.com/new",
        )

    @activity.defn(name="verify_onboarding_pull_request")
    def verify_pr(request: VerifyPullRequestRequest) -> PullRequestCheck:
        seen["verified"].append(request.pull_request_url)
        if request.pull_request_url.endswith("/1"):
            return PullRequestCheck(url=request.pull_request_url, problems=["edited"])
        return PullRequestCheck(url=request.pull_request_url, number=2)

    async def scenario():
        async with await WorkflowEnvironment.start_local(
            dev_server_existing_path=shutil.which("temporal")
        ) as env:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                async with Worker(
                    env.client,
                    task_queue="catalog-onboarding",
                    workflows=[CatalogOnboardingWorkflow],
                    activities=[inspect, validate, verify_pr],
                    activity_executor=executor,
                    workflow_runner=create_workflow_runner(),
                ):
                    status = await env.client.execute_update_with_start_workflow(
                        CatalogOnboardingWorkflow.ensure,
                        start_workflow_operation=WithStartWorkflowOperation(
                            CatalogOnboardingWorkflow.run,
                            StartOnboardingRequest(owner_email="ada@temporal.io"),
                            id="catalog-onboarding/ada@temporal.io",
                            task_queue="catalog-onboarding",
                            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
                        ),
                    )
                    assert status.phase == "NeedsRepo"
                    handle = env.client.get_workflow_handle(
                        "catalog-onboarding/ada@temporal.io"
                    )

                    status = await handle.execute_update(
                        CatalogOnboardingWorkflow.set_repo,
                        SetRepoRequest(repo_url=REPO.url),
                    )
                    assert (status.phase, status.revision, status.ready) == (
                        "Drafting",
                        1,
                        True,
                    )

                    blank = dict(status.document)
                    blank["spec"] = {
                        **blank["spec"],
                        "registry": {"title": "", "summary": "x"},
                    }
                    status = await handle.execute_update(
                        CatalogOnboardingWorkflow.update_draft,
                        UpdateDraftRequest(base_revision=1, document=blank),
                    )
                    assert status.revision == 2
                    assert not status.ready
                    assert status.errors[0].pointer == "/spec/registry/title"

                    with pytest.raises(WorkflowUpdateFailedError):
                        await handle.execute_update(
                            CatalogOnboardingWorkflow.update_draft,
                            UpdateDraftRequest(base_revision=1, document=blank),
                        )
                    with pytest.raises(WorkflowUpdateFailedError):
                        await handle.execute_update(
                            CatalogOnboardingWorkflow.submit_pull_request,
                            SubmitPullRequestRequest(pull_request_url="https://x/2"),
                        )

                    fixed = status.yaml.replace("title: ''", "title: Money")
                    status = await handle.execute_update(
                        CatalogOnboardingWorkflow.update_draft,
                        UpdateDraftRequest(base_revision=2, yaml=fixed),
                    )
                    assert status.ready
                    assert status.document["spec"]["registry"]["title"] == "Money"
                    drafting = status

                    status = await handle.execute_update(
                        CatalogOnboardingWorkflow.submit_pull_request,
                        SubmitPullRequestRequest(pull_request_url="https://x/1"),
                    )
                    assert status.phase == "Drafting"
                    assert status.last_error == "edited"

                    status = await handle.execute_update(
                        CatalogOnboardingWorkflow.submit_pull_request,
                        SubmitPullRequestRequest(pull_request_url="https://x/2"),
                    )
                    assert status.phase == "HandedOff"
                    result = await handle.result()
                    assert result["pull_request_url"] == "https://x/2"
                    history = await handle.fetch_history()

                    # A continue-as-new carryover resumes the draft as it was.
                    resumed = await env.client.execute_update_with_start_workflow(
                        CatalogOnboardingWorkflow.ensure,
                        start_workflow_operation=WithStartWorkflowOperation(
                            CatalogOnboardingWorkflow.run,
                            StartOnboardingRequest(
                                owner_email="ada@temporal.io",
                                carried_status=drafting,
                            ),
                            id="catalog-onboarding/carried",
                            task_queue="catalog-onboarding",
                            id_conflict_policy=WorkflowIDConflictPolicy.USE_EXISTING,
                        ),
                    )
                    assert resumed == drafting

            await Replayer(
                workflows=[CatalogOnboardingWorkflow],
                workflow_runner=create_workflow_runner(),
            ).replay_workflow(history)

    asyncio.run(scenario())
    assert seen["verified"] == ["https://x/1", "https://x/2"]


@pytest.mark.skipif(
    not os.environ.get("DEMO_REGISTRY_TEST_TEMPORAL_HISTORY"),
    reason="Requires an explicitly enabled local Temporal server",
)
def test_agent_runs_propose_without_editing_and_can_be_stopped():
    seen: dict[str, list] = {"agent": [], "validated": []}
    release = {"cancelled": False}

    @activity.defn(name="inspect_onboarding_repo")
    def inspect(request: InspectRepoRequest) -> OnboardingRepo:
        return REPO

    @activity.defn(name="validate_onboarding_draft")
    def validate(request: ValidateDraftRequest) -> DraftValidation:
        seen["validated"].append(request.document)
        return DraftValidation(
            document=request.document,
            yaml=activities.render_yaml(request.document),
            project_path="projects/demo/money-transfer-demo.yaml",
        )

    @activity.defn(name="run_onboarding_agent")
    def run_agent(request: AgentRunRequest) -> AgentRunResult:
        seen["agent"].append(request)
        if request.mode == "section":
            # Block until the workflow cancels us, heartbeating like the real one.
            # The SDK may raise the cancel inside this thread at any point.
            try:
                while not activity.is_cancelled():
                    activity.heartbeat({"progress": "Reading"})
                    activity.wait_for_cancelled_sync(timeout=0.2)
                raise CancelledError("stopped")
            finally:
                release["cancelled"] = activity.is_cancelled()
        drafted = {**request.document, "metadata": {"name": "drafted"}}
        return AgentRunResult(
            summary="Drafted it.",
            fields=[AgentField(pointer="/metadata/name", reason="repo name")],
            document=drafted,
            patch="diff --git a/x b/x\n",
            prompt="Fix it.",
            cost_usd=0.3,
            num_turns=9,
        )

    async def wait_for_agent(handle, phase):
        for _ in range(100):
            status = await handle.query(CatalogOnboardingWorkflow.status)
            if status["agent"] and status["agent"]["phase"] == phase:
                return status
            await asyncio.sleep(0.1)
        raise AssertionError(f"agent never reached {phase}")

    async def scenario():
        async with await WorkflowEnvironment.start_local(
            dev_server_existing_path=shutil.which("temporal")
        ) as env:
            with concurrent.futures.ThreadPoolExecutor() as executor:
                async with Worker(
                    env.client,
                    task_queue="catalog-onboarding",
                    workflows=[CatalogOnboardingWorkflow],
                    activities=[inspect, validate, run_agent],
                    activity_executor=executor,
                    workflow_runner=create_workflow_runner(),
                    # Deliver the cancel on the next heartbeat, not 24s later.
                    max_heartbeat_throttle_interval=timedelta(milliseconds=200),
                    default_heartbeat_throttle_interval=timedelta(milliseconds=200),
                ):
                    handle = await env.client.start_workflow(
                        CatalogOnboardingWorkflow.run,
                        StartOnboardingRequest(owner_email="ada@temporal.io"),
                        id="catalog-onboarding/agent",
                        task_queue="catalog-onboarding",
                    )
                    status = await handle.execute_update(
                        CatalogOnboardingWorkflow.set_repo,
                        SetRepoRequest(repo_url=REPO.url),
                    )
                    original = status.document

                    status = await handle.execute_update(
                        CatalogOnboardingWorkflow.request_agent,
                        AgentRequest(mode="draft"),
                    )
                    assert status.agent.phase == "Running"
                    done = await wait_for_agent(handle, "Done")
                    proposal = done["agent"]["proposal"]
                    assert proposal["summary"] == "Drafted it."
                    assert proposal["document"]["metadata"]["name"] == "drafted"
                    assert proposal["patch"].startswith("diff")
                    # A proposal, not an edit: the draft is unchanged.
                    assert done["document"] == original
                    assert done["revision"] == 1
                    assert seen["validated"][-1]["metadata"]["name"] == "drafted"

                    await handle.execute_update(
                        CatalogOnboardingWorkflow.request_agent,
                        AgentRequest(
                            mode="section",
                            pointer="/spec/components",
                            question="Which are workers?",
                        ),
                    )
                    with pytest.raises(WorkflowUpdateFailedError):
                        await handle.execute_update(
                            CatalogOnboardingWorkflow.request_agent,
                            AgentRequest(mode="draft"),
                        )
                    stopping = await handle.execute_update(
                        CatalogOnboardingWorkflow.cancel_agent
                    )
                    assert stopping.agent.stopping
                    cancelled = await wait_for_agent(handle, "Cancelled")
                    assert cancelled["agent"]["stopping"] is False
                    assert cancelled["agent_runs"] == 2
                    assert seen["agent"][1].run_id == "catalog-onboarding/agent#2"
                    assert seen["agent"][1].question == "Which are workers?"

                    await handle.execute_update(CatalogOnboardingWorkflow.abandon)
                    await handle.result()
                    history = await handle.fetch_history()

            await Replayer(
                workflows=[CatalogOnboardingWorkflow],
                workflow_runner=create_workflow_runner(),
            ).replay_workflow(history)

    asyncio.run(scenario())
    assert release["cancelled"]
