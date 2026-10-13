"""Characterization of the Temporal Cloud reconcile saga.

This pins the exact activity call sequence, the exact request payloads, and the
exact merged status for a project with no additional namespaces -- the shape
every shipped project has. It exists so the multi-namespace refactor can be
shown to preserve behavior rather than asserted to. If a change here is needed
to make this pass, the refactor is wrong, not the test.

Three scenarios, because each takes a different path through the saga:

  steady state       the runtime-secret suffix is already owned, so the path
                     cannot move and the secret is checked once
  first provision    no suffix yet, so the saga mints one, rebuilds the plan,
                     and re-checks the secret at the new path
  search attributes  adds the one batched activity, and pins the delete delta,
                     which is computed against a single flat status blob
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

import demo_registry_operator.workflows.demo_project as demo_project_workflow
from demo_registry_operator.activities.external_resources import models
from demo_registry_operator.workflows.demo_project import (
    DemoProjectWorkflow,
    demo_project_with_temporal_runtime_secret_path,
)
from demo_registry_operator.workflows.messages import DemoProjectRunRequest


CHECKED_AT = "2026-06-20T00:00:00+00:00"
NAMESPACE = "tmprl-dem-cld-demo"
LEGACY_PATH = "tmprl-dem-cld/demo/temporal"
STABLE_SUFFIX = "stable123"
STABLE_PATH = f"tmprl-dem-cld/demo/temporal-{STABLE_SUFFIX}"
MINTED_SUFFIX = "deadbeefcafe"
MINTED_PATH = f"tmprl-dem-cld/demo/temporal-{MINTED_SUFFIX}"


def project() -> dict[str, Any]:
    return {
        "apiVersion": "registry.tmprl-demo.cloud/v1alpha1",
        "kind": "DemoProject",
        "metadata": {"name": "demo", "generation": 1},
        "spec": {
            "components": {
                "worker": {"image": "example.com/demo/worker:latest", "worker": True},
            },
        },
    }


def activity_results(runtime_secret_path: str) -> dict[str, Any]:
    """One canned result per activity on the apiKey happy path."""
    return {
        "create_or_verify_temporal_cloud_namespace": models.TemporalCloudNamespaceResult(
            status={"phase": "Ready", "namespace": NAMESPACE},
            namespace=NAMESPACE,
            grpc_address=f"{NAMESPACE}.acct1.tmprl.cloud:7233",
            auth_mode="apiKey",
            created=True,
            region="aws-us-west-2",
        ),
        "ensure_temporal_cloud_namespace_tags": models.TemporalCloudStatusResult(
            status={"phase": "Ready", "action": "updated"},
        ),
        "ensure_temporal_cloud_namespace_readers_access": models.TemporalCloudStatusResult(
            status={
                "phase": "Ready",
                "readersGroup": {"name": "tmprl-demo-cld-read", "action": "granted"},
            },
        ),
        "reconcile_temporal_cloud_search_attributes": models.TemporalCloudSearchAttributesResult(
            statuses=[
                {
                    "phase": "Ready",
                    "searchAttribute": {
                        "name": "OrderId",
                        "type": "keyword",
                        "action": "created",
                    },
                },
                {
                    "phase": "Ready",
                    "searchAttribute": {
                        "name": "Retired",
                        "type": "keyword",
                        "action": "deleted",
                    },
                },
            ],
        ),
        "create_or_verify_temporal_cloud_service_account": models.TemporalCloudServiceAccountResult(
            status={"phase": "Ready"},
            service_account_id="sa-123",
            service_account_name="demo-runtime",
            created=True,
        ),
        "check_temporal_runtime_secret": models.TemporalRuntimeSecretCheckResult(
            status={"phase": "Ready"},
            runtime_secret_path=runtime_secret_path,
            state="Missing",
            exists=False,
            rotate=True,
        ),
        "create_temporal_api_key_candidate_secret": models.TemporalCloudCredentialStageResult(
            status={"phase": "Ready"},
            promote_required=True,
            candidate_api_key_id="key-abc",
            candidate_secret_path=f"{runtime_secret_path}/candidate-abc",
        ),
        "run_temporal_runtime_candidate_canary": models.TemporalCloudStatusResult(
            status={"phase": "Ready"},
        ),
        "promote_temporal_runtime_credential": models.TemporalCloudPromotionResult(
            status={"phase": "Ready", "apiKey": {"keyId": "key-abc"}},
            promoted=True,
        ),
        "delete_temporal_runtime_candidate_secret": models.TemporalCloudStatusResult(
            status={"phase": "Ready", "action": "deleted"},
        ),
        "mirror_temporal_runtime_secret": models.TemporalCloudStatusResult(
            status={"phase": "Ready", "action": "mirrored"},
        ),
    }


def run_saga(
    monkeypatch,
    *,
    owned_suffix: str | None,
    search_attributes: dict[str, Any] | None = None,
    previous_status: dict[str, Any] | None = None,
    sequenced_results: dict[str, list[Any]] | None = None,
    nexus_endpoint: bool = False,
):
    """Drive the real saga with a recording stub, returning (calls, status, workflow).

    sequenced_results maps an activity name to the results its successive calls
    return, for paths where one activity is called more than once.
    """
    workflow = DemoProjectWorkflow(DemoProjectRunRequest(project_name="demo"))
    demo_project = project()
    if search_attributes:
        demo_project["spec"]["temporal"] = {"searchAttributes": search_attributes}
    if owned_suffix:
        workflow._temporal_runtime_secret_suffix = owned_suffix
        demo_project = demo_project_with_temporal_runtime_secret_path(
            demo_project, f"tmprl-dem-cld/demo/temporal-{owned_suffix}"
        )
        expected_path = f"tmprl-dem-cld/demo/temporal-{owned_suffix}"
    else:
        expected_path = MINTED_PATH

    results = activity_results(expected_path)
    sequenced = {name: list(values) for name, values in (sequenced_results or {}).items()}
    calls: list[tuple[str, Any]] = []

    async def fake_execute_activity(activity, request, **kwargs):
        name = getattr(activity, "__name__", str(activity))
        calls.append((name, request))
        if sequenced.get(name):
            return sequenced[name].pop(0)
        if name not in results:
            raise AssertionError(f"unexpected activity in the saga: {name}")
        return results[name]

    monkeypatch.setattr(
        demo_project_workflow.workflow,
        "info",
        lambda: SimpleNamespace(workflow_id="DemoProject/demo", run_id="run-1"),
    )
    monkeypatch.setattr(
        demo_project_workflow.workflow,
        "now",
        lambda: datetime(2026, 6, 20, tzinfo=UTC),
    )
    monkeypatch.setattr(
        demo_project_workflow.workflow,
        "uuid4",
        lambda: SimpleNamespace(hex=MINTED_SUFFIX + "0000000000000000000"),
    )
    monkeypatch.setattr(
        demo_project_workflow.workflow, "execute_activity", fake_execute_activity
    )

    external_plan = demo_project_workflow.external_resources.plan_external_resources(
        demo_project,
        checked_at=CHECKED_AT,
    )
    if nexus_endpoint:
        external_plan = with_nexus_endpoint(external_plan)
        results["delete_nexus_endpoints"] = models.NexusEndpointResult(
            status={"phase": "Deleted", "endpoints": []},
            endpoint_ids={},
        )
        results["reconcile_nexus_endpoints"] = models.NexusEndpointResult(
            status={"phase": "Ready", "endpoints": []},
            endpoint_ids={"orders": "endpoint-orders"},
        )
    status = asyncio.run(
        workflow._reconcile_temporal_cloud(
            external_plan, previous_status=previous_status or {}
        )
    )
    return calls, status, workflow


# A project with no spec.temporal.searchAttributes never reaches the
# search-attribute activity; the step still emits a NotRequired status block.
STEADY_STATE_SEQUENCE = [
    "create_or_verify_temporal_cloud_namespace",
    "ensure_temporal_cloud_namespace_tags",
    "ensure_temporal_cloud_namespace_readers_access",
    "create_or_verify_temporal_cloud_service_account",
    "check_temporal_runtime_secret",
    "create_temporal_api_key_candidate_secret",
    "run_temporal_runtime_candidate_canary",
    "promote_temporal_runtime_credential",
    "delete_temporal_runtime_candidate_secret",
    "mirror_temporal_runtime_secret",
]

# First provision re-checks the runtime secret once the path has moved.
FIRST_PROVISION_SEQUENCE = [
    "create_or_verify_temporal_cloud_namespace",
    "ensure_temporal_cloud_namespace_tags",
    "ensure_temporal_cloud_namespace_readers_access",
    "create_or_verify_temporal_cloud_service_account",
    "check_temporal_runtime_secret",
    "check_temporal_runtime_secret",
    "create_temporal_api_key_candidate_secret",
    "run_temporal_runtime_candidate_canary",
    "promote_temporal_runtime_credential",
    "delete_temporal_runtime_candidate_secret",
    "mirror_temporal_runtime_secret",
]

# Declaring search attributes inserts exactly one activity, after the
# namespace metadata steps (tags, readers access).
SEARCH_ATTRIBUTE_SEQUENCE = [
    "create_or_verify_temporal_cloud_namespace",
    "ensure_temporal_cloud_namespace_tags",
    "ensure_temporal_cloud_namespace_readers_access",
    "reconcile_temporal_cloud_search_attributes",
    "create_or_verify_temporal_cloud_service_account",
    "check_temporal_runtime_secret",
    "create_temporal_api_key_candidate_secret",
    "run_temporal_runtime_candidate_canary",
    "promote_temporal_runtime_credential",
    "delete_temporal_runtime_candidate_secret",
    "mirror_temporal_runtime_secret",
]

PINNED_STATUS_KEYS = {
    "phase",
    "namespace",
    "apiKey",
    "searchAttributes",
    "runtimeSecretMirror",
}


# --- steady state ------------------------------------------------------------


def test_steady_state_calls_exactly_this_activity_sequence(monkeypatch):
    calls, _status, _workflow = run_saga(monkeypatch, owned_suffix=STABLE_SUFFIX)
    assert [name for name, _request in calls] == STEADY_STATE_SEQUENCE


def test_steady_state_targets_the_derived_namespace_on_every_request(monkeypatch):
    calls, _status, _workflow = run_saga(monkeypatch, owned_suffix=STABLE_SUFFIX)
    seen = 0
    for name, request in calls:
        temporal_project = getattr(request, "project", None)
        if temporal_project is None:
            continue
        seen += 1
        # No target override anywhere: every activity resolves the namespace from
        # metadata.name, exactly as it did before the refactor.
        assert temporal_project.target_namespace is None, name
        assert temporal_project.namespace == NAMESPACE, name
        assert temporal_project.runtime_secret_path == STABLE_PATH, name
        assert temporal_project.as_demo_project()["spec"]["temporal"] == {
            "auth": {"mode": "apiKey"},
            "runtimeSecretPath": STABLE_PATH,
        }, name
    assert seen == len(STEADY_STATE_SEQUENCE)


def test_steady_state_uses_one_candidate_token_for_the_whole_pass(monkeypatch):
    calls, _status, workflow = run_saga(monkeypatch, owned_suffix=STABLE_SUFFIX)
    tokens = {
        request.candidate_secret_token
        for _name, request in calls
        if getattr(request, "candidate_secret_token", None) is not None
    }
    assert tokens == {workflow._tc_token}


def test_steady_state_returns_this_merged_status(monkeypatch):
    _calls, status, _workflow = run_saga(monkeypatch, owned_suffix=STABLE_SUFFIX)
    assert status == {
        "phase": "Ready",
        "namespace": NAMESPACE,
        "apiKey": {"keyId": "key-abc"},
        "searchAttributes": {
            "phase": "NotRequired",
            "checkedAt": CHECKED_AT,
            "namespace": NAMESPACE,
            "ownershipPolicy": "delete-only-previously-managed",
            "attributes": [],
            "managedNames": [],
        },
        "runtimeSecretMirror": {"phase": "Ready", "action": "mirrored"},
    }


def test_steady_state_status_keys_are_pinned(monkeypatch):
    # The status shape is a contract: the catalog ConfigMap, printer columns, and
    # temporal_auth_rotation_state all read it. Adding namespaces must not move it.
    _calls, status, _workflow = run_saga(monkeypatch, owned_suffix=STABLE_SUFFIX)
    assert set(status) == PINNED_STATUS_KEYS


def test_steady_state_records_state_for_compensation(monkeypatch):
    _calls, _status, workflow = run_saga(monkeypatch, owned_suffix=STABLE_SUFFIX)
    assert workflow._tc_state is not None
    assert workflow._tc_state.promoted is True
    assert workflow._tc_plan is not None


# --- first provision ---------------------------------------------------------


def test_first_provision_calls_exactly_this_activity_sequence(monkeypatch):
    calls, _status, _workflow = run_saga(monkeypatch, owned_suffix=None)
    assert [name for name, _request in calls] == FIRST_PROVISION_SEQUENCE


def test_first_provision_moves_the_runtime_secret_path_once(monkeypatch):
    calls, _status, workflow = run_saga(monkeypatch, owned_suffix=None)
    checks = [
        request for name, request in calls if name == "check_temporal_runtime_secret"
    ]
    assert checks[0].project.runtime_secret_path == LEGACY_PATH
    assert checks[1].project.runtime_secret_path == MINTED_PATH
    assert workflow._temporal_runtime_secret_suffix == MINTED_SUFFIX
    assert workflow._tc_plan.temporal_project.runtime_secret_path == MINTED_PATH


def test_first_provision_status_keys_are_pinned(monkeypatch):
    _calls, status, _workflow = run_saga(monkeypatch, owned_suffix=None)
    assert set(status) == PINNED_STATUS_KEYS


def test_marked_for_deletion_runtime_secret_moves_an_owned_path(monkeypatch):
    # Secrets Manager refuses writes to a secret scheduled for deletion, so an
    # owned suffix must not pin the saga to it.
    marked = models.TemporalRuntimeSecretCheckResult(
        status={"phase": "Ready"},
        runtime_secret_path=STABLE_PATH,
        state="MarkedForDeletion",
        exists=True,
        rotate=True,
    )
    missing = models.TemporalRuntimeSecretCheckResult(
        status={"phase": "Ready"},
        runtime_secret_path=MINTED_PATH,
        state="Missing",
        exists=False,
        rotate=True,
    )
    calls, _status, workflow = run_saga(
        monkeypatch,
        owned_suffix=STABLE_SUFFIX,
        sequenced_results={"check_temporal_runtime_secret": [marked, missing]},
    )
    checks = [
        request for name, request in calls if name == "check_temporal_runtime_secret"
    ]
    assert [check.project.runtime_secret_path for check in checks] == [
        STABLE_PATH,
        MINTED_PATH,
    ]
    promote = next(
        request
        for name, request in calls
        if name == "promote_temporal_runtime_credential"
    )
    assert promote.project.runtime_secret_path == MINTED_PATH
    assert workflow._temporal_runtime_secret_suffix == MINTED_SUFFIX


# --- region drift ------------------------------------------------------------


REGION_DRIFT_TEARDOWN = [
    "delete_temporal_cloud_api_key",
    "delete_temporal_cloud_service_account",
    "disable_temporal_cloud_namespace_delete_protection",
    "delete_temporal_cloud_namespace",
    "delete_temporal_cloud_runtime_secret",
    "delete_temporal_cloud_mtls_ca_secret",
]


def region_drift_results() -> dict[str, list[Any]]:
    drifted = models.TemporalCloudNamespaceResult(
        status={
            "phase": "RegionDrift",
            "namespace": NAMESPACE,
            "desiredRegion": "aws-ca-central-1",
        },
        namespace=NAMESPACE,
        grpc_address=f"{NAMESPACE}.acct1.tmprl.cloud:7233",
        auth_mode="apiKey",
        created=False,
        region="gcp-us-central1",
    )
    recreated = models.TemporalCloudNamespaceResult(
        status={"phase": "Ready", "namespace": NAMESPACE},
        namespace=NAMESPACE,
        grpc_address=f"{NAMESPACE}.acct1.tmprl.cloud:7233",
        auth_mode="apiKey",
        created=True,
        region="aws-ca-central-1",
    )
    deleted = models.TemporalCloudStatusResult(status={"phase": "Deleted"})
    return {
        "create_or_verify_temporal_cloud_namespace": [drifted, recreated],
        **{name: [deleted] for name in REGION_DRIFT_TEARDOWN},
    }


def test_region_drift_recreates_credentials_at_a_fresh_secret_path(monkeypatch):
    calls, _status, workflow = run_saga(
        monkeypatch,
        owned_suffix=STABLE_SUFFIX,
        sequenced_results=region_drift_results(),
    )
    paths = {
        name: request.project.runtime_secret_path
        for name, request in calls
        if getattr(request, "project", None) is not None
    }
    # The teardown schedules the old path for deletion...
    assert paths["delete_temporal_cloud_runtime_secret"] == STABLE_PATH
    # ...so everything after it writes credentials somewhere else.
    for name in (
        "check_temporal_runtime_secret",
        "create_temporal_api_key_candidate_secret",
        "promote_temporal_runtime_credential",
        "mirror_temporal_runtime_secret",
    ):
        assert paths[name] == MINTED_PATH, name
    assert workflow._temporal_runtime_secret_suffix == MINTED_SUFFIX
    assert workflow._tc_plan.temporal_project.runtime_secret_path == MINTED_PATH


def test_region_drift_deletes_nexus_endpoints_before_the_namespace(monkeypatch):
    # Temporal Cloud refuses to delete a namespace an endpoint targets.
    calls, status, _workflow = run_saga(
        monkeypatch,
        owned_suffix=STABLE_SUFFIX,
        sequenced_results=region_drift_results(),
        nexus_endpoint=True,
    )
    names = [name for name, _request in calls]
    assert names.index("delete_nexus_endpoints") < names.index(
        "delete_temporal_cloud_namespace"
    )
    # Recreated once the namespace in the new region is Ready.
    assert names[-1] == "reconcile_nexus_endpoints"
    assert status["nexusEndpoints"]["phase"] == "Ready"


# --- search attributes -------------------------------------------------------


def test_search_attributes_add_exactly_one_activity(monkeypatch):
    calls, _status, _workflow = run_saga(
        monkeypatch,
        owned_suffix=STABLE_SUFFIX,
        search_attributes={"OrderId": {"type": "keyword"}},
    )
    assert [name for name, _request in calls] == SEARCH_ATTRIBUTE_SEQUENCE


def test_search_attribute_delta_deletes_only_previously_managed_names(monkeypatch):
    # The delete set is computed against one flat status blob. Pinned here because
    # sharing that blob across namespaces is what would make a loop destructive.
    calls, _status, _workflow = run_saga(
        monkeypatch,
        owned_suffix=STABLE_SUFFIX,
        search_attributes={"OrderId": {"type": "keyword"}},
        previous_status={
            "externalResources": {
                "temporalCloud": {
                    "searchAttributes": {
                        "managedNames": ["OrderId", "Retired"],
                        "attributes": [
                            {"name": "OrderId", "type": "keyword"},
                            {"name": "Retired", "type": "keyword"},
                        ],
                    }
                }
            }
        },
    )
    request = next(
        request
        for name, request in calls
        if name == "reconcile_temporal_cloud_search_attributes"
    )
    assert [attribute.name for attribute in request.desired] == ["OrderId"]
    assert list(request.delete_names) == ["Retired"]
    assert request.namespace.namespace == NAMESPACE


# --- the loop over namespace targets -----------------------------------------


def with_additional_namespace(external_plan, label="alpha"):
    """Attach one declared namespace, the way the planner will once wired."""
    from dataclasses import replace

    project = external_plan.temporal_project
    spec = models.TemporalNamespaceSpec(
        label=label,
        namespace=f"tmprl-dem-cld-demo-{label}",
        auth_mode=project.auth_mode,
        runtime_secret_path=f"tmprl-dem-cld/demo/temporal-{label}",
        mtls_ca_secret_path=f"tmprl-dem-cld/demo/temporal-{label}/ca",
        components={"sidecar": {"build": "app", "worker": True}},
    )
    return replace(
        external_plan,
        temporal_project=replace(project, additional_namespaces=(spec,)),
    )


def test_a_project_without_declared_namespaces_yields_one_target():
    external_plan = demo_project_workflow.external_resources.plan_external_resources(
        project(), checked_at=CHECKED_AT
    )
    targets = demo_project_workflow.temporal_namespace_targets(external_plan)
    assert [target.label for target in targets] == [None]
    # The primary target's plan is the plan itself, not a rebuild of it.
    assert targets[0].external_plan(external_plan) is external_plan


def test_declared_namespaces_become_additional_targets():
    external_plan = with_additional_namespace(
        demo_project_workflow.external_resources.plan_external_resources(
            project(), checked_at=CHECKED_AT
        )
    )
    targets = demo_project_workflow.temporal_namespace_targets(external_plan)
    assert [target.label for target in targets] == [None, "alpha"]

    sibling = targets[1].external_plan(external_plan).temporal_project
    assert sibling.target_namespace == "tmprl-dem-cld-demo-alpha"
    assert sibling.runtime_secret_path == "tmprl-dem-cld/demo/temporal-alpha"
    assert sibling.components == {"sidecar": {"build": "app", "worker": True}}
    # The projection must not recurse: a sibling declares no siblings of its own.
    assert sibling.additional_namespaces == ()
    # And every activity resolves it to the sibling namespace, not the primary.
    assert (
        demo_project_workflow.resources.project_temporal_namespace(
            sibling.as_demo_project()
        )
        == "tmprl-dem-cld-demo-alpha"
    )


def test_candidate_token_is_unchanged_for_the_primary_target():
    external_plan = demo_project_workflow.external_resources.plan_external_resources(
        project(), checked_at=CHECKED_AT
    )
    common = dict(workflow_id="DemoProject/demo", run_id="run-1", reconcile_sequence=3)
    assert demo_project_workflow.temporal_candidate_secret_token(
        external_plan, **common
    ) == demo_project_workflow.temporal_candidate_secret_token(
        external_plan, **common, target_namespace=None
    )


def test_candidate_token_differs_per_target():
    external_plan = demo_project_workflow.external_resources.plan_external_resources(
        project(), checked_at=CHECKED_AT
    )
    common = dict(workflow_id="DemoProject/demo", run_id="run-1", reconcile_sequence=3)
    primary = demo_project_workflow.temporal_candidate_secret_token(
        external_plan, **common
    )
    alpha = demo_project_workflow.temporal_candidate_secret_token(
        external_plan, **common, target_namespace="alpha"
    )
    beta = demo_project_workflow.temporal_candidate_secret_token(
        external_plan, **common, target_namespace="beta"
    )
    assert len({primary, alpha, beta}) == 3


def test_status_is_unchanged_when_no_namespace_is_declared():
    merged = demo_project_workflow.merge_temporal_cloud_namespace_status(
        {None: {"phase": "Ready", "namespace": NAMESPACE}}
    )
    assert merged == {"phase": "Ready", "namespace": NAMESPACE}
    assert "additionalNamespaces" not in merged


def test_additional_namespace_status_hangs_off_a_sibling_key():
    merged = demo_project_workflow.merge_temporal_cloud_namespace_status(
        {
            None: {"phase": "Ready", "namespace": NAMESPACE},
            "alpha": {"phase": "Ready", "namespace": "tmprl-dem-cld-demo-alpha"},
        }
    )
    # The primary blob stays exactly where every existing reader looks for it.
    assert merged["phase"] == "Ready"
    assert merged["namespace"] == NAMESPACE
    assert merged["additionalNamespaces"] == {
        "alpha": {"phase": "Ready", "namespace": "tmprl-dem-cld-demo-alpha"}
    }


def run_two_namespace_saga(monkeypatch, *, barrier_names: set[str] | None = None):
    workflow = DemoProjectWorkflow(DemoProjectRunRequest(project_name="demo"))
    workflow._temporal_runtime_secret_suffix = STABLE_SUFFIX
    demo_project = demo_project_with_temporal_runtime_secret_path(
        project(), STABLE_PATH
    )
    demo_project["spec"]["temporal"]["searchAttributes"] = {"OrderId": {"type": "keyword"}}

    calls: list[tuple[str, Any]] = []
    barriers = {name: asyncio.Event() for name in barrier_names or set()}
    arrivals: dict[str, list[str]] = {name: [] for name in barriers}

    async def fake_execute_activity(activity, request, **kwargs):
        name = getattr(activity, "__name__", str(activity))
        calls.append((name, request))
        target = getattr(request, "project", None)
        target_name = target.target_namespace or NAMESPACE
        if name in barriers:
            arrivals[name].append(target_name)
            if len(arrivals[name]) == 2:
                barriers[name].set()
            await barriers[name].wait()
        path = getattr(target, "runtime_secret_path", STABLE_PATH)
        results = activity_results(path)
        if name not in results:
            raise AssertionError(f"unexpected activity in the saga: {name}")
        if name == "create_or_verify_temporal_cloud_namespace":
            namespace = target.target_namespace or NAMESPACE
            return models.TemporalCloudNamespaceResult(
                status={"phase": "Ready", "namespace": namespace},
                namespace=namespace,
                grpc_address=f"{namespace}.acct1.tmprl.cloud:7233",
                auth_mode="apiKey",
                created=True,
                region="aws-us-west-2",
            )
        return results[name]

    monkeypatch.setattr(
        demo_project_workflow.workflow,
        "info",
        lambda: SimpleNamespace(workflow_id="DemoProject/demo", run_id="run-1"),
    )
    monkeypatch.setattr(
        demo_project_workflow.workflow,
        "now",
        lambda: datetime(2026, 6, 20, tzinfo=UTC),
    )
    monkeypatch.setattr(
        demo_project_workflow.workflow, "execute_activity", fake_execute_activity
    )

    external_plan = with_additional_namespace(
        demo_project_workflow.external_resources.plan_external_resources(
            demo_project, checked_at=CHECKED_AT
        )
    )
    async def run_reconcile():
        return await asyncio.wait_for(
            workflow._reconcile_temporal_cloud(external_plan, previous_status={}),
            timeout=2,
        )

    status = asyncio.run(run_reconcile())
    return calls, status, workflow, arrivals


def test_both_namespaces_are_created(monkeypatch):
    calls, _status, _workflow, _arrivals = run_two_namespace_saga(monkeypatch)
    created = [
        request.project.target_namespace or NAMESPACE
        for name, request in calls
        if name == "create_or_verify_temporal_cloud_namespace"
    ]
    assert created == [NAMESPACE, "tmprl-dem-cld-demo-alpha"]


def test_each_namespace_stages_against_its_own_secret_path(monkeypatch):
    calls, _status, _workflow, _arrivals = run_two_namespace_saga(monkeypatch)
    paths = [
        request.project.runtime_secret_path
        for name, request in calls
        if name == "create_temporal_api_key_candidate_secret"
    ]
    assert paths == [STABLE_PATH, "tmprl-dem-cld/demo/temporal-alpha"]


def test_each_namespace_uses_a_distinct_candidate_token(monkeypatch):
    calls, _status, _workflow, _arrivals = run_two_namespace_saga(monkeypatch)
    tokens = [
        request.candidate_secret_token
        for _name, request in calls
        if getattr(request, "candidate_secret_token", None) is not None
    ]
    # Distinct per target, so the two never overwrite each other's staged secret.
    assert len(set(tokens)) == 2


def test_project_scoped_steps_run_only_for_the_primary_namespace(monkeypatch):
    calls, _status, _workflow, _arrivals = run_two_namespace_saga(monkeypatch)
    names = [name for name, _request in calls]
    # Search attributes would delete the other namespace's attributes; the mirror
    # would overwrite the workers' live credentials.
    assert names.count("reconcile_temporal_cloud_search_attributes") == 1
    assert names.count("mirror_temporal_runtime_secret") == 1


def test_two_namespace_status_keeps_the_primary_blob_in_place(monkeypatch):
    _calls, status, _workflow, _arrivals = run_two_namespace_saga(monkeypatch)
    assert status["namespace"] == NAMESPACE
    assert status["apiKey"] == {"keyId": "key-abc"}
    assert set(status["additionalNamespaces"]) == {"alpha"}
    assert status["additionalNamespaces"]["alpha"]["namespace"] == (
        "tmprl-dem-cld-demo-alpha"
    )


def test_both_targets_are_recorded_for_compensation(monkeypatch):
    _calls, _status, workflow, _arrivals = run_two_namespace_saga(monkeypatch)
    assert [context.target.label for context in workflow._tc_targets] == [None, "alpha"]
    # The project-level plan slot still tracks the primary target only.
    assert workflow._tc_plan.temporal_project.target_namespace is None


def test_namespace_pipelines_run_independent_steps_concurrently(monkeypatch):
    barrier_names = {
        "create_or_verify_temporal_cloud_namespace",
        "ensure_temporal_cloud_namespace_tags",
        "ensure_temporal_cloud_namespace_readers_access",
        "create_or_verify_temporal_cloud_service_account",
        "check_temporal_runtime_secret",
        "create_temporal_api_key_candidate_secret",
        "run_temporal_runtime_candidate_canary",
        "promote_temporal_runtime_credential",
        "delete_temporal_runtime_candidate_secret",
    }

    _calls, status, _workflow, arrivals = run_two_namespace_saga(
        monkeypatch,
        barrier_names=barrier_names,
    )

    expected = {NAMESPACE, "tmprl-dem-cld-demo-alpha"}
    assert {name: set(targets) for name, targets in arrivals.items()} == {
        name: expected for name in barrier_names
    }
    assert status["phase"] == "Ready"
    assert status["additionalNamespaces"]["alpha"]["phase"] == "Ready"


def test_namespace_failure_waits_for_sibling_to_settle_before_raising(monkeypatch):
    workflow = DemoProjectWorkflow(DemoProjectRunRequest(project_name="demo"))
    workflow._temporal_runtime_secret_suffix = STABLE_SUFFIX
    demo_project = demo_project_with_temporal_runtime_secret_path(
        project(), STABLE_PATH
    )
    handler = "tmprl-dem-cld-demo-alpha"
    handler_failed = asyncio.Event()
    primary_finished = asyncio.Event()
    compensations: list[str] = []

    async def fake_execute_activity(activity, request, **kwargs):
        name = getattr(activity, "__name__", str(activity))
        target = getattr(request, "project", None)
        target_name = target.target_namespace or NAMESPACE
        path = getattr(target, "runtime_secret_path", STABLE_PATH)
        results = activity_results(path)
        if name == "create_or_verify_temporal_cloud_namespace":
            return models.TemporalCloudNamespaceResult(
                status={"phase": "Ready", "namespace": target_name},
                namespace=target_name,
                grpc_address=f"{target_name}.acct1.tmprl.cloud:7233",
                auth_mode="apiKey",
                created=True,
                region="aws-us-west-2",
            )
        if name == "run_temporal_runtime_candidate_canary":
            if target_name == handler:
                handler_failed.set()
                raise RuntimeError("handler candidate failed")
            await handler_failed.wait()
        result = results[name]
        if name == "mirror_temporal_runtime_secret":
            primary_finished.set()
        return result

    async def fake_compensate(
        external_plan,
        *,
        state,
        candidate_secret_token,
    ):
        compensations.append(
            external_plan.temporal_project.target_namespace or NAMESPACE
        )

    monkeypatch.setattr(
        demo_project_workflow.workflow,
        "info",
        lambda: SimpleNamespace(workflow_id="DemoProject/demo", run_id="run-1"),
    )
    monkeypatch.setattr(
        demo_project_workflow.workflow,
        "now",
        lambda: datetime(2026, 6, 20, tzinfo=UTC),
    )
    monkeypatch.setattr(
        demo_project_workflow.workflow, "execute_activity", fake_execute_activity
    )
    monkeypatch.setattr(
        workflow, "_compensate_temporal_cloud_reconcile", fake_compensate
    )
    external_plan = with_additional_namespace(
        demo_project_workflow.external_resources.plan_external_resources(
            demo_project, checked_at=CHECKED_AT
        )
    )

    async def run_reconcile():
        return await asyncio.wait_for(
            workflow._reconcile_temporal_cloud(external_plan, previous_status={}),
            timeout=2,
        )

    with pytest.raises(RuntimeError, match="handler candidate failed"):
        asyncio.run(run_reconcile())

    assert primary_finished.is_set()
    assert compensations == [handler, NAMESPACE]


# --- Nexus Endpoints ---------------------------------------------------------


def with_nexus_endpoint(external_plan, target="primary"):
    from dataclasses import replace

    endpoint = models.NexusEndpointSpec(
        name="orders",
        target_namespace=target,
        target_task_queue="orders-tq",
        allowed_caller_namespaces=("alpha",),
    )
    return replace(
        external_plan,
        temporal_project=replace(
            external_plan.temporal_project, nexus_endpoints=(endpoint,)
        ),
    )


def run_nexus_saga(monkeypatch, *, namespace_phase="Ready"):
    workflow = DemoProjectWorkflow(DemoProjectRunRequest(project_name="demo"))
    workflow._temporal_runtime_secret_suffix = STABLE_SUFFIX
    demo_project = demo_project_with_temporal_runtime_secret_path(project(), STABLE_PATH)
    calls: list[tuple[str, Any]] = []

    async def fake_execute_activity(activity, request, **kwargs):
        name = getattr(activity, "__name__", str(activity))
        calls.append((name, request))
        if name == "reconcile_nexus_endpoints":
            return models.NexusEndpointResult(
                status={"phase": "Ready", "endpoints": []},
                endpoint_ids={"orders": "endpoint-orders"},
            )
        if name == "create_or_verify_temporal_cloud_namespace":
            return models.TemporalCloudNamespaceResult(
                status={"phase": namespace_phase, "namespace": NAMESPACE},
                namespace=NAMESPACE,
                grpc_address=f"{NAMESPACE}.acct1.tmprl.cloud:7233",
                auth_mode="apiKey",
                created=True,
                region="aws-us-west-2",
            )
        return activity_results(STABLE_PATH)[name]

    monkeypatch.setattr(
        demo_project_workflow.workflow,
        "info",
        lambda: SimpleNamespace(workflow_id="DemoProject/demo", run_id="run-1"),
    )
    monkeypatch.setattr(
        demo_project_workflow.workflow,
        "now",
        lambda: datetime(2026, 6, 20, tzinfo=UTC),
    )
    monkeypatch.setattr(
        demo_project_workflow.workflow, "execute_activity", fake_execute_activity
    )

    external_plan = with_nexus_endpoint(
        demo_project_workflow.external_resources.plan_external_resources(
            demo_project, checked_at=CHECKED_AT
        )
    )
    status = asyncio.run(
        workflow._reconcile_temporal_cloud(external_plan, previous_status={})
    )
    return calls, status


def test_nexus_endpoints_reconcile_after_the_namespaces(monkeypatch):
    calls, status = run_nexus_saga(monkeypatch)
    names = [name for name, _request in calls]
    assert names[-1] == "reconcile_nexus_endpoints"
    assert names.index("create_or_verify_temporal_cloud_namespace") < names.index(
        "reconcile_nexus_endpoints"
    )
    assert status["nexusEndpoints"]["phase"] == "Ready"


def test_nexus_endpoints_are_skipped_when_a_namespace_is_not_ready(monkeypatch):
    # An endpoint cannot target a namespace that never came up.
    calls, status = run_nexus_saga(monkeypatch, namespace_phase="Error")
    assert "reconcile_nexus_endpoints" not in [name for name, _request in calls]
    assert "nexusEndpoints" not in status


def test_no_nexus_activity_when_none_are_declared(monkeypatch):
    calls, status, _workflow = run_saga(monkeypatch, owned_suffix=STABLE_SUFFIX)
    assert "reconcile_nexus_endpoints" not in [name for name, _request in calls]
    assert "nexusEndpoints" not in status


def run_finalize(monkeypatch, *, namespaces=None):
    """Drive the real finalize teardown, recording (activity, target namespace)."""
    workflow = DemoProjectWorkflow(DemoProjectRunRequest(project_name="demo"))
    demo_project = project()
    demo_project["spec"]["temporal"] = {
        "namespaces": namespaces or {},
        "nexusEndpoints": {
            "demo-orders": {
                "targetNamespace": "primary",
                "targetTaskQueue": "orders-tq",
                "allowedCallerNamespaces": ["primary"],
            }
        },
    }
    calls: list[tuple[str, str | None]] = []

    async def fake_execute_activity(activity, request, **kwargs):
        name = getattr(activity, "__name__", str(activity))
        target = getattr(getattr(request, "project", None), "target_namespace", None)
        calls.append((name, target))
        if name.startswith("delete_nexus"):
            return models.NexusEndpointResult(status={"phase": "Deleted"}, endpoint_ids={})
        return models.TemporalCloudStatusResult(status={"phase": "Deleted"})

    monkeypatch.setattr(
        demo_project_workflow.workflow,
        "info",
        lambda: SimpleNamespace(workflow_id="DemoProject/demo", run_id="run-1"),
    )
    monkeypatch.setattr(
        demo_project_workflow.workflow,
        "now",
        lambda: datetime(2026, 6, 20, tzinfo=UTC),
    )
    monkeypatch.setattr(
        demo_project_workflow.workflow, "execute_activity", fake_execute_activity
    )

    plan = workflow._project_finalization_external_plan(
        demo_project, {"checkedAt": CHECKED_AT}
    )
    request = demo_project_workflow.external_resources.DeleteTemporalRuntimeCandidateSecretRequest(
        checked_at=plan.checked_at, project=plan.temporal_project
    )
    status = asyncio.run(
        workflow._delete_project_temporal_cloud_resources(
            {"checkedAt": CHECKED_AT}, plan, request
        )
    )
    return calls, status


NAMESPACE_TEARDOWN_ACTIVITIES = [
    "delete_temporal_cloud_api_key",
    "delete_temporal_cloud_service_account",
    "disable_temporal_cloud_namespace_delete_protection",
    "delete_temporal_cloud_namespace",
    "delete_temporal_cloud_runtime_secret",
    "delete_temporal_cloud_mtls_ca_secret",
]


def test_finalize_deletes_endpoints_before_any_namespace(monkeypatch):
    names = [name for name, _t in run_finalize(monkeypatch)[0]]
    assert names[0] == "delete_nexus_endpoints"
    assert names.index("delete_nexus_endpoints") < names.index(
        "delete_temporal_cloud_namespace"
    )


def test_finalize_without_declared_namespaces_is_unchanged(monkeypatch):
    calls, _status = run_finalize(monkeypatch)
    assert [name for name, _t in calls] == [
        "delete_nexus_endpoints",
        *NAMESPACE_TEARDOWN_ACTIVITIES,
    ]
    # Nothing is retargeted: every delete resolves the project's own namespace.
    assert {target for _n, target in calls} == {None}


def test_finalize_tears_down_every_declared_namespace(monkeypatch):
    calls, status = run_finalize(
        monkeypatch, namespaces={"alpha": {}, "beta": {}}
    )
    names = [name for name, _t in calls]
    assert names == [
        "delete_nexus_endpoints",
        *NAMESPACE_TEARDOWN_ACTIVITIES,  # beta, newest first
        *NAMESPACE_TEARDOWN_ACTIVITIES,  # alpha
        *NAMESPACE_TEARDOWN_ACTIVITIES,  # the project's own, last
    ]
    targets = [target for _n, target in calls[1:]]
    assert targets[0:6] == ["tmprl-dem-cld-demo-beta"] * 6
    assert targets[6:12] == ["tmprl-dem-cld-demo-alpha"] * 6
    assert targets[12:18] == [None] * 6
    assert sorted(status["additionalNamespaces"]) == ["alpha", "beta"]


def test_finalize_tears_down_a_namespace_only_a_nexus_endpoint_references(monkeypatch):
    # No component binds to it, so it is only required because it was declared.
    calls, _status = run_finalize(monkeypatch, namespaces={"lonely": {}})
    deleted = [
        target
        for name, target in calls
        if name == "delete_temporal_cloud_namespace"
    ]
    assert deleted == ["tmprl-dem-cld-demo-lonely", None]


# --- orphaned namespaces: reported, never deleted ---------------------------


def previous_status_with_labels(*labels):
    return {
        "externalResources": {
            "temporalCloud": {
                "additionalNamespaces": {label: {"phase": "Ready"} for label in labels}
            }
        }
    }


def test_no_orphan_report_when_nothing_was_dropped(monkeypatch):
    _calls, status, _workflow = run_saga(monkeypatch, owned_suffix=STABLE_SUFFIX)
    assert "orphanedNamespaces" not in status


def test_dropping_a_declared_namespace_is_reported(monkeypatch):
    # The project declares none now, but the previous pass reconciled alpha.
    _calls, status, _workflow = run_saga(
        monkeypatch,
        owned_suffix=STABLE_SUFFIX,
        previous_status=previous_status_with_labels("alpha"),
    )
    orphaned = status["orphanedNamespaces"]
    assert orphaned["phase"] == "Orphaned"
    assert [entry["label"] for entry in orphaned["namespaces"]] == ["alpha"]
    assert orphaned["namespaces"][0]["namespace"] == "tmprl-dem-cld-demo-alpha"
    assert orphaned["namespaces"][0]["runtimeSecretPath"] == (
        "tmprl-dem-cld/demo/temporal-alpha"
    )


def test_orphan_report_never_deletes_anything(monkeypatch):
    calls, _status, _workflow = run_saga(
        monkeypatch,
        owned_suffix=STABLE_SUFFIX,
        previous_status=previous_status_with_labels("alpha", "beta"),
    )
    # Detection is derived from status; it must not reach for a delete activity.
    assert not [name for name, _r in calls if name.startswith("delete_temporal_cloud")]
    assert [name for name, _r in calls] == STEADY_STATE_SEQUENCE


def test_previous_label_reader_tolerates_missing_status():
    from demo_registry_operator.workflows.demo_project import (
        previous_temporal_namespace_labels,
    )

    assert previous_temporal_namespace_labels({}) == []
    assert previous_temporal_namespace_labels({"externalResources": None}) == []
    assert previous_temporal_namespace_labels(
        {"externalResources": {"temporalCloud": {}}}
    ) == []
    assert previous_temporal_namespace_labels(
        {"externalResources": {"temporalCloud": {"additionalNamespaces": "nope"}}}
    ) == []
    assert previous_temporal_namespace_labels(previous_status_with_labels("b", "a")) == [
        "a",
        "b",
    ]
