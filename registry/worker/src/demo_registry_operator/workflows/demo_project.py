from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

from demo_registry_operator import errors, resources
from demo_registry_operator.activities import external_resources
from demo_registry_operator.activities import force_reconcile as force_reconcile_activities
from demo_registry_operator.activities import reconcile as reconcile_activities
from demo_registry_operator.activities import revision as revision_activities
from demo_registry_operator.workflows import demo_project_artifact_build as artifact_build_workflow
from demo_registry_operator.workflows import demo_project_kubernetes_runtime as kubernetes_runtime_workflow
from demo_registry_operator.workflows import demo_project_serverless_lambda as serverless_lambda_workflow
from demo_registry_operator.workflows import messages


MAX_RECONCILES_PER_WORKFLOW_RUN = 50

COMMON_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=8,
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)
STATUS_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=12,
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)
CHECK_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=3,
    non_retryable_error_types=errors.NON_RETRYABLE_ERROR_TYPES,
)
TEMPORAL_RUNTIME_CANDIDATE_CANARY_START_TO_CLOSE_TIMEOUT = timedelta(minutes=10)
TEMPORAL_RUNTIME_CANDIDATE_CANARY_HEARTBEAT_TIMEOUT = timedelta(minutes=2)
FAILED_RECONCILE_PHASES = {
    "CandidateNotReady",
    "Error",
    "SmokeCheckFailed",
    "WorkerRolloutFailed",
}
# Terminal rollout outcomes: the rollout was attempted and failed definitively.
# We settle (remember the identity) so the entity workflow stops re-attempting
# the identical rollout; recovery requires a new reconcile identity (new source)
# or an explicit force_reconcile.
TERMINAL_ROLLOUT_FAILURE_PHASES = {
    "SmokeCheckFailed",
    "WorkerRolloutFailed",
}
# Phases where the reconcile settled while blocked on an external resource the
# operator does not own (e.g. a project-owner AWS Secrets Manager value that must
# be created out-of-band). Nothing changes the reconcile key when that value later
# appears, so the entity workflow re-verifies on a durable timer until it clears.
EXTERNAL_RESOURCE_WAIT_PHASES = {
    "WaitingForExternalResources",
}
EXTERNAL_RESOURCE_RECHECK_INTERVAL = timedelta(minutes=2)
FORCE_TEMPORAL_AUTH_ROTATION_REASON = "rotate-temporal-auth"


@dataclass
class TemporalCloudReconcileState:
    namespace: external_resources.TemporalCloudNamespaceResult | None = None
    search_attributes: dict[str, Any] | None = None
    service_account: external_resources.TemporalCloudServiceAccountResult | None = None
    ca: external_resources.TemporalCloudCredentialStageResult | None = None
    mtls_config: external_resources.TemporalCloudStatusResult | None = None
    stage: external_resources.TemporalCloudCredentialStageResult | None = None
    canary: external_resources.TemporalCloudStatusResult | None = None
    ca_promote: external_resources.TemporalCloudPromotionResult | None = None
    promote: (
        external_resources.TemporalCloudPromotionResult | external_resources.TemporalCloudCredentialStageResult | None
    ) = None
    mirror: external_resources.TemporalCloudStatusResult | None = None
    ca_promoted: bool = False
    promoted: bool = False
    # Region-drift bookkeeping: when the namespace was destroyed and recreated in
    # a new region this reconcile, report it in status so the migration (and data
    # loss) is visible rather than looking like a plain first-time create.
    desired_region: str | None = None
    previous_region: str | None = None
    region_recreated: bool = False

    def merged_status(self) -> dict[str, Any]:
        if self.namespace is None:
            return {}
        credential = self.promote if self.promote is not None else self.stage
        merged = merge_temporal_cloud_status(
            self.namespace.status,
            ca_status=self.ca.status if self.ca else None,
            mtls_config_status=(
                self.mtls_config.status if self.mtls_config else None
            ),
            service_account_status=(
                self.service_account.status if self.service_account else None
            ),
            search_attributes_status=self.search_attributes,
            credential_status=credential.status if credential else None,
            mirror_status=self.mirror.status if self.mirror else None,
        )
        if self.region_recreated:
            merged["regionMigration"] = {
                "recreated": True,
                "fromRegion": self.previous_region,
                "toRegion": self.desired_region,
            }
        return merged


@dataclass
class TemporalRuntimeStageOutcome:
    external_plan: Any
    staged: bool


@dataclass
class DemoProjectReconcileInput:
    demo_project: dict[str, Any] | None
    force_reconcile: dict[str, str] | None
    force_reconcile_requested: bool
    rotation_due: bool

    @property
    def force_temporal_auth_rotation(self) -> bool:
        return (
            self.force_reconcile_requested
            and isinstance(self.force_reconcile, dict)
            and self.force_reconcile.get("reason") == FORCE_TEMPORAL_AUTH_ROTATION_REASON
        )


@dataclass
class DemoProjectExternalResources:
    demo_project: dict[str, Any]
    status: dict[str, Any]


@dataclass
class DemoProjectFinalizationDeletes:
    namespace_action: str
    source_action: str
    source_auth_action: str
    runtime_namespace_actions: list[dict[str, Any]]
    revision_actions: list[dict[str, Any]]


def reconcile_identity(demo_project: dict[str, Any]) -> dict[str, str]:
    annotations = resources.metadata_annotations(demo_project)
    identity = {
        "reconcileKey": resources.demo_project_reconcile_key(demo_project),
    }
    source_commit_sha = annotations.get(resources.SOURCE_COMMIT_ANNOTATION)
    if source_commit_sha:
        identity["sourceCommitSha"] = source_commit_sha
    source_artifact_digest = annotations.get(resources.SOURCE_ARTIFACT_DIGEST_ANNOTATION)
    if source_artifact_digest:
        identity["sourceArtifactDigest"] = source_artifact_digest
    return identity


def force_reconcile_request(
    token: str,
    reason: str | None = None,
) -> dict[str, str] | None:
    if not isinstance(token, str) or not token.strip():
        return None
    request = {"token": token.strip()}
    if isinstance(reason, str) and reason.strip():
        request["reason"] = reason.strip()
    return request


def status_with_serverless_lambda(
    runtime_status: dict[str, Any],
    serverless_status: dict[str, Any] | None,
) -> dict[str, Any]:
    if not isinstance(serverless_status, dict):
        return runtime_status

    merged = {
        **runtime_status,
        "serverlessWorkerStatus": serverless_status,
    }
    phase = serverless_status.get("phase")
    if phase not in {"Ready", "NotRequired"}:
        return {
            **merged,
            "phase": "Error",
            "message": "Serverless Lambda worker reconciliation failed.",
        }
    return merged


def status_with_temporal_runtime_secret_path(
    status: dict[str, Any],
    demo_project: dict[str, Any],
) -> dict[str, Any]:
    if not resources.temporal_components(demo_project):
        return status
    return {
        **status,
        "temporalRuntimeSecretPath": resources.temporal_secret_remote_path(
            demo_project
        ),
    }


def previous_reconcile_status(
    workflow_status: dict[str, Any],
    demo_project: dict[str, Any],
) -> dict[str, Any]:
    persisted_status = demo_project.get("status")
    if not isinstance(persisted_status, dict):
        persisted_status = {}
    return {**persisted_status, **workflow_status}


def status_reconcile_identity(status: dict[str, Any]) -> dict[str, str] | None:
    reconcile_key = status.get("reconcileKey")
    if not isinstance(reconcile_key, str) or not reconcile_key:
        return None
    identity = {"reconcileKey": reconcile_key}
    build_status = status.get("buildStatus")
    build_status = build_status if isinstance(build_status, dict) else {}
    source_commit_sha = status.get("sourceCommitSha") or build_status.get(
        "sourceCommitSha"
    )
    if isinstance(source_commit_sha, str) and source_commit_sha:
        identity["sourceCommitSha"] = source_commit_sha
    source_artifact_digest = status.get("sourceArtifactDigest") or build_status.get(
        "sourceArtifactDigest"
    )
    if isinstance(source_artifact_digest, str) and source_artifact_digest:
        identity["sourceArtifactDigest"] = source_artifact_digest
    return identity


def rotation_reconcile_identity(
    demo_project: dict[str, Any],
    rotation: dict[str, Any] | None,
) -> dict[str, str]:
    identity = reconcile_identity(demo_project)
    rotate_at = rotation.get("rotateAt") if isinstance(rotation, dict) else None
    auth_mode = rotation.get("authMode") if isinstance(rotation, dict) else None
    key_id = rotation.get("keyId") if isinstance(rotation, dict) else None
    client_serial = (
        rotation.get("clientCertSerialNumber") if isinstance(rotation, dict) else None
    )
    if isinstance(auth_mode, str) and auth_mode:
        identity["temporalAuthMode"] = auth_mode
    if isinstance(rotate_at, str) and rotate_at:
        identity["temporalAuthRotateAt"] = rotate_at
    if isinstance(key_id, str) and key_id:
        identity["temporalApiKeyId"] = key_id
    if isinstance(client_serial, str) and client_serial:
        identity["temporalClientCertSerialNumber"] = client_serial
    return identity


def demo_project_with_temporal_api_key(
    demo_project: dict[str, Any],
    temporal_status: dict[str, Any],
) -> dict[str, Any]:
    api_key = temporal_status.get("apiKey")
    if not isinstance(api_key, dict):
        return demo_project

    key_id = api_key.get("keyId")
    if not isinstance(key_id, str) or not key_id:
        return demo_project

    metadata = demo_project.get("metadata", {})
    annotations = metadata.get("annotations", {}) if isinstance(metadata, dict) else {}
    rendered = {**demo_project}
    rendered_metadata = {**metadata} if isinstance(metadata, dict) else {}
    rendered_annotations = {**annotations} if isinstance(annotations, dict) else {}
    rendered_annotations[resources.TEMPORAL_API_KEY_ID_ANNOTATION] = key_id

    expiry_time = api_key.get("expiryTime")
    if isinstance(expiry_time, str) and expiry_time:
        rendered_annotations[resources.TEMPORAL_API_KEY_EXPIRY_ANNOTATION] = expiry_time
    created_at = api_key.get("createdAt")
    if isinstance(created_at, str) and created_at:
        rendered_annotations[resources.TEMPORAL_API_KEY_SYNCED_AT_ANNOTATION] = created_at

    rendered_metadata["annotations"] = rendered_annotations
    rendered["metadata"] = rendered_metadata
    return rendered


def demo_project_with_temporal_runtime_secret_path(
    demo_project: dict[str, Any],
    runtime_secret_path: str,
) -> dict[str, Any]:
    rendered = {**demo_project}
    spec = demo_project.get("spec")
    rendered_spec = {**spec} if isinstance(spec, dict) else {}
    temporal = rendered_spec.get("temporal")
    rendered_temporal = {**temporal} if isinstance(temporal, dict) else {}
    rendered_temporal["runtimeSecretPath"] = runtime_secret_path
    rendered_spec["temporal"] = rendered_temporal
    rendered["spec"] = rendered_spec
    return rendered


def temporal_auth_rotation_state(
    temporal_status: dict[str, Any],
) -> dict[str, Any] | None:
    api_key = temporal_status.get("apiKey")
    if isinstance(api_key, dict):
        expiry_time = api_key.get("expiryTime")
        if not isinstance(expiry_time, str) or not expiry_time:
            return None

        expiry = parse_datetime(expiry_time)
        if expiry is None:
            return None

        rotate_before_days = api_key.get("rotateBeforeExpiryDays")
        if not isinstance(rotate_before_days, int) or rotate_before_days < 0:
            rotate_before_days = 10
        rotate_at = expiry - timedelta(days=rotate_before_days)

        state: dict[str, Any] = {
            "authMode": "apiKey",
            "expiryTime": expiry.isoformat(),
            "rotateAt": rotate_at.isoformat(),
            "rotateBeforeExpiryDays": rotate_before_days,
        }
        key_id = api_key.get("keyId")
        if isinstance(key_id, str) and key_id:
            state["keyId"] = key_id
        return state

    mtls = temporal_status.get("mtls")
    mtls = mtls if isinstance(mtls, dict) else {}
    client = mtls.get("clientCertificate")
    if not isinstance(client, dict):
        return None
    rotate_at = client.get("rotateAt")
    if not isinstance(rotate_at, str) or not rotate_at:
        return None
    parsed_rotate_at = parse_datetime(rotate_at)
    if parsed_rotate_at is None:
        return None
    state = {
        "authMode": "mtls",
        "rotateAt": parsed_rotate_at.isoformat(),
    }
    serial = client.get("serialNumber")
    if isinstance(serial, str) and serial:
        state["clientCertSerialNumber"] = serial
    return state


def temporal_api_key_rotation_state(
    temporal_status: dict[str, Any],
) -> dict[str, Any] | None:
    state = temporal_auth_rotation_state(temporal_status)
    if isinstance(state, dict) and state.get("authMode") == "apiKey":
        return state
    return None


def previous_temporal_search_attributes(status: dict[str, Any]) -> dict[str, str]:
    search_attributes = previous_temporal_search_attributes_status(status)
    if not search_attributes:
        return {}

    managed_names = {
        name
        for name in search_attributes.get("managedNames", [])
        if isinstance(name, str) and name
    }
    attributes = search_attributes.get("attributes")
    if not isinstance(attributes, list):
        return {}

    managed: dict[str, str] = {}
    for attribute in attributes:
        if not isinstance(attribute, dict):
            continue
        name = attribute.get("name")
        if not isinstance(name, str) or not name:
            continue
        if managed_names and name not in managed_names:
            continue
        if not managed_names and attribute.get("managed") is not True:
            continue
        managed[name] = resources.temporal_search_attribute_type(
            attribute.get("type")
        )
    return dict(sorted(managed.items()))


def previous_temporal_search_attributes_status(
    status: dict[str, Any],
) -> dict[str, Any]:
    external_resources_status = status.get("externalResources")
    if isinstance(external_resources_status, dict):
        temporal_cloud = external_resources_status.get("temporalCloud")
        if isinstance(temporal_cloud, dict):
            search_attributes = temporal_cloud.get("searchAttributes")
            if isinstance(search_attributes, dict):
                return search_attributes

    temporal_cloud = status.get("temporalCloud")
    if isinstance(temporal_cloud, dict):
        search_attributes = temporal_cloud.get("searchAttributes")
        if isinstance(search_attributes, dict):
            return search_attributes
    return {}


def temporal_search_attributes_reconcile_status(
    checked_at: str,
    *,
    namespace: str | None,
    desired: dict[str, external_resources.TemporalCloudSearchAttribute],
    previous_managed: dict[str, str],
    statuses: list[dict[str, Any]],
) -> dict[str, Any]:
    attributes: list[dict[str, Any]] = []
    managed_names: set[str] = set()
    deleted: list[str] = []
    phases: set[str] = set()
    reason: str | None = None
    message: str | None = None

    for status in statuses:
        phase = status.get("phase")
        if isinstance(phase, str):
            phases.add(phase)
        source = status.get("searchAttribute")
        if not isinstance(source, dict):
            continue
        name = source.get("name")
        if not isinstance(name, str) or not name:
            continue
        action = source.get("action")
        entry = dict(source)
        managed = (
            action == "created"
            or (
                name in previous_managed
                and action in {"verified", "created", "drifted"}
            )
        )
        entry["managed"] = managed
        attributes.append(entry)
        if action == "deleted":
            deleted.append(name)
        elif managed:
            managed_names.add(name)
        if status.get("reason") and reason is None:
            reason = str(status["reason"])
        if status.get("message") and message is None:
            message = str(status["message"])

    if "Error" in phases:
        phase = "Error"
    elif "ManualSetupRequired" in phases:
        phase = "ManualSetupRequired"
    elif statuses:
        phase = "Ready"
    else:
        phase = "NotRequired"

    rendered: dict[str, Any] = {
        "phase": phase,
        "checkedAt": checked_at,
        "namespace": namespace,
        "ownershipPolicy": "delete-only-previously-managed",
        "attributes": sorted(attributes, key=lambda item: str(item.get("name", ""))),
        "managedNames": sorted(managed_names),
    }
    if deleted:
        rendered["deletedNames"] = sorted(deleted)
    if reason:
        rendered["reason"] = reason
    if message:
        rendered["message"] = message
    return rendered


def rotation_datetime(rotation: dict[str, Any] | None) -> datetime | None:
    if not isinstance(rotation, dict):
        return None
    rotate_at = rotation.get("rotateAt")
    if not isinstance(rotate_at, str) or not rotate_at:
        return None
    return parse_datetime(rotate_at)


def temporal_candidate_secret_token(
    external_plan: Any,
    *,
    workflow_id: str,
    run_id: str,
    reconcile_sequence: int,
) -> str:
    project = external_plan.temporal_project
    parts = [
        workflow_id,
        run_id,
        str(reconcile_sequence),
        str(external_plan.checked_at),
        str(project.name),
        str(project.namespace),
        str(project.auth_mode),
        str(project.runtime_secret_path),
        str(project.mtls_ca_secret_path),
    ]
    payload = "".join(f"{len(part)}:{part}" for part in parts)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return digest[:20]


def parse_datetime(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


@workflow.defn
class DemoProjectWorkflow:
    """Durable entity workflow for one DemoProject."""

    @workflow.init
    def __init__(
        self,
        request: messages.DemoProjectRunRequest,
    ) -> None:
        self._project_name = request.project_name
        self._pending_spec: dict[str, Any] | None = None
        self._pending_reconcile_identity: dict[str, str] | None = None
        self._current_reconcile_identity: dict[str, str] | None = None
        self._completed_reconcile_identity: dict[str, str] | None = None
        self._reconcile_sequence = 0
        self._shutdown = False
        self._finalize = False
        self._continue_as_new_for_worker_version = False
        self._pending_force_reconcile: dict[str, str] | None = None
        self._current_force_reconcile_token: str | None = None
        self._completed_force_reconcile_token: str | None = None
        self._finalize_spec: dict[str, Any] | None = None
        self._last_build_status: dict[str, Any] | None = None
        self._last_demo_project: dict[str, Any] | None = None
        self._temporal_api_key_rotation: dict[str, Any] | None = None
        self._temporal_runtime_secret_suffix: str | None = (
            request.carried_temporal_runtime_secret_suffix
        )
        # Captured per-reconcile so a failure anywhere in the pass (incl. child
        # workflows) can compensate the Temporal Cloud resources created THIS pass.
        self._tc_state: TemporalCloudReconcileState | None = None
        self._tc_plan: Any = None
        self._tc_token: str | None = None
        self._status: dict[str, Any] = {
            "phase": "Starting",
            "message": "Workflow has not received project state.",
        }

    @workflow.run
    async def run(
        self,
        request: messages.DemoProjectRunRequest,
    ) -> None:
        project_name = request.project_name
        self._project_name = project_name
        self._apply_carried_state(request, project_name)

        while not self._shutdown:
            await self._wait_for_next_action()
            if self._shutdown:
                break

            if self._finalize:
                await self._run_finalize(project_name)
                return

            if self._continue_as_new_for_worker_version:
                self._continue_as_new_if_needed(project_name)
                continue

            reconcile_input = await self._select_demo_project_for_reconcile(
                project_name
            )
            reconciled = await self._run_demo_project_reconcile(
                project_name,
                reconcile_input,
            )
            if reconciled:
                self._complete_current_reconcile()
            self._continue_as_new_if_needed(project_name)

        self._status = {
            "project": project_name,
            "phase": "Stopped",
            "message": "Workflow received shutdown signal.",
        }

    def _apply_carried_state(
        self,
        request: messages.DemoProjectRunRequest,
        project_name: str,
    ) -> None:
        # Do NOT clobber a pending spec set by a start signal. With
        # SignalWithStart the reconcile signal is delivered on the first workflow
        # task and its handler sets self._pending_spec before/around run()'s body;
        # only adopt a spec actually carried over from continue-as-new.
        if request.carried_pending_spec is not None:
            self._pending_spec = request.carried_pending_spec
            self._pending_reconcile_identity = reconcile_identity(
                request.carried_pending_spec
            )
        if request.carried_last_build_status is not None:
            self._last_build_status = request.carried_last_build_status
        if request.carried_last_demo_project is not None:
            self._last_demo_project = request.carried_last_demo_project
        if request.carried_temporal_api_key_rotation is not None:
            self._temporal_api_key_rotation = (
                request.carried_temporal_api_key_rotation
            )
        if request.carried_temporal_runtime_secret_suffix is not None:
            self._temporal_runtime_secret_suffix = (
                request.carried_temporal_runtime_secret_suffix
            )
        if request.carried_pending_force_reconcile is not None:
            self._pending_force_reconcile = request.carried_pending_force_reconcile
        if request.carried_completed_force_reconcile_token is not None:
            self._completed_force_reconcile_token = (
                request.carried_completed_force_reconcile_token
            )
        if (
            request.carried_completed_reconcile_identity is not None
            and remember_completed_reconcile_identity(request.carried_status)
        ):
            self._completed_reconcile_identity = (
                request.carried_completed_reconcile_identity
            )
        if request.carried_status is not None:
            self._status = request.carried_status
            if self._last_build_status is None:
                self._last_build_status = rememberable_build_status(
                    request.carried_status.get("buildStatus")
                )
            if (
                self._completed_reconcile_identity is None
                and remember_completed_reconcile_identity(request.carried_status)
            ):
                self._completed_reconcile_identity = status_reconcile_identity(
                    request.carried_status
                )
            return

        self._status = {
            "project": project_name,
            "phase": "Waiting",
            "message": "Waiting for DemoProject reconcile signal.",
        }

    async def _select_demo_project_for_reconcile(
        self,
        project_name: str,
    ) -> DemoProjectReconcileInput:
        force_reconcile = self._pending_force_reconcile
        force_reconcile_requested = force_reconcile is not None
        rotation_due = (
            not force_reconcile_requested
            and self._pending_spec is None
            and self._temporal_api_key_rotation_due()
            and self._last_demo_project is not None
        )
        # A bare re-check wake-up (no signal, no rotation) while blocked on an
        # external resource: re-reconcile the last-known spec so the newly-present
        # resource is picked up. Yields to any real pending spec/force signal.
        recheck_due = (
            not force_reconcile_requested
            and self._pending_spec is None
            and not rotation_due
            and self._external_resources_pending()
        )

        if force_reconcile_requested:
            assert force_reconcile is not None
            demo_project = await self._load_forced_demo_project_reconcile(
                project_name,
                force_reconcile,
            )
        elif rotation_due:
            demo_project = self._last_demo_project
            self._current_reconcile_identity = rotation_reconcile_identity(
                demo_project,
                self._temporal_api_key_rotation,
            )
        elif recheck_due:
            demo_project = self._last_demo_project
            self._current_reconcile_identity = reconcile_identity(demo_project)
        else:
            demo_project = self._pending_spec
            self._pending_spec = None
            self._current_reconcile_identity = (
                self._pending_reconcile_identity
                if self._pending_reconcile_identity is not None
                else reconcile_identity(demo_project)
            )
            self._pending_reconcile_identity = None

        return DemoProjectReconcileInput(
            demo_project=demo_project,
            force_reconcile=force_reconcile,
            force_reconcile_requested=force_reconcile_requested,
            rotation_due=rotation_due,
        )

    async def _load_forced_demo_project_reconcile(
        self,
        project_name: str,
        force_reconcile: dict[str, str],
    ) -> dict[str, Any]:
        self._pending_force_reconcile = None
        self._pending_spec = None
        self._pending_reconcile_identity = None
        result = await workflow.execute_activity(
            force_reconcile_activities.load_current_demo_project_for_reconcile,
            force_reconcile_activities.LoadCurrentDemoProjectForReconcileRequest(
                name=project_name,
                token=force_reconcile["token"],
                reason=force_reconcile.get("reason"),
            ),
            start_to_close_timeout=timedelta(minutes=3),
            retry_policy=COMMON_RETRY_POLICY,
        )
        demo_project = result.resource
        self._current_force_reconcile_token = force_reconcile["token"]
        self._current_reconcile_identity = reconcile_identity(demo_project)
        return demo_project

    async def _run_demo_project_reconcile(
        self,
        project_name: str,
        reconcile_input: DemoProjectReconcileInput,
    ) -> bool:
        demo_project = reconcile_input.demo_project
        if demo_project is None:
            self._temporal_api_key_rotation = None
            return False

        self._reconcile_sequence += 1
        self._recover_temporal_runtime_secret_suffix(demo_project)
        previous_status = previous_reconcile_status(self._status, demo_project)
        self._status = {
            "project": project_name,
            "phase": (
                "RotatingTemporalAuth" if reconcile_input.rotation_due else "Reconciling"
            ),
        }

        # Reset the per-reconcile Temporal Cloud capture used for saga compensation.
        self._tc_state = None
        self._tc_plan = None
        self._tc_token = None
        try:
            external = await self._reconcile_external_resources(
                demo_project,
                previous_status=previous_status,
                force_temporal_auth_rotation=(
                    reconcile_input.force_temporal_auth_rotation
                ),
            )
            build_status = await self._build_project_artifact(
                external.demo_project,
                external.status,
            )
            serverless_status = await self._reconcile_serverless_lambda_if_needed(
                project_name,
                external.demo_project,
                external.status,
                build_status,
            )
            runtime_status = await self._reconcile_kubernetes_runtime(
                project_name,
                external.demo_project,
                external.status,
                build_status,
                previous_status,
            )
        except Exception as error:  # noqa: BLE001 - saga compensation boundary
            # A provisioning activity or a child workflow failed unexpectedly
            # (raised, vs. returning a rollout-failure status). Without this the
            # exception would fail the entity workflow AND orphan the Temporal
            # Cloud namespace / credentials / secrets created this pass.
            return await self._handle_reconcile_failure(
                project_name, demo_project, error
            )
        self._status = status_with_temporal_runtime_secret_path(
            status_with_serverless_lambda(
                runtime_status,
                serverless_status,
            ),
            external.demo_project,
        )
        await self._record_status(external.demo_project, self._status)
        return True

    async def _reconcile_external_resources(
        self,
        demo_project: dict[str, Any],
        *,
        previous_status: dict[str, Any],
        force_temporal_auth_rotation: bool,
    ) -> DemoProjectExternalResources:
        demo_project = self._demo_project_with_known_runtime_secret_path(demo_project)
        external_plan = external_resources.plan_external_resources(
            demo_project,
            checked_at=workflow.now().astimezone(UTC).isoformat(),
            force_temporal_auth_rotation=force_temporal_auth_rotation,
        )
        ecr_status = await self._reconcile_ecr_repositories(external_plan)
        temporal_status = await self._reconcile_temporal_cloud(
            external_plan,
            previous_status=previous_status,
        )
        if self._tc_plan is not None:
            external_plan = self._tc_plan
            demo_project = external_plan.demo_project
        demo_project = demo_project_with_temporal_api_key(
            demo_project,
            temporal_status,
        )
        self._last_demo_project = demo_project
        self._temporal_api_key_rotation = temporal_auth_rotation_state(
            temporal_status
        )
        secrets_status = await workflow.execute_activity(
            external_resources.verify_secrets_manager_resources,
            external_plan.secrets_manager,
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        status = external_resources.external_resources_status(
            external_plan,
            ecr_status=ecr_status,
            temporal_status=temporal_status,
            secrets_status=secrets_status.status,
        )
        return DemoProjectExternalResources(demo_project=demo_project, status=status)

    def _demo_project_with_known_runtime_secret_path(
        self,
        demo_project: dict[str, Any],
    ) -> dict[str, Any]:
        if (
            not self._temporal_runtime_secret_suffix
            or not resources.temporal_components(demo_project)
        ):
            return demo_project
        path = resources.temporal_secret_remote_path_with_suffix(
            demo_project,
            self._temporal_runtime_secret_suffix,
        )
        return demo_project_with_temporal_runtime_secret_path(demo_project, path)

    def _recover_temporal_runtime_secret_suffix(
        self,
        demo_project: dict[str, Any],
    ) -> None:
        if self._temporal_runtime_secret_suffix is not None:
            return
        status = demo_project.get("status")
        runtime_secret_path = resources.temporal_runtime_secret_path_from_status(
            status
        )
        suffix = resources.temporal_secret_suffix_from_remote_path(
            demo_project,
            runtime_secret_path,
        )
        if suffix is not None:
            self._temporal_runtime_secret_suffix = suffix

    def _external_plan_with_new_runtime_secret_path(
        self,
        external_plan: Any,
    ) -> Any:
        if self._temporal_runtime_secret_suffix is None:
            self._temporal_runtime_secret_suffix = workflow.uuid4().hex[:12]
        path = resources.temporal_secret_remote_path_with_suffix(
            external_plan.demo_project,
            self._temporal_runtime_secret_suffix,
        )
        demo_project = demo_project_with_temporal_runtime_secret_path(
            external_plan.demo_project,
            path,
        )
        return external_resources.plan_external_resources(
            demo_project,
            checked_at=external_plan.checked_at,
            force_temporal_auth_rotation=(
                external_plan.force_temporal_auth_rotation
            ),
        )

    def _runtime_secret_path_can_move(
        self,
        external_plan: Any,
        runtime_secret: external_resources.TemporalRuntimeSecretCheckResult,
    ) -> bool:
        if runtime_secret.state not in {"Missing", "MarkedForDeletion"}:
            return False
        if self._temporal_runtime_secret_suffix is not None:
            return False
        legacy_project = {
            "metadata": {"name": external_plan.temporal_project.name},
            "spec": {},
        }
        return external_plan.temporal_project.runtime_secret_path == (
            resources.temporal_secret_remote_path(legacy_project)
        )

    async def _reconcile_ecr_repositories(
        self,
        external_plan: Any,
    ) -> dict[str, Any]:
        ecr_repository_activities = [
            workflow.execute_activity(
                external_resources.create_or_verify_ecr_repository,
                repository,
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=COMMON_RETRY_POLICY,
            )
            for repository in external_plan.ecr_repositories
        ]
        ecr_repositories = list(await asyncio.gather(*ecr_repository_activities))
        return external_resources.ecr_repositories_status(
            external_plan,
            ecr_repositories,
        )

    async def _build_project_artifact(
        self,
        demo_project: dict[str, Any],
        external_resource_status: dict[str, Any],
    ) -> dict[str, Any]:
        artifact_build = await workflow.execute_child_workflow(
            artifact_build_workflow.DemoProjectArtifactBuildWorkflow.run,
            messages.DemoProjectArtifactBuildRequest(
                demoProject=demo_project,
                externalResources=external_resource_status,
                previousBuildStatus=(
                    None
                    if resources.force_reconcile_token(demo_project)
                    else self._last_build_status
                ),
            ),
            id=self._child_workflow_id("artifact-build"),
        )
        build_status = artifact_build.status
        remembered_build_status = rememberable_build_status(build_status)
        if remembered_build_status is not None:
            self._last_build_status = remembered_build_status
        return build_status

    async def _reconcile_serverless_lambda_if_needed(
        self,
        project_name: str,
        demo_project: dict[str, Any],
        external_resource_status: dict[str, Any],
        build_status: dict[str, Any],
    ) -> dict[str, Any] | None:
        if not resources.has_serverless_lambda_components(demo_project):
            return None

        serverless_result = await workflow.execute_child_workflow(
            serverless_lambda_workflow.DemoProjectServerlessLambdaWorkflow.run,
            messages.DemoProjectServerlessLambdaRequest(
                project=project_name,
                demoProject=demo_project,
                externalResources=external_resource_status,
                buildStatus=build_status,
            ),
            id=self._child_workflow_id("serverless-lambda"),
        )
        return serverless_result.status

    async def _reconcile_kubernetes_runtime(
        self,
        project_name: str,
        demo_project: dict[str, Any],
        external_resource_status: dict[str, Any],
        build_status: dict[str, Any],
        previous_status: dict[str, Any],
    ) -> dict[str, Any]:
        runtime_result = await workflow.execute_child_workflow(
            kubernetes_runtime_workflow.DemoProjectKubernetesRuntimeWorkflow.run,
            messages.DemoProjectKubernetesRuntimeRequest(
                project=project_name,
                demoProject=demo_project,
                externalResources=external_resource_status,
                buildStatus=build_status,
                previousStatus=previous_status,
            ),
            id=self._child_workflow_id("kubernetes-runtime"),
        )
        return runtime_result.status

    @workflow.signal
    def reconcile(self, request: messages.DemoProjectReconcileSignal) -> None:
        demo_project = request.demoProject
        identity = reconcile_identity(demo_project)
        if self._is_duplicate_reconcile_signal(identity):
            return
        self._pending_spec = demo_project
        self._pending_reconcile_identity = identity

    @workflow.signal
    def force_reconcile(self, signal: messages.ForceReconcileSignal) -> None:
        request = force_reconcile_request(signal.token, signal.reason)
        if request is None:
            return
        token = request["token"]
        if token in {
            self._pending_force_reconcile_token(),
            self._current_force_reconcile_token,
            self._completed_force_reconcile_token,
        }:
            return
        self._pending_force_reconcile = request

    @workflow.signal
    def check_worker_version(
        self,
        _signal: messages.CheckWorkerVersionSignal = messages.CheckWorkerVersionSignal(),
    ) -> None:
        if workflow.info().is_target_worker_deployment_version_changed():
            self._continue_as_new_for_worker_version = True

    @workflow.signal
    def finalize(self, request: messages.DemoProjectFinalizeSignal) -> None:
        self._finalize_spec = request.demoProject
        self._finalize = True

    @workflow.signal
    def shutdown(self, _signal: messages.ShutdownSignal = messages.ShutdownSignal()) -> None:
        self._shutdown = True

    async def _reconcile_temporal_cloud(
        self,
        external_plan: Any,
        *,
        previous_status: dict[str, Any],
    ) -> dict[str, Any]:
        candidate_secret_token = temporal_candidate_secret_token(
            external_plan,
            workflow_id=workflow.info().workflow_id,
            run_id=workflow.info().run_id,
            reconcile_sequence=self._reconcile_sequence,
        )
        state = TemporalCloudReconcileState()
        # Capture for reconcile-level saga compensation: `state` mutates in place
        # as each resource is created, so a later failure (incl. a child workflow)
        # can tear down exactly what this pass created.
        self._tc_state = state
        self._tc_plan = external_plan
        self._tc_token = candidate_secret_token

        try:
            await self._stage_temporal_mtls_ca_if_needed(
                external_plan,
                state,
                candidate_secret_token,
            )
            await self._create_or_verify_temporal_cloud_namespace(
                external_plan,
                state,
                candidate_secret_token,
            )
            namespace = state.namespace
            assert namespace is not None
            if namespace.phase == "RegionDrift":
                # The namespace exists in a different region than desired. Region
                # is immutable, so tear it down and recreate it in the new region
                # (destroys the namespace's data — driven by the region change).
                await self._teardown_temporal_cloud_namespace_on_region_drift(
                    external_plan,
                    state,
                    candidate_secret_token,
                )
                await self._create_or_verify_temporal_cloud_namespace(
                    external_plan,
                    state,
                    candidate_secret_token,
                )
                namespace = state.namespace
                assert namespace is not None
            if namespace.phase != "Ready":
                await self._delete_mtls_ca_candidate_secret_if_needed(
                    external_plan,
                    state,
                    candidate_secret_token,
                )
                return namespace.status

            await self._ensure_temporal_cloud_namespace_tags(external_plan, state)

            await self._reconcile_temporal_cloud_search_attributes(
                external_plan,
                state,
                previous_status=previous_status,
            )
            if (
                isinstance(state.search_attributes, dict)
                and state.search_attributes.get("phase") == "Error"
            ):
                return state.merged_status()

            stage_outcome = await self._stage_temporal_runtime_credential(
                external_plan,
                state,
                candidate_secret_token,
            )
            external_plan = stage_outcome.external_plan
            self._tc_plan = external_plan
            if not stage_outcome.staged:
                return state.merged_status()
            if state.stage is None or state.stage.phase != "Ready":
                return state.merged_status()

            await self._promote_temporal_runtime_credential_if_needed(
                external_plan,
                state,
                candidate_secret_token,
            )
            state.mirror = await self._mirror_temporal_runtime_secret(
                external_plan
            )
            return state.merged_status()
        except Exception:
            await self._compensate_temporal_cloud_reconcile(
                external_plan,
                state=state,
                candidate_secret_token=candidate_secret_token,
            )
            raise

    async def _stage_temporal_mtls_ca_if_needed(
        self,
        external_plan: Any,
        state: TemporalCloudReconcileState,
        candidate_secret_token: str,
    ) -> None:
        project = external_plan.temporal_project
        if project.auth_mode != resources.TEMPORAL_AUTH_MODE_MTLS:
            return

        state.ca = await workflow.execute_activity(
            external_resources.stage_temporal_mtls_ca,
            external_resources.StageTemporalMtlsCaRequest(
                checked_at=external_plan.checked_at,
                project=project,
                candidate_secret_token=candidate_secret_token,
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )

    async def _create_or_verify_temporal_cloud_namespace(
        self,
        external_plan: Any,
        state: TemporalCloudReconcileState,
        candidate_secret_token: str,
    ) -> None:
        state.namespace = await workflow.execute_activity(
            external_resources.create_or_verify_temporal_cloud_namespace,
            external_resources.CreateOrVerifyTemporalCloudNamespaceRequest(
                checked_at=external_plan.checked_at,
                project=external_plan.temporal_project,
                candidate_ca_secret_path=(
                    state.ca.candidate_ca_secret_path if state.ca else None
                ),
                candidate_secret_token=candidate_secret_token,
            ),
            start_to_close_timeout=timedelta(minutes=10),
            heartbeat_timeout=timedelta(minutes=1),
            retry_policy=COMMON_RETRY_POLICY,
        )

    async def _ensure_temporal_cloud_namespace_tags(
        self,
        external_plan: Any,
        state: TemporalCloudReconcileState,
    ) -> None:
        namespace = state.namespace
        assert namespace is not None
        tags = resources.namespace_tags(external_plan.demo_project)
        if not tags:
            return
        # BEST-EFFORT. Tags are non-critical namespace metadata with no independent
        # lifecycle. This runs inside the reconcile's compensating try and BEFORE
        # credential promotion, so if a tag failure propagated it would trigger
        # _compensate_temporal_cloud_reconcile and TEAR DOWN the just-created
        # namespace. Swallow + log instead: a tagging problem must never fail the
        # demo or roll back its namespace. (CancelledError is a BaseException, so
        # cooperative cancellation still propagates.)
        try:
            await workflow.execute_activity(
                external_resources.ensure_temporal_cloud_namespace_tags,
                external_resources.EnsureTemporalCloudNamespaceTagsRequest(
                    checked_at=external_plan.checked_at,
                    project=external_plan.temporal_project,
                    namespace=namespace,
                    tags=tuple(tags.items()),
                ),
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=COMMON_RETRY_POLICY,
            )
        except Exception as error:  # noqa: BLE001
            workflow.logger.warning(
                "Namespace tag convergence failed for %s (non-fatal): %s",
                namespace.namespace,
                error,
            )

    async def _teardown_temporal_cloud_namespace_on_region_drift(
        self,
        external_plan: Any,
        state: TemporalCloudReconcileState,
        candidate_secret_token: str,
    ) -> None:
        namespace = state.namespace
        assert namespace is not None
        state.previous_region = namespace.region
        state.desired_region = namespace.status.get("desiredRegion")
        # Destroy the drifted namespace with the same steps (and order) as project
        # finalize. The namespace name is derived from the project and is unchanged
        # by a region change, so these target exactly the old namespace; each step
        # is idempotent and tolerates "already gone" on retry/replay.
        temporal_delete_request = external_resources.DeleteTemporalRuntimeCandidateSecretRequest(
            checked_at=external_plan.checked_at,
            project=external_plan.temporal_project,
            candidate_secret_token=candidate_secret_token,
        )
        await workflow.execute_activity(
            external_resources.delete_temporal_cloud_api_key,
            temporal_delete_request,
            start_to_close_timeout=timedelta(minutes=5),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        await workflow.execute_activity(
            external_resources.delete_temporal_cloud_service_account,
            temporal_delete_request,
            start_to_close_timeout=timedelta(minutes=5),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        await workflow.execute_activity(
            external_resources.disable_temporal_cloud_namespace_delete_protection,
            temporal_delete_request,
            start_to_close_timeout=timedelta(minutes=5),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        await workflow.execute_activity(
            external_resources.delete_temporal_cloud_namespace,
            temporal_delete_request,
            start_to_close_timeout=timedelta(minutes=5),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        await workflow.execute_activity(
            external_resources.delete_temporal_cloud_runtime_secret,
            temporal_delete_request,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        await workflow.execute_activity(
            external_resources.delete_temporal_cloud_mtls_ca_secret,
            external_resources.DeleteTemporalCloudMtlsCaSecretRequest(
                checked_at=external_plan.checked_at,
                project=external_plan.temporal_project,
            ),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        # The old namespace and its credentials are gone; the caller recreates the
        # namespace in the new region and the reconcile continues, minting fresh
        # runtime credentials.
        state.region_recreated = True

    async def _delete_mtls_ca_candidate_secret_if_needed(
        self,
        external_plan: Any,
        state: TemporalCloudReconcileState,
        candidate_secret_token: str,
    ) -> None:
        if external_plan.temporal_project.auth_mode != resources.TEMPORAL_AUTH_MODE_MTLS:
            return
        await self._delete_temporal_candidate_secret(
            external_plan,
            ca=state.ca,
            candidate_secret_token=candidate_secret_token,
        )

    async def _reconcile_temporal_cloud_search_attributes(
        self,
        external_plan: Any,
        state: TemporalCloudReconcileState,
        *,
        previous_status: dict[str, Any],
    ) -> None:
        project = external_plan.temporal_project
        if state.namespace is None:
            return

        desired = {
            attribute.name: attribute
            for attribute in project.search_attributes
        }
        previous_managed = previous_temporal_search_attributes(previous_status)
        delete_names = sorted(set(previous_managed) - set(desired))

        # Every search attribute mutates the same namespace spec under
        # resource_version optimistic concurrency. Fanning out one activity per
        # attribute (asyncio.gather) makes them race and conflict ("resource
        # version mismatch"). Reconcile the whole set in one batched activity.
        statuses: list[dict[str, Any]] = []
        if desired or delete_names:
            result = await workflow.execute_activity(
                external_resources.reconcile_temporal_cloud_search_attributes,
                external_resources.ReconcileTemporalCloudSearchAttributesRequest(
                    checked_at=external_plan.checked_at,
                    project=project,
                    namespace=state.namespace,
                    desired=tuple(desired.values()),
                    delete_names=tuple(delete_names),
                ),
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=COMMON_RETRY_POLICY,
            )
            statuses = result.statuses

        state.search_attributes = temporal_search_attributes_reconcile_status(
            external_plan.checked_at,
            namespace=state.namespace.namespace,
            desired=desired,
            previous_managed=previous_managed,
            statuses=statuses,
        )

    async def _stage_temporal_runtime_credential(
        self,
        external_plan: Any,
        state: TemporalCloudReconcileState,
        candidate_secret_token: str,
    ) -> TemporalRuntimeStageOutcome:
        project = external_plan.temporal_project
        if state.namespace is None:
            return TemporalRuntimeStageOutcome(external_plan, False)
        if not project.temporal_required:
            return TemporalRuntimeStageOutcome(external_plan, False)

        if project.auth_mode == resources.TEMPORAL_AUTH_MODE_MTLS:
            state.mtls_config = await workflow.execute_activity(
                external_resources.configure_temporal_cloud_namespace_mtls,
                external_resources.ConfigureTemporalCloudNamespaceMtlsRequest(
                    checked_at=external_plan.checked_at,
                    project=project,
                    namespace=state.namespace,
                    candidate_ca_secret_path=(
                        state.ca.candidate_ca_secret_path if state.ca else None
                    ),
                    candidate_secret_token=candidate_secret_token,
                ),
                start_to_close_timeout=timedelta(minutes=10),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=COMMON_RETRY_POLICY,
            )
            state.stage = await workflow.execute_activity(
                external_resources.stage_temporal_mtls_client_certificate,
                external_resources.StageTemporalMtlsClientCertificateRequest(
                    checked_at=external_plan.checked_at,
                    project=project,
                    namespace=state.namespace,
                    ca=state.ca,
                    force_rotation=external_plan.force_temporal_auth_rotation,
                    candidate_secret_token=candidate_secret_token,
                ),
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=COMMON_RETRY_POLICY,
            )
            return TemporalRuntimeStageOutcome(external_plan, True)

        if project.auth_mode == resources.TEMPORAL_AUTH_MODE_API_KEY:
            state.service_account = await workflow.execute_activity(
                external_resources.create_or_verify_temporal_cloud_service_account,
                external_resources.CreateOrVerifyTemporalCloudServiceAccountRequest(
                    checked_at=external_plan.checked_at,
                    project=project,
                    namespace=state.namespace,
                ),
                start_to_close_timeout=timedelta(minutes=10),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=COMMON_RETRY_POLICY,
            )
            runtime_secret = await workflow.execute_activity(
                external_resources.check_temporal_runtime_secret,
                external_resources.CheckTemporalRuntimeSecretRequest(
                    checked_at=external_plan.checked_at,
                    project=project,
                    namespace=state.namespace,
                    force_rotation=external_plan.force_temporal_auth_rotation,
                ),
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=COMMON_RETRY_POLICY,
            )

            if self._runtime_secret_path_can_move(external_plan, runtime_secret):
                external_plan = self._external_plan_with_new_runtime_secret_path(
                    external_plan
                )
                project = external_plan.temporal_project
                self._tc_plan = external_plan
                runtime_secret = await workflow.execute_activity(
                    external_resources.check_temporal_runtime_secret,
                    external_resources.CheckTemporalRuntimeSecretRequest(
                        checked_at=external_plan.checked_at,
                        project=project,
                        namespace=state.namespace,
                        force_rotation=(
                            external_plan.force_temporal_auth_rotation
                        ),
                    ),
                    start_to_close_timeout=timedelta(minutes=5),
                    heartbeat_timeout=timedelta(minutes=1),
                    retry_policy=COMMON_RETRY_POLICY,
                )

            if not runtime_secret.rotate:
                state.stage = external_resources.TemporalCloudCredentialStageResult(
                    status=runtime_secret.status,
                    promote_required=False,
                )
                return TemporalRuntimeStageOutcome(external_plan, True)

            state.stage = await workflow.execute_activity(
                external_resources.create_temporal_api_key_candidate_secret,
                external_resources.CreateTemporalApiKeyCandidateSecretRequest(
                    checked_at=external_plan.checked_at,
                    project=project,
                    namespace=state.namespace,
                    service_account=state.service_account,
                    runtime_secret=runtime_secret,
                    candidate_secret_token=candidate_secret_token,
                ),
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=COMMON_RETRY_POLICY,
            )
            return TemporalRuntimeStageOutcome(external_plan, True)

        return TemporalRuntimeStageOutcome(external_plan, False)

    async def _promote_temporal_runtime_credential_if_needed(
        self,
        external_plan: Any,
        state: TemporalCloudReconcileState,
        candidate_secret_token: str,
    ) -> None:
        if state.stage is None:
            return
        if not state.stage.promote_required:
            state.promote = state.stage
            return

        state.canary = await workflow.execute_activity(
            external_resources.run_temporal_runtime_candidate_canary,
            external_resources.RunTemporalRuntimeCandidateCanaryRequest(
                checked_at=external_plan.checked_at,
                project=external_plan.temporal_project,
                stage=state.stage,
            ),
            start_to_close_timeout=(
                TEMPORAL_RUNTIME_CANDIDATE_CANARY_START_TO_CLOSE_TIMEOUT
            ),
            heartbeat_timeout=(
                TEMPORAL_RUNTIME_CANDIDATE_CANARY_HEARTBEAT_TIMEOUT
            ),
            retry_policy=CHECK_RETRY_POLICY,
        )
        if state.stage.candidate_ca_secret_path:
            state.ca_promote = await workflow.execute_activity(
                external_resources.promote_temporal_mtls_ca,
                external_resources.PromoteTemporalMtlsCaRequest(
                    checked_at=external_plan.checked_at,
                    project=external_plan.temporal_project,
                    stage=state.stage,
                ),
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=COMMON_RETRY_POLICY,
            )
            state.ca_promoted = state.ca_promote.ca_promoted

        promote = await workflow.execute_activity(
            external_resources.promote_temporal_runtime_credential,
            external_resources.PromoteTemporalRuntimeCredentialRequest(
                checked_at=external_plan.checked_at,
                project=external_plan.temporal_project,
                stage=state.stage,
                canary=state.canary,
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        if state.ca_promote is not None:
            promote = external_resources.TemporalCloudPromotionResult(
                status=merge_promoted_status(
                    promote.status,
                    state.ca_promote.status,
                ),
                promoted=promote.promoted,
                ca_promoted=state.ca_promote.ca_promoted,
            )
        state.promote = promote
        state.promoted = True
        await self._delete_temporal_candidate_secret(
            external_plan,
            stage=state.stage,
            ca=state.ca,
            candidate_secret_token=candidate_secret_token,
        )

    async def _mirror_temporal_runtime_secret(
        self,
        external_plan: Any,
    ) -> external_resources.TemporalCloudStatusResult:
        return await workflow.execute_activity(
            external_resources.mirror_temporal_runtime_secret,
            external_resources.MirrorTemporalRuntimeSecretRequest(
                checked_at=external_plan.checked_at,
                project=external_plan.temporal_project,
            ),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )

    async def _delete_temporal_candidate_secret(
        self,
        external_plan: Any,
        *,
        stage: external_resources.TemporalCloudCredentialStageResult | None = None,
        ca: external_resources.TemporalCloudCredentialStageResult | None = None,
        candidate_secret_token: str | None = None,
    ) -> None:
        candidate_secret_path = stage.candidate_secret_path if stage else None
        candidate_ca_secret_path = None
        if stage and stage.candidate_ca_secret_path:
            candidate_ca_secret_path = stage.candidate_ca_secret_path
        elif ca and ca.candidate_ca_secret_path:
            candidate_ca_secret_path = ca.candidate_ca_secret_path
        await workflow.execute_activity(
            external_resources.delete_temporal_runtime_candidate_secret,
            external_resources.DeleteTemporalRuntimeCandidateSecretRequest(
                checked_at=external_plan.checked_at,
                project=external_plan.temporal_project,
                candidate_secret_path=candidate_secret_path,
                candidate_ca_secret_path=candidate_ca_secret_path,
                candidate_secret_token=(
                    candidate_secret_token
                    or (stage.candidate_secret_token if stage else None)
                    or (ca.candidate_secret_token if ca else None)
                ),
            ),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )

    async def _compensate_temporal_cloud_reconcile(
        self,
        external_plan: Any,
        *,
        state: TemporalCloudReconcileState,
        candidate_secret_token: str | None,
    ) -> None:
        if state.promoted:
            return

        await self._compensate_temporal_candidate_api_key(
            external_plan,
            state,
        )
        await self._delete_temporal_candidate_secret(
            external_plan,
            stage=state.stage,
            ca=state.ca,
            candidate_secret_token=candidate_secret_token,
        )
        await self._compensate_temporal_service_account(
            external_plan,
            state,
        )
        await self._compensate_temporal_namespace(
            external_plan,
            state,
        )

    async def _compensate_temporal_candidate_api_key(
        self,
        external_plan: Any,
        state: TemporalCloudReconcileState,
    ) -> None:
        candidate_key_id = (
            state.stage.candidate_api_key_id if state.stage else None
        )
        if candidate_key_id:
            await workflow.execute_activity(
                external_resources.delete_temporal_cloud_api_key_by_id,
                external_resources.DeleteTemporalCloudApiKeyRequest(
                    checked_at=external_plan.checked_at,
                    project=external_plan.temporal_project,
                    key_id=candidate_key_id,
                ),
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=COMMON_RETRY_POLICY,
            )

    async def _compensate_temporal_service_account(
        self,
        external_plan: Any,
        state: TemporalCloudReconcileState,
    ) -> None:
        service_account_id = (
            state.service_account.service_account_id
            if state.service_account and state.service_account.created
            else None
        )
        if service_account_id:
            await workflow.execute_activity(
                external_resources.delete_temporal_cloud_service_account_by_id,
                external_resources.DeleteTemporalCloudServiceAccountRequest(
                    checked_at=external_plan.checked_at,
                    project=external_plan.temporal_project,
                    service_account_id=service_account_id,
                ),
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=COMMON_RETRY_POLICY,
            )

    async def _compensate_temporal_namespace(
        self,
        external_plan: Any,
        state: TemporalCloudReconcileState,
    ) -> None:
        namespace_name = (
            state.namespace.namespace
            if state.namespace and state.namespace.created
            else None
        )
        if namespace_name:
            if state.ca_promoted:
                await workflow.execute_activity(
                    external_resources.delete_temporal_cloud_mtls_ca_secret,
                    external_resources.DeleteTemporalCloudMtlsCaSecretRequest(
                        checked_at=external_plan.checked_at,
                        project=external_plan.temporal_project,
                    ),
                    start_to_close_timeout=timedelta(minutes=2),
                    retry_policy=COMMON_RETRY_POLICY,
                )
            await workflow.execute_activity(
                external_resources.disable_temporal_cloud_namespace_delete_protection_by_name,
                external_resources.TemporalCloudNamespaceRequest(
                    checked_at=external_plan.checked_at,
                    project=external_plan.temporal_project,
                    namespace=namespace_name,
                ),
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=COMMON_RETRY_POLICY,
            )
            await workflow.execute_activity(
                external_resources.delete_temporal_cloud_namespace_by_name,
                external_resources.TemporalCloudNamespaceRequest(
                    checked_at=external_plan.checked_at,
                    project=external_plan.temporal_project,
                    namespace=namespace_name,
                ),
                start_to_close_timeout=timedelta(minutes=10),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=COMMON_RETRY_POLICY,
            )

    async def _handle_reconcile_failure(
        self,
        project_name: str,
        demo_project: dict[str, Any],
        error: BaseException,
    ) -> bool:
        # Decide first-time vs established from whichever known state still carries
        # the live active version — the current reconcile input first, then the
        # last one we saw. The signal payload carries the active-version status
        # subset (watch.active_version_status) for exactly this decision; without
        # it an established project reads as first-time and a transient rollout
        # error would fail the whole entity workflow instead of settling.
        record_target = demo_project
        if not project_has_active_version(record_target) and project_has_active_version(
            self._last_demo_project
        ):
            record_target = self._last_demo_project
        if not project_has_active_version(record_target):
            # First-time provision (no active version to fall back to): tear down
            # everything created this pass so we don't orphan the Temporal Cloud
            # namespace / credentials / secrets, then re-raise to fail the entity.
            await self._compensate_first_time_external_resources()
            raise error
        # Established project: a new rollout failed. Preserve the active/production
        # resources, settle to a terminal rollout-failure status, and keep the
        # long-lived entity workflow running (it retries on the next change).
        self._status = self._failed_rollout_status(record_target)
        await self._record_status(record_target, self._status)
        return True

    def _failed_rollout_status(
        self,
        demo_project: dict[str, Any],
    ) -> dict[str, Any]:
        status: dict[str, Any] = {
            "project": self._project_name,
            "phase": "WorkerRolloutFailed",
            "message": (
                "Reconcile failed unexpectedly; rollout did not complete. "
                "The active version was preserved."
            ),
        }
        current = demo_project.get("status") if isinstance(demo_project, dict) else None
        if isinstance(current, dict):
            for key in (
                "activeImageVersion",
                "activeImages",
                "activeRuntimeNamespace",
                "activeRevision",
                "activeRouteServices",
            ):
                if current.get(key) is not None:
                    status[key] = current[key]
        return status

    async def _compensate_first_time_external_resources(self) -> None:
        plan = self._tc_plan
        state = self._tc_state
        if plan is None or state is None:
            return
        # Full teardown of resources created THIS pass, mirroring the finalize
        # order (api key -> service account -> namespace -> secrets). No `promoted`
        # guard: a failed first provision must not leave a promoted-but-unused
        # credential or namespace behind.
        await self._compensate_temporal_candidate_api_key(plan, state)
        await self._compensate_temporal_service_account(plan, state)
        await self._compensate_temporal_namespace(plan, state)
        if state.namespace is not None and state.namespace.created:
            await self._delete_temporal_runtime_secret(plan)
        await self._delete_temporal_candidate_secret(
            plan,
            stage=state.stage,
            ca=state.ca,
            candidate_secret_token=self._tc_token,
        )

    async def _delete_temporal_runtime_secret(self, external_plan: Any) -> None:
        await workflow.execute_activity(
            external_resources.delete_temporal_cloud_runtime_secret,
            external_resources.DeleteTemporalRuntimeCandidateSecretRequest(
                checked_at=external_plan.checked_at,
                project=external_plan.temporal_project,
            ),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )

    async def _run_finalize(self, project_name: str) -> None:
        demo_project = (
            self._finalize_spec
            or self._pending_spec
            or {"metadata": {"name": project_name}}
        )
        finalize_plan = await self._load_project_finalization_plan(demo_project)
        result = reconcile_activities.completed_activity_status(finalize_plan)
        if result is None:
            delete_actions = await self._delete_project_owned_resources(finalize_plan)
            external = await self._delete_project_external_resources(
                demo_project,
                finalize_plan,
            )
            result = reconcile_activities.project_finalization_status(
                finalize_plan,
                namespace_action=delete_actions.namespace_action,
                source_action=delete_actions.source_action,
                source_auth_action=delete_actions.source_auth_action,
                runtime_namespace_actions=delete_actions.runtime_namespace_actions,
                revision_actions=delete_actions.revision_actions,
                external=external,
            )
            self._status = finalized_project_status(project_name, result)
            await self._record_status(demo_project, self._status)
            await self._remove_project_finalizer(finalize_plan)
        self._status = finalized_project_status(project_name, result)

    async def _load_project_finalization_plan(
        self,
        demo_project: dict[str, Any],
    ) -> dict[str, Any]:
        finalize_plan_result = await workflow.execute_activity(
            reconcile_activities.load_project_finalization_plan,
            reconcile_activities.ProjectFinalizationRequest(demo_project),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        return finalize_plan_result.plan

    async def _delete_project_owned_resources(
        self,
        finalize_plan: dict[str, Any],
    ) -> DemoProjectFinalizationDeletes:
        runtime_namespace_actions = []
        for namespace in finalize_plan.get("runtimeNamespaces", []):
            if not isinstance(namespace, str) or not namespace:
                continue
            action = await workflow.execute_activity(
                reconcile_activities.delete_project_namespace_resource,
                reconcile_activities.ProjectNamespaceDeleteRequest(namespace),
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=COMMON_RETRY_POLICY,
            )
            runtime_namespace_actions.append({
                "name": namespace,
                "action": action.action,
            })

        revision_actions = await self._delete_project_revisions(
            finalize_plan["project"]
        )
        namespace_action = await workflow.execute_activity(
            reconcile_activities.delete_project_namespace_resource,
            reconcile_activities.ProjectNamespaceDeleteRequest(finalize_plan["namespace"]),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        source_action = await workflow.execute_activity(
            reconcile_activities.delete_project_source_repository,
            reconcile_activities.ResourceDeleteRequest(
                namespace=finalize_plan["sourceWatchNamespace"],
                name=finalize_plan["gitRepository"],
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        source_auth_action = await workflow.execute_activity(
            reconcile_activities.delete_project_source_auth_secret,
            reconcile_activities.OptionalResourceDeleteRequest(
                namespace=finalize_plan["sourceWatchNamespace"],
                name=finalize_plan.get("sourceAuthSecret"),
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        return DemoProjectFinalizationDeletes(
            namespace_action=namespace_action.action,
            source_action=source_action.action,
            source_auth_action=source_auth_action.action,
            runtime_namespace_actions=runtime_namespace_actions,
            revision_actions=revision_actions,
        )

    async def _delete_project_revisions(self, project: str) -> list[dict[str, Any]]:
        decision = await workflow.execute_activity(
            revision_activities.plan_revision_deletes,
            revision_activities.PlanRevisionDeletesRequest(
                project=project,
                protected_revisions=[],
            ),
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=COMMON_RETRY_POLICY,
        )
        actions: list[dict[str, Any]] = []
        for name in decision.get("delete") or []:
            result = await workflow.execute_activity(
                revision_activities.delete_revision,
                revision_activities.DeleteRevisionRequest(name=name),
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=COMMON_RETRY_POLICY,
            )
            actions.append({
                "name": name,
                "action": result.get("action"),
            })
        return actions

    async def _delete_project_external_resources(
        self,
        demo_project: dict[str, Any],
        finalize_plan: dict[str, Any],
    ) -> dict[str, Any]:
        finalize_external_plan = self._project_finalization_external_plan(
            demo_project,
            finalize_plan,
        )
        temporal_delete_request = external_resources.DeleteTemporalRuntimeCandidateSecretRequest(
            checked_at=finalize_external_plan.checked_at,
            project=finalize_external_plan.temporal_project,
        )
        serverless_lambda_teardown = await workflow.execute_activity(
            external_resources.delete_serverless_lambda_resources,
            external_resources.DeleteServerlessLambdaResourcesRequest(
                checked_at=finalize_external_plan.checked_at,
                demo_project=demo_project,
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        temporal_teardown = await self._delete_project_temporal_cloud_resources(
            finalize_plan,
            finalize_external_plan,
            temporal_delete_request,
        )
        ecr_teardown = await workflow.execute_activity(
            external_resources.delete_ecr_repositories,
            external_resources.DeleteEcrRepositoriesRequest(
                checked_at=finalize_external_plan.checked_at,
                repositories=finalize_external_plan.ecr_repositories,
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        return external_resources.external_resource_finalization_status(
            finalize_plan,
            temporal_status=temporal_teardown,
            ecr_status=ecr_teardown.status,
            serverless_lambda_status=serverless_lambda_teardown.status,
        )

    def _project_finalization_external_plan(
        self,
        demo_project: dict[str, Any],
        finalize_plan: dict[str, Any],
    ) -> Any:
        finalize_checked_at = finalize_plan.get("checkedAt")
        if not isinstance(finalize_checked_at, str) or not finalize_checked_at:
            finalize_checked_at = workflow.now().astimezone(UTC).isoformat()
        demo_project = self._demo_project_with_known_runtime_secret_path(demo_project)
        return external_resources.plan_external_resources(
            demo_project,
            checked_at=finalize_checked_at,
        )

    async def _delete_project_temporal_cloud_resources(
        self,
        finalize_plan: dict[str, Any],
        finalize_external_plan: Any,
        temporal_delete_request: external_resources.DeleteTemporalRuntimeCandidateSecretRequest,
    ) -> dict[str, Any]:
        temporal_api_key_teardown = await workflow.execute_activity(
            external_resources.delete_temporal_cloud_api_key,
            temporal_delete_request,
            start_to_close_timeout=timedelta(minutes=5),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        temporal_service_account_teardown = await workflow.execute_activity(
            external_resources.delete_temporal_cloud_service_account,
            temporal_delete_request,
            start_to_close_timeout=timedelta(minutes=5),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        temporal_namespace_protection_teardown = await workflow.execute_activity(
            external_resources.disable_temporal_cloud_namespace_delete_protection,
            temporal_delete_request,
            start_to_close_timeout=timedelta(minutes=5),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        temporal_namespace_teardown = await workflow.execute_activity(
            external_resources.delete_temporal_cloud_namespace,
            temporal_delete_request,
            start_to_close_timeout=timedelta(minutes=5),
            heartbeat_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        temporal_secret_teardown = await workflow.execute_activity(
            external_resources.delete_temporal_cloud_runtime_secret,
            temporal_delete_request,
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        temporal_mtls_ca_teardown = await workflow.execute_activity(
            external_resources.delete_temporal_cloud_mtls_ca_secret,
            external_resources.DeleteTemporalCloudMtlsCaSecretRequest(
                checked_at=finalize_external_plan.checked_at,
                project=finalize_external_plan.temporal_project,
            ),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        return external_resources.temporal_cloud_finalization_status(
            finalize_plan,
            api_key_status=temporal_api_key_teardown.status,
            service_account_status=temporal_service_account_teardown.status,
            namespace_protection_status=temporal_namespace_protection_teardown.status,
            namespace_status=temporal_namespace_teardown.status,
            runtime_secret_status=temporal_secret_teardown.status,
            mtls_ca_secret_status=temporal_mtls_ca_teardown.status,
        )

    async def _remove_project_finalizer(
        self,
        finalize_plan: dict[str, Any],
    ) -> None:
        await workflow.execute_activity(
            reconcile_activities.remove_project_finalizer,
            reconcile_activities.ProjectFinalizerRemovalRequest(
                project=finalize_plan["project"]
            ),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=STATUS_RETRY_POLICY,
        )

    async def _record_status(
        self,
        demo_project: dict[str, Any],
        status: dict[str, Any],
    ) -> None:
        request = reconcile_activities.RecordDemoProjectStatusRequest(
            demo_project=demo_project,
            status=status_with_force_reconcile(status, demo_project),
        )
        await workflow.execute_activity(
            reconcile_activities.record_demo_project_status,
            request,
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=STATUS_RETRY_POLICY,
        )
        await workflow.execute_activity(
            reconcile_activities.record_catalog_project_status,
            request,
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=STATUS_RETRY_POLICY,
        )

    def _child_workflow_id(self, purpose: str) -> str:
        return (
            f"{workflow.info().workflow_id}/"
            f"reconcile-{self._reconcile_sequence}/"
            f"{purpose}"
        )

    @workflow.query
    def status(self) -> dict[str, Any]:
        return self._status

    def _continue_as_new_if_needed(self, project_name: str) -> None:
        if self._shutdown:
            return
        if (
            self._reconcile_sequence < MAX_RECONCILES_PER_WORKFLOW_RUN
            and not workflow.info().is_continue_as_new_suggested()
            and not self._continue_as_new_for_worker_version
        ):
            return
        self._continue_as_new_for_worker_version = False
        workflow.continue_as_new(
            args=[
                messages.DemoProjectRunRequest(
                    project_name=project_name,
                    carried_status=self._status,
                    carried_pending_spec=self._pending_spec,
                    carried_last_build_status=self._last_build_status,
                    carried_completed_reconcile_identity=(
                        self._completed_reconcile_identity
                    ),
                    carried_last_demo_project=self._last_demo_project,
                    carried_temporal_api_key_rotation=(
                        self._temporal_api_key_rotation
                    ),
                    carried_temporal_runtime_secret_suffix=(
                        self._temporal_runtime_secret_suffix
                    ),
                    carried_pending_force_reconcile=(
                        self._pending_force_reconcile
                    ),
                    carried_completed_force_reconcile_token=(
                        self._completed_force_reconcile_token
                    ),
                )
            ],
            initial_versioning_behavior=(
                workflow.ContinueAsNewVersioningBehavior.AUTO_UPGRADE
            ),
        )

    async def _wait_for_next_action(self) -> None:
        while True:
            if self._has_pending_action() or self._temporal_api_key_rotation_due():
                return

            timeout, timeout_summary = self._next_wait_timeout()
            if timeout is None:
                await workflow.wait_condition(self._has_pending_action)
                return
            try:
                await workflow.wait_condition(
                    self._has_pending_action,
                    timeout=timeout,
                    timeout_summary=timeout_summary,
                )
                return
            except asyncio.TimeoutError:
                return

    def _has_pending_action(self) -> bool:
        return (
            self._pending_spec is not None
            or self._pending_force_reconcile is not None
            or self._finalize
            or self._shutdown
            or self._continue_as_new_for_worker_version
        )

    def _temporal_api_key_rotation_due(self) -> bool:
        rotate_at = rotation_datetime(self._temporal_api_key_rotation)
        return rotate_at is not None and workflow.now() >= rotate_at

    def _temporal_api_key_rotation_timeout(self) -> timedelta | None:
        rotate_at = rotation_datetime(self._temporal_api_key_rotation)
        if rotate_at is None:
            return None
        delay = rotate_at - workflow.now()
        if delay.total_seconds() <= 0:
            return timedelta(seconds=0)
        return delay

    def _external_resources_pending(self) -> bool:
        # A reconcile that settled in WaitingForExternalResources is blocked on a
        # resource we don't control (a missing Secrets Manager value). Re-verify
        # the last-known spec on a timer so the project converges once it appears.
        return (
            self._last_demo_project is not None
            and isinstance(self._status, dict)
            and self._status.get("phase") in EXTERNAL_RESOURCE_WAIT_PHASES
        )

    def _external_resource_recheck_timeout(self) -> timedelta | None:
        if not self._external_resources_pending():
            return None
        return EXTERNAL_RESOURCE_RECHECK_INTERVAL

    def _next_wait_timeout(self) -> tuple[timedelta | None, str | None]:
        # Wake on whichever durable timer fires first: a due Temporal auth
        # rotation or the next external-resource re-check.
        rotation = self._temporal_api_key_rotation_timeout()
        recheck = self._external_resource_recheck_timeout()
        if rotation is not None and (recheck is None or rotation <= recheck):
            return rotation, "temporal-auth-rotation"
        if recheck is not None:
            return recheck, "external-resource-recheck"
        return None, None

    def _is_duplicate_reconcile_signal(self, identity: dict[str, str]) -> bool:
        return any(
            identity == comparable
            for comparable in (
                self._pending_reconcile_identity,
                self._current_reconcile_identity,
                self._completed_reconcile_identity,
            )
            if comparable is not None
        )

    def _complete_current_reconcile(self) -> None:
        if remember_completed_reconcile_identity(self._status):
            self._completed_reconcile_identity = self._current_reconcile_identity
            if self._current_force_reconcile_token is not None:
                self._completed_force_reconcile_token = (
                    self._current_force_reconcile_token
                )
        elif self._completed_reconcile_identity == self._current_reconcile_identity:
            self._completed_reconcile_identity = None
            if self._completed_force_reconcile_token == self._current_force_reconcile_token:
                self._completed_force_reconcile_token = None
        self._current_reconcile_identity = None
        self._current_force_reconcile_token = None

    def _pending_force_reconcile_token(self) -> str | None:
        if not isinstance(self._pending_force_reconcile, dict):
            return None
        token = self._pending_force_reconcile.get("token")
        return token if isinstance(token, str) and token else None


def project_has_active_version(demo_project: Any) -> bool:
    """True if the project has already promoted a version (not a first provision).

    A first-time provision has no active version; if it fails it should compensate
    and fail the entity workflow (empty state). An established project preserves its
    active resources and settles to a failed rollout.
    """
    if not isinstance(demo_project, dict):
        return False
    status = demo_project.get("status")
    if not isinstance(status, dict):
        return False
    return bool(status.get("activeImageVersion"))


def remember_completed_reconcile_identity(status: Any) -> bool:
    if not isinstance(status, dict):
        return False
    phase = status.get("phase")
    if not isinstance(phase, str) or not phase:
        return False
    if phase in TERMINAL_ROLLOUT_FAILURE_PHASES:
        return True  # settle; retry only on new identity or force_reconcile
    return phase not in FAILED_RECONCILE_PHASES and not phase.endswith("Failed")


def rememberable_build_status(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    if value.get("phase") not in {"Pushed", "Resolved"}:
        return None
    source_commit_sha = value.get("sourceCommitSha")
    images = value.get("images")
    if not isinstance(source_commit_sha, str) or not source_commit_sha:
        return None
    if not isinstance(images, dict) or not images:
        return None
    return value


def status_with_force_reconcile(
    status: dict[str, Any],
    resource: dict[str, Any],
) -> dict[str, Any]:
    force_status = resources.force_reconcile_status(resource)
    if force_status is None:
        return status
    return {
        **status,
        resources.BASE_RECONCILE_KEY_STATUS_FIELD: resources.demo_project_reconcile_key(
            resources.resource_without_force_reconcile(resource)
        ),
        "forceReconcile": force_status,
    }


def finalized_project_status(
    project_name: str,
    finalize_status: dict[str, Any],
) -> dict[str, Any]:
    return {
        "project": project_name,
        "phase": "Finalized",
        "message": "Project removed from git; resources torn down and workflow completed.",
        "finalizeStatus": finalize_status,
    }


def merge_temporal_cloud_status(
    namespace_status: dict[str, Any],
    *,
    ca_status: dict[str, Any] | None,
    mtls_config_status: dict[str, Any] | None,
    service_account_status: dict[str, Any] | None,
    search_attributes_status: dict[str, Any] | None,
    credential_status: dict[str, Any] | None,
    mirror_status: dict[str, Any] | None,
) -> dict[str, Any]:
    status = {**namespace_status}
    credential = credential_status if isinstance(credential_status, dict) else {}
    service_account = (
        service_account_status if isinstance(service_account_status, dict) else {}
    )
    mtls_ca = ca_status if isinstance(ca_status, dict) else {}
    mtls_config = mtls_config_status if isinstance(mtls_config_status, dict) else {}

    if isinstance(service_account.get("serviceAccount"), dict):
        status["serviceAccount"] = service_account["serviceAccount"]
    if isinstance(credential.get("serviceAccount"), dict):
        status["serviceAccount"] = credential["serviceAccount"]
    if isinstance(credential.get("apiKey"), dict):
        status["apiKey"] = credential["apiKey"]

    if isinstance(search_attributes_status, dict):
        status["searchAttributes"] = search_attributes_status
        if search_attributes_status.get("phase") == "Error":
            status["phase"] = "Error"
            for key in ("reason", "message"):
                value = search_attributes_status.get(key)
                if isinstance(value, str) and value:
                    status[key] = value

    mtls = {}
    for source in (mtls_ca.get("mtls"), mtls_config.get("mtls"), credential.get("mtls")):
        if isinstance(source, dict):
            mtls = merge_nested_status(mtls, source)
    if mtls:
        status["mtls"] = mtls

    promoted = credential.get("promoted")
    if isinstance(promoted, dict):
        status["promoted"] = promoted
    if isinstance(mirror_status, dict):
        status["runtimeSecretMirror"] = mirror_status
    return status


def merge_nested_status(
    left: dict[str, Any],
    right: dict[str, Any],
) -> dict[str, Any]:
    merged = {**left}
    for key, value in right.items():
        existing = merged.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            merged[key] = merge_nested_status(existing, value)
        else:
            merged[key] = value
    return merged


def temporal_candidate_api_key_id(status: dict[str, Any] | None) -> str | None:
    if not isinstance(status, dict):
        return None
    key_id = status.get("candidateApiKeyId")
    return key_id if isinstance(key_id, str) and key_id else None


def temporal_candidate_ca_secret_path(status: dict[str, Any] | None) -> str | None:
    if not isinstance(status, dict):
        return None
    path = status.get("candidateCaSecretRemotePath")
    return path if isinstance(path, str) and path else None


def nested_status_bool(status: dict[str, Any] | None, *keys: str) -> bool:
    current: Any = status
    for key in keys:
        if not isinstance(current, dict):
            return False
        current = current.get(key)
    return current is True


def merge_promoted_status(
    runtime_status: dict[str, Any],
    ca_status: dict[str, Any],
) -> dict[str, Any]:
    promoted = {}
    runtime_promoted = runtime_status.get("promoted")
    if isinstance(runtime_promoted, dict):
        promoted.update(runtime_promoted)
    ca_promoted = ca_status.get("promoted")
    if isinstance(ca_promoted, dict):
        promoted.update(ca_promoted)
    return {
        **runtime_status,
        "promoted": promoted,
    }


def temporal_created_service_account_id(status: dict[str, Any] | None) -> str | None:
    if not isinstance(status, dict):
        return None
    service_account = status.get("serviceAccount")
    if not isinstance(service_account, dict):
        return None
    if service_account.get("action") != "created":
        return None
    service_account_id = service_account.get("id")
    return service_account_id if isinstance(service_account_id, str) else None


def temporal_created_namespace_name(status: dict[str, Any] | None) -> str | None:
    if not isinstance(status, dict):
        return None
    namespace = status.get("namespace")
    if not isinstance(namespace, dict):
        return None
    if namespace.get("action") != "created":
        return None
    name = namespace.get("name")
    return name if isinstance(name, str) else None
