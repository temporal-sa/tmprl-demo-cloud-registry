from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC, datetime, timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from demo_registry_operator.activities.external_resources import (
        ConfigureTemporalCloudNamespaceMtlsRequest,
        CreateOrVerifyTemporalCloudNamespaceRequest,
        CreateOrVerifyTemporalCloudServiceAccountRequest,
        DeleteEcrRepositoriesRequest,
        DeleteTemporalCloudApiKeyRequest,
        DeleteTemporalCloudMtlsCaSecretRequest,
        DeleteTemporalCloudServiceAccountRequest,
        DeleteTemporalRuntimeCandidateSecretRequest,
        MirrorTemporalRuntimeSecretRequest,
        PromoteTemporalMtlsCaRequest,
        PromoteTemporalRuntimeCredentialRequest,
        RunTemporalRuntimeCandidateCanaryRequest,
        StageTemporalApiKeyCandidateRequest,
        StageTemporalMtlsCaRequest,
        StageTemporalMtlsClientCertificateRequest,
        TemporalCloudCredentialStageResult,
        TemporalCloudNamespaceRequest,
        TemporalCloudNamespaceResult,
        TemporalCloudPromotionResult,
        TemporalCloudServiceAccountResult,
        TemporalCloudStatusResult,
        configure_temporal_cloud_namespace_mtls,
        create_or_verify_ecr_repository,
        create_or_verify_temporal_cloud_namespace,
        create_or_verify_temporal_cloud_service_account,
        delete_ecr_repositories,
        delete_temporal_cloud_api_key,
        delete_temporal_cloud_api_key_by_id,
        delete_temporal_cloud_mtls_ca_secret,
        delete_temporal_cloud_namespace,
        delete_temporal_cloud_namespace_by_name,
        delete_temporal_cloud_runtime_secret,
        delete_temporal_cloud_service_account,
        delete_temporal_cloud_service_account_by_id,
        delete_temporal_runtime_candidate_secret,
        disable_temporal_cloud_namespace_delete_protection,
        disable_temporal_cloud_namespace_delete_protection_by_name,
        external_resource_finalization_status,
        external_resources_status,
        plan_external_resources,
        ecr_repositories_status,
        mirror_temporal_runtime_secret,
        promote_temporal_mtls_ca,
        promote_temporal_runtime_credential,
        run_temporal_runtime_candidate_canary,
        stage_temporal_api_key_candidate,
        stage_temporal_mtls_ca,
        stage_temporal_mtls_client_certificate,
        temporal_cloud_finalization_status,
        verify_secrets_manager_resources,
    )
    from demo_registry_operator.activities.force_reconcile import (
        LoadCurrentDemoProjectForReconcileRequest,
        load_current_demo_project_for_reconcile,
    )
    from demo_registry_operator.activities.reconcile import (
        OptionalResourceDeleteRequest,
        ProjectFinalizationRequest,
        ProjectFinalizerRemovalRequest,
        ProjectNamespaceDeleteRequest,
        RecordDemoProjectStatusRequest,
        ResourceDeleteRequest,
        completed_activity_status,
        delete_project_namespace_resource,
        delete_project_source_auth_secret,
        delete_project_source_repository,
        load_project_finalization_plan,
        project_finalization_status,
        record_catalog_project_status,
        record_demo_project_status,
        remove_project_finalizer,
    )
    from demo_registry_operator.errors import NON_RETRYABLE_ERROR_TYPES
    from demo_registry_operator.resources import (
        SOURCE_ARTIFACT_DIGEST_ANNOTATION,
        SOURCE_COMMIT_ANNOTATION,
        TEMPORAL_API_KEY_EXPIRY_ANNOTATION,
        TEMPORAL_API_KEY_ID_ANNOTATION,
        TEMPORAL_API_KEY_SYNCED_AT_ANNOTATION,
        TEMPORAL_AUTH_MODE_API_KEY,
        TEMPORAL_AUTH_MODE_MTLS,
        BASE_RECONCILE_KEY_STATUS_FIELD,
        demo_project_reconcile_key,
        force_reconcile_status,
        force_reconcile_token,
        metadata_annotations,
        resource_without_force_reconcile,
    )
    from demo_registry_operator.workflows.messages import (
        CheckWorkerVersionSignal,
        DemoProjectArtifactBuildRequest,
        DemoProjectFinalizeSignal,
        DemoProjectKubernetesRuntimeRequest,
        DemoProjectReconcileSignal,
        DemoProjectRunRequest,
        ForceReconcileSignal,
        ShutdownSignal,
    )
    from demo_registry_operator.workflows.demo_project_runtime import (
        DemoProjectArtifactBuildWorkflow,
        DemoProjectKubernetesRuntimeWorkflow,
    )


MAX_RECONCILES_PER_WORKFLOW_RUN = 50

COMMON_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=8,
    non_retryable_error_types=NON_RETRYABLE_ERROR_TYPES,
)
STATUS_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=12,
    non_retryable_error_types=NON_RETRYABLE_ERROR_TYPES,
)
CHECK_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=3,
    non_retryable_error_types=NON_RETRYABLE_ERROR_TYPES,
)
TEMPORAL_RUNTIME_CANDIDATE_CANARY_START_TO_CLOSE_TIMEOUT = timedelta(minutes=10)
TEMPORAL_RUNTIME_CANDIDATE_CANARY_HEARTBEAT_TIMEOUT = timedelta(minutes=2)
FAILED_RECONCILE_PHASES = {
    "CandidateNotReady",
    "Error",
    "SmokeCheckFailed",
    "WorkerRolloutFailed",
}
FORCE_TEMPORAL_AUTH_ROTATION_REASON = "rotate-temporal-auth"


def reconcile_identity(demo_project: dict[str, Any]) -> dict[str, str]:
    annotations = metadata_annotations(demo_project)
    identity = {
        "reconcileKey": demo_project_reconcile_key(demo_project),
    }
    source_commit_sha = annotations.get(SOURCE_COMMIT_ANNOTATION)
    if source_commit_sha:
        identity["sourceCommitSha"] = source_commit_sha
    source_artifact_digest = annotations.get(SOURCE_ARTIFACT_DIGEST_ANNOTATION)
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
    rendered_annotations[TEMPORAL_API_KEY_ID_ANNOTATION] = key_id

    expiry_time = api_key.get("expiryTime")
    if isinstance(expiry_time, str) and expiry_time:
        rendered_annotations[TEMPORAL_API_KEY_EXPIRY_ANNOTATION] = expiry_time
    created_at = api_key.get("createdAt")
    if isinstance(created_at, str) and created_at:
        rendered_annotations[TEMPORAL_API_KEY_SYNCED_AT_ANNOTATION] = created_at

    rendered_metadata["annotations"] = rendered_annotations
    rendered["metadata"] = rendered_metadata
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
        request: DemoProjectRunRequest,
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
        self._status: dict[str, Any] = {
            "phase": "Starting",
            "message": "Workflow has not received project state.",
        }

    @workflow.run
    async def run(
        self,
        request: DemoProjectRunRequest,
    ) -> None:
        project_name = request.project_name
        self._project_name = project_name
        # Do NOT clobber a pending spec set by a start signal. With
        # SignalWithStart the reconcile signal is delivered on the first workflow
        # task and its handler sets self._pending_spec before/around run()'s body;
        # unconditionally assigning carried_pending_spec (None on a fresh start)
        # would wipe it, leaving wait_condition permanently false. Only adopt a
        # spec actually carried over from continue-as-new.
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
        else:
            self._status = {
                "project": project_name,
                "phase": "Waiting",
                "message": "Waiting for DemoProject reconcile signal.",
            }

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

            force_reconcile = self._pending_force_reconcile
            force_reconcile_requested = force_reconcile is not None
            rotation_due = (
                not force_reconcile_requested
                and self._pending_spec is None
                and self._temporal_api_key_rotation_due()
                and self._last_demo_project is not None
            )
            if force_reconcile_requested:
                assert force_reconcile is not None
                self._pending_force_reconcile = None
                self._pending_spec = None
                self._pending_reconcile_identity = None
                demo_project = await workflow.execute_activity(
                    load_current_demo_project_for_reconcile,
                    LoadCurrentDemoProjectForReconcileRequest(
                        name=project_name,
                        token=force_reconcile["token"],
                        reason=force_reconcile.get("reason"),
                    ),
                    start_to_close_timeout=timedelta(minutes=3),
                    retry_policy=COMMON_RETRY_POLICY,
                )
                demo_project = demo_project.resource
                self._current_force_reconcile_token = force_reconcile["token"]
                self._current_reconcile_identity = reconcile_identity(demo_project)
            elif rotation_due:
                demo_project = self._last_demo_project
                self._current_reconcile_identity = rotation_reconcile_identity(
                    demo_project,
                    self._temporal_api_key_rotation,
                )
            else:
                demo_project = self._pending_spec
                self._pending_spec = None
                self._current_reconcile_identity = (
                    self._pending_reconcile_identity
                    if self._pending_reconcile_identity is not None
                    else reconcile_identity(demo_project)
                )
                self._pending_reconcile_identity = None

            if demo_project is None:
                self._temporal_api_key_rotation = None
                continue

            self._reconcile_sequence += 1
            self._status = {
                "project": project_name,
                "phase": (
                    "RotatingTemporalAuth" if rotation_due else "Reconciling"
                ),
            }
            force_temporal_auth_rotation = (
                force_reconcile_requested
                and isinstance(force_reconcile, dict)
                and force_reconcile.get("reason")
                == FORCE_TEMPORAL_AUTH_ROTATION_REASON
            )
            external_plan = plan_external_resources(
                demo_project,
                checked_at=workflow.now().astimezone(UTC).isoformat(),
                force_temporal_auth_rotation=force_temporal_auth_rotation,
            )
            ecr_repository_activities = [
                workflow.execute_activity(
                    create_or_verify_ecr_repository,
                    repository,
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=COMMON_RETRY_POLICY,
                )
                for repository in external_plan.ecr_repositories
            ]
            ecr_repositories = list(
                await asyncio.gather(*ecr_repository_activities)
            )
            ecr_status = ecr_repositories_status(external_plan, ecr_repositories)
            temporal_status = await self._reconcile_temporal_cloud(external_plan)
            demo_project = demo_project_with_temporal_api_key(demo_project, temporal_status)
            self._last_demo_project = demo_project
            self._temporal_api_key_rotation = temporal_auth_rotation_state(
                temporal_status
            )
            secrets_status = await workflow.execute_activity(
                verify_secrets_manager_resources,
                external_plan.secrets_manager,
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=COMMON_RETRY_POLICY,
            )
            external_resources = external_resources_status(
                external_plan,
                ecr_status=ecr_status,
                temporal_status=temporal_status,
                secrets_status=secrets_status.status,
            )
            artifact_build = await workflow.execute_child_workflow(
                DemoProjectArtifactBuildWorkflow.run,
                DemoProjectArtifactBuildRequest(
                    demoProject=demo_project,
                    externalResources=external_resources,
                    previousBuildStatus=(
                        None
                        if force_reconcile_token(demo_project)
                        else self._last_build_status
                    ),
                ),
                id=self._child_workflow_id("artifact-build"),
            )
            build_status = artifact_build.status
            remembered_build_status = rememberable_build_status(build_status)
            if remembered_build_status is not None:
                self._last_build_status = remembered_build_status
            runtime_result = await workflow.execute_child_workflow(
                DemoProjectKubernetesRuntimeWorkflow.run,
                DemoProjectKubernetesRuntimeRequest(
                    project=project_name,
                    demoProject=demo_project,
                    externalResources=external_resources,
                    buildStatus=build_status,
                ),
                id=self._child_workflow_id("kubernetes-runtime"),
            )
            self._status = runtime_result.status
            await self._record_status(demo_project, self._status)

            self._complete_current_reconcile()
            self._continue_as_new_if_needed(project_name)

        self._status = {
            "project": project_name,
            "phase": "Stopped",
            "message": "Workflow received shutdown signal.",
        }

    @workflow.signal
    def reconcile(self, request: DemoProjectReconcileSignal) -> None:
        demo_project = request.demoProject
        identity = reconcile_identity(demo_project)
        if self._is_duplicate_reconcile_signal(identity):
            return
        self._pending_spec = demo_project
        self._pending_reconcile_identity = identity

    @workflow.signal
    def force_reconcile(self, signal: ForceReconcileSignal) -> None:
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
        _signal: CheckWorkerVersionSignal = CheckWorkerVersionSignal(),
    ) -> None:
        if workflow.info().is_target_worker_deployment_version_changed():
            self._continue_as_new_for_worker_version = True

    @workflow.signal
    def finalize(self, request: DemoProjectFinalizeSignal) -> None:
        self._finalize_spec = request.demoProject
        self._finalize = True

    @workflow.signal
    def shutdown(self, _signal: ShutdownSignal = ShutdownSignal()) -> None:
        self._shutdown = True

    async def _reconcile_temporal_cloud(
        self,
        external_plan: Any,
    ) -> dict[str, Any]:
        project = external_plan.temporal_project
        auth_mode = project.auth_mode
        candidate_secret_token = temporal_candidate_secret_token(
            external_plan,
            workflow_id=workflow.info().workflow_id,
            run_id=workflow.info().run_id,
            reconcile_sequence=self._reconcile_sequence,
        )
        namespace: TemporalCloudNamespaceResult | None = None
        service_account: TemporalCloudServiceAccountResult | None = None
        ca: TemporalCloudCredentialStageResult | None = None
        mtls_config: TemporalCloudStatusResult | None = None
        stage: TemporalCloudCredentialStageResult | None = None
        canary: TemporalCloudStatusResult | None = None
        ca_promote: TemporalCloudPromotionResult | None = None
        promote: TemporalCloudPromotionResult | TemporalCloudCredentialStageResult | None = None
        mirror: TemporalCloudStatusResult | None = None
        ca_promoted = False
        promoted = False

        try:
            if auth_mode == TEMPORAL_AUTH_MODE_MTLS:
                ca = await workflow.execute_activity(
                    stage_temporal_mtls_ca,
                    StageTemporalMtlsCaRequest(
                        checked_at=external_plan.checked_at,
                        project=project,
                        candidate_secret_token=candidate_secret_token,
                    ),
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=COMMON_RETRY_POLICY,
                )

            namespace = await workflow.execute_activity(
                create_or_verify_temporal_cloud_namespace,
                CreateOrVerifyTemporalCloudNamespaceRequest(
                    checked_at=external_plan.checked_at,
                    project=project,
                    candidate_ca_secret_path=(
                        ca.candidate_ca_secret_path if ca else None
                    ),
                    candidate_secret_token=candidate_secret_token,
                ),
                start_to_close_timeout=timedelta(minutes=10),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=COMMON_RETRY_POLICY,
            )
            if namespace.phase != "Ready":
                if auth_mode == TEMPORAL_AUTH_MODE_MTLS:
                    await self._delete_temporal_candidate_secret(
                        external_plan,
                        ca=ca,
                        candidate_secret_token=candidate_secret_token,
                    )
                return namespace.status

            if auth_mode == TEMPORAL_AUTH_MODE_MTLS:
                mtls_config = await workflow.execute_activity(
                    configure_temporal_cloud_namespace_mtls,
                    ConfigureTemporalCloudNamespaceMtlsRequest(
                        checked_at=external_plan.checked_at,
                        project=project,
                        namespace=namespace,
                        candidate_ca_secret_path=(
                            ca.candidate_ca_secret_path if ca else None
                        ),
                        candidate_secret_token=candidate_secret_token,
                    ),
                    start_to_close_timeout=timedelta(minutes=10),
                    heartbeat_timeout=timedelta(minutes=1),
                    retry_policy=COMMON_RETRY_POLICY,
                )
                stage = await workflow.execute_activity(
                    stage_temporal_mtls_client_certificate,
                    StageTemporalMtlsClientCertificateRequest(
                        checked_at=external_plan.checked_at,
                        project=project,
                        namespace=namespace,
                        ca=ca,
                        force_rotation=external_plan.force_temporal_auth_rotation,
                        candidate_secret_token=candidate_secret_token,
                    ),
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=COMMON_RETRY_POLICY,
                )
            elif auth_mode == TEMPORAL_AUTH_MODE_API_KEY:
                service_account = await workflow.execute_activity(
                    create_or_verify_temporal_cloud_service_account,
                    CreateOrVerifyTemporalCloudServiceAccountRequest(
                        checked_at=external_plan.checked_at,
                        project=project,
                        namespace=namespace,
                    ),
                    start_to_close_timeout=timedelta(minutes=10),
                    heartbeat_timeout=timedelta(minutes=1),
                    retry_policy=COMMON_RETRY_POLICY,
                )
                stage = await workflow.execute_activity(
                    stage_temporal_api_key_candidate,
                    StageTemporalApiKeyCandidateRequest(
                        checked_at=external_plan.checked_at,
                        project=project,
                        namespace=namespace,
                        service_account=service_account,
                        force_rotation=external_plan.force_temporal_auth_rotation,
                        candidate_secret_token=candidate_secret_token,
                    ),
                    start_to_close_timeout=timedelta(minutes=5),
                    heartbeat_timeout=timedelta(minutes=1),
                    retry_policy=COMMON_RETRY_POLICY,
                )
            else:
                return namespace.status

            if stage is None or stage.phase != "Ready":
                return merge_temporal_cloud_status(
                    namespace.status,
                    ca_status=ca.status if ca else None,
                    mtls_config_status=mtls_config.status if mtls_config else None,
                    service_account_status=(
                        service_account.status if service_account else None
                    ),
                    credential_status=stage.status if stage else None,
                    mirror_status=mirror.status if mirror else None,
                )

            if stage.promote_required:
                canary = await workflow.execute_activity(
                    run_temporal_runtime_candidate_canary,
                    RunTemporalRuntimeCandidateCanaryRequest(
                        checked_at=external_plan.checked_at,
                        project=project,
                        stage=stage,
                    ),
                    start_to_close_timeout=(
                        TEMPORAL_RUNTIME_CANDIDATE_CANARY_START_TO_CLOSE_TIMEOUT
                    ),
                    heartbeat_timeout=(
                        TEMPORAL_RUNTIME_CANDIDATE_CANARY_HEARTBEAT_TIMEOUT
                    ),
                    retry_policy=CHECK_RETRY_POLICY,
                )
                if stage.candidate_ca_secret_path:
                    ca_promote = await workflow.execute_activity(
                        promote_temporal_mtls_ca,
                        PromoteTemporalMtlsCaRequest(
                            checked_at=external_plan.checked_at,
                            project=project,
                            stage=stage,
                        ),
                        start_to_close_timeout=timedelta(minutes=5),
                        retry_policy=COMMON_RETRY_POLICY,
                    )
                    ca_promoted = ca_promote.ca_promoted
                promote = await workflow.execute_activity(
                    promote_temporal_runtime_credential,
                    PromoteTemporalRuntimeCredentialRequest(
                        checked_at=external_plan.checked_at,
                        project=project,
                        stage=stage,
                        canary=canary,
                    ),
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=COMMON_RETRY_POLICY,
                )
                if ca_promote is not None:
                    promote = TemporalCloudPromotionResult(
                        status=merge_promoted_status(
                            promote.status,
                            ca_promote.status,
                        ),
                        promoted=promote.promoted,
                        ca_promoted=ca_promote.ca_promoted,
                    )
                promoted = True
                await self._delete_temporal_candidate_secret(
                    external_plan,
                    stage=stage,
                    ca=ca,
                    candidate_secret_token=candidate_secret_token,
                )
            else:
                promote = stage

            mirror = await workflow.execute_activity(
                mirror_temporal_runtime_secret,
                MirrorTemporalRuntimeSecretRequest(
                    checked_at=external_plan.checked_at,
                    project=project,
                ),
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=COMMON_RETRY_POLICY,
            )
            return merge_temporal_cloud_status(
                namespace.status,
                ca_status=ca.status if ca else None,
                mtls_config_status=mtls_config.status if mtls_config else None,
                service_account_status=(
                    service_account.status if service_account else None
                ),
                credential_status=promote.status if promote else None,
                mirror_status=mirror.status if mirror else None,
            )
        except Exception:
            await self._compensate_temporal_cloud_reconcile(
                external_plan,
                namespace=namespace,
                service_account=service_account,
                stage=stage,
                ca=ca,
                ca_promoted=ca_promoted,
                promoted=promoted,
                candidate_secret_token=candidate_secret_token,
            )
            raise

    async def _delete_temporal_candidate_secret(
        self,
        external_plan: Any,
        *,
        stage: TemporalCloudCredentialStageResult | None = None,
        ca: TemporalCloudCredentialStageResult | None = None,
        candidate_secret_token: str | None = None,
    ) -> None:
        candidate_secret_path = stage.candidate_secret_path if stage else None
        candidate_ca_secret_path = None
        if stage and stage.candidate_ca_secret_path:
            candidate_ca_secret_path = stage.candidate_ca_secret_path
        elif ca and ca.candidate_ca_secret_path:
            candidate_ca_secret_path = ca.candidate_ca_secret_path
        await workflow.execute_activity(
            delete_temporal_runtime_candidate_secret,
            DeleteTemporalRuntimeCandidateSecretRequest(
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
        namespace: TemporalCloudNamespaceResult | None,
        service_account: TemporalCloudServiceAccountResult | None,
        stage: TemporalCloudCredentialStageResult | None,
        ca: TemporalCloudCredentialStageResult | None,
        ca_promoted: bool,
        promoted: bool,
        candidate_secret_token: str | None,
    ) -> None:
        if promoted:
            return

        candidate_key_id = stage.candidate_api_key_id if stage else None
        if candidate_key_id:
            await workflow.execute_activity(
                delete_temporal_cloud_api_key_by_id,
                DeleteTemporalCloudApiKeyRequest(
                    checked_at=external_plan.checked_at,
                    project=external_plan.temporal_project,
                    key_id=candidate_key_id,
                ),
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=COMMON_RETRY_POLICY,
            )

        await self._delete_temporal_candidate_secret(
            external_plan,
            stage=stage,
            ca=ca,
            candidate_secret_token=candidate_secret_token,
        )

        service_account_id = (
            service_account.service_account_id
            if service_account and service_account.created
            else None
        )
        if service_account_id:
            await workflow.execute_activity(
                delete_temporal_cloud_service_account_by_id,
                DeleteTemporalCloudServiceAccountRequest(
                    checked_at=external_plan.checked_at,
                    project=external_plan.temporal_project,
                    service_account_id=service_account_id,
                ),
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=COMMON_RETRY_POLICY,
            )

        namespace_name = namespace.namespace if namespace and namespace.created else None
        if namespace_name:
            if ca_promoted:
                await workflow.execute_activity(
                    delete_temporal_cloud_mtls_ca_secret,
                    DeleteTemporalCloudMtlsCaSecretRequest(
                        checked_at=external_plan.checked_at,
                        project=external_plan.temporal_project,
                    ),
                    start_to_close_timeout=timedelta(minutes=2),
                    retry_policy=COMMON_RETRY_POLICY,
                )
            await workflow.execute_activity(
                disable_temporal_cloud_namespace_delete_protection_by_name,
                TemporalCloudNamespaceRequest(
                    checked_at=external_plan.checked_at,
                    project=external_plan.temporal_project,
                    namespace=namespace_name,
                ),
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=COMMON_RETRY_POLICY,
            )
            await workflow.execute_activity(
                delete_temporal_cloud_namespace_by_name,
                TemporalCloudNamespaceRequest(
                    checked_at=external_plan.checked_at,
                    project=external_plan.temporal_project,
                    namespace=namespace_name,
                ),
                start_to_close_timeout=timedelta(minutes=10),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=COMMON_RETRY_POLICY,
            )

    async def _run_finalize(self, project_name: str) -> None:
        demo_project = (
            self._finalize_spec
            or self._pending_spec
            or {"metadata": {"name": project_name}}
        )
        finalize_plan_result = await workflow.execute_activity(
            load_project_finalization_plan,
            ProjectFinalizationRequest(demo_project),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        finalize_plan = finalize_plan_result.plan
        result = completed_activity_status(finalize_plan)
        if result is None:
            finalize_checked_at = finalize_plan.get("checkedAt")
            if not isinstance(finalize_checked_at, str) or not finalize_checked_at:
                finalize_checked_at = workflow.now().astimezone(UTC).isoformat()
            finalize_external_plan = plan_external_resources(
                demo_project,
                checked_at=finalize_checked_at,
            )
            temporal_delete_request = DeleteTemporalRuntimeCandidateSecretRequest(
                checked_at=finalize_external_plan.checked_at,
                project=finalize_external_plan.temporal_project,
            )
            namespace_action = await workflow.execute_activity(
                delete_project_namespace_resource,
                ProjectNamespaceDeleteRequest(finalize_plan["namespace"]),
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=COMMON_RETRY_POLICY,
            )
            namespace_action = namespace_action.action
            source_action = await workflow.execute_activity(
                delete_project_source_repository,
                ResourceDeleteRequest(
                    namespace=finalize_plan["sourceWatchNamespace"],
                    name=finalize_plan["gitRepository"],
                ),
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=COMMON_RETRY_POLICY,
            )
            source_action = source_action.action
            source_auth_action = await workflow.execute_activity(
                delete_project_source_auth_secret,
                OptionalResourceDeleteRequest(
                    namespace=finalize_plan["sourceWatchNamespace"],
                    name=finalize_plan.get("sourceAuthSecret"),
                ),
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=COMMON_RETRY_POLICY,
            )
            source_auth_action = source_auth_action.action
            temporal_api_key_teardown = await workflow.execute_activity(
                delete_temporal_cloud_api_key,
                temporal_delete_request,
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=timedelta(minutes=2),
                retry_policy=COMMON_RETRY_POLICY,
            )
            temporal_service_account_teardown = await workflow.execute_activity(
                delete_temporal_cloud_service_account,
                temporal_delete_request,
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=timedelta(minutes=2),
                retry_policy=COMMON_RETRY_POLICY,
            )
            temporal_namespace_protection_teardown = await workflow.execute_activity(
                disable_temporal_cloud_namespace_delete_protection,
                temporal_delete_request,
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=timedelta(minutes=2),
                retry_policy=COMMON_RETRY_POLICY,
            )
            temporal_namespace_teardown = await workflow.execute_activity(
                delete_temporal_cloud_namespace,
                temporal_delete_request,
                start_to_close_timeout=timedelta(minutes=5),
                heartbeat_timeout=timedelta(minutes=2),
                retry_policy=COMMON_RETRY_POLICY,
            )
            temporal_secret_teardown = await workflow.execute_activity(
                delete_temporal_cloud_runtime_secret,
                temporal_delete_request,
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=COMMON_RETRY_POLICY,
            )
            temporal_mtls_ca_teardown = await workflow.execute_activity(
                delete_temporal_cloud_mtls_ca_secret,
                DeleteTemporalCloudMtlsCaSecretRequest(
                    checked_at=finalize_external_plan.checked_at,
                    project=finalize_external_plan.temporal_project,
                ),
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=COMMON_RETRY_POLICY,
            )
            temporal_teardown = temporal_cloud_finalization_status(
                finalize_plan,
                api_key_status=temporal_api_key_teardown.status,
                service_account_status=temporal_service_account_teardown.status,
                namespace_protection_status=temporal_namespace_protection_teardown.status,
                namespace_status=temporal_namespace_teardown.status,
                runtime_secret_status=temporal_secret_teardown.status,
                mtls_ca_secret_status=temporal_mtls_ca_teardown.status,
            )
            ecr_teardown = await workflow.execute_activity(
                delete_ecr_repositories,
                DeleteEcrRepositoriesRequest(
                    checked_at=finalize_external_plan.checked_at,
                    repositories=finalize_external_plan.ecr_repositories,
                ),
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=COMMON_RETRY_POLICY,
            )
            external = external_resource_finalization_status(
                finalize_plan,
                temporal_status=temporal_teardown,
                ecr_status=ecr_teardown.status,
            )
            result = project_finalization_status(
                finalize_plan,
                namespace_action=namespace_action,
                source_action=source_action,
                source_auth_action=source_auth_action,
                external=external,
            )
            self._status = {
                "project": project_name,
                "phase": "Finalized",
                "message": "Project removed from git; resources torn down and workflow completed.",
                "finalizeStatus": result,
            }
            await self._record_status(demo_project, self._status)
            await workflow.execute_activity(
                remove_project_finalizer,
                ProjectFinalizerRemovalRequest(project=finalize_plan["project"]),
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=STATUS_RETRY_POLICY,
            )
        self._status = {
            "project": project_name,
            "phase": "Finalized",
            "message": "Project removed from git; resources torn down and workflow completed.",
            "finalizeStatus": result,
        }

    async def _record_status(
        self,
        demo_project: dict[str, Any],
        status: dict[str, Any],
    ) -> None:
        request = RecordDemoProjectStatusRequest(
            demo_project=demo_project,
            status=status_with_force_reconcile(status, demo_project),
        )
        await workflow.execute_activity(
            record_demo_project_status,
            request,
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=STATUS_RETRY_POLICY,
        )
        await workflow.execute_activity(
            record_catalog_project_status,
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
                DemoProjectRunRequest(
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
                    carried_pending_force_reconcile=(
                        self._pending_force_reconcile
                    ),
                    carried_completed_force_reconcile_token=(
                        self._completed_force_reconcile_token
                    ),
                )
            ],
        )

    async def _wait_for_next_action(self) -> None:
        while True:
            if self._has_pending_action() or self._temporal_api_key_rotation_due():
                return

            timeout = self._temporal_api_key_rotation_timeout()
            if timeout is None:
                await workflow.wait_condition(self._has_pending_action)
                return
            try:
                await workflow.wait_condition(
                    self._has_pending_action,
                    timeout=timeout,
                    timeout_summary="temporal-auth-rotation",
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


def remember_completed_reconcile_identity(status: Any) -> bool:
    if not isinstance(status, dict):
        return False
    phase = status.get("phase")
    if not isinstance(phase, str) or not phase:
        return False
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
    force_status = force_reconcile_status(resource)
    if force_status is None:
        return status
    return {
        **status,
        BASE_RECONCILE_KEY_STATUS_FIELD: demo_project_reconcile_key(
            resource_without_force_reconcile(resource)
        ),
        "forceReconcile": force_status,
    }


def merge_temporal_cloud_status(
    namespace_status: dict[str, Any],
    *,
    ca_status: dict[str, Any] | None,
    mtls_config_status: dict[str, Any] | None,
    service_account_status: dict[str, Any] | None,
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
