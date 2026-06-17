from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

with workflow.unsafe.imports_passed_through():
    from demo_registry_operator.activities.builds.codebuild.execution import (
        StartProjectImageBuildRequest,
        WaitForProjectImageBuildRequest,
        start_project_image_build,
        wait_for_project_image_build,
    )
    from demo_registry_operator.activities.builds.planning import (
        ResolveProjectImageBuildRequest,
        resolve_project_image_build_plan,
    )
    from demo_registry_operator.activities.builds.status import completed_build_status
    from demo_registry_operator.activities.canary import (
        apply_registry_operator_canary_project,
        delete_registry_operator_canary_project,
        ApplyRegistryOperatorCanaryProjectRequest,
        DeleteRegistryOperatorCanaryProjectRequest,
        RegistryOperatorCanaryPlanRequest,
        RegistryOperatorCanaryPromotionWaitRequest,
        RegistryOperatorCanaryRotationWaitRequest,
        RotateRegistryOperatorCanaryCredentialsRequest,
        SignalRegistryOperatorCanaryReconcileRequest,
        WaitRegistryOperatorCanaryCleanupRequest,
        prepare_registry_operator_canary_plan,
        rotate_registry_operator_canary_credentials,
        signal_registry_operator_canary_reconcile,
        wait_for_registry_operator_canary_promotion,
        wait_for_registry_operator_canary_rotation,
        wait_registry_operator_canary_cleanup,
    )
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
        ActivityActionResult,
        ActivityStatusResult,
        ApplyDemoProjectResourcesRequest,
        CandidateReadinessRequest,
        OptionalResourceDeleteRequest,
        ProjectFinalizationRequest,
        ProjectFinalizerRemovalRequest,
        ProjectNamespaceDeleteRequest,
        PromoteCandidateRoutesRequest,
        PruneStaleRuntimeCandidatesRequest,
        RecordDemoProjectStatusRequest,
        ResourceDeleteRequest,
        SmokeCheckRequest,
        WorkerDeploymentRolloutRequest,
        apply_demo_project_resources,
        completed_activity_status,
        delete_project_namespace_resource,
        delete_project_source_auth_secret,
        delete_project_source_repository,
        load_project_finalization_plan,
        plan_rollout,
        project_finalization_status,
        promote_candidate_routes,
        prune_stale_runtime_candidates,
        record_catalog_project_status,
        record_demo_project_status,
        remove_project_finalizer,
        run_smoke_check,
        wait_for_candidate_readiness,
        wait_for_worker_deployment_rollouts,
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
        worker_deployment_rollout_wait_seconds,
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
BUILD_WAIT_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=15),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=5,
    non_retryable_error_types=NON_RETRYABLE_ERROR_TYPES,
)
CHECK_RETRY_POLICY = RetryPolicy(
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(seconds=30),
    maximum_attempts=3,
    non_retryable_error_types=NON_RETRYABLE_ERROR_TYPES,
)
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
        project_name: str,
        carried_status: dict[str, Any] | None = None,
        carried_pending_spec: dict[str, Any] | None = None,
        carried_last_build_status: dict[str, Any] | None = None,
        carried_completed_reconcile_identity: dict[str, str] | None = None,
        carried_last_demo_project: dict[str, Any] | None = None,
        carried_temporal_api_key_rotation: dict[str, Any] | None = None,
        carried_pending_force_reconcile: dict[str, str] | None = None,
        carried_completed_force_reconcile_token: str | None = None,
    ) -> None:
        self._project_name = project_name
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
        project_name: str,
        carried_status: dict[str, Any] | None = None,
        carried_pending_spec: dict[str, Any] | None = None,
        carried_last_build_status: dict[str, Any] | None = None,
        carried_completed_reconcile_identity: dict[str, str] | None = None,
        carried_last_demo_project: dict[str, Any] | None = None,
        carried_temporal_api_key_rotation: dict[str, Any] | None = None,
        carried_pending_force_reconcile: dict[str, str] | None = None,
        carried_completed_force_reconcile_token: str | None = None,
    ) -> None:
        self._project_name = project_name
        # Do NOT clobber a pending spec set by a start signal. With
        # SignalWithStart the reconcile signal is delivered on the first workflow
        # task and its handler sets self._pending_spec before/around run()'s body;
        # unconditionally assigning carried_pending_spec (None on a fresh start)
        # would wipe it, leaving wait_condition permanently false. Only adopt a
        # spec actually carried over from continue-as-new.
        if carried_pending_spec is not None:
            self._pending_spec = carried_pending_spec
            self._pending_reconcile_identity = reconcile_identity(carried_pending_spec)
        if carried_last_build_status is not None:
            self._last_build_status = carried_last_build_status
        if carried_last_demo_project is not None:
            self._last_demo_project = carried_last_demo_project
        if carried_temporal_api_key_rotation is not None:
            self._temporal_api_key_rotation = carried_temporal_api_key_rotation
        if carried_pending_force_reconcile is not None:
            self._pending_force_reconcile = carried_pending_force_reconcile
        if carried_completed_force_reconcile_token is not None:
            self._completed_force_reconcile_token = (
                carried_completed_force_reconcile_token
            )
        if (
            carried_completed_reconcile_identity is not None
            and remember_completed_reconcile_identity(carried_status)
        ):
            self._completed_reconcile_identity = carried_completed_reconcile_identity
        if carried_status is not None:
            self._status = carried_status
            if self._last_build_status is None:
                self._last_build_status = rememberable_build_status(
                    carried_status.get("buildStatus")
                )
            if (
                self._completed_reconcile_identity is None
                and remember_completed_reconcile_identity(carried_status)
            ):
                self._completed_reconcile_identity = status_reconcile_identity(
                    carried_status
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
            ecr_repositories = []
            for repository in external_plan.ecr_repositories:
                ecr_repositories.append(
                    await workflow.execute_activity(
                        create_or_verify_ecr_repository,
                        repository,
                        start_to_close_timeout=timedelta(minutes=5),
                        retry_policy=COMMON_RETRY_POLICY,
                    )
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
            build_plan_result = await workflow.execute_activity(
                resolve_project_image_build_plan,
                ResolveProjectImageBuildRequest(
                    demo_project=demo_project,
                    external_resources=external_resources,
                    previous_build_status=(
                        None
                        if force_reconcile_token(demo_project)
                        else self._last_build_status
                    ),
                ),
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=COMMON_RETRY_POLICY,
            )
            build_plan = build_plan_result.plan
            build_status = completed_build_status(build_plan)
            if build_status is None:
                started_build_result = await workflow.execute_activity(
                    start_project_image_build,
                    StartProjectImageBuildRequest(build_plan),
                    start_to_close_timeout=timedelta(minutes=5),
                    heartbeat_timeout=timedelta(minutes=1),
                    retry_policy=COMMON_RETRY_POLICY,
                )
                started_build = started_build_result.plan
                build_status = completed_build_status(started_build)
                if build_status is None:
                    build_status_result = await workflow.execute_activity(
                        wait_for_project_image_build,
                        WaitForProjectImageBuildRequest(started_build),
                        start_to_close_timeout=timedelta(hours=1),
                        heartbeat_timeout=timedelta(minutes=2),
                        retry_policy=BUILD_WAIT_RETRY_POLICY,
                    )
                    build_status = build_status_result.status
            remembered_build_status = rememberable_build_status(build_status)
            if remembered_build_status is not None:
                self._last_build_status = remembered_build_status
            reconcile_result = await workflow.execute_activity(
                apply_demo_project_resources,
                ApplyDemoProjectResourcesRequest(
                    demo_project=demo_project,
                    external_resources=external_resources,
                    build_status=build_status,
                ),
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=COMMON_RETRY_POLICY,
            )
            reconcile_status = reconcile_result.status
            self._status = reconcile_status

            smoke_plan = {
                "project": project_name,
                "demoProject": demo_project,
                "host": reconcile_status.get("desired", {}).get("host"),
                "candidate": reconcile_status.get("candidateRolloutStatus", {}),
                "smokeCheck": demo_project.get("spec", {}).get("smokeCheck"),
                "workerDeployments": reconcile_status.get(
                    "candidateRolloutStatus",
                    {},
                ).get("workerDeployments", {}),
                "workerDeploymentTimeoutSeconds": (
                    worker_deployment_rollout_wait_seconds(
                        demo_project,
                        base_timeout_seconds=300,
                    )
                ),
            }

            if reconcile_status.get("phase") == "WorkerRolloutPending":
                worker_rollout_activity = await workflow.execute_activity(
                    wait_for_worker_deployment_rollouts,
                    WorkerDeploymentRolloutRequest(
                        demo_project=demo_project,
                        worker_deployments=smoke_plan.get("workerDeployments", {}),
                        timeout_seconds=smoke_plan.get("workerDeploymentTimeoutSeconds"),
                    ),
                    start_to_close_timeout=timedelta(
                        seconds=int(
                            smoke_plan.get("workerDeploymentTimeoutSeconds", 300)
                        )
                        + 60
                    ),
                    heartbeat_timeout=timedelta(minutes=2),
                    retry_policy=CHECK_RETRY_POLICY,
                )
                worker_rollout_result = worker_rollout_activity.status
                if not worker_rollout_result.get("ok"):
                    candidate_status = reconcile_status.get(
                        "candidateRolloutStatus",
                        {},
                    )
                    self._status = {
                        **reconcile_status,
                        "phase": "WorkerRolloutFailed",
                        "message": "WorkerDeployment rollout did not become ready; active route was not promoted.",
                        "workerDeploymentStatus": worker_rollout_result,
                        "candidateRolloutStatus": {
                            **candidate_status,
                            "phase": "WorkerRolloutFailed",
                            "message": "WorkerDeployment rollout did not become ready; active route was not promoted.",
                        },
                    }
                    await self._record_status(demo_project, self._status)
                    self._complete_current_reconcile()
                    self._continue_as_new_if_needed(project_name)
                    continue

                reconcile_status = {
                    **reconcile_status,
                    "phase": (
                        "SmokeCheckPending"
                        if demo_project.get("spec", {}).get("smokeCheck")
                        else "WorkerRolloutReady"
                    ),
                    "message": "WorkerDeployment rollout is ready.",
                    "workerDeploymentStatus": worker_rollout_result,
                }
                self._status = reconcile_status
                await self._record_status(demo_project, self._status)

                if not demo_project.get("spec", {}).get("smokeCheck"):
                    promotion_result = await workflow.execute_activity(
                        promote_candidate_routes,
                        PromoteCandidateRoutesRequest(
                            project=project_name,
                            demo_project=demo_project,
                            candidate=smoke_plan.get("candidate", {}),
                        ),
                        start_to_close_timeout=timedelta(minutes=5),
                        retry_policy=COMMON_RETRY_POLICY,
                    )
                    promotion = promotion_result.status
                    candidate_status = promotion.get(
                        "candidate",
                        reconcile_status.get("candidateRolloutStatus", {}),
                    )
                    prune_status = await self._prune_promoted_candidate(
                        demo_project,
                        candidate_status,
                    )
                    promoted_status = {
                        **reconcile_status,
                        "phase": promotion.get("phase", "Promoted"),
                        "message": promotion.get("message"),
                        "activeImages": candidate_status.get("images", {}),
                        "activeRouteServices": candidate_status.get("services", {}),
                        "candidateRolloutStatus": {
                            **candidate_status,
                            "phase": "Promoted",
                            "completedAt": worker_rollout_result.get("checkedAt"),
                            "message": "WorkerDeployment rollout passed and active route resources were promoted.",
                        },
                        "appliedResources": [
                            *reconcile_status.get("appliedResources", []),
                            *promotion.get("appliedResources", []),
                        ],
                        "pruneStatus": prune_status,
                        "prunedResources": prune_status.get("resources", []),
                    }
                    active_version = candidate_status.get("imageVersion")
                    if isinstance(active_version, str):
                        promoted_status["activeImageVersion"] = active_version
                    self._status = promoted_status
                    await self._record_status(demo_project, self._status)
                    self._complete_current_reconcile()
                    self._continue_as_new_if_needed(project_name)
                    continue

            if reconcile_status.get("phase") == "SmokeCheckPending":
                readiness_activity = await workflow.execute_activity(
                    wait_for_candidate_readiness,
                    CandidateReadinessRequest(
                        demo_project=demo_project,
                        candidate=smoke_plan.get("candidate", {}),
                    ),
                    start_to_close_timeout=timedelta(minutes=10),
                    heartbeat_timeout=timedelta(minutes=2),
                    retry_policy=CHECK_RETRY_POLICY,
                )
                readiness_result = readiness_activity.status
                if not readiness_result.get("ok"):
                    candidate_status = reconcile_status.get(
                        "candidateRolloutStatus",
                        {},
                    )
                    self._status = {
                        **reconcile_status,
                        "phase": "CandidateNotReady",
                        "message": "Candidate did not become ready; active route was not promoted.",
                        "candidateRolloutStatus": {
                            **candidate_status,
                            "phase": "CandidateNotReady",
                            "message": "Candidate did not become ready; active route was not promoted.",
                        },
                        "readinessStatus": readiness_result,
                    }
                    await self._record_status(demo_project, self._status)
                    self._complete_current_reconcile()
                    self._continue_as_new_if_needed(project_name)
                    continue

                self._status = {
                    **reconcile_status,
                    "readinessStatus": readiness_result,
                }
                await self._record_status(demo_project, self._status)

                smoke_activity = await workflow.execute_activity(
                    run_smoke_check,
                    SmokeCheckRequest(
                        project=project_name,
                        demo_project=demo_project,
                        host=smoke_plan.get("host"),
                        candidate=smoke_plan.get("candidate", {}),
                        smoke_check=smoke_plan.get("smokeCheck"),
                    ),
                    start_to_close_timeout=timedelta(minutes=5),
                    heartbeat_timeout=timedelta(minutes=2),
                    retry_policy=CHECK_RETRY_POLICY,
                )
                smoke_result = smoke_activity.status
                if not smoke_result.get("ok"):
                    candidate_status = reconcile_status.get(
                        "candidateRolloutStatus",
                        {},
                    )
                    self._status = {
                        **reconcile_status,
                        "phase": "SmokeCheckFailed",
                        "message": "Candidate smoke check failed; active route was not promoted.",
                        **(
                            {"readinessStatus": readiness_result}
                            if readiness_result
                            else {}
                        ),
                        "candidateRolloutStatus": {
                            **candidate_status,
                            "phase": "SmokeCheckFailed",
                            "message": "Candidate smoke check failed; active route was not promoted.",
                        },
                        "smokeCheckStatus": {
                            "phase": "Failed",
                            "path": smoke_result.get("smokeCheck", {}).get("path"),
                            "expectedStatus": smoke_result.get("expectedStatus"),
                            "actualStatus": smoke_result.get("actualStatus"),
                            "checkedAt": smoke_result.get("checkedAt"),
                            "message": smoke_result.get("message"),
                        },
                    }
                else:
                    promotion_result = await workflow.execute_activity(
                        promote_candidate_routes,
                        PromoteCandidateRoutesRequest(
                            project=project_name,
                            demo_project=demo_project,
                            candidate=smoke_plan.get("candidate", {}),
                        ),
                        start_to_close_timeout=timedelta(minutes=5),
                        retry_policy=COMMON_RETRY_POLICY,
                    )
                    promotion = promotion_result.status
                    candidate_status = promotion.get(
                        "candidate",
                        reconcile_status.get("candidateRolloutStatus", {}),
                    )
                    prune_status = await self._prune_promoted_candidate(
                        demo_project,
                        candidate_status,
                    )
                    promoted_status = {
                        **reconcile_status,
                        "phase": promotion.get("phase", "Promoted"),
                        "message": promotion.get("message"),
                        "activeImages": candidate_status.get("images", {}),
                        "activeRouteServices": candidate_status.get("services", {}),
                        **(
                            {"readinessStatus": readiness_result}
                            if readiness_result
                            else {}
                        ),
                        "candidateRolloutStatus": {
                            **candidate_status,
                            "phase": "Promoted",
                            "completedAt": smoke_result.get("checkedAt"),
                            "message": "Candidate passed smoke check and was promoted.",
                        },
                        "appliedResources": [
                            *reconcile_status.get("appliedResources", []),
                            *promotion.get("appliedResources", []),
                        ],
                        "pruneStatus": prune_status,
                        "prunedResources": prune_status.get("resources", []),
                        "smokeCheckStatus": {
                            "phase": "Passed",
                            "path": smoke_result.get("smokeCheck", {}).get("path"),
                            "expectedStatus": smoke_result.get("expectedStatus"),
                            "actualStatus": smoke_result.get("actualStatus"),
                            "checkedAt": smoke_result.get("checkedAt"),
                            "message": smoke_result.get("message"),
                        },
                    }
                    active_version = candidate_status.get("imageVersion")
                    if isinstance(active_version, str):
                        promoted_status["activeImageVersion"] = active_version
                    self._status = promoted_status

                await self._record_status(demo_project, self._status)
            else:
                await self._record_status(demo_project, self._status)

            self._complete_current_reconcile()
            self._continue_as_new_if_needed(project_name)

        self._status = {
            "project": project_name,
            "phase": "Stopped",
            "message": "Workflow received shutdown signal.",
        }

    @workflow.signal
    def reconcile(self, demo_project: dict[str, Any]) -> None:
        identity = reconcile_identity(demo_project)
        if self._is_duplicate_reconcile_signal(identity):
            return
        self._pending_spec = demo_project
        self._pending_reconcile_identity = identity

    @workflow.signal
    def force_reconcile(self, token: str, reason: str | None = None) -> None:
        request = force_reconcile_request(token, reason)
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
    def check_worker_version(self) -> None:
        if workflow.info().is_target_worker_deployment_version_changed():
            self._continue_as_new_for_worker_version = True

    @workflow.signal
    def finalize(self, demo_project: dict[str, Any]) -> None:
        self._finalize_spec = demo_project
        self._finalize = True

    @workflow.signal
    def shutdown(self) -> None:
        self._shutdown = True

    async def _reconcile_temporal_cloud(
        self,
        external_plan: Any,
    ) -> dict[str, Any]:
        project = external_plan.temporal_project
        auth_mode = project.auth_mode
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
                    ),
                    start_to_close_timeout=timedelta(minutes=5),
                    retry_policy=COMMON_RETRY_POLICY,
                )

            namespace = await workflow.execute_activity(
                create_or_verify_temporal_cloud_namespace,
                CreateOrVerifyTemporalCloudNamespaceRequest(
                    checked_at=external_plan.checked_at,
                    project=project,
                ),
                start_to_close_timeout=timedelta(minutes=10),
                heartbeat_timeout=timedelta(minutes=1),
                retry_policy=COMMON_RETRY_POLICY,
            )
            if namespace.phase != "Ready":
                if auth_mode == TEMPORAL_AUTH_MODE_MTLS:
                    await self._delete_temporal_candidate_secret(external_plan)
                return namespace.status

            if auth_mode == TEMPORAL_AUTH_MODE_MTLS:
                mtls_config = await workflow.execute_activity(
                    configure_temporal_cloud_namespace_mtls,
                    ConfigureTemporalCloudNamespaceMtlsRequest(
                        checked_at=external_plan.checked_at,
                        project=project,
                        namespace=namespace,
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
                    start_to_close_timeout=timedelta(minutes=3),
                    heartbeat_timeout=timedelta(seconds=30),
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
                await self._delete_temporal_candidate_secret(external_plan)
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
                ca_promoted=ca_promoted,
                promoted=promoted,
            )
            raise

    async def _delete_temporal_candidate_secret(
        self,
        external_plan: Any,
    ) -> None:
        await workflow.execute_activity(
            delete_temporal_runtime_candidate_secret,
            DeleteTemporalRuntimeCandidateSecretRequest(
                checked_at=external_plan.checked_at,
                project=external_plan.temporal_project,
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
        ca_promoted: bool,
        promoted: bool,
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

        await self._delete_temporal_candidate_secret(external_plan)

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

    async def _prune_promoted_candidate(
        self,
        demo_project: dict[str, Any],
        candidate_status: dict[str, Any],
    ) -> dict[str, Any]:
        active_version = candidate_status.get("imageVersion")
        if not isinstance(active_version, str) or not active_version:
            return {
                "phase": "Skipped",
                "message": "Promoted candidate did not report an image version.",
                "resources": [],
            }
        prune_result = await workflow.execute_activity(
            prune_stale_runtime_candidates,
            PruneStaleRuntimeCandidatesRequest(
                demo_project=demo_project,
                active_version=active_version,
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        return prune_result.status

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
                project_name,
                self._status,
                self._pending_spec,
                self._last_build_status,
                self._completed_reconcile_identity,
                self._last_demo_project,
                self._temporal_api_key_rotation,
                self._pending_force_reconcile,
                self._completed_force_reconcile_token,
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


@workflow.defn
class DemoRolloutWorkflow:
    """Smoke-test-gated rollout workflow for a demo update."""

    @workflow.run
    async def run(self, request: dict[str, Any]) -> dict[str, Any]:
        plan = plan_rollout(request)
        smoke_activity = await workflow.execute_activity(
            run_smoke_check,
            SmokeCheckRequest(
                project=str(plan.get("project", "unknown")),
                demo_project=plan.get("demoProject"),
                host=plan.get("host"),
                candidate=plan.get("candidate", {}),
                smoke_check=plan.get("smokeCheck"),
            ),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=CHECK_RETRY_POLICY,
        )
        smoke_result = smoke_activity.status
        if not smoke_result.get("ok"):
            return {
                "project": plan.get("project"),
                "phase": "SmokeCheckFailed",
                "smokeCheck": smoke_result,
            }

        promotion_result = await workflow.execute_activity(
            promote_candidate_routes,
            PromoteCandidateRoutesRequest(
                project=str(plan.get("project", "unknown")),
                demo_project=plan.get("demoProject", {}),
                candidate=plan.get("candidate", {}),
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        promotion = promotion_result.status
        prune_status = await self._prune_rollout_candidate(plan, promotion)
        return {
            **promotion,
            "pruneStatus": prune_status,
            "prunedResources": prune_status.get("resources", []),
            "smokeCheck": smoke_result,
        }

    async def _prune_rollout_candidate(
        self,
        plan: dict[str, Any],
        promotion: dict[str, Any],
    ) -> dict[str, Any]:
        demo_project = plan.get("demoProject")
        candidate = promotion.get("candidate", plan.get("candidate", {}))
        active_version = candidate.get("imageVersion") if isinstance(candidate, dict) else None
        if not isinstance(demo_project, dict) or not isinstance(active_version, str):
            return {"phase": "Skipped", "resources": []}
        prune_result = await workflow.execute_activity(
            prune_stale_runtime_candidates,
            PruneStaleRuntimeCandidatesRequest(
                demo_project=demo_project,
                active_version=active_version,
            ),
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=COMMON_RETRY_POLICY,
        )
        return prune_result.status


@workflow.defn
class RegistryOperatorCanaryWorkflow:
    """End-to-end gate workflow for registry-operator WorkerDeployment rollouts."""

    @workflow.run
    async def run(self, request: dict[str, Any]) -> dict[str, Any]:
        workflow_info = workflow.info()
        canary_results = await asyncio.gather(
            self._run_auth_canary(request, "apiKey", workflow_info),
            self._run_auth_canary(request, "mtls", workflow_info),
        )
        canaries = {
            str(result.get("authMode")): result for result in canary_results
        }
        ok = all(result.get("phase") == "Succeeded" for result in canary_results)
        result = {
            "phase": "Succeeded" if ok else "Failed",
            "canaries": canaries,
        }
        if result["phase"] != "Succeeded":
            raise ApplicationError(
                "Registry operator canary failed; one or more auth-mode canaries did not pass",
                result,
                type="RegistryOperatorCanaryFailed",
                non_retryable=True,
            )
        return result

    async def _run_auth_canary(
        self,
        request: dict[str, Any],
        auth_mode: str,
        workflow_info: Any,
    ) -> dict[str, Any]:
        branch_request = registry_operator_canary_branch_request(
            request,
            auth_mode=auth_mode,
            workflow_id=workflow_info.workflow_id,
            run_id=workflow_info.run_id,
        )
        plan_result = await workflow.execute_activity(
            prepare_registry_operator_canary_plan,
            RegistryOperatorCanaryPlanRequest(
                demo_project_template=(
                    branch_request.get("demoProjectTemplate")
                    or branch_request.get("demoProject")
                    or {}
                ),
                workflow_id=branch_request["workflowID"],
                run_id=branch_request["runID"],
                project_name_prefix=branch_request.get(
                    "projectNamePrefix",
                    "operator-canary",
                ),
                project_name=branch_request.get("projectName"),
                expected_source_commit_sha=branch_request.get("expectedSourceCommitSha"),
                expected_images=branch_request.get("expectedImages"),
                auth_mode=branch_request.get("authMode"),
                expected_operator_image=branch_request.get("expectedOperatorImage"),
                expected_operator_source_sha=branch_request.get(
                    "expectedOperatorSourceSha"
                ),
                timeout_seconds=int(branch_request.get("timeoutSeconds", 900)),
                poll_seconds=int(branch_request.get("pollSeconds", 10)),
                cleanup_timeout_seconds=int(
                    branch_request.get("cleanupTimeoutSeconds", 600)
                ),
                cleanup_policy=str(branch_request.get("cleanupPolicy", "Always")),
            ),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        plan = plan_result.plan
        apply_activity = await workflow.execute_activity(
            apply_registry_operator_canary_project,
            ApplyRegistryOperatorCanaryProjectRequest(
                project=plan["project"],
                namespace=plan["namespace"],
                demo_project=plan["demoProject"],
            ),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        apply_result = apply_activity.status
        signal_activity = await workflow.execute_activity(
            signal_registry_operator_canary_reconcile,
            SignalRegistryOperatorCanaryReconcileRequest(
                project=plan["project"],
                demo_project=plan["demoProject"],
                target_deployment_name=plan["targetDeploymentName"],
                target_build_id=plan["targetBuildID"],
            ),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=COMMON_RETRY_POLICY,
        )
        signal_result = signal_activity.status
        promotion_result: dict[str, Any] = {
            "phase": "NotStarted",
            "ok": False,
        }
        cleanup_result: dict[str, Any] = {
            "phase": "NotStarted",
            "ok": False,
        }
        rotation_signal_result: dict[str, Any] = {
            "phase": "NotStarted",
            "ok": False,
        }
        rotation_result: dict[str, Any] = {
            "phase": "NotStarted",
            "ok": False,
        }
        promotion_ok = False
        rotation_ok = False
        try:
            timeout_seconds = max(1, int(plan.get("timeoutSeconds", 900)))
            promotion_activity = await workflow.execute_activity(
                wait_for_registry_operator_canary_promotion,
                RegistryOperatorCanaryPromotionWaitRequest(
                    project=plan["project"],
                    namespace=plan["namespace"],
                    expected_source_commit_sha=plan.get("expectedSourceCommitSha"),
                    expected_images=plan.get("expectedImages", {}),
                    timeout_seconds=timeout_seconds,
                    poll_seconds=int(plan.get("pollSeconds", 10)),
                ),
                start_to_close_timeout=timedelta(seconds=timeout_seconds + 60),
                heartbeat_timeout=timedelta(minutes=2),
                retry_policy=CHECK_RETRY_POLICY,
            )
            promotion_result = promotion_activity.status
            promotion_ok = bool(promotion_result.get("ok"))
            if promotion_ok:
                rotation_signal_activity = await workflow.execute_activity(
                    rotate_registry_operator_canary_credentials,
                    RotateRegistryOperatorCanaryCredentialsRequest(
                        project=plan["project"],
                    ),
                    start_to_close_timeout=timedelta(minutes=2),
                    retry_policy=COMMON_RETRY_POLICY,
                )
                rotation_signal_result = rotation_signal_activity.status
                rotation_activity = await workflow.execute_activity(
                    wait_for_registry_operator_canary_rotation,
                    RegistryOperatorCanaryRotationWaitRequest(
                        project=plan["project"],
                        auth_mode=plan["authMode"],
                        rotation_token=str(rotation_signal_result.get("token") or ""),
                        timeout_seconds=timeout_seconds,
                        poll_seconds=int(plan.get("pollSeconds", 10)),
                    ),
                    start_to_close_timeout=timedelta(seconds=timeout_seconds + 60),
                    heartbeat_timeout=timedelta(minutes=2),
                    retry_policy=CHECK_RETRY_POLICY,
                )
                rotation_result = rotation_activity.status
                rotation_ok = bool(rotation_result.get("ok"))
        finally:
            delete_activity = await workflow.execute_activity(
                delete_registry_operator_canary_project,
                DeleteRegistryOperatorCanaryProjectRequest(
                    project=plan["project"],
                    cleanup_policy=str(plan.get("cleanupPolicy", "Always")),
                ),
                start_to_close_timeout=timedelta(minutes=2),
                heartbeat_timeout=timedelta(minutes=2),
                retry_policy=CHECK_RETRY_POLICY,
            )
            delete_result = delete_activity.status
            cleanup_result = delete_result
            if delete_result.get("phase") != "Skipped":
                cleanup_timeout_seconds = max(
                    1,
                    int(plan.get("cleanupTimeoutSeconds", 600)),
                )
                cleanup_activity = await workflow.execute_activity(
                    wait_registry_operator_canary_cleanup,
                    WaitRegistryOperatorCanaryCleanupRequest(
                        project=plan["project"],
                        namespace=plan["namespace"],
                        git_repository=plan["gitRepository"],
                        source_watch_namespace=plan["sourceWatchNamespace"],
                        catalog_status_namespace=plan["catalogStatusNamespace"],
                        catalog_status_mirror_namespace=plan[
                            "catalogStatusMirrorNamespace"
                        ],
                        catalog_status_config_map=plan["catalogStatusConfigMap"],
                        cleanup_policy=str(plan.get("cleanupPolicy", "Always")),
                        cleanup_timeout_seconds=cleanup_timeout_seconds,
                        poll_seconds=int(plan.get("pollSeconds", 10)),
                        delete_action=delete_result.get("deleteAction"),
                    ),
                    start_to_close_timeout=timedelta(
                        seconds=cleanup_timeout_seconds + 60
                    ),
                    heartbeat_timeout=timedelta(minutes=2),
                    retry_policy=CHECK_RETRY_POLICY,
                )
                cleanup_result = cleanup_activity.status

        succeeded = promotion_ok and rotation_ok and bool(cleanup_result.get("ok"))
        result = {
            "project": plan.get("project"),
            "authMode": auth_mode,
            "phase": "Succeeded" if succeeded else "Failed",
            "plan": plan,
            "apply": apply_result,
            "workflow": signal_result,
            "promotion": promotion_result,
            "rotationSignal": rotation_signal_result,
            "rotation": rotation_result,
            "cleanup": cleanup_result,
        }
        return result


def registry_operator_canary_branch_request(
    request: dict[str, Any],
    *,
    auth_mode: str,
    workflow_id: str,
    run_id: str,
) -> dict[str, Any]:
    suffix = "api-key" if auth_mode == "apiKey" else auth_mode
    prefix = request.get("projectNamePrefix")
    prefix = prefix if isinstance(prefix, str) and prefix else "operator-canary"
    rendered = {
        **request,
        "authMode": auth_mode,
        "workflowID": f"{workflow_id}/{auth_mode}",
        "runID": run_id,
        "projectNamePrefix": f"{prefix}-{suffix}",
    }
    project_name = request.get("projectName")
    if isinstance(project_name, str) and project_name:
        rendered["projectName"] = f"{project_name}-{suffix}"
    return rendered
