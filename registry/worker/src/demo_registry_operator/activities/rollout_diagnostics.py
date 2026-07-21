from __future__ import annotations

import gzip
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from temporalio import activity

from demo_registry_operator import errors, kube
from demo_registry_operator.config import WorkerConfig


S3_API_TIMEOUT_SECONDS = 10


@dataclass(frozen=True)
class CaptureRolloutDiagnosticsRequest:
    project: str
    candidate_version: str
    namespace: str
    failure_phase: str
    failure_status: dict[str, Any]
    label_selector: str | None = None


@dataclass(frozen=True)
class CaptureRolloutDiagnosticsResult:
    status: dict[str, Any]


@activity.defn
def capture_rollout_diagnostics(
    request: CaptureRolloutDiagnosticsRequest,
) -> CaptureRolloutDiagnosticsResult:
    """Capture bounded Kubernetes failure evidence and atomically publish it to S3."""
    config = WorkerConfig.from_env()
    bucket = config.rollout_diagnostics_bucket
    if not config.cluster:
        return CaptureRolloutDiagnosticsResult(
            {
                "phase": "Skipped",
                "ok": True,
                "reason": "UncoloredCluster",
                "message": "Rollout diagnostic capture is not enabled on the legacy cluster.",
            }
        )
    if not bucket:
        return CaptureRolloutDiagnosticsResult(
            {
                "phase": "Skipped",
                "ok": True,
                "reason": "BucketNotConfigured",
                "message": "No rollout diagnostics bucket is configured.",
            }
        )
    for field_name, value in (
        ("project", request.project),
        ("candidate_version", request.candidate_version),
        ("namespace", request.namespace),
        ("failure_phase", request.failure_phase),
    ):
        if not isinstance(value, str) or not value.strip():
            raise errors.non_retryable_error(
                f"Capture rollout diagnostics requires {field_name}.",
                error_type=errors.VALIDATION_ERROR,
            )

    activity.heartbeat(
        {
            "project": request.project,
            "namespace": request.namespace,
            "phase": "CollectingKubernetesDiagnostics",
        }
    )
    snapshot = kube.collect_namespace_diagnostics(
        request.namespace,
        label_selector=request.label_selector,
        progress_callback=activity.heartbeat,
    )
    captured_at = datetime.now(UTC)
    artifact = {
        "schemaVersion": 1,
        "capturedAt": captured_at.isoformat(),
        "cluster": config.cluster,
        "project": request.project,
        "candidateVersion": request.candidate_version,
        "namespace": request.namespace,
        "failure": {
            "phase": request.failure_phase,
            "status": request.failure_status,
        },
        "kubernetes": snapshot,
    }
    uncompressed = json.dumps(
        artifact,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        default=str,
    ).encode("utf-8")
    body = gzip.compress(uncompressed, mtime=0)
    digest = hashlib.sha256(body).hexdigest()
    timestamp = captured_at.strftime("%Y%m%dT%H%M%S.%fZ")
    key = "/".join(
        [
            "clusters",
            s3_key_segment(config.cluster),
            "projects",
            s3_key_segment(request.project),
            "candidates",
            s3_key_segment(request.candidate_version),
            f"{s3_key_segment(request.failure_phase)}-{timestamp}.json.gz",
        ]
    )

    try:
        import boto3
        from botocore.config import Config
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as error:
        raise errors.non_retryable_error(
            f"boto3 is required for rollout diagnostic capture: {error}",
            error_type=errors.CONFIGURATION_ERROR,
        ) from error

    s3 = boto3.session.Session(region_name=config.aws_region).client(
        "s3",
        config=Config(
            connect_timeout=S3_API_TIMEOUT_SECONDS,
            read_timeout=S3_API_TIMEOUT_SECONDS,
            retries={"total_max_attempts": 1, "mode": "standard"},
        ),
    )
    activity.heartbeat(
        {
            "project": request.project,
            "namespace": request.namespace,
            "phase": "UploadingDiagnostics",
            "bucket": bucket,
            "key": key,
        }
    )
    try:
        response = s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="application/json",
            ContentEncoding="gzip",
            ServerSideEncryption="AES256",
            Metadata={
                "cluster": config.cluster,
                "project": request.project,
                "failure-phase": request.failure_phase,
                "sha256": digest,
            },
        )
    except (BotoCoreError, ClientError) as error:
        raise errors.aws_application_error(
            error,
            operation=(
                f"Upload rollout diagnostics for {request.project} to s3://{bucket}/{key}"
            ),
        ) from error

    summary = snapshot.get("summary", {})
    return CaptureRolloutDiagnosticsResult(
        {
            "phase": "Captured",
            "ok": True,
            "capturedAt": captured_at.isoformat(),
            "cluster": config.cluster,
            "project": request.project,
            "candidateVersion": request.candidate_version,
            "namespace": request.namespace,
            "failurePhase": request.failure_phase,
            "s3Uri": f"s3://{bucket}/{key}",
            "bucket": bucket,
            "key": key,
            "etag": response.get("ETag"),
            "sha256": digest,
            "compressedBytes": len(body),
            "uncompressedBytes": len(uncompressed),
            "podCount": summary.get("podCount", 0),
            "eventCount": summary.get("eventCount", 0),
            "logEntryCount": summary.get("logEntryCount", 0),
            "partial": bool(summary.get("partial")),
        }
    )


def s3_key_segment(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip())
    return normalized.strip("-.") or "unknown"
