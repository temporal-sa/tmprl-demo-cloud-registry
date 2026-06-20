from dataclasses import dataclass
from typing import Any

from temporalio import activity

from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.errors import VALIDATION_ERROR, non_retryable_error
from demo_registry_operator.kube import read_demo_project, read_instruqt_course
from demo_registry_operator.reconcile_payloads import (
    demo_project_reconcile_payload_state,
    instruqt_course_reconcile_payload_state,
)
from demo_registry_operator.resources import resource_with_force_reconcile


@dataclass(frozen=True)
class LoadCurrentDemoProjectForReconcileRequest:
    name: str
    token: str
    reason: str | None = None


@dataclass(frozen=True)
class LoadCurrentInstruqtCourseForReconcileRequest:
    name: str
    token: str
    reason: str | None = None


@dataclass(frozen=True)
class CurrentResourceForReconcileResult:
    resource: dict[str, Any]


@activity.defn
def load_current_demo_project_for_reconcile(
    request: LoadCurrentDemoProjectForReconcileRequest,
) -> CurrentResourceForReconcileResult:
    demo_project = read_demo_project(required_string(request.name, "name"))
    if demo_project is None:
        raise non_retryable_error(
            f"DemoProject {request.name!r} was not found.",
            error_type=VALIDATION_ERROR,
        )

    state = demo_project_reconcile_payload_state(
        demo_project,
        WorkerConfig.from_env(),
    )
    if state.get("ready") is not True:
        raise non_retryable_error(
            str(state.get("message") or f"DemoProject {request.name!r} is not ready."),
            error_type=VALIDATION_ERROR,
        )
    payload = state.get("payload")
    if not isinstance(payload, dict):
        raise non_retryable_error(
            f"DemoProject {request.name!r} did not produce a reconcile payload.",
            error_type=VALIDATION_ERROR,
        )
    return CurrentResourceForReconcileResult(
        resource_with_force_reconcile(
            payload,
            required_string(request.token, "token"),
            optional_string(request.reason),
        )
    )


@activity.defn
def load_current_instruqt_course_for_reconcile(
    request: LoadCurrentInstruqtCourseForReconcileRequest,
) -> CurrentResourceForReconcileResult:
    instruqt_course = read_instruqt_course(required_string(request.name, "name"))
    if instruqt_course is None:
        raise non_retryable_error(
            f"InstruqtCourse {request.name!r} was not found.",
            error_type=VALIDATION_ERROR,
        )

    state = instruqt_course_reconcile_payload_state(
        instruqt_course,
        WorkerConfig.from_env(),
    )
    if state.get("ready") is not True:
        raise non_retryable_error(
            str(state.get("message") or f"InstruqtCourse {request.name!r} is not ready."),
            error_type=VALIDATION_ERROR,
        )
    payload = state.get("payload")
    if not isinstance(payload, dict):
        raise non_retryable_error(
            f"InstruqtCourse {request.name!r} did not produce a reconcile payload.",
            error_type=VALIDATION_ERROR,
        )
    return CurrentResourceForReconcileResult(
        resource_with_force_reconcile(
            payload,
            required_string(request.token, "token"),
            optional_string(request.reason),
        )
    )


def required_string(value: str, field: str) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise non_retryable_error(
        f"force reconcile request is missing {field}",
        error_type=VALIDATION_ERROR,
    )


def optional_string(value: str | None) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None
