from __future__ import annotations

from typing import Any

from temporalio import activity

from demo_registry_operator.config import WorkerConfig
from demo_registry_operator.errors import VALIDATION_ERROR, non_retryable_error
from demo_registry_operator.kube import read_demo_project, read_instruqt_course
from demo_registry_operator.resources import resource_with_force_reconcile


@activity.defn
def load_current_demo_project_for_reconcile(request: dict[str, Any]) -> dict[str, Any]:
    name = string_field(request, "name")
    token = string_field(request, "token")
    reason = optional_string_field(request, "reason")

    demo_project = read_demo_project(name)
    if demo_project is None:
        raise non_retryable_error(
            f"DemoProject {name!r} was not found.",
            error_type=VALIDATION_ERROR,
        )

    from demo_registry_operator import watch

    state = watch.demo_project_reconcile_payload_state(
        demo_project,
        WorkerConfig.from_env(),
    )
    if state.get("ready") is not True:
        raise non_retryable_error(
            str(state.get("message") or f"DemoProject {name!r} is not ready."),
            error_type=VALIDATION_ERROR,
        )
    payload = state.get("payload")
    if not isinstance(payload, dict):
        raise non_retryable_error(
            f"DemoProject {name!r} did not produce a reconcile payload.",
            error_type=VALIDATION_ERROR,
        )
    return resource_with_force_reconcile(payload, token, reason)


@activity.defn
def load_current_instruqt_course_for_reconcile(
    request: dict[str, Any],
) -> dict[str, Any]:
    name = string_field(request, "name")
    token = string_field(request, "token")
    reason = optional_string_field(request, "reason")

    instruqt_course = read_instruqt_course(name)
    if instruqt_course is None:
        raise non_retryable_error(
            f"InstruqtCourse {name!r} was not found.",
            error_type=VALIDATION_ERROR,
        )

    from demo_registry_operator import watch

    state = watch.instruqt_course_reconcile_payload_state(
        instruqt_course,
        WorkerConfig.from_env(),
    )
    if state.get("ready") is not True:
        raise non_retryable_error(
            str(state.get("message") or f"InstruqtCourse {name!r} is not ready."),
            error_type=VALIDATION_ERROR,
        )
    payload = state.get("payload")
    if not isinstance(payload, dict):
        raise non_retryable_error(
            f"InstruqtCourse {name!r} did not produce a reconcile payload.",
            error_type=VALIDATION_ERROR,
        )
    return resource_with_force_reconcile(payload, token, reason)


def string_field(value: dict[str, Any], key: str) -> str:
    field = value.get(key)
    if isinstance(field, str) and field.strip():
        return field.strip()
    raise non_retryable_error(
        f"force reconcile request is missing {key}",
        error_type=VALIDATION_ERROR,
    )


def optional_string_field(value: dict[str, Any], key: str) -> str | None:
    field = value.get(key)
    return field.strip() if isinstance(field, str) and field.strip() else None
