from __future__ import annotations

from typing import Any

from temporalio.exceptions import (
    ActivityError,
    ApplicationError,
    ChildWorkflowError,
    is_cancelled_exception,
)

VALIDATION_ERROR = "ValidationError"
CONFIGURATION_ERROR = "ConfigurationError"
PROVIDER_CONFIGURATION_ERROR = "ProviderConfigurationError"
PROVIDER_AUTHENTICATION_ERROR = "ProviderAuthenticationError"
PROVIDER_TRANSIENT_ERROR = "ProviderTransientError"
IMAGE_BUILD_FAILED = "ImageBuildFailed"
IMAGE_BUILD_TIMEOUT = "ImageBuildTimeout"
WORKER_ROLLOUT_FAILED = "WorkerRolloutFailed"
CANDIDATE_NOT_READY = "CandidateNotReady"
SMOKE_CHECK_FAILED = "SmokeCheckFailed"

NON_RETRYABLE_ERROR_TYPES = [
    VALIDATION_ERROR,
    CONFIGURATION_ERROR,
    PROVIDER_CONFIGURATION_ERROR,
    PROVIDER_AUTHENTICATION_ERROR,
    IMAGE_BUILD_FAILED,
    IMAGE_BUILD_TIMEOUT,
]


NON_RETRYABLE_AWS_ERROR_CODES = {
    "AccessDenied",
    "AccessDeniedException",
    "AuthFailure",
    "InvalidInputException",
    "InvalidParameterException",
    "InvalidParameterValueException",
    "NoSuchBucket",
    "ResourceNotFoundException",
    "UnrecognizedClientException",
    "UnauthorizedOperation",
    "ValidationException",
}


def non_retryable_error(
    message: str,
    *,
    error_type: str = VALIDATION_ERROR,
    details: Any | None = None,
) -> ApplicationError:
    if details is None:
        return ApplicationError(message, type=error_type, non_retryable=True)
    return ApplicationError(message, details, type=error_type, non_retryable=True)


def retryable_provider_error(message: str) -> ApplicationError:
    return ApplicationError(message, type=PROVIDER_TRANSIENT_ERROR)


def terminal_activity_error(
    status: dict[str, Any], *, error_type: str
) -> ApplicationError:
    # Keep the status payload for workflow recovery, while recording a real
    # ActivityTaskFailed event after the activity's own polling window ends.
    error = status.get("lastError") or status.get("error")
    if isinstance(error, dict):
        error = error.get("message")
    message = str(error or status.get("message") or error_type)
    return non_retryable_error(
        message, error_type=error_type, details={"status": status}
    )


def activity_failure_status(
    error: ActivityError, *, fallback_phase: str | None = None
) -> dict[str, Any]:
    if is_cancelled_exception(error):
        raise error
    cause = error.cause
    if isinstance(cause, ApplicationError) and cause.details:
        details = cause.details[0]
        if isinstance(details, dict) and isinstance(details.get("status"), dict):
            return {**details["status"], "activityFailure": failure_details(error)}
    if fallback_phase is not None:
        failure = failure_details(error)
        return {
            "phase": fallback_phase,
            "ok": False,
            "message": failure["message"],
            "activityFailure": failure,
        }
    # Build failures without a status payload propagate to the owning saga.
    raise error


def failure_details(error: BaseException) -> dict[str, Any]:
    details: dict[str, Any] = {}
    cause: BaseException | None = error
    while cause is not None:
        details["message"] = str(cause)
        details["type"] = type(cause).__name__
        if isinstance(cause, ApplicationError):
            details["message"] = cause.message
            details["type"] = cause.type or "ApplicationError"
        elif isinstance(cause, ActivityError):
            details["activityType"] = cause.activity_type
            details["activityId"] = cause.activity_id
            details["scheduledEventId"] = cause.scheduled_event_id
        elif isinstance(cause, ChildWorkflowError):
            details["workflowId"] = cause.workflow_id
            details["runId"] = cause.run_id
        cause = cause.__cause__
    return details


def aws_error_code(error: Exception) -> str | None:
    response = getattr(error, "response", None)
    if isinstance(response, dict):
        error_info = response.get("Error")
        if isinstance(error_info, dict) and isinstance(error_info.get("Code"), str):
            return error_info["Code"]
    return None


def aws_application_error(error: Exception, *, operation: str) -> ApplicationError:
    code = aws_error_code(error)
    message = f"{operation} failed"
    if code:
        message = f"{message} ({code})"
    message = f"{message}: {error}"

    if code in NON_RETRYABLE_AWS_ERROR_CODES:
        error_type = (
            PROVIDER_AUTHENTICATION_ERROR
            if code
            in {
                "AccessDenied",
                "AccessDeniedException",
                "AuthFailure",
                "UnrecognizedClientException",
                "UnauthorizedOperation",
            }
            else PROVIDER_CONFIGURATION_ERROR
        )
        return non_retryable_error(message, error_type=error_type)

    return retryable_provider_error(message)
