from __future__ import annotations

from typing import Any

from temporalio.exceptions import ApplicationError


VALIDATION_ERROR = "ValidationError"
CONFIGURATION_ERROR = "ConfigurationError"
PROVIDER_CONFIGURATION_ERROR = "ProviderConfigurationError"
PROVIDER_AUTHENTICATION_ERROR = "ProviderAuthenticationError"
PROVIDER_TRANSIENT_ERROR = "ProviderTransientError"
IMAGE_BUILD_FAILED = "ImageBuildFailed"
IMAGE_BUILD_TIMEOUT = "ImageBuildTimeout"

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
