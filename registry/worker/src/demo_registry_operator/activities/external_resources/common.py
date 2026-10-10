from __future__ import annotations

from datetime import UTC
from typing import Any

from demo_registry_operator.errors import CONFIGURATION_ERROR, non_retryable_error


def string_field(value: dict[str, Any], key: str) -> str:
    field = value.get(key)
    if isinstance(field, str) and field:
        return field
    raise non_retryable_error(
        f"External resource value is missing {key}",
        error_type=CONFIGURATION_ERROR,
    )


def string_optional(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def dict_field(value: dict[str, Any], key: str) -> dict[str, Any]:
    field = value.get(key)
    if isinstance(field, dict):
        return field
    raise non_retryable_error(
        f"External resource value is missing {key}",
        error_type=CONFIGURATION_ERROR,
    )


def authentication_error_message(message: str) -> bool:
    lowered = message.lower()
    return any(
        marker in lowered
        for marker in (
            "access denied",
            "authentication failed",
            "authentication error",
            "invalid api key",
            "invalid token",
            "expired token",
            "forbidden",
            "permission denied",
            "unauthenticated",
            "unauthorized",
        )
    )


def error_code(error: Exception) -> str | None:
    response = getattr(error, "response", None)
    if isinstance(response, dict):
        error_info = response.get("Error")
        if isinstance(error_info, dict) and isinstance(error_info.get("Code"), str):
            return error_info["Code"]
    return None


def isoformat_datetime(value: Any) -> str | None:
    if hasattr(value, "astimezone"):
        return value.astimezone(UTC).isoformat()
    return None
