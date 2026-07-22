from __future__ import annotations

import json
import os
from dataclasses import dataclass

from typing import Annotated

from pydantic import BaseModel, Field


_DURATION_PATTERN = r"^[1-9][0-9]*[smhd]$"


ModelName = Annotated[str, Field(min_length=1)]


class TokenPolicy(BaseModel):
    max_budget: float = Field(gt=0, allow_inf_nan=False)
    duration: str = Field(pattern=_DURATION_PATTERN)
    models: list[ModelName] | None = Field(default=None, min_length=1)
    default_model: str | None = None
    budget_duration: str | None = Field(default=None, pattern=_DURATION_PATTERN)
    rpm_limit: int | None = Field(default=None, gt=0)
    tpm_limit: int | None = Field(default=None, gt=0)
    max_parallel_requests: int | None = Field(default=None, gt=0)


@dataclass(frozen=True)
class Settings:
    aws_region: str
    dynamodb_table: str
    hmac_secrets: dict[str, str]
    max_clock_skew_seconds: int
    token_encryption_key: str
    litellm_base_url: str
    litellm_public_base_url: str
    litellm_master_key: str


def load_settings() -> Settings:
    return Settings(
        aws_region=os.environ.get("AWS_REGION", "us-west-1"),
        dynamodb_table=_required_env("BROKER_DYNAMODB_TABLE"),
        hmac_secrets=_load_hmac_secrets(),
        max_clock_skew_seconds=int(os.environ.get("BROKER_MAX_CLOCK_SKEW_SECONDS", "300")),
        token_encryption_key=_required_env("BROKER_TOKEN_ENCRYPTION_KEY"),
        litellm_base_url=_required_env("LITELLM_BASE_URL").rstrip("/"),
        litellm_public_base_url=_required_env("LITELLM_PUBLIC_BASE_URL").rstrip("/"),
        litellm_master_key=_required_env("LITELLM_MASTER_KEY"),
    )


def _required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} must be set")
    return value


def _load_hmac_secrets() -> dict[str, str]:
    raw = _required_env("BROKER_HMAC_SECRETS_JSON")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as err:
        raise RuntimeError("BROKER_HMAC_SECRETS_JSON must be valid JSON") from err
    if not isinstance(parsed, dict) or not parsed:
        raise RuntimeError("BROKER_HMAC_SECRETS_JSON must be a non-empty object")
    secrets = {str(key): str(value) for key, value in parsed.items() if str(value)}
    if not secrets:
        raise RuntimeError("BROKER_HMAC_SECRETS_JSON must contain at least one secret")
    return secrets
