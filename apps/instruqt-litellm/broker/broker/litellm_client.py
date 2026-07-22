from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import httpx

from .config import TokenPolicy


class LiteLLMClient:
    def __init__(self, *, base_url: str, master_key: str):
        self._base_url = base_url.rstrip("/")
        self._headers = {"Authorization": f"Bearer {master_key}"}

    async def generate_key(
        self,
        *,
        credential_key_id: str,
        track_id: str,
        participant_id: str,
        participant_email: str | None,
        track_slug: str | None,
        metadata: dict[str, Any],
        policy: TokenPolicy,
    ) -> dict[str, Any]:
        key_alias = _key_alias(credential_key_id, track_id, participant_id)
        payload: dict[str, Any] = {
            "key_alias": key_alias,
            "duration": policy.duration,
            "max_budget": policy.max_budget,
            "metadata": {
                **metadata,
                "source": "instruqt-litellm-broker",
                "broker_credential_key_id": credential_key_id,
                "track_id": track_id,
                "participant_id": participant_id,
            },
        }
        if policy.models is not None:
            payload["models"] = policy.models
        if participant_email:
            payload["user_id"] = participant_email
            payload["metadata"]["participant_email"] = participant_email
        if track_slug:
            payload["metadata"]["track_slug"] = track_slug
        if policy.budget_duration:
            payload["budget_duration"] = policy.budget_duration
        if policy.rpm_limit is not None:
            payload["rpm_limit"] = policy.rpm_limit
        if policy.tpm_limit is not None:
            payload["tpm_limit"] = policy.tpm_limit
        if policy.max_parallel_requests is not None:
            payload["max_parallel_requests"] = policy.max_parallel_requests

        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{self._base_url}/key/generate",
                headers=self._headers,
                json=payload,
            )
            response.raise_for_status()
            data = response.json()

        key = data.get("key") or data.get("token") or data.get("api_key")
        if not isinstance(key, str) or not key:
            raise RuntimeError("LiteLLM did not return a virtual key")
        return {
            "api_key": key,
            "key_alias": key_alias,
            "raw_response": data,
            "expires_at": _expires_at(policy.duration),
        }

    async def block_key(self, key: str) -> None:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{self._base_url}/key/block",
                headers=self._headers,
                json={"key": key},
            )
            response.raise_for_status()

    async def health(self) -> bool:
        try:
            async with httpx.AsyncClient(timeout=5) as client:
                response = await client.get(f"{self._base_url}/health")
            return response.status_code < 500
        except httpx.HTTPError:
            return False


def _key_alias(credential_key_id: str, track_id: str, participant_id: str) -> str:
    safe = "".join(
        char if char.isalnum() or char in {"-", "_"} else "-"
        for char in f"instruqt-{credential_key_id}-{track_id}-{participant_id}"
    )
    return safe[:128]


def _expires_at(duration: str) -> datetime:
    unit = duration[-1:]
    try:
        amount = int(duration[:-1])
    except ValueError as err:
        raise ValueError(f"Unsupported duration format: {duration}") from err
    multipliers = {
        "s": timedelta(seconds=1),
        "m": timedelta(minutes=1),
        "h": timedelta(hours=1),
        "d": timedelta(days=1),
    }
    if unit not in multipliers:
        raise ValueError(f"Unsupported duration format: {duration}")
    return datetime.now(UTC) + (multipliers[unit] * amount)
