from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request, status
from pydantic import BaseModel, Field

from .config import TokenPolicy, load_settings
from .crypto import TokenCipher
from .litellm_client import LiteLLMClient
from .security import VerifiedRequest, verify_request
from .store import BrokerStore, ExistingGrant, GrantRecord

app = FastAPI(title="Instruqt LiteLLM Broker", version="0.2.0")

_settings = load_settings()
_store = BrokerStore(table_name=_settings.dynamodb_table, region_name=_settings.aws_region)
_cipher = TokenCipher(_settings.token_encryption_key)
_litellm = LiteLLMClient(
    base_url=_settings.litellm_base_url,
    master_key=_settings.litellm_master_key,
)


class TokenRequest(TokenPolicy):
    track_id: str = Field(min_length=1)
    participant_id: str = Field(min_length=1)
    participant_email: str | None = None
    track_slug: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class RevokeRequest(BaseModel):
    track_id: str = Field(min_length=1)
    participant_id: str = Field(min_length=1)


class TokenResponse(BaseModel):
    idempotency_key: str
    litellm_base_url: str
    api_key: str
    default_model: str | None
    models: list[str] | None
    expires_at: datetime
    max_budget: float
    duration: str
    budget_duration: str | None
    rpm_limit: int | None
    tpm_limit: int | None
    max_parallel_requests: int | None
    reused: bool = False


class RevokeResponse(BaseModel):
    idempotency_key: str
    revoked: bool


async def _verified(request: Request) -> VerifiedRequest:
    body = await request.body()
    return verify_request(
        headers=request.headers,
        body=body,
        hmac_secrets=_settings.hmac_secrets,
        max_clock_skew_seconds=_settings.max_clock_skew_seconds,
        store=_store,
    )


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/readyz")
async def readyz() -> dict[str, Any]:
    return {
        "status": "ok",
        "litellm_reachable": await _litellm.health(),
    }


@app.post("/v1/token", response_model=TokenResponse)
async def create_token(
    request: TokenRequest,
    verified: VerifiedRequest = Depends(_verified),
) -> TokenResponse:
    policy = _policy_from_request(request)
    idempotency_key = _idempotency_key(
        verified.key_id,
        request.track_id,
        request.participant_id,
    )
    existing = _store.get_grant(idempotency_key)
    if existing is not None and existing.encrypted_api_key:
        return _response_from_existing(idempotency_key, existing, policy)

    generated = await _litellm.generate_key(
        credential_key_id=verified.key_id,
        track_id=request.track_id,
        participant_id=request.participant_id,
        participant_email=request.participant_email,
        track_slug=request.track_slug,
        metadata=request.metadata,
        policy=policy,
    )
    encrypted = _cipher.encrypt(generated["api_key"])
    expires_at = generated["expires_at"]
    record = GrantRecord(
        idempotency_key=idempotency_key,
        credential_key_id=verified.key_id,
        track_id=request.track_id,
        participant_id=request.participant_id,
        participant_email=request.participant_email,
        track_slug=request.track_slug,
        encrypted_api_key=encrypted,
        key_alias=generated["key_alias"],
        expires_at=expires_at,
        policy_json=json.dumps(policy.model_dump(mode="json"), sort_keys=True),
        litellm_response_json=json.dumps(generated["raw_response"], sort_keys=True),
        created_at=datetime.now(UTC),
    )
    stored = _store.put_grant_if_absent(record)
    if stored is not None and stored.encrypted_api_key:
        await _litellm.block_key(generated["api_key"])
        return _response_from_existing(idempotency_key, stored, policy)
    return _response(
        idempotency_key=idempotency_key,
        api_key=generated["api_key"],
        expires_at=expires_at,
        policy=policy,
        reused=False,
    )


@app.post("/v1/token/revoke", response_model=RevokeResponse)
async def revoke_token(
    request: RevokeRequest,
    verified: VerifiedRequest = Depends(_verified),
) -> RevokeResponse:
    idempotency_key = _idempotency_key(
        verified.key_id,
        request.track_id,
        request.participant_id,
    )
    existing = _store.get_grant(idempotency_key)
    if existing is None:
        return RevokeResponse(idempotency_key=idempotency_key, revoked=False)
    if existing.revoked_at is not None:
        return RevokeResponse(idempotency_key=idempotency_key, revoked=True)
    if existing.encrypted_api_key:
        await _litellm.block_key(_cipher.decrypt(existing.encrypted_api_key))
    _store.mark_revoked(idempotency_key)
    return RevokeResponse(idempotency_key=idempotency_key, revoked=True)


def _response_from_existing(
    idempotency_key: str,
    existing: ExistingGrant,
    requested_policy: TokenPolicy,
) -> TokenResponse:
    if existing.revoked_at is not None:
        raise HTTPException(status_code=status.HTTP_410_GONE, detail="Token was revoked")
    stored_policy = TokenPolicy.model_validate_json(existing.policy_json)
    if stored_policy != requested_policy:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="A token already exists for this credential, track, and participant with a different policy",
        )
    return _response(
        idempotency_key=idempotency_key,
        api_key=_cipher.decrypt(existing.encrypted_api_key),
        expires_at=existing.expires_at,
        policy=stored_policy,
        reused=True,
    )


def _response(
    *,
    idempotency_key: str,
    api_key: str,
    expires_at: datetime,
    policy: TokenPolicy,
    reused: bool,
) -> TokenResponse:
    return TokenResponse(
        idempotency_key=idempotency_key,
        litellm_base_url=_settings.litellm_public_base_url,
        api_key=api_key,
        default_model=policy.default_model,
        models=policy.models,
        expires_at=expires_at,
        max_budget=policy.max_budget,
        duration=policy.duration,
        budget_duration=policy.budget_duration,
        rpm_limit=policy.rpm_limit,
        tpm_limit=policy.tpm_limit,
        max_parallel_requests=policy.max_parallel_requests,
        reused=reused,
    )


def _policy_from_request(request: TokenRequest) -> TokenPolicy:
    return TokenPolicy.model_validate(
        request.model_dump(
            include={
                "max_budget",
                "duration",
                "models",
                "default_model",
                "budget_duration",
                "rpm_limit",
                "tpm_limit",
                "max_parallel_requests",
            }
        )
    )


def _idempotency_key(credential_key_id: str, track_id: str, participant_id: str) -> str:
    return f"{credential_key_id}:{track_id}:{participant_id}"
