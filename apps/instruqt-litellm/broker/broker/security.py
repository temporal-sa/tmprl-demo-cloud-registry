from __future__ import annotations

import hashlib
import hmac
import time
from dataclasses import dataclass
from typing import Mapping

from fastapi import HTTPException, status

from .store import BrokerStore


@dataclass(frozen=True)
class VerifiedRequest:
    key_id: str
    timestamp: int
    nonce: str


def verify_request(
    *,
    headers: Mapping[str, str],
    body: bytes,
    hmac_secrets: dict[str, str],
    max_clock_skew_seconds: int,
    store: BrokerStore,
) -> VerifiedRequest:
    key_id = _required_header(headers, "x-instruqt-key-id")
    timestamp_raw = _required_header(headers, "x-instruqt-timestamp")
    nonce = _required_header(headers, "x-instruqt-nonce")
    signature = _required_header(headers, "x-instruqt-signature")

    secret = hmac_secrets.get(key_id)
    if secret is None:
        _unauthorized("Unknown key id")
    try:
        timestamp = int(timestamp_raw)
    except ValueError:
        _unauthorized("Invalid timestamp")
    now = int(time.time())
    if abs(now - timestamp) > max_clock_skew_seconds:
        _unauthorized("Timestamp outside allowed clock skew")

    expected = _signature(secret=secret, timestamp=timestamp_raw, nonce=nonce, body=body)
    if not hmac.compare_digest(signature, expected):
        _unauthorized("Invalid signature")

    if not store.record_nonce(key_id=key_id, nonce=nonce, ttl_epoch=now + 86400):
        _unauthorized("Replay nonce")
    return VerifiedRequest(key_id=key_id, timestamp=timestamp, nonce=nonce)


def _signature(*, secret: str, timestamp: str, nonce: str, body: bytes) -> str:
    message = timestamp.encode("utf-8") + b"." + nonce.encode("utf-8") + b"." + body
    digest = hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def _required_header(headers: Mapping[str, str], name: str) -> str:
    value = headers.get(name)
    if value is None or not value.strip():
        _unauthorized(f"Missing {name}")
    return value.strip()


def _unauthorized(detail: str) -> None:
    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=detail)
