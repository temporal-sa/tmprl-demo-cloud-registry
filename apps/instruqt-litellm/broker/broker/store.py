from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import boto3
from botocore.exceptions import ClientError


@dataclass(frozen=True)
class GrantRecord:
    idempotency_key: str
    credential_key_id: str
    track_id: str
    participant_id: str
    participant_email: str | None
    track_slug: str | None
    encrypted_api_key: str
    key_alias: str
    expires_at: datetime
    policy_json: str
    litellm_response_json: str
    created_at: datetime


@dataclass(frozen=True)
class ExistingGrant:
    idempotency_key: str
    encrypted_api_key: str
    expires_at: datetime
    revoked_at: datetime | None
    policy_json: str


class BrokerStore:
    def __init__(self, *, table_name: str, region_name: str):
        self._table = boto3.resource("dynamodb", region_name=region_name).Table(table_name)

    def record_nonce(self, *, key_id: str, nonce: str, ttl_epoch: int) -> bool:
        try:
            self._table.put_item(
                Item={
                    "pk": f"nonce#{key_id}#{nonce}",
                    "kind": "nonce",
                    "expires_at_epoch": ttl_epoch,
                    "created_at": _now_iso(),
                },
                ConditionExpression="attribute_not_exists(pk)",
            )
            return True
        except ClientError as err:
            if err.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                return False
            raise

    def get_grant(self, idempotency_key: str) -> ExistingGrant | None:
        response = self._table.get_item(Key={"pk": f"grant#{idempotency_key}"})
        item = response.get("Item")
        if not item:
            return None
        return ExistingGrant(
            idempotency_key=idempotency_key,
            encrypted_api_key=str(item.get("encrypted_api_key") or ""),
            expires_at=_parse_datetime(str(item["expires_at"])),
            revoked_at=_parse_datetime(str(item["revoked_at"])) if item.get("revoked_at") else None,
            policy_json=str(item["policy_json"]),
        )

    def put_grant_if_absent(self, record: GrantRecord) -> ExistingGrant | None:
        item = {
            "pk": f"grant#{record.idempotency_key}",
            "kind": "grant",
            "idempotency_key": record.idempotency_key,
            "credential_key_id": record.credential_key_id,
            "track_id": record.track_id,
            "participant_id": record.participant_id,
            "encrypted_api_key": record.encrypted_api_key,
            "key_alias": record.key_alias,
            "expires_at": _format_datetime(record.expires_at),
            "expires_at_epoch": int(record.expires_at.timestamp()) + 86400,
            "policy_json": record.policy_json,
            "litellm_response_json": record.litellm_response_json,
            "created_at": _format_datetime(record.created_at),
        }
        if record.participant_email:
            item["participant_email"] = record.participant_email
        if record.track_slug:
            item["track_slug"] = record.track_slug
        try:
            self._table.put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(pk)",
            )
            return None
        except ClientError as err:
            if err.response.get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                raise
            return self.get_grant(record.idempotency_key)

    def mark_revoked(self, idempotency_key: str) -> None:
        self._table.update_item(
            Key={"pk": f"grant#{idempotency_key}"},
            UpdateExpression="set revoked_at = :revoked_at",
            ExpressionAttributeValues={":revoked_at": _now_iso()},
        )


def _now_iso() -> str:
    return _format_datetime(datetime.now(UTC))


def _format_datetime(value: datetime) -> str:
    return value.astimezone(UTC).isoformat()


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(UTC)
