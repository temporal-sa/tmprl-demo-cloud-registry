from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

from demo_registry_operator.activities.temporal_auth import (
    ApiKeyResult,
    reconcile_api_key_auth,
)


PROJECT = {
    "metadata": {"name": "banking-money-transfer"},
    "spec": {
        "components": {
            "worker": {"build": "app", "worker": True},
            "backend": {"build": "app", "temporalAccess": True},
        },
    },
}
RUNTIME_PATH = "tmprl-dem-cld/banking-money-transfer/temporal"
NAMESPACE = SimpleNamespace(
    qualified_name="tmprl-dem-cld-banking-money-transfer.acct1",
    grpc_address="tmprl-dem-cld-banking-money-transfer.acct1.tmprl.cloud:7233",
)


class FakeAuthReconciler:
    def __init__(self, *, existing_sa=None):
        self.existing_sa = existing_sa
        self.calls: list[tuple] = []

    async def find_service_account(self, name):
        self.calls.append(("find_service_account", name))
        return self.existing_sa

    async def create_service_account(self, name, qualified_namespace, description):
        self.calls.append(
            ("create_service_account", name, qualified_namespace, description)
        )
        return "sa-123"

    async def create_api_key(
        self, service_account_id, display_name, description, ttl_days
    ):
        self.calls.append(("create_api_key", service_account_id, display_name, ttl_days))
        return ApiKeyResult(
            key_id="key-abc",
            token="SECRET-TOKEN-VALUE",
            expiry_time="2027-06-13T00:00:00+00:00",
        )

    def called(self, name):
        return [call for call in self.calls if call[0] == name]


class FakeSecretStore:
    def __init__(self, initial=None):
        self.store = dict(initial or {})
        self.writes: list[tuple] = []

    def read_json(self, path):
        return self.store.get(path)

    def write_json(self, path, value, description=None):
        self.writes.append((path, value, description))
        self.store[path] = value


def reconcile(reconciler, secret_store):
    return asyncio.run(
        reconcile_api_key_auth(
            PROJECT,
            namespace=NAMESPACE,
            reconciler=reconciler,
            secret_store=secret_store,
            checked_at="2026-06-13T00:00:00+00:00",
            api_key_ttl_days=30,
            rotate_before_days=10,
            mirror_kubernetes_secrets=False,
        )
    )


def test_api_key_auth_creates_service_account_key_and_runtime_secret():
    reconciler = FakeAuthReconciler()
    secrets = FakeSecretStore()

    status = reconcile(reconciler, secrets)

    assert status["serviceAccount"]["action"] == "created"
    assert status["serviceAccount"]["id"] == "sa-123"
    assert status["apiKey"]["action"] == "created"
    assert status["apiKey"]["keyId"] == "key-abc"
    assert "SECRET-TOKEN-VALUE" not in json.dumps(status)

    secret = secrets.store[RUNTIME_PATH]
    assert secret["address"] == NAMESPACE.grpc_address
    assert secret["namespace"] == NAMESPACE.qualified_name
    assert secret["apiKey"] == "SECRET-TOKEN-VALUE"
    assert secret["tls"] == "true"
    assert secret["serviceAccountId"] == "sa-123"


def test_api_key_auth_reuses_fresh_existing_runtime_secret():
    reconciler = FakeAuthReconciler(existing_sa="sa-existing")
    secrets = FakeSecretStore(
        {
            RUNTIME_PATH: {
                "address": NAMESPACE.grpc_address,
                "namespace": NAMESPACE.qualified_name,
                "apiKey": "EXISTING-KEY",
                "tls": "true",
                "keyId": "key-old",
                "createdAt": "2026-06-01T00:00:00+00:00",
                "expiryTime": "2026-07-13T00:00:00+00:00",
            }
        }
    )

    status = reconcile(reconciler, secrets)

    assert status["serviceAccount"]["action"] == "verified"
    assert status["apiKey"]["action"] == "verified"
    assert status["apiKey"]["keyId"] == "key-old"
    assert reconciler.called("create_service_account") == []
    assert reconciler.called("create_api_key") == []
    assert secrets.writes == []
