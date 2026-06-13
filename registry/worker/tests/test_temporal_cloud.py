from __future__ import annotations

import asyncio
import json

from demo_registry_operator.activities.temporal_cloud import (
    ApiKeyResult,
    NamespaceInfo,
    finalize_temporal_cloud,
    reconcile_temporal_cloud,
)


PROJECT = {
    "metadata": {"name": "banking-money-transfer"},
    "spec": {
        "components": {
            "worker": {"build": "app", "worker": True},
            "backend": {"build": "app", "temporalAccess": True},
            "frontend": {"build": "app"},
        },
    },
}
RUNTIME_PATH = "tmprl-dem-cld/banking-money-transfer/temporal"


class FakeReconciler:
    def __init__(self, *, existing_namespace=None, existing_sa=None, regions=None):
        self.existing_namespace = existing_namespace
        self.existing_sa = existing_sa
        self.regions = regions or ["aws-us-west-2", "aws-ca-central-1", "gcp-us-central1"]
        self.calls: list[tuple] = []

    async def list_regions(self):
        self.calls.append(("list_regions",))
        return self.regions

    async def find_namespace(self, name):
        self.calls.append(("find_namespace", name))
        return self.existing_namespace

    async def create_namespace(self, name, region, retention_days, enable_delete_protection=True):
        self.calls.append(
            ("create_namespace", name, region, retention_days, enable_delete_protection)
        )
        return NamespaceInfo(
            qualified_name=f"{name}.acct1",
            grpc_address=f"{name}.acct1.tmprl.cloud:7233",
            web_address="https://cloud.temporal.io/namespaces/x",
            state="active",
            created=True,
        )

    async def find_service_account(self, name):
        self.calls.append(("find_service_account", name))
        return self.existing_sa

    async def create_service_account(self, name, qualified_namespace, description):
        self.calls.append(("create_service_account", name, qualified_namespace, description))
        return "sa-123"

    async def create_api_key(self, service_account_id, display_name, description, ttl_days):
        self.calls.append(("create_api_key", service_account_id, ttl_days))
        return ApiKeyResult(
            key_id="key-abc",
            token="SECRET-TOKEN-VALUE",
            expiry_time="2027-06-13T00:00:00+00:00",
        )

    async def delete_namespace(self, qualified_name):
        self.calls.append(("delete_namespace", qualified_name))
        return "deleted"

    async def delete_api_key(self, key_id):
        self.calls.append(("delete_api_key", key_id))
        return "deleted"

    async def delete_service_account(self, service_account_id):
        self.calls.append(("delete_service_account", service_account_id))
        return "deleted"

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

    def delete(self, path):
        self.deletes = getattr(self, "deletes", [])
        self.deletes.append(path)
        existed = path in self.store
        self.store.pop(path, None)
        return "scheduled-deletion" if existed else "not-found"


def reconcile(reconciler, secret_store, demo_project=PROJECT):
    return asyncio.run(
        reconcile_temporal_cloud(
            demo_project,
            reconciler=reconciler,
            secret_store=secret_store,
            account_id="acct1",
            region="aws-us-west-2",
            checked_at="2026-06-13T00:00:00+00:00",
        )
    )


def test_creates_namespace_service_account_and_key_when_absent():
    reconciler = FakeReconciler()
    secrets = FakeSecretStore()

    status = reconcile(reconciler, secrets)

    assert status["phase"] == "Ready"
    assert status["mode"] == "cloud-api"
    assert status["namespace"]["name"] == "tmprl-dem-cld-banking-money-transfer.acct1"
    assert status["namespace"]["action"] == "created"
    assert status["serviceAccount"]["id"] == "sa-123"
    assert status["apiKey"]["keyId"] == "key-abc"
    assert status["apiKey"]["action"] == "created"

    secret = secrets.store[RUNTIME_PATH]
    assert secret["address"] == "tmprl-dem-cld-banking-money-transfer.acct1.tmprl.cloud:7233"
    assert secret["namespace"] == "tmprl-dem-cld-banking-money-transfer.acct1"
    assert secret["apiKey"] == "SECRET-TOKEN-VALUE"
    assert secret["tls"] == "true"
    assert secret["keyId"] == "key-abc"

    # The one-time token must never appear in the returned status.
    assert "SECRET-TOKEN-VALUE" not in json.dumps(status)


def test_idempotent_reuses_existing_runtime_key():
    reconciler = FakeReconciler(
        existing_namespace=NamespaceInfo(
            qualified_name="tmprl-dem-cld-banking-money-transfer.acct1",
            grpc_address="tmprl-dem-cld-banking-money-transfer.acct1.tmprl.cloud:7233",
            web_address="https://cloud.temporal.io/namespaces/x",
            state="active",
            created=False,
        ),
        existing_sa="sa-existing",
    )
    secrets = FakeSecretStore(
        {
            RUNTIME_PATH: {
                "address": "tmprl-dem-cld-banking-money-transfer.acct1.tmprl.cloud:7233",
                "namespace": "tmprl-dem-cld-banking-money-transfer.acct1",
                "apiKey": "EXISTING-KEY",
                "tls": "true",
                "keyId": "key-old",
            }
        }
    )

    status = reconcile(reconciler, secrets)

    assert status["phase"] == "Ready"
    assert reconciler.called("create_namespace") == []
    assert reconciler.called("create_service_account") == []
    assert reconciler.called("create_api_key") == []
    assert secrets.writes == []
    assert status["namespace"]["action"] == "verified"
    assert status["apiKey"]["action"] == "verified"
    assert status["apiKey"]["keyId"] == "key-old"


def test_existing_namespace_but_missing_key_creates_only_the_key():
    reconciler = FakeReconciler(
        existing_namespace=NamespaceInfo(
            qualified_name="tmprl-dem-cld-banking-money-transfer.acct1",
            grpc_address="tmprl-dem-cld-banking-money-transfer.acct1.tmprl.cloud:7233",
            web_address="https://cloud.temporal.io/namespaces/x",
            state="active",
            created=False,
        ),
        existing_sa=None,
    )
    secrets = FakeSecretStore()

    status = reconcile(reconciler, secrets)

    assert reconciler.called("create_namespace") == []
    assert len(reconciler.called("create_service_account")) == 1
    assert len(reconciler.called("create_api_key")) == 1
    assert secrets.store[RUNTIME_PATH]["apiKey"] == "SECRET-TOKEN-VALUE"
    assert status["namespace"]["action"] == "verified"
    assert status["serviceAccount"]["action"] == "created"


def test_deleted_namespace_is_treated_as_absent_and_recreated():
    reconciler = FakeReconciler(
        existing_namespace=NamespaceInfo(
            qualified_name="tmprl-dem-cld-banking-money-transfer.acct1",
            grpc_address="tmprl-dem-cld-banking-money-transfer.acct1.tmprl.cloud:7233",
            web_address="https://cloud.temporal.io/namespaces/x",
            state="RESOURCE_STATE_DELETED",
            created=False,
        ),
    )
    secrets = FakeSecretStore()

    status = reconcile(reconciler, secrets)

    assert status["phase"] == "Ready"
    assert len(reconciler.called("create_namespace")) == 1
    assert status["namespace"]["action"] == "created"


def test_unusable_namespace_state_surfaces_error_without_creating():
    reconciler = FakeReconciler(
        existing_namespace=NamespaceInfo(
            qualified_name="tmprl-dem-cld-banking-money-transfer.acct1",
            grpc_address="tmprl-dem-cld-banking-money-transfer.acct1.tmprl.cloud:7233",
            web_address="https://cloud.temporal.io/namespaces/x",
            state="RESOURCE_STATE_ACTIVATION_FAILED",
            created=False,
        ),
    )
    secrets = FakeSecretStore()

    status = reconcile(reconciler, secrets)

    assert status["phase"] == "Error"
    assert status["reason"] == "NamespaceUnusable"
    assert reconciler.called("create_namespace") == []
    assert secrets.writes == []


def test_region_not_available_fails_fast_without_creating():
    reconciler = FakeReconciler(regions=["aws-ca-central-1", "gcp-us-central1"])
    secrets = FakeSecretStore()

    # The default reconcile() helper requests region "aws-us-west-2", which this
    # account cannot provision.
    status = reconcile(reconciler, secrets)

    assert status["phase"] == "Error"
    assert status["reason"] == "RegionUnavailable"
    assert status["availableRegions"] == ["aws-ca-central-1", "gcp-us-central1"]
    assert reconciler.called("create_namespace") == []
    assert secrets.writes == []


def test_not_required_when_no_temporal_components():
    project = {
        "metadata": {"name": "static-site"},
        "spec": {"components": {"frontend": {"build": "app"}}},
    }
    reconciler = FakeReconciler()
    secrets = FakeSecretStore()

    status = reconcile(reconciler, secrets, demo_project=project)

    assert status["phase"] == "NotRequired"
    assert reconciler.calls == []
    assert secrets.writes == []


def finalize(reconciler, secret_store, demo_project=PROJECT):
    return asyncio.run(
        finalize_temporal_cloud(
            demo_project,
            reconciler=reconciler,
            secret_store=secret_store,
            checked_at="2026-06-13T00:00:00+00:00",
        )
    )


def test_finalize_deletes_namespace_service_account_key_and_secret():
    reconciler = FakeReconciler()
    secrets = FakeSecretStore(
        {
            RUNTIME_PATH: {
                "namespace": "tmprl-dem-cld-banking-money-transfer.acct1",
                "apiKey": "KEY",
                "keyId": "key-abc",
                "serviceAccountId": "sa-123",
            }
        }
    )

    status = finalize(reconciler, secrets)

    assert status["phase"] == "Deleted"
    assert reconciler.called("delete_api_key") == [("delete_api_key", "key-abc")]
    assert reconciler.called("delete_service_account") == [
        ("delete_service_account", "sa-123")
    ]
    assert reconciler.called("delete_namespace") == [
        ("delete_namespace", "tmprl-dem-cld-banking-money-transfer.acct1")
    ]
    assert RUNTIME_PATH not in secrets.store
    assert status["deleted"]["secret"] == "scheduled-deletion"


def test_finalize_is_idempotent_when_nothing_exists():
    reconciler = FakeReconciler()  # find_namespace returns None
    secrets = FakeSecretStore()  # no runtime secret

    status = finalize(reconciler, secrets)

    assert status["phase"] == "Deleted"
    assert reconciler.called("delete_namespace") == []
    assert reconciler.called("delete_api_key") == []
    assert "secret" not in status["deleted"]


def test_finalize_not_required_without_temporal_components():
    project = {
        "metadata": {"name": "static-site"},
        "spec": {"components": {"frontend": {"build": "app"}}},
    }
    reconciler = FakeReconciler()
    secrets = FakeSecretStore()

    status = finalize(reconciler, secrets, demo_project=project)

    assert status["phase"] == "NotRequired"
    assert reconciler.calls == []
