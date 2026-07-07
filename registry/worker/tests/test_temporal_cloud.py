from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

from demo_registry_operator.activities.temporal_cloud import (
    ApiKeyResult,
    CloudApiReconciler,
    NamespaceInfo,
    check_temporal_api_key_runtime_secret,
    configure_temporal_cloud_namespace_mtls,
    create_temporal_api_key_candidate_secret,
    delete_temporal_cloud_api_key,
    delete_temporal_cloud_runtime_secret,
    ensure_temporal_cloud_search_attribute,
    delete_temporal_runtime_candidate_secret,
    delete_temporal_cloud_search_attribute_value,
    ensure_temporal_cloud_namespace,
    finalize_temporal_cloud,
    namespace_info_from_status,
    namespace_status,
    promote_temporal_runtime_secret,
    reconcile_temporal_cloud,
    stage_temporal_api_key_candidate,
    temporal_mtls_ca_candidate_secret_path,
    temporal_runtime_candidate_secret_path,
)
from demo_registry_operator.resources import temporal_mtls_ca_secret_remote_path


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
CANDIDATE_TOKEN = "1a2b3c4d5e6f7890abcd"


def project_with_runtime_secret_path(path):
    project = json.loads(json.dumps(PROJECT))
    project.setdefault("spec", {})["temporal"] = {"runtimeSecretPath": path}
    return project


class FakeReconciler:
    def __init__(
        self,
        *,
        existing_namespace=None,
        existing_sa=None,
        regions=None,
        search_attributes=None,
    ):
        self.existing_namespace = existing_namespace
        self.existing_sa = existing_sa
        self.regions = regions or ["aws-us-west-2", "aws-ca-central-1", "gcp-us-central1"]
        self.search_attributes = dict(search_attributes or {})
        self.calls: list[tuple] = []

    async def list_regions(self):
        self.calls.append(("list_regions",))
        return self.regions

    async def find_namespace(self, name):
        self.calls.append(("find_namespace", name))
        return self.existing_namespace

    async def create_namespace(
        self,
        name,
        region,
        retention_days,
        auth_mode="apiKey",
        accepted_client_ca=None,
        certificate_common_name=None,
        enable_delete_protection=True,
    ):
        self.calls.append(
            (
                "create_namespace",
                name,
                region,
                retention_days,
                auth_mode,
                bool(accepted_client_ca),
                certificate_common_name,
                enable_delete_protection,
            )
        )
        return NamespaceInfo(
            qualified_name=f"{name}.acct1",
            grpc_address=f"{name}.acct1.tmprl.cloud:7233",
            web_address="https://cloud.temporal.io/namespaces/x",
            state="active",
            auth_mode=auth_mode,
            region=region,
            created=True,
        )

    async def configure_namespace_mtls(
        self,
        qualified_name,
        accepted_client_ca,
        certificate_common_name,
    ):
        self.calls.append(
            (
                "configure_namespace_mtls",
                qualified_name,
                bool(accepted_client_ca),
                certificate_common_name,
            )
        )
        return "updated"

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

    async def create_or_verify_search_attribute(self, qualified_name, name, attribute_type):
        self.calls.append(
            (
                "create_or_verify_search_attribute",
                qualified_name,
                name,
                attribute_type,
            )
        )
        current_type = self.search_attributes.get(name)
        if current_type == attribute_type:
            return {"action": "verified", "currentType": current_type}
        if current_type is not None:
            return {"action": "drifted", "currentType": current_type}
        self.search_attributes[name] = attribute_type
        return {"action": "created", "currentType": attribute_type}

    async def delete_search_attribute(self, qualified_name, name):
        self.calls.append(("delete_search_attribute", qualified_name, name))
        current_type = self.search_attributes.pop(name, None)
        if current_type is None:
            return {"action": "not-found", "currentType": None}
        return {"action": "deleted", "currentType": current_type}

    async def delete_namespace(self, qualified_name):
        self.calls.append(("delete_namespace", qualified_name))
        return "deleted"

    async def disable_namespace_delete_protection(self, qualified_name):
        self.calls.append(("disable_namespace_delete_protection", qualified_name))
        return "disabled"

    async def delete_api_key(self, key_id):
        self.calls.append(("delete_api_key", key_id))
        return "deleted"

    async def delete_service_account(self, service_account_id):
        self.calls.append(("delete_service_account", service_account_id))
        return "deleted"

    def called(self, name):
        return [call for call in self.calls if call[0] == name]


class FakeSecretStore:
    def __init__(self, initial=None, read_errors=None):
        self.store = dict(initial or {})
        self.read_errors = dict(read_errors or {})
        self.writes: list[tuple] = []

    def read_json(self, path):
        if path in self.read_errors:
            raise self.read_errors[path]
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


class AwsInvalidRequest(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.response = {"Error": {"Code": "InvalidRequestException"}}


async def passing_canary(credential):
    return {
        "phase": "Passed",
        "ok": True,
        "authMode": "apiKey" if credential.get("apiKey") else "mtls",
    }


def reconcile(reconciler, secret_store, demo_project=PROJECT, *, runtime_canary=passing_canary):
    return asyncio.run(
        reconcile_temporal_cloud(
            demo_project,
            reconciler=reconciler,
            secret_store=secret_store,
            account_id="acct1",
        region="aws-us-west-2",
        checked_at="2026-06-13T00:00:00+00:00",
        mirror_kubernetes_secrets=False,
        runtime_canary=runtime_canary,
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
                "expiryTime": "2026-07-13T00:00:00+00:00",
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


def test_create_or_verify_search_attribute_defaults_to_keyword():
    reconciler = FakeReconciler()
    namespace_status = {
        "namespace": {
            "name": "tmprl-dem-cld-banking-money-transfer.acct1",
            "grpcAddress": "tmprl-dem-cld-banking-money-transfer.acct1.tmprl.cloud:7233",
            "action": "verified",
        }
    }

    status = asyncio.run(
        ensure_temporal_cloud_search_attribute(
            {
                **PROJECT,
                "spec": {
                    **PROJECT["spec"],
                    "temporal": {"searchAttributes": {"CustomerId": {}}},
                },
            },
            namespace_status_value=namespace_status,
            search_attribute={"name": "CustomerId"},
            reconciler=reconciler,
            checked_at="2026-06-13T00:00:00+00:00",
        )
    )

    assert status["phase"] == "Ready"
    assert status["searchAttribute"] == {
        "name": "CustomerId",
        "type": "keyword",
        "action": "created",
        "currentType": "keyword",
    }
    assert reconciler.search_attributes == {"CustomerId": "keyword"}


def test_create_or_verify_search_attribute_reports_type_drift():
    reconciler = FakeReconciler(search_attributes={"CustomerId": "keyword"})
    namespace_status = {
        "namespace": {
            "name": "tmprl-dem-cld-banking-money-transfer.acct1",
            "grpcAddress": "tmprl-dem-cld-banking-money-transfer.acct1.tmprl.cloud:7233",
            "action": "verified",
        }
    }

    status = asyncio.run(
        ensure_temporal_cloud_search_attribute(
            {
                **PROJECT,
                "spec": {
                    **PROJECT["spec"],
                    "temporal": {
                        "searchAttributes": {"CustomerId": {"type": "double"}}
                    },
                },
            },
            namespace_status_value=namespace_status,
            search_attribute={"name": "CustomerId", "type": "double"},
            reconciler=reconciler,
            checked_at="2026-06-13T00:00:00+00:00",
        )
    )

    assert status["phase"] == "Error"
    assert status["reason"] == "SearchAttributeTypeDrift"
    assert status["searchAttribute"]["currentType"] == "keyword"


def test_delete_search_attribute_deletes_requested_managed_name_only():
    reconciler = FakeReconciler(
        search_attributes={"OwnedAttribute": "bool", "ExternalAttribute": "keyword"}
    )
    namespace_status = {
        "namespace": {
            "name": "tmprl-dem-cld-banking-money-transfer.acct1",
            "grpcAddress": "tmprl-dem-cld-banking-money-transfer.acct1.tmprl.cloud:7233",
            "action": "verified",
        }
    }

    status = asyncio.run(
        delete_temporal_cloud_search_attribute_value(
            PROJECT,
            namespace_status_value=namespace_status,
            search_attribute={"name": "OwnedAttribute", "type": "bool"},
            reconciler=reconciler,
            checked_at="2026-06-13T00:00:00+00:00",
        )
    )

    assert status["phase"] == "Deleted"
    assert status["searchAttribute"]["action"] == "deleted"
    assert reconciler.search_attributes == {"ExternalAttribute": "keyword"}


def test_reconcile_search_attribute_only_project_does_not_create_runtime_credentials():
    project = {
        "metadata": {"name": "search-only"},
        "spec": {
            "components": {"frontend": {"build": "app"}},
            "temporal": {
                "searchAttributes": {
                    "CustomerId": {},
                }
            },
        },
    }
    reconciler = FakeReconciler()
    secrets = FakeSecretStore()

    status = reconcile(reconciler, secrets, demo_project=project)

    assert status["phase"] == "Ready"
    assert status["searchAttributes"]["managedNames"] == ["CustomerId"]
    assert reconciler.called("create_service_account") == []
    assert reconciler.called("create_api_key") == []
    assert secrets.store == {}


def test_existing_runtime_key_without_expiry_is_rotated():
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

    assert len(reconciler.called("create_api_key")) == 1
    assert status["apiKey"]["action"] == "rotated"
    assert status["apiKey"]["previousKeyId"] == "key-old"
    assert status["apiKey"]["rotationReason"] == "MissingExpiryTime"
    assert secrets.store[RUNTIME_PATH]["apiKey"] == "SECRET-TOKEN-VALUE"
    assert secrets.store[RUNTIME_PATH]["previousKeyId"] == "key-old"


def test_existing_runtime_key_inside_rotation_window_is_rotated():
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
                "expiryTime": "2026-06-20T00:00:00+00:00",
            }
        }
    )

    status = reconcile(reconciler, secrets)

    assert len(reconciler.called("create_api_key")) == 1
    assert status["apiKey"]["action"] == "rotated"
    assert status["apiKey"]["previousKeyId"] == "key-old"
    assert status["apiKey"]["rotationReason"] == "RotateBeforeExpiry"


def test_stage_api_key_candidate_does_not_promote_or_return_token():
    reconciler = FakeReconciler(existing_sa="sa-existing")
    secrets = FakeSecretStore()
    namespace_status = {
        "namespace": {
            "name": "tmprl-dem-cld-banking-money-transfer.acct1",
            "grpcAddress": "tmprl-dem-cld-banking-money-transfer.acct1.tmprl.cloud:7233",
            "action": "verified",
        }
    }
    service_account_status = {
        "serviceAccount": {
            "id": "sa-existing",
            "name": "banking-money-transfer-runtime",
        }
    }

    status = asyncio.run(
        stage_temporal_api_key_candidate(
            PROJECT,
            namespace_status_value=namespace_status,
            service_account_status=service_account_status,
            reconciler=reconciler,
            secret_store=secrets,
            checked_at="2026-06-13T00:00:00+00:00",
            candidate_secret_token=CANDIDATE_TOKEN,
        )
    )

    candidate_path = temporal_runtime_candidate_secret_path(PROJECT, CANDIDATE_TOKEN)
    assert status["promoteRequired"] is True
    assert status["candidateSecretToken"] == CANDIDATE_TOKEN
    assert status["candidateApiKeyId"] == "key-abc"
    assert RUNTIME_PATH not in secrets.store
    assert secrets.store[candidate_path]["apiKey"] == "SECRET-TOKEN-VALUE"
    assert secrets.store[candidate_path]["candidateSecretToken"] == CANDIDATE_TOKEN
    assert "SECRET-TOKEN-VALUE" not in json.dumps(status)


def test_check_runtime_secret_treats_marked_for_deletion_as_missing():
    secrets = FakeSecretStore(
        read_errors={
            RUNTIME_PATH: AwsInvalidRequest(
                "You can't perform this operation on the secret because it was marked for deletion."
            )
        }
    )
    namespace_status = {
        "namespace": {
            "name": "tmprl-dem-cld-banking-money-transfer.acct1",
            "grpcAddress": "tmprl-dem-cld-banking-money-transfer.acct1.tmprl.cloud:7233",
        }
    }

    status = check_temporal_api_key_runtime_secret(
        PROJECT,
        namespace_status_value=namespace_status,
        secret_store=secrets,
        checked_at="2026-06-13T00:00:00+00:00",
    )

    assert status["rotate"] is True
    assert status["runtimeSecret"] == {
        "secretRemotePath": RUNTIME_PATH,
        "state": "MarkedForDeletion",
        "exists": False,
    }
    assert status["rotation"]["reason"] == "MissingRuntimeSecret"


def test_create_api_key_candidate_secret_uses_checked_runtime_metadata():
    reconciler = FakeReconciler(existing_sa="sa-existing")
    secrets = FakeSecretStore()
    namespace_status = {
        "namespace": {
            "name": "tmprl-dem-cld-banking-money-transfer.acct1",
            "grpcAddress": "tmprl-dem-cld-banking-money-transfer.acct1.tmprl.cloud:7233",
            "action": "verified",
        }
    }
    service_account_status = {
        "serviceAccount": {
            "id": "sa-existing",
            "name": "banking-money-transfer-runtime",
        }
    }
    runtime_secret_status = {
        "rotation": {"rotate": True, "reason": "MarkedForDeletion"},
        "apiKey": {
            "keyId": "key-old",
            "expiryTime": "2026-06-30T00:00:00+00:00",
        },
    }

    status = asyncio.run(
        create_temporal_api_key_candidate_secret(
            PROJECT,
            namespace_status_value=namespace_status,
            service_account_status=service_account_status,
            runtime_secret_status=runtime_secret_status,
            reconciler=reconciler,
            secret_store=secrets,
            checked_at="2026-06-13T00:00:00+00:00",
            candidate_secret_token=CANDIDATE_TOKEN,
        )
    )

    candidate_path = temporal_runtime_candidate_secret_path(PROJECT, CANDIDATE_TOKEN)
    assert status["promoteRequired"] is True
    assert status["apiKey"]["action"] == "rotated"
    assert status["apiKey"]["previousKeyId"] == "key-old"
    assert secrets.store[candidate_path]["previousKeyId"] == "key-old"
    assert "SECRET-TOKEN-VALUE" not in json.dumps(status)


def test_promote_api_key_candidate_after_canary():
    candidate_path = temporal_runtime_candidate_secret_path(PROJECT)
    secrets = FakeSecretStore(
        {
            candidate_path: {
                "address": "tmprl-dem-cld-banking-money-transfer.acct1.tmprl.cloud:7233",
                "namespace": "tmprl-dem-cld-banking-money-transfer.acct1",
                "apiKey": "SECRET-TOKEN-VALUE",
                "tls": "true",
                "keyId": "key-abc",
                "serviceAccountId": "sa-existing",
                "createdAt": "2026-06-13T00:00:00+00:00",
                "rotationReason": "MissingRuntimeSecret",
            }
        }
    )
    stage_status = {
        "phase": "Ready",
        "promoteRequired": True,
        "candidateSecretRemotePath": candidate_path,
        "apiKey": {
            "keyId": "key-abc",
            "action": "created",
        },
    }
    canary = {"phase": "Passed", "ok": True, "authMode": "apiKey"}

    status = promote_temporal_runtime_secret(
        PROJECT,
        stage_status=stage_status,
        canary_status=canary,
        secret_store=secrets,
        checked_at="2026-06-13T00:01:00+00:00",
    )

    assert status["promoted"]["runtimeSecret"] is True
    assert secrets.store[RUNTIME_PATH]["apiKey"] == "SECRET-TOKEN-VALUE"
    assert secrets.store[RUNTIME_PATH]["canary"] == canary
    assert status["apiKey"]["canary"] == canary
    assert "SECRET-TOKEN-VALUE" not in json.dumps(status)


def test_candidate_secret_cleanup_is_idempotent():
    candidate_path = temporal_runtime_candidate_secret_path(PROJECT)
    secrets = FakeSecretStore({candidate_path: {"apiKey": "SECRET-TOKEN-VALUE"}})

    status = delete_temporal_runtime_candidate_secret(
        PROJECT,
        secret_store=secrets,
        checked_at="2026-06-13T00:02:00+00:00",
    )

    assert candidate_path not in secrets.store
    assert status["deleted"]["runtimeCandidateSecret"] == "scheduled-deletion"
    assert status["deleted"]["mtlsCaCandidateSecret"] == "not-found"


def test_candidate_secret_paths_can_be_tokenized_for_no_overlap():
    assert temporal_runtime_candidate_secret_path(PROJECT, CANDIDATE_TOKEN) == (
        f"{RUNTIME_PATH}/candidate-{CANDIDATE_TOKEN}"
    )
    assert temporal_mtls_ca_candidate_secret_path(PROJECT, CANDIDATE_TOKEN) == (
        f"{RUNTIME_PATH}/ca-candidate-{CANDIDATE_TOKEN}"
    )


def test_candidate_secret_cleanup_uses_tracked_exact_paths():
    runtime_candidate_path = temporal_runtime_candidate_secret_path(
        PROJECT,
        CANDIDATE_TOKEN,
    )
    ca_candidate_path = temporal_mtls_ca_candidate_secret_path(
        PROJECT,
        CANDIDATE_TOKEN,
    )
    legacy_candidate_path = temporal_runtime_candidate_secret_path(PROJECT)
    secrets = FakeSecretStore(
        {
            runtime_candidate_path: {"apiKey": "SECRET-TOKEN-VALUE"},
            ca_candidate_path: {"caCert": "CA"},
            legacy_candidate_path: {"apiKey": "LEGACY"},
        }
    )

    status = delete_temporal_runtime_candidate_secret(
        PROJECT,
        secret_store=secrets,
        checked_at="2026-06-13T00:02:00+00:00",
        candidate_secret_path=runtime_candidate_path,
        candidate_ca_secret_path=ca_candidate_path,
        candidate_secret_token=CANDIDATE_TOKEN,
    )

    assert runtime_candidate_path not in secrets.store
    assert ca_candidate_path not in secrets.store
    assert legacy_candidate_path in secrets.store
    assert status["deleted"]["runtimeCandidateSecret"] == "scheduled-deletion"
    assert status["deleted"]["mtlsCaCandidateSecret"] == "scheduled-deletion"
    assert status["paths"] == {
        "runtimeCandidateSecret": runtime_candidate_path,
        "mtlsCaCandidateSecret": ca_candidate_path,
    }


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


def test_existing_namespace_auth_mode_switch_requires_finalize():
    project = {
        **PROJECT,
        "spec": {
            **PROJECT["spec"],
            "temporal": {"auth": {"mode": "mtls"}},
        },
    }
    reconciler = FakeReconciler(
        existing_namespace=NamespaceInfo(
            qualified_name="tmprl-dem-cld-banking-money-transfer.acct1",
            grpc_address="tmprl-dem-cld-banking-money-transfer.acct1.tmprl.cloud:7233",
            web_address="https://cloud.temporal.io/namespaces/x",
            state="active",
            auth_mode="apiKey",
            created=False,
        ),
    )
    secrets = FakeSecretStore()

    status = reconcile(reconciler, secrets, demo_project=project)

    assert status["phase"] == "Error"
    assert status["reason"] == "TemporalAuthModeSwitchRequiresFinalize"
    assert status["namespace"]["authMode"] == "apiKey"
    assert reconciler.called("create_namespace") == []
    assert reconciler.called("configure_namespace_mtls") == []
    assert secrets.writes == []


def test_create_mtls_namespace_uses_tokenized_candidate_ca_material():
    project = {
        **PROJECT,
        "spec": {
            **PROJECT["spec"],
            "temporal": {"auth": {"mode": "mtls"}},
        },
    }
    ca_candidate_path = temporal_mtls_ca_candidate_secret_path(
        project,
        CANDIDATE_TOKEN,
    )
    reconciler = FakeReconciler()
    secrets = FakeSecretStore(
        {
            ca_candidate_path: {
                "caCert": "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----",
            }
        }
    )

    status = asyncio.run(
        ensure_temporal_cloud_namespace(
            project,
            reconciler=reconciler,
            secret_store=secrets,
            account_id="acct1",
            region="aws-us-west-2",
            checked_at="2026-06-13T00:00:00+00:00",
            candidate_ca_secret_path=ca_candidate_path,
            candidate_secret_token=CANDIDATE_TOKEN,
        )
    )

    create_call = reconciler.called("create_namespace")[0]
    assert status["phase"] == "Ready"
    assert status["namespace"]["authMode"] == "mtls"
    assert create_call[4] == "mtls"
    assert create_call[5] is True
    assert create_call[6] == "temporal-namespace:tmprl-dem-cld-banking-money-transfer"


def test_configure_mtls_namespace_uses_tokenized_candidate_ca_material():
    project = {
        **PROJECT,
        "spec": {
            **PROJECT["spec"],
            "temporal": {"auth": {"mode": "mtls"}},
        },
    }
    ca_candidate_path = temporal_mtls_ca_candidate_secret_path(
        project,
        CANDIDATE_TOKEN,
    )
    reconciler = FakeReconciler()
    secrets = FakeSecretStore(
        {
            ca_candidate_path: {
                "caCert": "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----",
            }
        }
    )

    status = asyncio.run(
        configure_temporal_cloud_namespace_mtls(
            project,
            namespace_status_value={
                "namespace": {
                    "name": "tmprl-dem-cld-banking-money-transfer.acct1",
                    "grpcAddress": (
                        "tmprl-dem-cld-banking-money-transfer.acct1."
                        "tmprl.cloud:7233"
                    ),
                    "authMode": "mtls",
                }
            },
            reconciler=reconciler,
            secret_store=secrets,
            checked_at="2026-06-13T00:00:00+00:00",
            candidate_ca_secret_path=ca_candidate_path,
            candidate_secret_token=CANDIDATE_TOKEN,
        )
    )

    configure_call = reconciler.called("configure_namespace_mtls")[0]
    assert status["phase"] == "Ready"
    assert status["mtls"]["namespaceConfigAction"] == "updated"
    assert configure_call == (
        "configure_namespace_mtls",
        "tmprl-dem-cld-banking-money-transfer.acct1",
        True,
        "temporal-namespace:tmprl-dem-cld-banking-money-transfer",
    )


def test_mtls_auth_mode_creates_namespace_ca_client_secret_and_canaries():
    project = {
        **PROJECT,
        "spec": {
            **PROJECT["spec"],
            "temporal": {"auth": {"mode": "mtls"}},
        },
    }
    reconciler = FakeReconciler()
    secrets = FakeSecretStore()

    status = reconcile(reconciler, secrets, demo_project=project)

    assert status["phase"] == "Ready"
    assert status["auth"]["mode"] == "mtls"
    assert status["namespace"]["authMode"] == "mtls"
    assert "apiKey" not in status
    assert "serviceAccount" not in status
    assert status["mtls"]["action"] == "created"
    assert status["mtls"]["clientCertificate"]["canary"]["ok"] is True
    assert reconciler.called("create_namespace")[0][4] == "mtls"
    assert len(reconciler.called("configure_namespace_mtls")) == 1

    ca_secret = secrets.store[temporal_mtls_ca_secret_remote_path(project)]
    assert ca_secret["caCert"].startswith("-----BEGIN CERTIFICATE-----")
    assert ca_secret["caKey"].startswith("-----BEGIN PRIVATE KEY-----")
    runtime_secret = secrets.store[RUNTIME_PATH]
    assert runtime_secret["authMode"] == "mtls"
    assert runtime_secret["address"] == (
        "tmprl-dem-cld-banking-money-transfer.acct1.tmprl.cloud:7233"
    )
    assert runtime_secret["namespace"] == (
        "tmprl-dem-cld-banking-money-transfer.acct1"
    )
    assert runtime_secret["caCert"] == ca_secret["caCert"]
    assert runtime_secret["clientCert"].startswith("-----BEGIN CERTIFICATE-----")
    assert runtime_secret["clientKey"].startswith("-----BEGIN PRIVATE KEY-----")
    assert "apiKey" not in runtime_secret
    assert "SECRET-TOKEN-VALUE" not in json.dumps(status)


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
    assert reconciler.called("disable_namespace_delete_protection") == [
        (
            "disable_namespace_delete_protection",
            "tmprl-dem-cld-banking-money-transfer.acct1",
        )
    ]
    assert reconciler.called("delete_namespace") == [
        ("delete_namespace", "tmprl-dem-cld-banking-money-transfer.acct1")
    ]
    assert RUNTIME_PATH not in secrets.store
    assert status["deleted"]["secret"] == "scheduled-deletion"


def test_temporal_cloud_cleanup_uses_explicit_runtime_secret_path():
    explicit_path = "tmprl-dem-cld/banking-money-transfer/temporal-stable123"
    project = project_with_runtime_secret_path(explicit_path)
    reconciler = FakeReconciler()
    secrets = FakeSecretStore(
        {
            explicit_path: {
                "namespace": "tmprl-dem-cld-banking-money-transfer.acct1",
                "keyId": "key-explicit",
            },
            RUNTIME_PATH: {"keyId": "key-legacy"},
        }
    )

    api_key_status = asyncio.run(
        delete_temporal_cloud_api_key(
            project,
            reconciler=reconciler,
            secret_store=secrets,
            checked_at="2026-06-13T00:00:00+00:00",
        )
    )
    secret_status = delete_temporal_cloud_runtime_secret(
        project,
        secret_store=secrets,
        checked_at="2026-06-13T00:00:00+00:00",
    )

    assert api_key_status["deleted"]["apiKey"] == "deleted"
    assert reconciler.called("delete_api_key") == [
        ("delete_api_key", "key-explicit")
    ]
    assert secret_status["deleted"]["secret"] == "scheduled-deletion"
    assert explicit_path not in secrets.store
    assert RUNTIME_PATH in secrets.store


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


def test_cloud_api_delete_namespace_retries_stale_resource_version():
    class FakeCloudService:
        def __init__(self):
            self.resource_versions = ["old-version", "new-version"]
            self.deletes: list[str] = []

        async def get_namespace(self, request):
            resource_version = self.resource_versions.pop(0)
            lifecycle = SimpleNamespace(enable_delete_protection=False)
            spec = SimpleNamespace(lifecycle=lifecycle)
            namespace = SimpleNamespace(
                spec=spec,
                resource_version=resource_version,
            )
            return SimpleNamespace(namespace=namespace)

        async def delete_namespace(self, request):
            self.deletes.append(request.resource_version)
            if request.resource_version == "old-version":
                raise RuntimeError("invalid resource version")
            return SimpleNamespace(async_operation=SimpleNamespace(id="", state=0))

    service = FakeCloudService()
    reconciler = CloudApiReconciler(service, account_id="acct1")

    result = asyncio.run(
        reconciler.delete_namespace("tmprl-dem-cld-banking-money-transfer.acct1")
    )

    assert result == "deleted"
    assert service.deletes == ["old-version", "new-version"]


def test_cloud_api_disable_namespace_delete_protection_is_separate_update():
    from temporalio.api.cloud.namespace.v1 import NamespaceSpec

    class FakeCloudService:
        def __init__(self):
            self.updates = []

        async def get_namespace(self, request):
            spec = NamespaceSpec()
            spec.lifecycle.enable_delete_protection = True
            namespace = SimpleNamespace(
                spec=spec,
                resource_version="namespace-version-1",
            )
            return SimpleNamespace(namespace=namespace)

        async def update_namespace(self, request):
            self.updates.append(
                (
                    request.namespace,
                    request.resource_version,
                    request.spec.lifecycle.enable_delete_protection,
                )
            )
            return SimpleNamespace(async_operation=SimpleNamespace(id="", state=0))

    service = FakeCloudService()
    reconciler = CloudApiReconciler(service, account_id="acct1")

    result = asyncio.run(
        reconciler.disable_namespace_delete_protection(
            "tmprl-dem-cld-banking-money-transfer.acct1"
        )
    )

    assert result == "disabled"
    assert service.updates == [
        (
            "tmprl-dem-cld-banking-money-transfer.acct1",
            "namespace-version-1",
            False,
        )
    ]


def test_cloud_api_create_search_attribute_preserves_existing_and_retries_stale_resource_version():
    from temporalio.api.cloud.namespace.v1 import NamespaceSpec

    values = NamespaceSpec.SearchAttributeType

    class FakeCloudService:
        def __init__(self):
            self.resource_versions = ["old-version", "new-version"]
            self.updates = []

        async def get_namespace(self, request):
            spec = NamespaceSpec()
            spec.search_attributes["ExistingAttribute"] = (
                values.SEARCH_ATTRIBUTE_TYPE_KEYWORD
            )
            return SimpleNamespace(
                namespace=SimpleNamespace(
                    spec=spec,
                    resource_version=self.resource_versions.pop(0),
                )
            )

        async def update_namespace(self, request):
            self.updates.append(
                (
                    request.resource_version,
                    dict(request.spec.search_attributes),
                )
            )
            if request.resource_version == "old-version":
                raise RuntimeError("invalid resource version")
            return SimpleNamespace(async_operation=SimpleNamespace(id="", state=0))

    service = FakeCloudService()
    reconciler = CloudApiReconciler(service, account_id="acct1")

    result = asyncio.run(
        reconciler.create_or_verify_search_attribute(
            "tmprl-dem-cld-banking-money-transfer.acct1",
            "CustomerId",
            "keyword",
        )
    )

    assert result == {"action": "created", "currentType": "keyword"}
    assert [update[0] for update in service.updates] == [
        "old-version",
        "new-version",
    ]
    assert service.updates[-1][1] == {
        "ExistingAttribute": values.SEARCH_ATTRIBUTE_TYPE_KEYWORD,
        "CustomerId": values.SEARCH_ATTRIBUTE_TYPE_KEYWORD,
    }


def test_cloud_api_search_attribute_type_drift_does_not_update_namespace():
    from temporalio.api.cloud.namespace.v1 import NamespaceSpec

    values = NamespaceSpec.SearchAttributeType

    class FakeCloudService:
        def __init__(self):
            self.updates = []

        async def get_namespace(self, request):
            spec = NamespaceSpec()
            spec.search_attributes["CustomerId"] = (
                values.SEARCH_ATTRIBUTE_TYPE_KEYWORD
            )
            return SimpleNamespace(
                namespace=SimpleNamespace(
                    spec=spec,
                    resource_version="namespace-version-1",
                )
            )

        async def update_namespace(self, request):
            self.updates.append(request)
            return SimpleNamespace(async_operation=SimpleNamespace(id="", state=0))

    service = FakeCloudService()
    reconciler = CloudApiReconciler(service, account_id="acct1")

    result = asyncio.run(
        reconciler.create_or_verify_search_attribute(
            "tmprl-dem-cld-banking-money-transfer.acct1",
            "CustomerId",
            "double",
        )
    )

    assert result == {"action": "drifted", "currentType": "keyword"}
    assert service.updates == []


def test_cloud_api_delete_api_key_uses_resource_version():
    class FakeCloudService:
        def __init__(self):
            self.deleted = []

        async def get_api_key(self, request):
            assert request.key_id == "key-abc"
            return SimpleNamespace(
                api_key=SimpleNamespace(resource_version="key-version-1")
            )

        async def delete_api_key(self, request):
            self.deleted.append((request.key_id, request.resource_version))
            return SimpleNamespace(async_operation=SimpleNamespace(id="", state=0))

    service = FakeCloudService()
    reconciler = CloudApiReconciler(service, account_id="acct1")

    result = asyncio.run(reconciler.delete_api_key("key-abc"))

    assert result == "deleted"
    assert service.deleted == [("key-abc", "key-version-1")]


def test_cloud_api_delete_service_account_uses_resource_version():
    class FakeCloudService:
        def __init__(self):
            self.deleted = []

        async def get_service_account(self, request):
            assert request.service_account_id == "sa-123"
            return SimpleNamespace(
                service_account=SimpleNamespace(resource_version="sa-version-1")
            )

        async def delete_service_account(self, request):
            self.deleted.append(
                (request.service_account_id, request.resource_version)
            )
            return SimpleNamespace(async_operation=SimpleNamespace(id="", state=0))

    service = FakeCloudService()
    reconciler = CloudApiReconciler(service, account_id="acct1")

    result = asyncio.run(reconciler.delete_service_account("sa-123"))

    assert result == "deleted"
    assert service.deleted == [("sa-123", "sa-version-1")]


# --- region plumbing + region-drift detection --------------------------------


def test_namespace_status_round_trips_region():
    info = NamespaceInfo(
        qualified_name="ns.acct1",
        grpc_address="ns.acct1.tmprl.cloud:7233",
        state="active",
        auth_mode="apiKey",
        region="gcp-us-central1",
    )

    status = namespace_status(info, created=False)
    assert status["region"] == "gcp-us-central1"

    rebuilt = namespace_info_from_status({"namespace": status})
    assert rebuilt.region == "gcp-us-central1"

    # A pre-region status round-trips to None rather than raising.
    legacy = namespace_info_from_status(
        {"namespace": {"name": "ns.acct1", "grpcAddress": "ns.acct1.tmprl.cloud:7233"}}
    )
    assert legacy.region is None


def test_cloud_api_namespace_info_reads_spec_region():
    reconciler = CloudApiReconciler(None, account_id="acct1")
    namespace = SimpleNamespace(
        namespace="ns.acct1",
        endpoints=SimpleNamespace(web_address="https://web"),
        state=0,
        spec=SimpleNamespace(
            regions=["aws-ca-central-1"],
            mtls_auth=SimpleNamespace(enabled=False),
            api_key_auth=SimpleNamespace(enabled=True),
        ),
    )

    info = reconciler._namespace_info(namespace, created=False)
    assert info.region == "aws-ca-central-1"
    assert info.auth_mode == "apiKey"

    namespace.spec.regions = []
    assert reconciler._namespace_info(namespace, created=False).region is None


def test_ensure_namespace_signals_region_drift_when_region_differs():
    existing = NamespaceInfo(
        qualified_name="tmprl-dem-cld-banking-money-transfer.acct1",
        grpc_address="tmprl-dem-cld-banking-money-transfer.acct1.tmprl.cloud:7233",
        state="active",
        auth_mode="apiKey",
        region="gcp-us-central1",
    )
    reconciler = FakeReconciler(existing_namespace=existing)

    status = asyncio.run(
        ensure_temporal_cloud_namespace(
            PROJECT,
            reconciler=reconciler,
            secret_store=FakeSecretStore(),
            account_id="acct1",
            region="aws-ca-central-1",
            checked_at="2026-06-13T00:00:00+00:00",
        )
    )

    assert status["phase"] == "RegionDrift"
    assert status["reason"] == "RegionDrift"
    assert status["currentRegion"] == "gcp-us-central1"
    assert status["desiredRegion"] == "aws-ca-central-1"
    # Drift must neither create nor adopt; the workflow tears down first.
    assert reconciler.called("create_namespace") == []


def test_ensure_namespace_no_drift_when_region_matches():
    existing = NamespaceInfo(
        qualified_name="tmprl-dem-cld-banking-money-transfer.acct1",
        grpc_address="tmprl-dem-cld-banking-money-transfer.acct1.tmprl.cloud:7233",
        state="active",
        auth_mode="apiKey",
        region="aws-ca-central-1",
    )
    reconciler = FakeReconciler(existing_namespace=existing)

    status = asyncio.run(
        ensure_temporal_cloud_namespace(
            PROJECT,
            reconciler=reconciler,
            secret_store=FakeSecretStore(),
            account_id="acct1",
            region="aws-ca-central-1",
            checked_at="2026-06-13T00:00:00+00:00",
        )
    )

    assert status["phase"] == "Ready"
    assert status["namespace"]["region"] == "aws-ca-central-1"
    assert reconciler.called("create_namespace") == []


def test_ensure_namespace_unknown_region_never_signals_drift():
    # A namespace whose region is unknown (e.g. provisioned before region
    # plumbing) is adopted, never destroyed, even if it differs from desired.
    existing = NamespaceInfo(
        qualified_name="tmprl-dem-cld-banking-money-transfer.acct1",
        grpc_address="tmprl-dem-cld-banking-money-transfer.acct1.tmprl.cloud:7233",
        state="active",
        auth_mode="apiKey",
        region=None,
    )
    reconciler = FakeReconciler(existing_namespace=existing)

    status = asyncio.run(
        ensure_temporal_cloud_namespace(
            PROJECT,
            reconciler=reconciler,
            secret_store=FakeSecretStore(),
            account_id="acct1",
            region="aws-ca-central-1",
            checked_at="2026-06-13T00:00:00+00:00",
        )
    )

    assert status["phase"] == "Ready"
    assert reconciler.called("create_namespace") == []
