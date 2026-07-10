from __future__ import annotations

from demo_registry_operator import derive
from demo_registry_operator.activities.builds import source_config
from demo_registry_operator.resources import (
    catalog_entry,
    catalog_payload,
    component_ports,
    container,
    external_resource_status,
    ingress_routes,
    project_metadata,
    project_temporal_namespace,
    resolve_temporal_cloud_region,
    temporal_mtls_status,
    temporal_auth_mode,
    temporal_region,
    temporal_secret_remote_path,
    temporal_secret_remote_path_with_suffix,
    temporal_search_attributes,
    worker_runtime_external_secret,
)


def project(name, **spec):
    return {"metadata": {"name": name}, "spec": spec}


# --- derivation (single source of truth) ---------------------------------


def test_kubernetes_namespace_prefixes():
    assert derive.kubernetes_namespace("banking") == "tmprl-dem-cld-banking"


def test_temporal_namespace_short_name_is_just_prefixed():
    assert (
        derive.temporal_namespace("banking-money-transfer")
        == "tmprl-dem-cld-banking-money-transfer"
    )


def test_temporal_namespace_long_name_is_hash_truncated_to_39():
    name = "banking-app-money-transfer"  # 14-char prefix pushes it over 39
    result = derive.temporal_namespace(name)
    assert result.startswith("tmprl-dem-cld-banking-app")
    assert len(result) <= derive.TEMPORAL_NAMESPACE_MAX_LENGTH
    # hash-suffixed and deterministic
    assert result == derive.temporal_namespace(name)
    assert result != "tmprl-dem-cld-banking-app-money-transfer"


def test_operator_namespace_helper_matches_derive():
    assert (
        project_temporal_namespace(project("banking"))
        == derive.temporal_namespace("banking")
    )


def test_secret_prefix_and_host():
    assert derive.secret_prefix("banking") == "tmprl-dem-cld/banking"
    assert derive.host("banking-app", "banking") == "banking-app.tmprl-demo.cloud"
    assert derive.host(None, "banking") == "banking.tmprl-demo.cloud"


def test_temporal_secret_path_defaults_to_legacy_project_path():
    assert (
        temporal_secret_remote_path(project("dejavu-tacos"))
        == "tmprl-dem-cld/dejavu-tacos/temporal"
    )


def test_temporal_secret_path_uses_planned_runtime_path_override():
    proj = project(
        "dejavu-tacos",
        temporal={
            "runtimeSecretPath": "tmprl-dem-cld/dejavu-tacos/temporal-planned"
        },
    )

    assert (
        temporal_secret_remote_path(proj)
        == "tmprl-dem-cld/dejavu-tacos/temporal-planned"
    )


def test_temporal_secret_path_with_workflow_suffix():
    assert (
        temporal_secret_remote_path_with_suffix(project("dejavu-tacos"), "AbC123")
        == "tmprl-dem-cld/dejavu-tacos/temporal-abc123"
    )


def test_worker_runtime_external_secret_uses_planned_remote_path():
    proj = {
        "metadata": {"name": "dejavu-tacos"},
        "spec": {
            "temporal": {
                "runtimeSecretPath": "tmprl-dem-cld/dejavu-tacos/temporal-owned"
            },
            "components": {"worker": {"build": "worker", "worker": True}},
        },
    }
    remote_keys = {
        item["remoteRef"]["key"]
        for item in worker_runtime_external_secret(proj)["spec"]["data"]
    }

    assert remote_keys == {temporal_secret_remote_path(proj)}


# --- defaults ------------------------------------------------------------


def test_single_unnamed_port_defaults_to_http_and_serviceport():
    ports = component_ports({"ports": [{"containerPort": 8080}]})
    assert ports == [{"containerPort": 8080, "servicePort": 8080, "name": "http"}]


def test_multiple_ports_are_not_auto_named():
    ports = component_ports(
        {"ports": [{"containerPort": 80, "name": "http"}, {"containerPort": 9090, "name": "metrics"}]}
    )
    assert {p["name"] for p in ports} == {"http", "metrics"}


def test_default_ingress_route_targets_serviceport_80_component():
    proj = project(
        "banking-money-transfer",
        components={
            "frontend": {"build": "frontend", "ports": [{"containerPort": 5173, "servicePort": 80}]},
            "backend": {"build": "backend", "ports": [{"containerPort": 8000, "servicePort": 8000}]},
            "worker": {"build": "worker", "worker": True},
        },
        ingress={"subdomain": "banking-app"},
    )
    assert ingress_routes(proj) == [{"component": "frontend", "port": "http"}]


def test_explicit_routes_are_respected():
    proj = project(
        "x",
        ingress={"subdomain": "x", "routes": [{"component": "api", "port": "http"}]},
    )
    assert ingress_routes(proj) == [{"component": "api", "port": "http"}]


def test_source_branch_defaults_to_main():
    cfg = source_config(project("x", source={"repo": "https://github.com/temporal-sa/x"}))
    assert cfg == {
        "repo": "https://github.com/temporal-sa/x",
        "branch": "main",
        "codebuildRepo": "https://github.com/temporal-sa/x",
        "sourceAuthSecretPath": "tmprl-dem-cld/platform/github-temporal-sa-readonly",
    }


def test_source_branch_explicit_is_kept():
    cfg = source_config(
        project("x", source={"repo": "https://github.com/temporal-sa/x", "branch": "dev"})
    )
    assert cfg["branch"] == "dev"


def test_container_clears_absent_command_and_args_for_patch_semantics():
    rendered = container(
        project("x"),
        "web",
        {"build": "web"},
        "example.com/web:git-abc123",
    )

    assert rendered["command"] is None
    assert rendered["args"] is None


def test_container_keeps_explicit_command_and_empty_args():
    rendered = container(
        project("x"),
        "web",
        {"build": "web", "command": ["web"], "args": []},
        "example.com/web:git-abc123",
    )

    assert rendered["command"] == ["web"]
    assert rendered["args"] == []


def test_temporal_auth_mode_defaults_to_api_key():
    assert temporal_auth_mode(project("banking")) == "apiKey"


def test_temporal_auth_mode_uses_project_config():
    proj = project("banking", temporal={"auth": {"mode": "mtls"}})

    assert temporal_auth_mode(proj) == "mtls"


def test_temporal_search_attributes_default_to_keyword_and_support_all_types():
    proj = project(
        "banking",
        temporal={
            "searchAttributes": {
                "CustomerId": {},
                "CartTotal": {"type": "double"},
                "IsPriority": {"type": "bool"},
                "OrderCount": {"type": "int"},
                "PlacedAt": {"type": "datetime"},
                "Notes": {"type": "text"},
                "Tags": {"type": "keywordList"},
            }
        },
    )

    assert temporal_search_attributes(proj) == {
        "CartTotal": "double",
        "CustomerId": "keyword",
        "IsPriority": "bool",
        "Notes": "text",
        "OrderCount": "int",
        "PlacedAt": "datetime",
        "Tags": "keywordList",
    }


def test_temporal_auth_status_is_exposed_in_project_metadata():
    metadata = project_metadata(
        project(
            "banking",
            temporal={"auth": {"mode": "apiKey"}},
            components={"worker": {"build": "worker", "worker": True}},
        )
    )

    assert metadata["temporal"]["auth"] == {
        "mode": "apiKey",
        "defaulted": False,
        "modeSwitchPolicy": "destructive-recreate",
    }
    assert metadata["temporal"]["runtime"]["apiKey"]["secretProperties"] == [
        "address",
        "namespace",
        "apiKey",
        "tls",
    ]


def test_external_resource_status_exposes_mtls_auth_intent():
    status = external_resource_status(
        project(
            "banking",
            temporal={"auth": {"mode": "mtls"}},
            builds={"worker": {"context": ".", "dockerfile": "Dockerfile"}},
            components={"worker": {"build": "worker", "worker": True}},
        )
    )

    temporal = status["temporalCloud"]
    assert temporal["auth"]["mode"] == "mtls"
    assert status["secretsManager"]["temporalCredentials"]["requiredProperties"] == [
        "address",
        "namespace",
        "tls",
        "caCert",
        "clientCert",
        "clientKey",
    ]
    assert temporal["mtls"] == temporal_mtls_status(
        project(
            "banking",
            temporal={"auth": {"mode": "mtls"}},
            components={"worker": {"build": "worker", "worker": True}},
        )
    )
    assert temporal["mtls"]["caCertificate"]["rotation"]["rotateEveryDays"] == 90
    assert temporal["mtls"]["clientCertificate"]["rotation"]["rotateEveryDays"] == 7


def test_external_resource_status_exposes_search_attributes():
    status = external_resource_status(
        project(
            "banking",
            temporal={
                "searchAttributes": {
                    "CustomerId": {},
                    "CartTotal": {"type": "double"},
                }
            },
        )
    )

    search_attributes = status["temporalCloud"]["searchAttributes"]
    assert status["temporalCloud"]["phase"] == "ManualSetupRequired"
    assert search_attributes["ownershipPolicy"] == "delete-only-previously-managed"
    assert search_attributes["attributes"] == [
        {"name": "CartTotal", "type": "double", "action": "create-or-verify"},
        {"name": "CustomerId", "type": "keyword", "action": "create-or-verify"},
    ]


# --- catalog -------------------------------------------------------------


def test_catalog_entry_defaults_title_when_registry_block_absent():
    entry = catalog_entry(
        project(
            "banking-money-transfer",
            source={"repo": "https://github.com/temporal-sa/x"},
            ingress={"subdomain": "banking-app"},
        )
    )
    assert entry["title"] == "Banking Money Transfer"
    assert entry["host"] == "banking-app.tmprl-demo.cloud"
    assert entry["temporalNamespace"] == "tmprl-dem-cld-banking-money-transfer"
    assert entry["catalogStatus"]["configMap"]["key"] == "banking-money-transfer.json"


def test_catalog_entry_uses_registry_block_when_present():
    entry = catalog_entry(
        project(
            "x",
            registry={"title": "My App", "summary": "Does things", "tags": ["Demo"]},
            source={"repo": "https://github.com/temporal-sa/x"},
        )
    )
    assert entry["title"] == "My App"
    assert entry["summary"] == "Does things"
    assert entry["tags"] == ["Demo"]


def test_catalog_payload_sorts_projects_and_sets_base_domain():
    payload = catalog_payload([project("b"), project("a")])
    assert payload["baseDomain"] == "tmprl-demo.cloud"
    assert [p["name"] for p in payload["projects"]] == ["a", "b"]


def test_temporal_region_reads_spec_and_defaults_none():
    assert (
        temporal_region(project("banking", temporal={"region": "aws-ca-central-1"}))
        == "aws-ca-central-1"
    )
    assert temporal_region(project("banking")) is None
    assert temporal_region(project("banking", temporal={})) is None


def test_resolve_temporal_cloud_region_precedence():
    # An explicit spec.temporal.region always wins, even for serverless.
    assert (
        resolve_temporal_cloud_region(
            explicit_region="gcp-europe-west1",
            serverless=True,
            operator_region="gcp-us-central1",
            serverless_region="aws-ca-central-1",
        )
        == "gcp-europe-west1"
    )
    # Serverless projects default to the AWS region.
    assert (
        resolve_temporal_cloud_region(
            explicit_region=None,
            serverless=True,
            operator_region="gcp-us-central1",
            serverless_region="aws-ca-central-1",
        )
        == "aws-ca-central-1"
    )
    # Everything else defaults to the operator-wide region.
    assert (
        resolve_temporal_cloud_region(
            explicit_region=None,
            serverless=False,
            operator_region="gcp-us-central1",
            serverless_region="aws-ca-central-1",
        )
        == "gcp-us-central1"
    )
