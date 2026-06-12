from __future__ import annotations

from demo_registry_operator import derive
from demo_registry_operator.activities.builds import source_config
from demo_registry_operator.resources import (
    catalog_entry,
    catalog_payload,
    component_ports,
    container,
    ingress_routes,
    project_temporal_namespace,
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
    assert cfg == {"repo": "https://github.com/temporal-sa/x", "branch": "main"}


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
