from __future__ import annotations

from pathlib import Path

import yaml

from demo_registry_operator.resources import (
    TEMPORAL_SECRET_NAME,
    TEMPORAL_WORKER_RUNTIME_SECRET_NAME,
    component_temporal_namespace_label,
    components_bound_to_namespace_label,
    env_from_refs,
    external_secret,
    project_temporal_namespace,
    project_temporal_namespace_for_label,
    runtime_secret_resources,
    temporal_credentials_secret_needed,
    temporal_input_ref_secret_name,
    temporal_namespace_labels,
    temporal_secret_name,
    temporal_secret_remote_path,
    temporal_secret_remote_path_for_label,
    temporal_secret_remote_path_with_suffix,
    temporal_worker_runtime_secret_name,
    temporal_worker_runtime_secret_needed,
    worker_runtime_external_secret,
)

ROOT = Path(__file__).resolve().parents[3]
PROJECTS = ROOT / "projects"


def real_demo_projects() -> list[dict]:
    documents = []
    for path in sorted(PROJECTS.glob("**/*.yaml")):
        document = yaml.safe_load(path.read_text())
        if isinstance(document, dict) and document.get("kind") == "DemoProject":
            documents.append(document)
    return documents


def project(name, **spec):
    return {"metadata": {"name": name}, "spec": spec}


# --- the load-bearing identity: no label means exactly today's behavior ------


def test_namespace_for_label_none_matches_the_unlabelled_derivation():
    # Asserted against every real project, because this identity is what makes the
    # primary namespace provably unchanged rather than merely similar.
    for demo_project in real_demo_projects():
        assert project_temporal_namespace_for_label(
            demo_project, None
        ) == project_temporal_namespace(demo_project)


def test_secret_names_without_a_label_are_the_stable_constants():
    assert temporal_secret_name() == TEMPORAL_SECRET_NAME
    assert temporal_secret_name(None) == TEMPORAL_SECRET_NAME
    assert temporal_worker_runtime_secret_name() == TEMPORAL_WORKER_RUNTIME_SECRET_NAME
    assert (
        temporal_worker_runtime_secret_name(None) == TEMPORAL_WORKER_RUNTIME_SECRET_NAME
    )


def test_remote_path_without_a_label_matches_the_unlabelled_path():
    for demo_project in real_demo_projects():
        assert temporal_secret_remote_path_for_label(
            demo_project, None
        ) == temporal_secret_remote_path(demo_project)


def test_remote_path_for_label_reuses_the_suffix_helper():
    # Assert the reuse, not just a matching string, so a future edit to one cannot
    # silently diverge from the other.
    demo_project = project("banking-money-transfer")
    assert temporal_secret_remote_path_for_label(
        demo_project, "alpha"
    ) == temporal_secret_remote_path_with_suffix(demo_project, "alpha")


def test_input_ref_secret_name_unchanged_for_unbound_components():
    plain = {"build": "app", "worker": True}
    versioned = {"build": "app", "worker": {"versioning": {"enabled": True}}}
    assert temporal_input_ref_secret_name(plain, "temporal.apiKey") == TEMPORAL_SECRET_NAME
    assert (
        temporal_input_ref_secret_name(plain, "temporal.address") == TEMPORAL_SECRET_NAME
    )
    assert (
        temporal_input_ref_secret_name(versioned, "temporal.apiKey")
        == TEMPORAL_WORKER_RUNTIME_SECRET_NAME
    )
    # address is not a worker-runtime ref, so it stays on the credentials Secret
    assert (
        temporal_input_ref_secret_name(versioned, "temporal.address")
        == TEMPORAL_SECRET_NAME
    )


def test_env_from_refs_unchanged_for_unbound_components():
    demo_project = project("demo", components={})
    assert env_from_refs(demo_project, "worker", {"build": "app", "worker": True}) == [
        {"secretRef": {"name": TEMPORAL_SECRET_NAME}}
    ]
    assert env_from_refs(
        demo_project, "worker", {"build": "app", "worker": {"versioning": {"enabled": True}}}
    ) == [{"secretRef": {"name": TEMPORAL_WORKER_RUNTIME_SECRET_NAME}}]


def test_real_projects_render_only_the_stable_temporal_secret_names():
    # None of the shipped projects declare namespaces, so no labelled sibling
    # ExternalSecret may appear for any of them.
    for demo_project in real_demo_projects():
        rendered = runtime_secret_resources(demo_project)
        temporal_names = {
            item["metadata"]["name"]
            for item in rendered
            if item["metadata"]["name"].startswith("temporal-")
        }
        assert temporal_names <= {
            TEMPORAL_SECRET_NAME,
            TEMPORAL_WORKER_RUNTIME_SECRET_NAME,
        }, f"{demo_project['metadata']['name']} rendered {temporal_names}"


# --- labelled behavior -------------------------------------------------------


def test_namespace_for_label_derives_a_distinct_namespace():
    demo_project = project("banking-money-transfer")
    primary = project_temporal_namespace(demo_project)
    alpha = project_temporal_namespace_for_label(demo_project, "alpha")
    beta = project_temporal_namespace_for_label(demo_project, "beta")
    assert alpha != primary and beta != primary and alpha != beta
    assert alpha.startswith("tmprl-dem-cld-")
    # Temporal Cloud caps namespace names at 39 characters.
    assert len(alpha) <= 39


def test_namespace_for_label_truncates_long_names_deterministically():
    demo_project = project("durable-ad-campaign-typescript-workshop")
    first = project_temporal_namespace_for_label(demo_project, "alpha")
    assert len(first) <= 39
    assert first == project_temporal_namespace_for_label(demo_project, "alpha")
    assert first != project_temporal_namespace_for_label(demo_project, "beta")


def test_labelled_secret_names_are_distinct_and_dns_safe():
    alpha = temporal_secret_name("alpha")
    beta = temporal_secret_name("beta")
    assert alpha != beta
    assert alpha != TEMPORAL_SECRET_NAME
    assert alpha.startswith(f"{TEMPORAL_SECRET_NAME}-")
    for name in (alpha, beta, temporal_worker_runtime_secret_name("alpha")):
        assert len(name) <= 63
        assert name == name.lower()
        assert name.strip("-") == name


def test_labelled_secret_name_is_stable_for_a_long_label():
    long_label = "a" * 200
    name = temporal_secret_name(long_label)
    assert len(name) <= 63
    assert name == temporal_secret_name(long_label)


def test_component_label_reader():
    assert component_temporal_namespace_label({"build": "app"}) is None
    assert component_temporal_namespace_label({"temporalNamespace": ""}) is None
    assert component_temporal_namespace_label({"temporalNamespace": "  "}) is None
    assert component_temporal_namespace_label({"temporalNamespace": " alpha "}) == "alpha"
    assert component_temporal_namespace_label({"temporalNamespace": 7}) is None


def test_namespace_labels_accessor():
    assert temporal_namespace_labels(project("x")) == {}
    assert temporal_namespace_labels(project("x", temporal={})) == {}
    assert temporal_namespace_labels(project("x", temporal={"namespaces": "nope"})) == {}
    labels = temporal_namespace_labels(
        project("x", temporal={"namespaces": {"alpha": {"region": "aws-us-west-2"}}})
    )
    assert labels == {"alpha": {"region": "aws-us-west-2"}}


def test_bound_component_reads_the_labelled_secret():
    bound = {"build": "app", "worker": True, "temporalNamespace": "alpha"}
    assert (
        temporal_input_ref_secret_name(bound, "temporal.apiKey")
        == temporal_secret_name("alpha")
    )
    assert env_from_refs(project("demo"), "worker", bound) == [
        {"secretRef": {"name": temporal_secret_name("alpha")}}
    ]


def test_bound_versioned_worker_reads_the_labelled_runtime_secret():
    bound = {
        "build": "app",
        "worker": {"versioning": {"enabled": True}},
        "temporalNamespace": "alpha",
    }
    assert (
        temporal_input_ref_secret_name(bound, "temporal.apiKey")
        == temporal_worker_runtime_secret_name("alpha")
    )
    assert env_from_refs(project("demo"), "worker", bound) == [
        {"secretRef": {"name": temporal_worker_runtime_secret_name("alpha")}}
    ]


def test_secret_needed_predicates_partition_components_by_label():
    demo_project = project(
        "demo",
        temporal={"namespaces": {"alpha": {}}},
        components={
            "unbound": {"build": "app", "worker": True},
            "bound": {"build": "app", "worker": True, "temporalNamespace": "alpha"},
        },
    )
    assert temporal_credentials_secret_needed(demo_project) is True
    assert temporal_credentials_secret_needed(demo_project, "alpha") is True
    assert temporal_credentials_secret_needed(demo_project, "beta") is False
    assert temporal_worker_runtime_secret_needed(demo_project, "alpha") is False


def test_components_bound_to_namespace_label_selects_unbound_for_none():
    selected = {
        "unbound": {"build": "app"},
        "bound": {"build": "app", "temporalNamespace": "alpha"},
    }
    assert sorted(components_bound_to_namespace_label(selected, None)) == ["unbound"]
    assert sorted(components_bound_to_namespace_label(selected, "alpha")) == ["bound"]
    assert components_bound_to_namespace_label(selected, "beta") == {}


def test_runtime_secret_resources_renders_a_sibling_pair_per_label():
    demo_project = project(
        "demo",
        temporal={"namespaces": {"alpha": {}}},
        components={
            "unbound": {"build": "app", "worker": True},
            "bound": {"build": "app", "worker": True, "temporalNamespace": "alpha"},
        },
    )
    names = [item["metadata"]["name"] for item in runtime_secret_resources(demo_project)]
    assert TEMPORAL_SECRET_NAME in names
    assert temporal_secret_name("alpha") in names


def test_labelled_external_secrets_point_at_the_labelled_remote_path():
    demo_project = project("demo", temporal={"namespaces": {"alpha": {}}})
    expected = temporal_secret_remote_path_for_label(demo_project, "alpha")
    rendered = external_secret(
        demo_project, name=temporal_secret_name("alpha"), label="alpha"
    )
    assert rendered["metadata"]["name"] == temporal_secret_name("alpha")
    assert rendered["spec"]["target"]["name"] == temporal_secret_name("alpha")
    assert {entry["remoteRef"]["key"] for entry in rendered["spec"]["data"]} == {expected}

    runtime = worker_runtime_external_secret(
        demo_project, name=temporal_worker_runtime_secret_name("alpha"), label="alpha"
    )
    assert {entry["remoteRef"]["key"] for entry in runtime["spec"]["data"]} == {expected}


def test_unlabelled_external_secret_is_unchanged():
    demo_project = project("demo")
    rendered = external_secret(demo_project)
    assert rendered["metadata"]["name"] == TEMPORAL_SECRET_NAME
    assert rendered["spec"]["target"]["name"] == TEMPORAL_SECRET_NAME
    assert {entry["remoteRef"]["key"] for entry in rendered["spec"]["data"]} == {
        temporal_secret_remote_path(demo_project)
    }


# --- the synthetic per-target override the saga uses -------------------------


def test_namespace_override_wins_over_the_derived_name():
    overridden = project("demo", temporal={"namespaceOverride": "tmprl-dem-cld-demo-alpha"})
    assert project_temporal_namespace(overridden) == "tmprl-dem-cld-demo-alpha"


def test_blank_namespace_override_falls_back_to_the_derived_name():
    for value in ("", "   ", None, 7, []):
        demo_project = project("demo", temporal={"namespaceOverride": value})
        assert project_temporal_namespace(demo_project) == project_temporal_namespace(
            project("demo")
        )
