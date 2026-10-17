from __future__ import annotations

import copy
from pathlib import Path

import pytest
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
    worker_deployment_resource,
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


def test_real_projects_render_exactly_the_temporal_secrets_they_declare():
    # A project renders a labelled sibling ExternalSecret if and only if it
    # declares that namespace label. This used to assert that no shipped project
    # declared any -- true until agent-gateway became the first to, so the test
    # now checks the derivation rather than the era. Anything outside the
    # allowed set means a Secret name was derived from something other than a
    # declared label, which is the failure worth catching.
    for demo_project in real_demo_projects():
        rendered = runtime_secret_resources(demo_project)
        temporal_names = {
            item["metadata"]["name"]
            for item in rendered
            if item["metadata"]["name"].startswith("temporal-")
        }
        allowed = {TEMPORAL_SECRET_NAME, TEMPORAL_WORKER_RUNTIME_SECRET_NAME}
        for label in temporal_namespace_labels(demo_project):
            allowed.add(temporal_secret_name(label))
            allowed.add(temporal_worker_runtime_secret_name(label))
        assert temporal_names <= allowed, (
            f"{demo_project['metadata']['name']} rendered {temporal_names}, "
            f"allowed {allowed}"
        )


def test_projects_without_declared_namespaces_render_only_stable_names():
    # The other half of the pair, kept separate so a regression that started
    # rendering labelled Secrets for an unlabelled project still fails loudly.
    for demo_project in real_demo_projects():
        if temporal_namespace_labels(demo_project):
            continue
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


def test_binding_a_versioned_worker_is_rejected():
    # The Worker Controller's Connection and WorkerDeployment both resolve the
    # project's primary namespace and credentials, and neither is label-aware, so
    # a bound versioned worker would poll one namespace while its pod
    # authenticates to another. Refuse to render rather than emit that.
    bound = {
        "build": "app",
        "worker": {"versioning": {"enabled": True}},
        "temporalNamespace": "alpha",
    }
    with pytest.raises(ValueError, match="cannot combine temporalNamespace"):
        worker_deployment_resource(
            project("demo"),
            "worker",
            bound,
            "img:git-x",
            version="v1",
            external_resources=None,
        )


def test_unbound_versioned_worker_still_renders():
    unbound = {"build": "app", "worker": {"versioning": {"enabled": True}}}
    rendered = worker_deployment_resource(
        project("demo"),
        "worker",
        unbound,
        "img:git-x",
        version="v1",
        external_resources=None,
    )
    assert rendered["kind"] == "WorkerDeployment"


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


# --- the activity payload projection ----------------------------------------


def temporal_cloud_project(**overrides):
    from demo_registry_operator.activities.external_resources.models import (
        TemporalCloudProject,
    )

    fields = {
        "name": "banking-money-transfer",
        "namespace": "tmprl-dem-cld-banking-money-transfer",
        "kubernetes_namespace": "tmprl-dem-cld-banking-money-transfer",
        "auth_mode": "apiKey",
        "components": {"worker": {"build": "app", "worker": True}},
        "runtime_secret_path": "tmprl-dem-cld/banking-money-transfer/temporal",
        "mtls_ca_secret_path": "tmprl-dem-cld/banking-money-transfer/temporal/ca",
    }
    fields.update(overrides)
    return TemporalCloudProject(**fields)


def test_as_demo_project_is_unchanged_without_a_target_namespace():
    # Pinned against the literal dict rather than a recomputed one: this is the
    # projection every activity has always received, and it must not move.
    assert temporal_cloud_project().as_demo_project() == {
        "metadata": {"name": "banking-money-transfer"},
        "spec": {
            "components": {"worker": {"build": "app", "worker": True}},
            "temporal": {
                "auth": {"mode": "apiKey"},
                "runtimeSecretPath": "tmprl-dem-cld/banking-money-transfer/temporal",
            },
        },
    }


def test_as_demo_project_stays_unchanged_when_only_siblings_are_populated():
    # Declaring namespaces on the project-level model must not alter the payload;
    # only a per-target projection carries an override.
    from demo_registry_operator.activities.external_resources.models import (
        TemporalNamespaceSpec,
    )

    with_siblings = temporal_cloud_project(
        additional_namespaces=(
            TemporalNamespaceSpec(
                label="alpha",
                namespace="tmprl-dem-cld-banking-money-transfer-alpha",
                auth_mode="apiKey",
                runtime_secret_path="tmprl-dem-cld/banking-money-transfer/temporal-alpha",
                mtls_ca_secret_path="tmprl-dem-cld/banking-money-transfer/temporal-alpha/ca",
                components={},
            ),
        )
    )
    assert with_siblings.as_demo_project() == temporal_cloud_project().as_demo_project()


def test_as_demo_project_emits_the_override_for_a_target():
    projected = temporal_cloud_project(
        target_namespace="tmprl-dem-cld-banking-money-transfer-alpha"
    ).as_demo_project()
    assert (
        projected["spec"]["temporal"]["namespaceOverride"]
        == "tmprl-dem-cld-banking-money-transfer-alpha"
    )
    # And the operator resolves the projection back to that namespace.
    assert (
        project_temporal_namespace(projected)
        == "tmprl-dem-cld-banking-money-transfer-alpha"
    )


def test_nexus_endpoints_required_is_false_by_default():
    assert temporal_cloud_project().nexus_endpoints_required is False


# --- end-to-end: a real CR through the planner (deliberately generic labels) --


SCENARIO = {
    "apiVersion": "registry.tmprl-demo.cloud/v1alpha1",
    "kind": "DemoProject",
    "metadata": {"name": "scenario"},
    "spec": {
        "source": {"repo": "https://github.com/temporal-sa/scenario"},
        "builds": {"app": {"context": "."}},
        "temporal": {
            "namespaces": {
                "alpha": {"region": "aws-us-west-2"},
                "beta": {"retentionDays": 14},
            },
            "nexusEndpoints": {
                "scenario-orders": {
                    "targetNamespace": "beta",
                    "targetTaskQueue": "orders-tq",
                    "allowedCallerNamespaces": ["primary", "alpha"],
                }
            },
        },
        "components": {
            "unbound": {"build": "app", "worker": True},
            "alpha-worker": {"build": "app", "worker": True, "temporalNamespace": "alpha"},
            "beta-worker": {"build": "app", "worker": True, "temporalNamespace": "beta"},
        },
    },
}


def scenario_plan():
    from demo_registry_operator.activities.external_resources import planning

    return planning.plan_external_resources(
        SCENARIO, checked_at="2026-06-20T00:00:00+00:00"
    )


def test_scenario_plans_one_target_per_declared_namespace():
    project = scenario_plan().temporal_project
    assert [spec.label for spec in project.additional_namespaces] == ["alpha", "beta"]
    assert project.namespace == "tmprl-dem-cld-scenario"
    assert [spec.namespace for spec in project.additional_namespaces] == [
        "tmprl-dem-cld-scenario-alpha",
        "tmprl-dem-cld-scenario-beta",
    ]


def test_scenario_partitions_components_across_namespaces():
    project = scenario_plan().temporal_project
    assert sorted(project.components) == ["unbound"]
    by_label = {spec.label: sorted(spec.components) for spec in project.additional_namespaces}
    assert by_label == {"alpha": ["alpha-worker"], "beta": ["beta-worker"]}


def test_scenario_gives_each_namespace_its_own_secret_paths():
    project = scenario_plan().temporal_project
    paths = {spec.label: spec.runtime_secret_path for spec in project.additional_namespaces}
    assert paths == {
        "alpha": "tmprl-dem-cld/scenario/temporal-alpha",
        "beta": "tmprl-dem-cld/scenario/temporal-beta",
    }
    # Distinct from the primary, so promotion cannot overwrite it.
    assert project.runtime_secret_path not in paths.values()


def test_scenario_carries_per_namespace_region_and_retention():
    specs = {spec.label: spec for spec in scenario_plan().temporal_project.additional_namespaces}
    assert specs["alpha"].region == "aws-us-west-2"
    assert specs["alpha"].retention_days is None
    assert specs["beta"].retention_days == 14
    # Never carried per namespace: the schema rejects the key outright.
    assert specs["beta"].search_attributes == ()
    assert specs["alpha"].search_attributes == ()


def test_task_queue_fairness_is_planned_per_namespace():
    from demo_registry_operator.activities.external_resources import planning
    from demo_registry_operator.workflows import demo_project as demo_project_workflow

    assert scenario_plan().temporal_project.task_queue_fairness is False

    scenario = copy.deepcopy(SCENARIO)
    scenario["spec"]["temporal"]["fairness"] = True
    scenario["spec"]["temporal"]["namespaces"]["beta"]["fairness"] = True
    plan = planning.plan_external_resources(
        scenario, checked_at="2026-06-20T00:00:00+00:00"
    )

    project = plan.temporal_project
    assert project.task_queue_fairness is True
    specs = {spec.label: spec for spec in project.additional_namespaces}
    assert specs["alpha"].task_queue_fairness is False
    assert specs["beta"].task_queue_fairness is True
    # Each target's projection asks for its own namespace's setting, not the
    # project's.
    projected = {
        target.label: target.external_plan(plan).temporal_project.task_queue_fairness
        for target in demo_project_workflow.temporal_namespace_targets(plan)
    }
    assert projected == {None: True, "alpha": False, "beta": True}


def test_scenario_namespaces_inherit_the_project_auth_mode():
    project = scenario_plan().temporal_project
    assert {spec.auth_mode for spec in project.additional_namespaces} == {
        project.auth_mode
    }


def test_scenario_plans_the_nexus_endpoint_with_labels():
    endpoints = scenario_plan().temporal_project.nexus_endpoints
    assert len(endpoints) == 1
    endpoint = endpoints[0]
    assert endpoint.name == "scenario-orders"
    assert endpoint.target_namespace == "beta"
    assert endpoint.target_task_queue == "orders-tq"
    assert endpoint.allowed_caller_namespaces == ("primary", "alpha")


def test_scenario_renders_a_secret_per_namespace_and_binds_components():
    rendered = {
        item["metadata"]["name"]: item for item in runtime_secret_resources(SCENARIO)
    }
    assert TEMPORAL_SECRET_NAME in rendered
    assert temporal_secret_name("alpha") in rendered
    assert temporal_secret_name("beta") in rendered

    # Each labelled ExternalSecret pulls from its own Secrets Manager path.
    assert {
        entry["remoteRef"]["key"]
        for entry in rendered[temporal_secret_name("beta")]["spec"]["data"]
    } == {"tmprl-dem-cld/scenario/temporal-beta"}

    components = SCENARIO["spec"]["components"]
    assert env_from_refs(SCENARIO, "unbound", components["unbound"]) == [
        {"secretRef": {"name": TEMPORAL_SECRET_NAME}}
    ]
    assert env_from_refs(SCENARIO, "beta-worker", components["beta-worker"]) == [
        {"secretRef": {"name": temporal_secret_name("beta")}}
    ]


def load_validator():
    """Load scripts/validate_projects.py, which is standalone and not importable."""
    import importlib.util
    import sys

    spec = importlib.util.spec_from_file_location(
        "validate_projects_scenario", ROOT / "scripts/validate_projects.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_scenario_passes_the_repository_validator():
    module = load_validator()
    document = module.ProjectDocument(path=ROOT / "projects/scenario.yaml", data=SCENARIO)
    assert module.validate_temporal_namespace_labels(document) == []
    assert module.validate_component_temporal_namespace_refs(document) == []
    assert module.validate_nexus_endpoints(document) == []


# --- what makes a namespace "required" --------------------------------------


def cloud_project(**overrides):
    from demo_registry_operator.activities.external_resources.models import (
        NexusEndpointSpec,
        TemporalCloudProject,
    )

    fields = {
        "name": "demo",
        "namespace": "tmprl-dem-cld-demo",
        "kubernetes_namespace": "tmprl-dem-cld-demo",
        "auth_mode": "apiKey",
        "components": {},
        "runtime_secret_path": "tmprl-dem-cld/demo/temporal",
        "mtls_ca_secret_path": "tmprl-dem-cld/demo/temporal/ca",
    }
    fields.update(overrides)
    return TemporalCloudProject(**fields), NexusEndpointSpec


def test_a_declared_namespace_is_required_even_with_no_components():
    # Its only reference may be a Nexus endpoint. Without this it is planned and
    # then never created, and never torn down.
    project, _ = cloud_project(target_namespace="tmprl-dem-cld-demo-lonely")
    assert project.temporal_required is True
    assert project.namespace_required is True


def test_the_project_namespace_is_still_not_required_when_unused():
    project, _ = cloud_project()
    assert project.temporal_required is False
    assert project.namespace_required is False


def test_the_project_namespace_is_required_when_an_endpoint_references_it():
    project, endpoint = cloud_project()
    as_target = project.__class__(
        **{**project.__dict__, "nexus_endpoints": (
            endpoint(name="e", target_namespace="primary", target_task_queue="tq"),
        )}
    )
    assert as_target.namespace_required is True

    as_caller = project.__class__(
        **{**project.__dict__, "nexus_endpoints": (
            endpoint(
                name="e",
                target_namespace="alpha",
                target_task_queue="tq",
                allowed_caller_namespaces=("primary",),
            ),
        )}
    )
    assert as_caller.namespace_required is True


def test_an_endpoint_between_other_namespaces_does_not_require_the_project_one():
    project, endpoint = cloud_project()
    unrelated = project.__class__(
        **{**project.__dict__, "nexus_endpoints": (
            endpoint(
                name="e",
                target_namespace="beta",
                target_task_queue="tq",
                allowed_caller_namespaces=("alpha",),
            ),
        )}
    )
    assert unrelated.namespace_required is False


def test_declaring_search_attributes_on_a_namespace_is_rejected():
    # Accepted-and-ignored was the earlier behavior and the worst option: an
    # author got no error and no attributes.
    module = load_validator()
    document = module.ProjectDocument(
        path=ROOT / "projects/scenario.yaml",
        data={
            "kind": "DemoProject",
            "metadata": {"name": "scenario"},
            "spec": {
                "temporal": {
                    "namespaces": {"alpha": {"searchAttributes": {"Tier": {}}}}
                }
            },
        },
    )
    assert [error.message for error in
            module.validate_temporal_namespace_labels(document)] == [
        "temporal.namespaces.alpha.searchAttributes is not supported; search "
        "attributes are managed on the project's own namespace only, via "
        "spec.temporal.searchAttributes"
    ]
