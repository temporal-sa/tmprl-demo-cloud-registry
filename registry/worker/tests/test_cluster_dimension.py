"""P5 cluster/color dimension.

DEMO_REGISTRY_CLUSTER scopes the task queue, entity workflow ids, the
worker-update batch-signal query, the generated Traefik CRD group, and the
external-dns target annotation so blue/green can share one Temporal namespace and
one Route53 zone without cross-routing. The load-bearing invariant is that every
one of those is a BYTE-IDENTICAL no-op when the var is unset — that is what keeps
the hand-built sa-demo pet (which sets no color) working unchanged.
"""

from types import SimpleNamespace

from demo_registry_operator import derive, resources, worker
from demo_registry_operator.config import WorkerConfig

# banking_project renders a full, valid IngressRoute pair; reuse it rather than
# hand-building a fixture whose port schema must line up exactly.
from test_promotion import banking_project


# --- config: task queue derivation -----------------------------------------

def test_task_queue_uncolored_is_unchanged(monkeypatch):
    monkeypatch.delenv("DEMO_REGISTRY_CLUSTER", raising=False)
    monkeypatch.delenv("DEMO_REGISTRY_TASK_QUEUE", raising=False)
    config = WorkerConfig.from_env()
    assert config.cluster is None
    assert config.task_queue == "registry-management"


def test_task_queue_colored_gets_suffix(monkeypatch):
    monkeypatch.setenv("DEMO_REGISTRY_CLUSTER", "blue")
    monkeypatch.delenv("DEMO_REGISTRY_TASK_QUEUE", raising=False)
    config = WorkerConfig.from_env()
    assert config.cluster == "blue"
    assert config.task_queue == "registry-management-blue"


def test_explicit_task_queue_overrides_color(monkeypatch):
    # An explicitly set queue wins verbatim, matching pre-P5 os.getenv semantics.
    monkeypatch.setenv("DEMO_REGISTRY_CLUSTER", "blue")
    monkeypatch.setenv("DEMO_REGISTRY_TASK_QUEUE", "custom-queue")
    config = WorkerConfig.from_env()
    assert config.task_queue == "custom-queue"


def test_blank_cluster_is_treated_as_unset(monkeypatch):
    monkeypatch.setenv("DEMO_REGISTRY_CLUSTER", "   ")
    monkeypatch.delenv("DEMO_REGISTRY_TASK_QUEUE", raising=False)
    config = WorkerConfig.from_env()
    assert config.cluster is None
    assert config.task_queue == "registry-management"


def test_registry_task_queue_derive():
    # Single source shared by config.from_env and resources.platform_env_vars.
    assert derive.registry_task_queue(None) == "registry-management"
    assert derive.registry_task_queue("blue") == "registry-management-blue"
    assert derive.registry_task_queue("green") == "registry-management-green"


# A minimal registry catalog self-demo (the annotation is what gates the
# operator/web self-management env injection in platform_env_vars).
_CATALOG_PROJECT = {
    "apiVersion": "registry.tmprl-demo.cloud/v1alpha1",
    "kind": "DemoProject",
    "metadata": {
        "name": "registry-operator",
        "annotations": {"registry.tmprl-demo.cloud/catalog": "true"},
    },
    "spec": {"components": {"registry-operator": {}, "web": {}}},
}


def test_self_rendered_operator_gets_color_not_explicit_queue(monkeypatch):
    # GAP-1: the self-rendered operator worker gets DEMO_REGISTRY_CLUSTER (so its
    # own config.from_env derives registry-management-<color>) and must NOT carry an
    # explicit DEMO_REGISTRY_TASK_QUEUE (which would win over the color derivation).
    monkeypatch.setenv("DEMO_REGISTRY_CLUSTER", "blue")
    env = resources.platform_env_vars(_CATALOG_PROJECT, "registry-operator", "img", {})
    assert {"name": "DEMO_REGISTRY_CLUSTER", "value": "blue"} in env
    assert not any(e["name"] == "DEMO_REGISTRY_TASK_QUEUE" for e in env)


def test_self_rendered_web_gets_color_derived_queue(monkeypatch):
    # GAP-1: the catalog web reads DEMO_REGISTRY_TASK_QUEUE directly (it does not run
    # config.from_env), so the operator injects the color-derived queue so
    # CrashableWorkspaceWorkflow starts land where this color's operator polls.
    monkeypatch.setenv("DEMO_REGISTRY_CLUSTER", "blue")
    env = resources.platform_env_vars(_CATALOG_PROJECT, "web", "img", {})
    assert {
        "name": "DEMO_REGISTRY_TASK_QUEUE",
        "value": "registry-management-blue",
    } in env


def test_self_rendered_web_uncolored_queue(monkeypatch):
    monkeypatch.delenv("DEMO_REGISTRY_CLUSTER", raising=False)
    env = resources.platform_env_vars(_CATALOG_PROJECT, "web", "img", {})
    assert {"name": "DEMO_REGISTRY_TASK_QUEUE", "value": "registry-management"} in env


# --- derive: entity workflow ids (single source of truth) -------------------

def test_workflow_ids_uncolored_are_unchanged():
    assert derive.demo_project_workflow_id("banking") == "demo-project/banking"
    assert derive.instruqt_course_workflow_id("fundamentals") == (
        "instruqt-course/fundamentals"
    )


def test_workflow_ids_colored_are_scoped():
    assert derive.demo_project_workflow_id("banking", "blue") == (
        "demo-project/blue/banking"
    )
    assert derive.instruqt_course_workflow_id("fundamentals", "green") == (
        "instruqt-course/green/fundamentals"
    )


def test_live_swing_host_is_derived_from_base_domain():
    assert derive.LIVE_SWING_HOST == "live.tmprl-demo.cloud"


# --- worker: namespace-wide batch-signal query ------------------------------

def test_worker_update_query_uncolored_is_unscoped():
    # The batch signal must NOT gain an id scope when uncolored, or the pet's
    # query would change. Byte-identical to the pinned form in test_worker.py.
    assert worker.running_entity_workflows_query() == (
        '(WorkflowType = "DemoProjectWorkflow" '
        'OR WorkflowType = "InstruqtCourseWorkflow") '
        'AND ExecutionStatus = "Running"'
    )


def test_worker_update_query_colored_scopes_to_this_color():
    query = worker.running_entity_workflows_query("blue")
    # Still matches only this color's entity ids, so a blue worker-update handoff
    # cannot batch-signal the pet's (or green's) running workflows.
    assert query.endswith(
        ' AND (WorkflowId STARTS_WITH "demo-project/blue/" '
        'OR WorkflowId STARTS_WITH "instruqt-course/blue/")'
    )
    assert query.startswith(worker.running_entity_workflows_query())


# --- resources: Traefik CRD group + external-dns target ---------------------

def test_operator_cluster_reads_env(monkeypatch):
    monkeypatch.delenv("DEMO_REGISTRY_CLUSTER", raising=False)
    assert resources.operator_cluster() is None
    monkeypatch.setenv("DEMO_REGISTRY_CLUSTER", "blue")
    assert resources.operator_cluster() == "blue"
    monkeypatch.setenv("DEMO_REGISTRY_CLUSTER", "  ")
    assert resources.operator_cluster() is None


def test_traefik_api_version_follows_color(monkeypatch):
    monkeypatch.delenv("DEMO_REGISTRY_CLUSTER", raising=False)
    assert resources.traefik_api_version() == "traefik.containo.us/v1alpha1"
    monkeypatch.setenv("DEMO_REGISTRY_CLUSTER", "blue")
    assert resources.traefik_api_version() == "traefik.io/v1alpha1"


def test_ingress_uncolored_is_byte_identical(monkeypatch):
    monkeypatch.delenv("DEMO_REGISTRY_CLUSTER", raising=False)
    routes = resources.ingress_resources(banking_project())
    assert routes, "expected the banking project to render IngressRoutes"
    for route in routes:
        assert route["apiVersion"] == "traefik.containo.us/v1alpha1"
        assert (
            "external-dns.alpha.kubernetes.io/target"
            not in route["metadata"]["annotations"]
        )


def test_ingress_colored_emits_v3_and_external_dns_target(monkeypatch):
    monkeypatch.setenv("DEMO_REGISTRY_CLUSTER", "blue")
    routes = resources.ingress_resources(banking_project())
    assert routes
    # Both the websecure base and the web/http-redirect variant carry the target.
    assert len(routes) == 2
    for route in routes:
        assert route["apiVersion"] == "traefik.io/v1alpha1"
        assert route["metadata"]["annotations"][
            "external-dns.alpha.kubernetes.io/target"
        ] == "live.tmprl-demo.cloud"


def _websecure_route(routes):
    return next(r for r in routes if r["spec"]["entryPoints"] == ["websecure"])


def test_tls_uncolored_uses_per_host_cert(monkeypatch):
    # The pet keeps its per-host Let's Encrypt cert: the websecure route names a
    # secret and promotion_resources emits a matching Certificate.
    monkeypatch.delenv("DEMO_REGISTRY_CLUSTER", raising=False)
    base = _websecure_route(resources.ingress_resources(banking_project()))
    assert base["spec"]["tls"] == {"secretName": "banking-money-transfer-tls"}
    rendered = resources.promotion_resources(banking_project())
    assert any(r["kind"] == "Certificate" for r in rendered)


def test_tls_colored_uses_wildcard_default_store(monkeypatch):
    # Colored clusters serve the wildcard cert via Traefik's default TLSStore:
    # the websecure route has an EMPTY tls block and NO per-host Certificate is
    # minted (so a new per-rollout namespace never triggers a fresh LE issuance).
    monkeypatch.setenv("DEMO_REGISTRY_CLUSTER", "blue")
    base = _websecure_route(resources.ingress_resources(banking_project()))
    assert base["spec"]["tls"] == {}
    rendered = resources.promotion_resources(banking_project())
    assert not any(r["kind"] == "Certificate" for r in rendered)


# --- self-canary color propagation ------------------------------------------

def test_self_canary_inherits_color_into_rendered_worker(monkeypatch):
    # The per-cluster configMap only feeds the bootstrap/watch pod; the reconciler
    # is a self-rendered WorkerDeployment whose env comes from the catalog spec.
    # platform_env_vars must inject the color so that worker polls the color queue.
    catalog_project = {
        "metadata": {
            "name": "registry",
            "annotations": {"registry.tmprl-demo.cloud/catalog": "true"},
        },
        "spec": {},
    }
    monkeypatch.delenv("DEMO_REGISTRY_CLUSTER", raising=False)
    uncolored = resources.platform_env_vars(
        catalog_project, "registry-operator", "img:tag", {}
    )
    assert not any(e["name"] == "DEMO_REGISTRY_CLUSTER" for e in uncolored)

    monkeypatch.setenv("DEMO_REGISTRY_CLUSTER", "blue")
    colored = resources.platform_env_vars(
        catalog_project, "registry-operator", "img:tag", {}
    )
    assert {"name": "DEMO_REGISTRY_CLUSTER", "value": "blue"} in colored
