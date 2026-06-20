from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class DemoProjectRunRequest:
    project_name: str
    carried_status: dict[str, Any] | None = None
    carried_pending_spec: dict[str, Any] | None = None
    carried_last_build_status: dict[str, Any] | None = None
    carried_completed_reconcile_identity: dict[str, str] | None = None
    carried_last_demo_project: dict[str, Any] | None = None
    carried_temporal_api_key_rotation: dict[str, Any] | None = None
    carried_temporal_runtime_secret_suffix: str | None = None
    carried_pending_force_reconcile: dict[str, str] | None = None
    carried_completed_force_reconcile_token: str | None = None


@dataclass(frozen=True)
class DemoProjectReconcileSignal:
    demoProject: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DemoProjectFinalizeSignal:
    demoProject: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DemoProjectArtifactBuildRequest:
    demoProject: dict[str, Any]
    externalResources: dict[str, Any]
    previousBuildStatus: dict[str, Any] | None = None


@dataclass(frozen=True)
class DemoProjectArtifactBuildResult:
    status: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DemoProjectKubernetesRuntimeRequest:
    project: str
    demoProject: dict[str, Any]
    externalResources: dict[str, Any]
    buildStatus: dict[str, Any]
    previousStatus: dict[str, Any] | None = None


@dataclass(frozen=True)
class DemoProjectKubernetesRuntimeResult:
    status: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class DemoProjectServerlessLambdaRequest:
    project: str
    demoProject: dict[str, Any]
    externalResources: dict[str, Any]
    buildStatus: dict[str, Any]


@dataclass(frozen=True)
class DemoProjectServerlessLambdaResult:
    status: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class InstruqtCourseRunRequest:
    course_name: str
    carried_status: dict[str, Any] | None = None
    carried_pending_spec: dict[str, Any] | None = None
    carried_last_build_status: dict[str, Any] | None = None
    carried_last_testing_build_status: dict[str, Any] | None = None
    carried_completed_reconcile_identity: dict[str, str] | None = None
    carried_pending_force_reconcile: dict[str, str] | None = None
    carried_completed_force_reconcile_token: str | None = None


@dataclass(frozen=True)
class InstruqtCourseReconcileSignal:
    instruqtCourse: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class InstruqtCourseFinalizeSignal:
    instruqtCourse: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ForceReconcileSignal:
    token: str
    reason: str | None = None


@dataclass(frozen=True)
class CheckWorkerVersionSignal:
    reason: str = "worker-version-update"


@dataclass(frozen=True)
class ShutdownSignal:
    reason: str | None = None


@dataclass(frozen=True)
class DemoRolloutRequest:
    project: str = "unknown"
    sourceCommitSha: str | None = None
    demoProject: dict[str, Any] | None = None
    host: str | None = None
    candidate: dict[str, Any] = field(default_factory=dict)
    smokeCheck: dict[str, Any] | None = None


@dataclass(frozen=True)
class DemoRolloutResult:
    project: str | None = None
    phase: str = "Unknown"
    status: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RegistryOperatorCanaryRequest:
    projectNamePrefix: str = "operator-canary"
    projectName: str | None = None
    demoProjectTemplate: dict[str, Any] | None = None
    demoProject: dict[str, Any] | None = None
    expectedSourceCommitSha: str | None = None
    expectedImages: dict[str, Any] | None = None
    authMode: str | None = None
    expectedOperatorImage: str | None = None
    expectedOperatorSourceSha: str | None = None
    timeoutSeconds: int = 900
    pollSeconds: int = 10
    cleanupTimeoutSeconds: int = 600
    cleanupPolicy: str = "Always"


@dataclass(frozen=True)
class RegistryOperatorAuthCanaryRequest:
    workflowID: str
    runID: str
    authMode: str
    projectNamePrefix: str = "operator-canary"
    projectName: str | None = None
    demoProjectTemplate: dict[str, Any] | None = None
    demoProject: dict[str, Any] | None = None
    expectedSourceCommitSha: str | None = None
    expectedImages: dict[str, Any] | None = None
    expectedOperatorImage: str | None = None
    expectedOperatorSourceSha: str | None = None
    timeoutSeconds: int = 900
    pollSeconds: int = 10
    cleanupTimeoutSeconds: int = 600
    cleanupPolicy: str = "Always"


@dataclass(frozen=True)
class RegistryOperatorAuthCanaryResult:
    project: str | None = None
    authMode: str = ""
    phase: str = "Unknown"
    plan: dict[str, Any] = field(default_factory=dict)
    apply: dict[str, Any] = field(default_factory=dict)
    workflow: dict[str, Any] = field(default_factory=dict)
    promotion: dict[str, Any] = field(default_factory=dict)
    rotationSignal: dict[str, Any] = field(default_factory=dict)
    rotation: dict[str, Any] = field(default_factory=dict)
    finalizeSignal: dict[str, Any] = field(default_factory=dict)
    cleanup: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RegistryOperatorCanaryResult:
    phase: str = "Unknown"
    canaries: dict[str, RegistryOperatorAuthCanaryResult] = field(default_factory=dict)


@dataclass(frozen=True)
class TemporalRuntimeCredentialCanaryRequest:
    nonce: str
    authMode: str


@dataclass(frozen=True)
class TemporalRuntimeCredentialCanaryResult:
    ok: bool
    nonce: str
    checkedAt: str
