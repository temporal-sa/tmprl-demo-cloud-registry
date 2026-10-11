"""Payloads for the isolated Lambda workspace workflow.

The lifecycle request matches the crashable workspace shape so continue-as-new
and the catalog client stay small. Lambda records travel with that request:
after continue-as-new the next run does not re-provision, and cleanup must
still know which functions and Worker Deployments it created.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from demo_registry_operator.activities.external_resources.models import (
    LambdaEnvironmentVariable,
    LambdaSecretEnvironmentVariable,
)


@dataclass(frozen=True)
class WorkspaceLambdaWorker:
    component: str
    function_name: str
    function_arn: str
    function_version_arn: str
    deployment_name: str
    build_id: str
    invoke_role_name: str
    temporal_secret_path: str


@dataclass(frozen=True)
class EnsureIsolatedWorkspaceRequest:
    demo_name: str
    owner_email: str
    ttl_seconds: int = 3600
    carried_already_provisioned: bool = False
    carried_expires_at: str | None = None
    carried_created_at: str | None = None
    carried_task_queue: str | None = None
    carried_workspace_id: str | None = None
    carried_lambda_workers: tuple[WorkspaceLambdaWorker, ...] = ()
    carried_initial_current: dict[str, str] = field(default_factory=dict)


@dataclass
class IsolatedWorkspaceStatus:
    phase: str
    step: str
    demo: str
    workspace_id: str
    namespace: str
    host: str
    app_url: str
    task_queue: str
    mode: str = "isolated-lambda"
    created_at: str | None = None
    expires_at: str | None = None
    last_error: str | None = None
    failure: dict[str, Any] = field(default_factory=dict)
    compensation_status: dict[str, Any] = field(default_factory=dict)
    lambda_workers: list[dict[str, Any]] = field(default_factory=list)
    initial_current: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class BuildIsolatedWorkspacePlanRequest:
    demo_name: str
    owner_email: str
    workspace_id: str
    images: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class IsolatedLambdaComponentPlan:
    project: str
    component: str
    function_name: str
    deployment_name: str
    build_id: str
    image_uri: str
    architecture: str
    memory_mb: int
    timeout_seconds: int
    execution_role_name: str
    invoke_role_name: str
    external_id: str
    env: dict[str, str]
    temporal_secret_path: str
    temporal_auth_mode: str
    secret_env: tuple[LambdaSecretEnvironmentVariable, ...] = ()
    mtls_file_path_env: tuple[LambdaEnvironmentVariable, ...] = ()
    worker_deployment_env: tuple[LambdaEnvironmentVariable, ...] = ()
    set_current: bool = False


@dataclass(frozen=True)
class IsolatedWorkspacePlan:
    resources: list[dict[str, Any]] = field(default_factory=list)
    task_queue: str = ""
    host: str = ""
    namespace: str = ""
    deployment_names: list[str] = field(default_factory=list)
    components: tuple[IsolatedLambdaComponentPlan, ...] = ()
    initial_current: dict[str, str] = field(default_factory=dict)
    initial_current_defaulted: tuple[str, ...] = ()


@dataclass(frozen=True)
class ResolveSharedLambdaRolesRequest:
    execution_role_name: str
    invoke_role_name: str


@dataclass(frozen=True)
class ResolveSharedLambdaRolesResult:
    execution_role_arn: str
    invoke_role_arn: str


@dataclass(frozen=True)
class UpdateWorkspaceLambdaInvokePolicyRequest:
    role_name: str
    function_arn: str
    function_version_arn: str
    remove: bool = False


@dataclass(frozen=True)
class UpdateWorkspaceLambdaInvokePolicyResult:
    action: str


@dataclass(frozen=True)
class DeleteWorkspaceLambdaFunctionsRequest:
    function_names: tuple[str, ...]


@dataclass(frozen=True)
class DeleteWorkspaceLambdaFunctionsResult:
    functions: tuple[dict[str, str], ...] = ()
