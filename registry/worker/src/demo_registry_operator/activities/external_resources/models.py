from dataclasses import dataclass
from typing import Any


TEMPORAL_BOOTSTRAP_SECRET_PROPERTIES = (
    "TEMPORAL_CLOUD_API_KEY",
    "TEMPORAL_CLOUD_ACCOUNT_ID",
    "TEMPORAL_CLOUD_REGION",
)


@dataclass(frozen=True)
class TemporalCloudSearchAttribute:
    name: str
    type: str = "keyword"


@dataclass(frozen=True)
class TemporalNamespaceSpec:
    """One additional namespace declared under spec.temporal.namespaces.

    ``auth_mode`` is inherited from the project rather than declared per
    namespace, so it is populated by the planner, not read from the label's spec.
    """

    label: str
    namespace: str
    auth_mode: str
    runtime_secret_path: str
    mtls_ca_secret_path: str
    components: dict[str, dict[str, Any]]
    search_attributes: tuple[TemporalCloudSearchAttribute, ...] = ()
    region: str | None = None
    # None leaves the namespace-creation request's own default in place, which is
    # what the project's original namespace uses.
    retention_days: int | None = None


@dataclass(frozen=True)
class NexusEndpointSpec:
    """One entry from spec.temporal.nexusEndpoints.

    Namespaces are held as the labels the project author wrote, with "primary"
    meaning the project's original namespace. Resolving a label to a derived
    namespace name -- and then to the qualified ``<name>.<account-id>`` id the
    Cloud API keys off -- happens in the activity, which is where every other
    namespace name is resolved too.
    """

    name: str
    target_namespace: str
    target_task_queue: str
    allowed_caller_namespaces: tuple[str, ...] = ()


@dataclass(frozen=True)
class TemporalCloudProject:
    name: str
    namespace: str
    kubernetes_namespace: str
    auth_mode: str
    components: dict[str, dict[str, Any]]
    runtime_secret_path: str
    mtls_ca_secret_path: str
    # Kept only for payload compatibility with rollout builds that briefly
    # carried Kubernetes object UID through this model. Runtime secret identity
    # is workflow-owned via runtime_secret_path, not derived from this field.
    uid: str | None = None
    search_attributes: tuple[TemporalCloudSearchAttribute, ...] = ()
    # Explicit spec.temporal.region (None when omitted; the operator resolves the
    # default). ``serverless`` is True when the project has AWS Lambda workers,
    # which selects the AWS region default when no explicit region is set.
    region: str | None = None
    serverless: bool = False
    # Set only on the per-target projection the saga hands each activity, so that
    # activity resolves a namespace other than the one derived from ``name``. None
    # on the project-level model, which is the only shape that exists until a
    # project declares spec.temporal.namespaces.
    target_namespace: str | None = None
    additional_namespaces: tuple[TemporalNamespaceSpec, ...] = ()
    nexus_endpoints: tuple[NexusEndpointSpec, ...] = ()

    @property
    def component_names(self) -> tuple[str, ...]:
        return tuple(sorted(self.components))

    @property
    def temporal_required(self) -> bool:
        # A projection for a declared namespace is required because it was
        # declared, even with no component bound to it -- a Nexus endpoint may be
        # its only reference. target_namespace is None on the project-level model
        # and on the primary target, so this is unchanged for them.
        return bool(self.components) or self.target_namespace is not None

    @property
    def namespace_required(self) -> bool:
        if self.temporal_required or self.search_attributes:
            return True
        # A Nexus endpoint that targets this namespace, or admits it as a caller,
        # needs it to exist -- even when every component is bound elsewhere.
        # "primary" is the reference a project writes for its own namespace; it
        # matches NEXUS_PRIMARY_NAMESPACE_REF on the reconciler side.
        return any(
            endpoint.target_namespace == "primary"
            or "primary" in endpoint.allowed_caller_namespaces
            for endpoint in self.nexus_endpoints
        )

    @property
    def nexus_endpoints_required(self) -> bool:
        return bool(self.nexus_endpoints)

    def as_demo_project(self) -> dict[str, Any]:
        temporal: dict[str, Any] = {
            "auth": {"mode": self.auth_mode},
            "runtimeSecretPath": self.runtime_secret_path,
        }
        if self.region:
            temporal["region"] = self.region
        if self.search_attributes:
            temporal["searchAttributes"] = {
                attribute.name: {"type": attribute.type}
                for attribute in self.search_attributes
            }
        # Emitted only when set, so the dict for a project with no additional
        # namespaces stays byte-identical to what activities have always received.
        if self.target_namespace:
            temporal["namespaceOverride"] = self.target_namespace
        # additional_namespaces and nexus_endpoints are deliberately not emitted:
        # they are planning inputs the workflow and the Nexus activity read off
        # this model directly, and emitting them would change the dict every
        # existing activity receives.
        return {
            "metadata": {"name": self.name},
            "spec": {
                "components": self.components,
                "temporal": temporal,
            },
        }


@dataclass(frozen=True)
class EcrRepositoryRequest:
    name: str
    project: str
    build: str
    components: tuple[str, ...]


@dataclass(frozen=True)
class EcrRepositoryResult:
    name: str
    project: str
    build: str
    components: tuple[str, ...]
    phase: str
    action: str
    region: str
    account_id: str | None
    repository_uri: str | None = None

    def as_status(self) -> dict[str, Any]:
        status: dict[str, Any] = {
            "name": self.name,
            "project": self.project,
            "build": self.build,
            "components": list(self.components),
            "phase": self.phase,
            "action": self.action,
            "region": self.region,
            "accountId": self.account_id,
        }
        if self.repository_uri:
            status["repositoryUri"] = self.repository_uri
        return status


@dataclass(frozen=True)
class DeleteEcrRepositoriesRequest:
    checked_at: str
    repositories: tuple[EcrRepositoryRequest, ...]


@dataclass(frozen=True)
class DeleteEcrRepositoriesResult:
    status: dict[str, Any]


@dataclass(frozen=True)
class EnsureLambdaExecutionRoleRequest:
    project: str
    component: str
    role_name: str
    secret_paths: tuple[str, ...] = ()


@dataclass(frozen=True)
class LambdaSecretEnvironmentVariable:
    name: str
    secret_path: str
    property_name: str


@dataclass(frozen=True)
class LambdaEnvironmentVariable:
    name: str
    value: str


@dataclass(frozen=True)
class EnsureLambdaExecutionRoleResult:
    status: dict[str, Any]
    role_name: str
    role_arn: str


@dataclass(frozen=True)
class EnsureLambdaFunctionRequest:
    project: str
    component: str
    function_name: str
    execution_role_arn: str
    image_uri: str
    architecture: str
    memory_mb: int
    timeout_seconds: int
    deployment_name: str
    build_id: str
    temporal_secret_path: str
    env: dict[str, str]
    temporal_auth_mode: str = "apiKey"
    secret_env: tuple[LambdaSecretEnvironmentVariable, ...] = ()
    mtls_file_path_env: tuple[LambdaEnvironmentVariable, ...] = ()
    worker_deployment_env: tuple[LambdaEnvironmentVariable, ...] = ()


@dataclass(frozen=True)
class EnsureLambdaFunctionResult:
    status: dict[str, Any]
    function_name: str
    function_arn: str
    function_version_arn: str
    version: str


@dataclass(frozen=True)
class EnsureTemporalLambdaInvokeRoleRequest:
    project: str
    component: str
    role_name: str
    function_arn: str
    function_version_arn: str
    external_id: str


@dataclass(frozen=True)
class EnsureTemporalLambdaInvokeRoleResult:
    status: dict[str, Any]
    role_name: str
    role_arn: str
    external_id: str


@dataclass(frozen=True)
class EnsureServerlessWorkerDeploymentRequest:
    project: str
    component: str
    temporal_secret_path: str
    deployment_name: str


@dataclass(frozen=True)
class EnsureServerlessWorkerDeploymentResult:
    status: dict[str, Any]


@dataclass(frozen=True)
class UpsertServerlessWorkerDeploymentVersionRequest:
    project: str
    component: str
    temporal_secret_path: str
    deployment_name: str
    build_id: str
    function_version_arn: str
    invoke_role_arn: str
    invoke_role_external_id: str


@dataclass(frozen=True)
class UpsertServerlessWorkerDeploymentVersionResult:
    status: dict[str, Any]


@dataclass(frozen=True)
class ValidateServerlessWorkerComputeConfigRequest:
    project: str
    component: str
    temporal_secret_path: str
    deployment_name: str
    build_id: str
    function_version_arn: str
    invoke_role_arn: str
    invoke_role_external_id: str


@dataclass(frozen=True)
class ValidateServerlessWorkerComputeConfigResult:
    status: dict[str, Any]


@dataclass(frozen=True)
class SetServerlessWorkerDeploymentCurrentRequest:
    project: str
    component: str
    temporal_secret_path: str
    deployment_name: str
    build_id: str


@dataclass(frozen=True)
class SetServerlessWorkerDeploymentCurrentResult:
    status: dict[str, Any]


@dataclass(frozen=True)
class DescribeServerlessWorkerDeploymentVersionRequest:
    project: str
    component: str
    temporal_secret_path: str
    deployment_name: str
    build_id: str


@dataclass(frozen=True)
class DescribeServerlessWorkerDeploymentVersionResult:
    status: dict[str, Any]


@dataclass(frozen=True)
class DeleteServerlessLambdaResourcesRequest:
    checked_at: str
    demo_project: dict[str, Any]


@dataclass(frozen=True)
class DeleteServerlessLambdaResourcesResult:
    status: dict[str, Any]


@dataclass(frozen=True)
class SecretRequirement:
    path: str
    required: bool
    managed_by: str
    required_properties: tuple[str, ...] = ()
    components: tuple[str, ...] = ()


@dataclass(frozen=True)
class VerifySecretsManagerResourcesRequest:
    checked_at: str
    secret_prefix: str
    temporal_bootstrap: SecretRequirement
    temporal_credentials: SecretRequirement
    temporal_mtls_ca: SecretRequirement
    user_managed: tuple[SecretRequirement, ...]


@dataclass(frozen=True)
class SecretsManagerVerificationResult:
    status: dict[str, Any]


@dataclass(frozen=True)
class ExternalResourcesPlan:
    demo_project: dict[str, Any]
    checked_at: str
    mode: str
    kubernetes_namespace: str
    force_temporal_auth_rotation: bool
    ecr_repositories: tuple[EcrRepositoryRequest, ...]
    temporal_project: TemporalCloudProject
    secrets_manager: VerifySecretsManagerResourcesRequest


@dataclass(frozen=True)
class TemporalCloudStatusResult:
    status: dict[str, Any]

    @property
    def phase(self) -> str | None:
        phase = self.status.get("phase")
        return phase if isinstance(phase, str) else None


@dataclass(frozen=True)
class TemporalCloudNamespaceResult:
    status: dict[str, Any]
    namespace: str | None
    grpc_address: str | None
    auth_mode: str | None
    created: bool
    region: str | None = None

    @property
    def phase(self) -> str | None:
        phase = self.status.get("phase")
        return phase if isinstance(phase, str) else None


@dataclass(frozen=True)
class TemporalCloudServiceAccountResult:
    status: dict[str, Any]
    service_account_id: str | None
    service_account_name: str | None
    created: bool

    @property
    def phase(self) -> str | None:
        phase = self.status.get("phase")
        return phase if isinstance(phase, str) else None


@dataclass(frozen=True)
class TemporalCloudSearchAttributeResult:
    status: dict[str, Any]
    name: str | None = None
    type: str | None = None
    action: str | None = None
    managed: bool = False

    @property
    def phase(self) -> str | None:
        phase = self.status.get("phase")
        return phase if isinstance(phase, str) else None


@dataclass(frozen=True)
class TemporalCloudCredentialStageResult:
    status: dict[str, Any]
    promote_required: bool
    candidate_api_key_id: str | None = None
    candidate_secret_path: str | None = None
    candidate_ca_secret_path: str | None = None
    candidate_secret_token: str | None = None

    @property
    def phase(self) -> str | None:
        phase = self.status.get("phase")
        return phase if isinstance(phase, str) else None


@dataclass(frozen=True)
class TemporalRuntimeSecretCheckResult:
    status: dict[str, Any]
    runtime_secret_path: str
    state: str
    exists: bool
    rotate: bool
    rotation_reason: str | None = None
    key_id: str | None = None
    expiry_time: str | None = None
    created_at: str | None = None


@dataclass(frozen=True)
class TemporalCloudPromotionResult:
    status: dict[str, Any]
    promoted: bool = False
    ca_promoted: bool = False

    @property
    def phase(self) -> str | None:
        phase = self.status.get("phase")
        return phase if isinstance(phase, str) else None


@dataclass(frozen=True)
class StageTemporalMtlsCaRequest:
    checked_at: str
    project: TemporalCloudProject
    candidate_secret_token: str | None = None


@dataclass(frozen=True)
class CreateOrVerifyTemporalCloudNamespaceRequest:
    checked_at: str
    project: TemporalCloudProject
    account_id: str | None = None
    region: str | None = None
    retention_days: int = 7
    enable_delete_protection: bool = True
    candidate_ca_secret_path: str | None = None
    candidate_secret_token: str | None = None


@dataclass(frozen=True)
class EnsureTemporalCloudNamespaceTagsRequest:
    checked_at: str
    project: TemporalCloudProject
    namespace: TemporalCloudNamespaceResult
    # (key, value) pairs rather than a dict so the payload stays an immutable,
    # order-stable tuple like the sibling search_attributes field.
    tags: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class TemporalCloudNamespaceTagsResult:
    status: dict[str, Any]

    @property
    def phase(self) -> str | None:
        phase = self.status.get("phase")
        return phase if isinstance(phase, str) else None


@dataclass(frozen=True)
class ConfigureTemporalCloudNamespaceMtlsRequest:
    checked_at: str
    project: TemporalCloudProject
    namespace: TemporalCloudNamespaceResult
    candidate_ca_secret_path: str | None = None
    candidate_secret_token: str | None = None


@dataclass(frozen=True)
class CreateOrVerifyTemporalCloudServiceAccountRequest:
    checked_at: str
    project: TemporalCloudProject
    namespace: TemporalCloudNamespaceResult


@dataclass(frozen=True)
class CreateOrVerifyTemporalCloudSearchAttributeRequest:
    checked_at: str
    project: TemporalCloudProject
    namespace: TemporalCloudNamespaceResult
    search_attribute: TemporalCloudSearchAttribute


@dataclass(frozen=True)
class DeleteTemporalCloudSearchAttributeRequest:
    checked_at: str
    project: TemporalCloudProject
    namespace: TemporalCloudNamespaceResult
    search_attribute: TemporalCloudSearchAttribute


@dataclass(frozen=True)
class ReconcileTemporalCloudSearchAttributesRequest:
    # Every search attribute mutates the *same* namespace spec, so the whole set
    # (creates/verifies + deletions) is reconciled by one activity in a single
    # namespace update. Fanning these out per-attribute causes optimistic
    # concurrency (resource_version) conflicts on the shared spec.
    checked_at: str
    project: TemporalCloudProject
    namespace: TemporalCloudNamespaceResult
    desired: tuple[TemporalCloudSearchAttribute, ...] = ()
    delete_names: tuple[str, ...] = ()


@dataclass(frozen=True)
class TemporalCloudSearchAttributesResult:
    # One status dict per attribute (same shape as the singular
    # TemporalCloudSearchAttributeResult.status), so the workflow can report
    # every attribute even though a single Cloud operation applied them all.
    statuses: list[dict[str, Any]]


@dataclass(frozen=True)
class StageTemporalApiKeyCandidateRequest:
    checked_at: str
    project: TemporalCloudProject
    namespace: TemporalCloudNamespaceResult
    service_account: TemporalCloudServiceAccountResult
    force_rotation: bool = False
    candidate_secret_token: str | None = None


@dataclass(frozen=True)
class CheckTemporalRuntimeSecretRequest:
    checked_at: str
    project: TemporalCloudProject
    namespace: TemporalCloudNamespaceResult
    force_rotation: bool = False


@dataclass(frozen=True)
class CreateTemporalApiKeyCandidateSecretRequest:
    checked_at: str
    project: TemporalCloudProject
    namespace: TemporalCloudNamespaceResult
    service_account: TemporalCloudServiceAccountResult
    runtime_secret: TemporalRuntimeSecretCheckResult
    candidate_secret_token: str | None = None


@dataclass(frozen=True)
class StageTemporalMtlsClientCertificateRequest:
    checked_at: str
    project: TemporalCloudProject
    namespace: TemporalCloudNamespaceResult
    ca: TemporalCloudCredentialStageResult
    force_rotation: bool = False
    candidate_secret_token: str | None = None


@dataclass(frozen=True)
class RunTemporalRuntimeCandidateCanaryRequest:
    checked_at: str
    project: TemporalCloudProject
    stage: TemporalCloudCredentialStageResult


@dataclass(frozen=True)
class PromoteTemporalRuntimeCredentialRequest:
    checked_at: str
    project: TemporalCloudProject
    stage: TemporalCloudCredentialStageResult
    canary: TemporalCloudStatusResult | None = None


@dataclass(frozen=True)
class PromoteTemporalMtlsCaRequest:
    checked_at: str
    project: TemporalCloudProject
    stage: TemporalCloudCredentialStageResult


@dataclass(frozen=True)
class DeleteTemporalRuntimeCandidateSecretRequest:
    checked_at: str
    project: TemporalCloudProject
    candidate_secret_path: str | None = None
    candidate_ca_secret_path: str | None = None
    candidate_secret_token: str | None = None


@dataclass(frozen=True)
class DeleteTemporalCloudMtlsCaSecretRequest:
    checked_at: str
    project: TemporalCloudProject


@dataclass(frozen=True)
class MirrorTemporalRuntimeSecretRequest:
    checked_at: str
    project: TemporalCloudProject


@dataclass(frozen=True)
class DeleteTemporalCloudApiKeyRequest:
    checked_at: str
    project: TemporalCloudProject
    key_id: str


@dataclass(frozen=True)
class DeleteTemporalCloudServiceAccountRequest:
    checked_at: str
    project: TemporalCloudProject
    service_account_id: str


@dataclass(frozen=True)
class TemporalCloudNamespaceRequest:
    checked_at: str
    project: TemporalCloudProject
    namespace: str


@dataclass(frozen=True)
class ReconcileNexusEndpointsRequest:
    checked_at: str
    project: TemporalCloudProject
    account_id: str | None = None


@dataclass(frozen=True)
class DeleteNexusEndpointsRequest:
    checked_at: str
    project: TemporalCloudProject


@dataclass(frozen=True)
class NexusEndpointResult:
    status: dict[str, Any]
    # Endpoint name -> Temporal Cloud endpoint id, for the endpoints this project
    # owns. Carried so finalize can delete by id without re-listing.
    endpoint_ids: dict[str, str]

    @property
    def phase(self) -> str | None:
        phase = self.status.get("phase")
        return phase if isinstance(phase, str) else None
