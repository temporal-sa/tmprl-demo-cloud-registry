package fixtureplan

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	namespace "go.temporal.io/cloud-sdk/api/namespace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// NamespaceProjectBinding is copied from an independently reviewed safe create
// attestation, never from a project name or untrusted project authoring spec.
// Validating this DTO cannot establish provider provenance: the reviewer must
// inspect the approved create receipt and independent exact-ID observation.
type NamespaceProjectBinding struct {
	ProjectID                               string `json:"projectID"`
	CreatedTime                             string `json:"createdTime"`
	ResourceVersion                         string `json:"resourceVersion"`
	AsyncOperationID                        string `json:"asyncOperationID"`
	ApprovedRequestSHA256                   string `json:"approvedRequestSHA256"`
	State                                   string `json:"state"`
	AsyncState                              string `json:"asyncState"`
	OperationInputExactSpecAndAsyncVerified bool   `json:"operationInputExactSpecAndAsyncVerified"`
	DeleteProtectionEnabled                 bool   `json:"deleteProtectionEnabled"`
}

type NamespaceDraft struct {
	Label         string
	NamespaceID   string
	RequestJSON   []byte
	RequestSHA256 string
	AsyncID       string
}

// NamespaceCreateReview emits only inert SDK-valid request bytes. It performs
// no cloud/auth/config reads and grants no authority to execute these requests.
func NamespaceCreateReview(binding NamespaceProjectBinding) ([]NamespaceDraft, Object, error) {
	projectRequest, projectReview, err := ProjectCreateReview()
	if err != nil {
		return nil, nil, err
	}
	projectHash := sha256.Sum256(projectRequest)
	_, birthErr := time.Parse(time.RFC3339Nano, binding.CreatedTime)
	if !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(binding.ProjectID) || binding.ProjectID == "eca773c382124db79bbfab0b429faa5a" || birthErr != nil || !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`).MatchString(binding.ResourceVersion) || binding.State != "RESOURCE_STATE_ACTIVE" || binding.AsyncState != "STATE_FULFILLED" || binding.AsyncOperationID != projectReview["asyncOperationID"] || binding.ApprovedRequestSHA256 != hex.EncodeToString(projectHash[:]) || !binding.OperationInputExactSpecAndAsyncVerified || !binding.DeleteProtectionEnabled {
		return nil, nil, errors.New("independently attested new protected Project binding required")
	}
	c, err := Context()
	if err != nil {
		return nil, nil, err
	}
	n, err := derive(c)
	if err != nil {
		return nil, nil, err
	}
	deny, err := namespaceName(c, "deny")
	if err != nil {
		return nil, nil, err
	}
	var drafts []NamespaceDraft
	var requestViews []Object
	for _, slot := range []struct{ label, name, purpose string }{{"control", n.control, "Durable test workflow control; no production queues or clients"}, {"target", n.target, "Scoped namespace adapter and SELF-key credential staging target"}, {"deny", deny, "Owned negative permission sentinel; no control workflows/workers"}} {
		spec := &namespace.NamespaceSpec{Name: slot.name, Replicas: []*namespace.ReplicaSpec{{Region: TemporalNamespaceRegion}}, RetentionDays: 1, ApiKeyAuth: &namespace.ApiKeyAuthSpec{Enabled: true}, Lifecycle: &namespace.LifecycleSpec{EnableDeleteProtection: true}}
		binary, err := proto.MarshalOptions{Deterministic: true}.Marshal(spec)
		if err != nil {
			return nil, nil, err
		}
		specHash := sha256.Sum256(binary)
		seed := sha256.Sum256([]byte(TemporalAccount + "|" + RunID + "|create-namespace-v1|" + slot.label + "|" + binding.ProjectID + "|" + binding.CreatedTime + "|" + hex.EncodeToString(specHash[:])))
		seed[6] = (seed[6] & 0x0f) | 0x50
		seed[8] = (seed[8] & 0x3f) | 0x80
		asyncID := fmt.Sprintf("%x-%x-%x-%x-%x", seed[:4], seed[4:6], seed[6:8], seed[8:10], seed[10:16])
		req := &cloudservice.CreateNamespaceRequest{Spec: spec, ProjectId: binding.ProjectID, AsyncOperationId: asyncID}
		raw, err := protojson.Marshal(req)
		if err != nil {
			return nil, nil, err
		}
		var view Object
		if err = json.Unmarshal(raw, &view); err != nil {
			return nil, nil, err
		}
		raw, err = json.MarshalIndent(view, "", "  ")
		if err != nil {
			return nil, nil, err
		}
		raw = append(raw, '\n')
		hash := sha256.Sum256(raw)
		draft := NamespaceDraft{Label: slot.label, NamespaceID: slot.name + "." + TemporalAccount, RequestJSON: raw, RequestSHA256: hex.EncodeToString(hash[:]), AsyncID: asyncID}
		drafts = append(drafts, draft)
		requestViews = append(requestViews, Object{"label": slot.label, "namespaceID": draft.NamespaceID, "requestFile": "temporal-namespace-" + slot.label + "-create.request.json", "requestSHA256": draft.RequestSHA256, "asyncOperationID": asyncID, "specDeterministicProtobufSHA256": hex.EncodeToString(specHash[:]), "purpose": slot.purpose})
	}
	return drafts, Object{
		"schemaVersion": "registry-temporal-namespace-create-review-v1", "createAuthorized": false, "accountID": TemporalAccount, "runID": RunID, "owner": c.Owner(), "projectBinding": binding, "requests": requestViews,
		"namespaceConfig":     Object{"region": TemporalNamespaceRegion, "replicas": 1, "retentionDays": 1, "apiKeyAuthEnabled": true, "mTLS": false, "deleteProtection": true, "searchAttributes": 0, "namespaceTags": 0, "provisionedCapacity": false, "noCredentialMaterial": true},
		"regionDecision":      "Root explicitly selected aws-us-east-2 from the actual account catalog, corroborated by exact GetRegion. Original aws-us-west-2 GetRegion was NotFound; original three names/requests/async IDs are superseded and never executed. AWS fixture region remains us-west-2. Region availability is not namespace quota/headroom proof; owner confirmation remains required.",
		"requiredPreflight":   []string{"fresh exact human OAuth principal/account anchor; OAuth never enters test workers", "independent exact new Project ID/birth/spec/protection observation plus reviewed creation provenance; structural DTO validation alone is not proof", "exact planned Namespace names absent and exact async IDs NotFound before any creation; no account-wide listings/name adoption", "verify entitlement for at most three namespaces and regional capacity; unmeasured shared quota headroom is not a successful proof", "root reviews exact request hashes and one-shot namespace transport/receipt handling before any live CreateNamespace"},
		"executionBoundary":   "Existing projectbootstrap helper is CreateProject-only and MUST NOT execute these requests. A separately reviewed one-shot namespace helper must enforce exact-method/target payload allowlist, fixed per-namespace async ID, durable before-dispatch marker, per-attempt auth budget/deadline and no process replay. This review grants no execution permission.",
		"ownership":           "Tags are omitted: test principals never require global tag authority. Ownership is independently bound from approved exact create intent/echoed async input, qualified namespace ID, immutable ProjectID and provider CreatedTime; names, owner DTOs and local request hashes alone never prove provider ownership.",
		"parentPreconditions": "CreateNamespace carries ProjectId but no Project resourceVersion precondition. Pre-read does not atomically fence parent lifecycle changes. Keep bootstrap lifecycle serialized and independently reverify exact project birth/spec/protection; never fallback to Default Project or reuse disappeared bound namespace names.",
		"unknownOutcome":      "Any error/late/lost reply retains its original dispatch marker and exact async intent; no new ID/request/process replay/name adoption. Exact async input and namespace CreatedTime must prove creation before binding. A previously bound namespace disappearance requires a new reviewed run/name, never recreation.",
		"budgets":             Object{"newNamespacesMaximum": 3, "bootstrapServiceAccountsMaximum": 4, "bootstrapKeysMaximum": 4, "controlConcurrentWorkflowsMaximum": 1, "controlConcurrentActivitiesMaximum": 1, "actualWorkflowStartsAuthorized": 0, "actualServiceAccountsOrKeysAuthorized": 0, "automaticCleanup": false},
		"principalPlan":       []Object{{"slot": "control-worker", "namespace": drafts[0].NamespaceID, "scope": "namespace-only WRITE"}, {"slot": "target-manager", "namespace": drafts[1].NamespaceID, "scope": "namespace-only ADMIN for exact namespace observation/update; cross-SA API-key CRUD unestablished, no admin fallback"}, {"slot": "target-runtime", "namespace": drafts[1].NamespaceID, "scope": "namespace-only WRITE; separate secure bootstrap then SELF-key CredentialManager with PreprovisionedServiceAccounts and CreateScopedServiceAccounts=false"}, {"slot": "target-other-owner", "namespace": drafts[1].NamespaceID, "scope": "namespace-only WRITE; separate owned SA/key sentinel to prove SELF versus other-owner denial"}},
		"denySentinel":        drafts[2].NamespaceID + " receives no control workloads and no namespace grants for target/control principals; separately authorized exact owned denial calls only, never production targets.",
		"excluded":            []string{"namespace creation by test principals", "global/account/project permissions in test code", "namespace moves/creation without ProjectID", "tag/global authority", "Nexus/users/groups", "API-key generation/output/storage", "mTLS CA material", "production requests", "delete protection disable/delete/automatic cleanup", "search attribute removal"},
	}, nil
}
