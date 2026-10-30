package fixtureplan

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	project "go.temporal.io/cloud-sdk/api/project/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// ProjectCreateReview renders an SDK-valid create body without making a call.
// Its account is implicit in the human OAuth session; the review therefore
// requires a fresh exact-account anchor before any future authorized execution.
func ProjectCreateReview() ([]byte, Object, error) {
	c, err := Context()
	if err != nil {
		return nil, nil, err
	}
	n, err := derive(c)
	if err != nil {
		return nil, nil, err
	}
	spec := &project.ProjectSpec{DisplayName: n.project, Description: "registry-operator-v2 fixture=" + RunID + "; owner-sha256=" + digest(c.Owner()) + "; bootstrap=create-project-v1", Lifecycle: &project.LifecycleSpec{EnableDeleteProtection: true}}
	binary, err := proto.MarshalOptions{Deterministic: true}.Marshal(spec)
	if err != nil {
		return nil, nil, err
	}
	specHash := sha256.Sum256(binary)
	seed := sha256.Sum256([]byte(TemporalAccount + "|" + RunID + "|create-project-v1|" + hex.EncodeToString(specHash[:])))
	seed[6] = (seed[6] & 0x0f) | 0x50
	seed[8] = (seed[8] & 0x3f) | 0x80
	operationID := fmt.Sprintf("%x-%x-%x-%x-%x", seed[:4], seed[4:6], seed[6:8], seed[8:10], seed[10:16])
	request := &cloudservice.CreateProjectRequest{Spec: spec, AsyncOperationId: operationID}
	protoJSON, err := protojson.Marshal(request)
	if err != nil {
		return nil, nil, err
	}
	// ProtoJSON does not promise byte-stable whitespace. Canonicalize its
	// schema-valid wire view before deriving an approval digest/artifact.
	var view Object
	if err = json.Unmarshal(protoJSON, &view); err != nil {
		return nil, nil, err
	}
	raw, err := json.MarshalIndent(view, "", "  ")
	if err != nil {
		return nil, nil, err
	}
	raw = append(raw, '\n')
	hash := sha256.Sum256(raw)
	args := []string{"temporal", "cloud", "project", "create", "--display-name", spec.DisplayName, "--description", spec.Description, "--enable-delete-protection", "--async-operation-id", operationID, "--async", "--disable-config-file", "--disable-config-env", "--disable-pop-up", "--command-timeout", "20s", "--client-connect-timeout", "5s", "-o", "json"}
	review := Object{
		"schemaVersion": "registry-project-create-review-v1", "createAuthorized": false, "expectedAccountID": TemporalAccount, "apiVersion": tc.SupportedAPIVersion, "sdkVersion": "go.temporal.io/cloud-sdk v0.21.0", "owner": c.Owner(), "runID": RunID, "displayName": spec.DisplayName, "descriptionOwnerReceipt": spec.Description, "deleteProtection": true, "asyncOperationID": operationID, "requestSHA256": hex.EncodeToString(hash[:]), "specDeterministicProtobufSHA256": hex.EncodeToString(specHash[:]), "requestType": "temporal.api.cloud.cloudservice.v1.CreateProjectRequest", "requestMethod": "CreateProject only; never UpdateProject or enablement", "requestFile": "temporal-project-create.request.json", "rejectedCLIArgvNotExecuted": args,
		"executionTransport": Object{"requiredHelper": "testinfra/projectbootstrap/cmd/create-project", "reviewAndExecutionAuthorizationStillRequired": true, "CLIRejectedReason": "Cloud extension v0.2.1 constructs SDK options with default seven-retry policy. CLI is not the reviewed one-shot mutation transport.", "credentialSource": "explicit private OAuth config/profile; unexpired access token only, no API-key fallback or refresh", "guard": "private reviewed approval/request digest, exact OAuth identity/account preflight, fsynced exclusive before-dispatch tombstone, single-use per-transport-attempt authentication budget, no automatic process replay", "outcome": "safe new immutable project ID/async receipt or honest unknown with canonical gRPC code; never provider text or credentials"},
		"accountPreflight":   Object{"requiredImmediatelyBeforeExecution": true, "oauthOnly": true, "configFileAndEnvDisabled": true, "noAPIKeyArgument": true, "knownAccountAnchorNamespace": "tmprl-dem-cld-registry-operator.a2dd6", "knownAccountAnchorProjectID": "eca773c382124db79bbfab0b429faa5a", "selectedReadMetadataOnly": true, "noAccountWideLists": true, "existingProjectNeverMutationTarget": true, "readDoesNotProveCustomProjectQuota": true},
		"uniqueness":         "Display name includes full disjoint ownership/run digest. No account-wide display-name search is authorized or claimed; display name is mutable and never sufficient for adoption. Only the newly returned immutable ProjectID is a binding.",
		"requiredOutcome":    []string{"capture exact CreateProjectResponse.ProjectId and matching asyncOperation.id immediately into safe durable receipt", "new ProjectID must be nonempty and differ from the protected existing project ID", "GetProject by that exact new ID must match specification, delete protection, CreatedTime and creation operation evidence", "pin immutable ID plus birth/resourceVersion/owner bootstrap receipt before constructing any Namespace Create request", "no namespaces, API keys or service accounts are created by this project-only request"},
		"unknownOutcome":     Object{"recordedBeforeDispatch": []string{"exact request digest/body", "fixed async ID", "expected account/owner/run", "fresh OAuth account preflight", "root execution approval"}, "allowedRead": "GetAsyncOperation only for this exact approved ID, and GetProject only if an independently returned new ID is known", "providerLimitation": "Pinned AsyncOperation schema has input/state/times but no documented output/resource ID; a lost CreateProject response may leave the generated ProjectID unproven.", "automaticReplay": false, "nameBasedIdempotentFlag": false, "accountWideDiscovery": false, "fallback": "Stop and retain the exact safe intent/unknown receipt. Exact same-request replay must be independently documented/proven before use; otherwise resolve through a separately approved provider/human evidence path. Never create a different project or use Default Project to hide uncertainty."},
		"excludedActions":    []string{"Projects enablement", "move existing resources", "existing project update/delete", "Namespace create without new bound ProjectID", "account/global admin API-key generation or reuse in test code", "service-account/API-key creation", "permission probes", "automatic rollback/delete"},
		"quotaGate":          "At most one new protected project; custom-project entitlement and available slots are unmeasured. A quota/feature error must stop, without enablement, support-setting changes or Default Project fallback.",
		"cleanupGate":        "A new project is retained/delete-protected until separately reviewed owned cleanup. Never disable/delete existing account projects; empty owned project requirement and explicit protection disable apply.",
		"primaryReferences":  []string{"https://docs.temporal.io/cloud/projects", "https://docs.temporal.io/cloud/manage-access/permissions-reference", "https://docs.temporal.io/evaluate/cloud/limits", "https://github.com/temporalio/cloud-sdk-go/tree/v0.21.0"},
	}
	return raw, review, nil
}
