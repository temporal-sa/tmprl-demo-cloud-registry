package temporalcloud

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"maps"
	"regexp"
	"strings"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	namespace "go.temporal.io/cloud-sdk/api/namespace/v1"
	operation "go.temporal.io/cloud-sdk/api/operation/v1"
	resource "go.temporal.io/cloud-sdk/api/resource/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type Adapter struct {
	transport    Transport
	owner        ownership.Context
	scope        ownership.Scope
	bindings     map[string]Binding
	creates      map[string]ownership.Intent
	regions      map[string]bool
	projectID    string
	capabilities Capabilities
	maxRetention int32
	timeout      time.Duration
	guard        MutationGuard
	now          func() time.Time
}

var namespaceNamePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,38}$`)
var digestPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

func New(config Config, transport Transport) (*Adapter, error) {
	if transport == nil || config.CallTimeout <= 0 || config.CallTimeout > time.Minute {
		return nil, fail(InvalidConfiguration)
	}
	scope, err := config.Ownership.Scope(ownership.Temporal)
	if err != nil {
		return nil, fail(InvalidConfiguration)
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	write := config.Capabilities.CreateNamespaces || config.Capabilities.UpdateNamespaces || config.Capabilities.DeleteNamespaces || config.Capabilities.GlobalTagWrites
	if write && config.Guard == nil {
		return nil, fail(InvalidConfiguration)
	}
	if (config.Capabilities.CreateNamespaces || config.Capabilities.UpdateNamespaces) && config.MaximumRetentionDays < 1 {
		return nil, fail(InvalidConfiguration)
	}
	if config.Capabilities.CreateNamespaces && (!text(config.CreationProjectID) || len(config.AllowedRegions) == 0) {
		return nil, fail(InvalidConfiguration)
	}
	a := &Adapter{transport: transport, owner: config.Ownership, scope: scope, bindings: map[string]Binding{}, creates: map[string]ownership.Intent{}, regions: map[string]bool{}, projectID: config.CreationProjectID,
		capabilities: config.Capabilities, maxRetention: config.MaximumRetentionDays, timeout: config.CallTimeout, guard: config.Guard, now: config.Now}
	for _, region := range config.AllowedRegions {
		if !text(region) {
			return nil, fail(InvalidConfiguration)
		}
		a.regions[region] = true
	}
	for _, binding := range config.Bindings {
		r := binding.Record
		if !namespaceNamePattern.MatchString(r.Ref.Name) || binding.NamespaceID != r.Ref.Name+"."+scope.AccountID || binding.CreatedAt.IsZero() || !text(binding.ProvenanceID) || r.Ref.Purpose != "namespace" || r.Ref.Scope != scope || r.Owner != a.owner.Owner() || r.Ref.ProviderID != IncarnationID(binding.NamespaceID, binding.CreatedAt) {
			return nil, fail(InvalidConfiguration)
		}
		// This validates ledger shape only. It is never substituted for the
		// independently read namespace incarnation/version before a mutation.
		validationTime := a.now()
		_, err := a.owner.Authorize(ownership.MutationRequest{Action: ownership.Adopt, Record: r, Observed: ledgerObservation(r, "configuration-validation"), ExpectedRevision: r.Revision,
			Writer: ownership.Writer{ID: "configuration-validation", Owner: r.Owner, Epoch: r.WriterEpoch, ExpiresAt: validationTime.Add(time.Minute)}, Now: validationTime, Fencing: ownership.ProviderConditional})
		if err != nil {
			return nil, fail(InvalidConfiguration)
		}
		if _, exists := a.bindings[binding.NamespaceID]; exists {
			return nil, fail(InvalidConfiguration)
		}
		a.bindings[binding.NamespaceID] = binding
	}
	for _, intent := range config.PlannedCreates {
		if !namespaceNamePattern.MatchString(intent.Name) || intent.Purpose != "namespace" || intent.Scope != scope {
			return nil, fail(InvalidConfiguration)
		}
		at := a.now()
		if _, err := a.owner.AuthorizeCreate(intent, ownership.Writer{ID: "configuration-validation", Owner: a.owner.Owner(), Epoch: 1, ExpiresAt: at.Add(time.Minute)}, at); err != nil {
			return nil, fail(InvalidConfiguration)
		}
		id := intent.Name + "." + scope.AccountID
		if _, exists := a.creates[id]; exists {
			return nil, fail(InvalidConfiguration)
		}
		if _, exists := a.bindings[id]; exists {
			return nil, fail(InvalidConfiguration)
		}
		a.creates[id] = intent
	}
	if len(a.bindings) == 0 && len(a.creates) == 0 {
		return nil, fail(InvalidConfiguration)
	}
	return a, nil
}

func (a *Adapter) Observe(ctx context.Context, namespaceID string) (Snapshot, error) {
	binding, ok := a.bindings[namespaceID]
	if !ok {
		return Snapshot{}, fail(OwnershipConflict)
	}
	ns, err := a.read(ctx, namespaceID)
	if err != nil {
		return Snapshot{}, err
	}
	if ns == nil {
		return Snapshot{NamespaceID: namespaceID, Exists: false}, nil
	}
	if err := a.verifyBirth(binding, ns); err != nil {
		return Snapshot{}, err
	}
	return snapshot(ns), nil
}

func (a *Adapter) read(ctx context.Context, id string) (*namespace.Namespace, error) {
	call, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	response, err := a.transport.GetNamespace(call, &cloudservice.GetNamespaceRequest{Namespace: id})
	if status.Code(err) == codes.NotFound {
		return nil, nil
	}
	if err != nil {
		return nil, providerError(err, Receipt{}, false)
	}
	if response == nil || response.Namespace == nil || response.Namespace.Spec == nil || response.Namespace.Namespace != id || response.Namespace.ResourceVersion == "" || response.Namespace.CreatedTime == nil || response.Namespace.CreatedTime.CheckValid() != nil {
		return nil, fail(ProviderResponseInvalid)
	}
	return proto.Clone(response.Namespace).(*namespace.Namespace), nil
}

func (a *Adapter) verifyBirth(binding Binding, ns *namespace.Namespace) error {
	if ns.Namespace != binding.NamespaceID || ns.Spec.Name != binding.Record.Ref.Name || !ns.CreatedTime.AsTime().Equal(binding.CreatedAt) || IncarnationID(ns.Namespace, ns.CreatedTime.AsTime()) != binding.Record.Ref.ProviderID {
		return fail(OwnershipConflict)
	}
	return nil
}

func (a *Adapter) StartUpdate(ctx context.Context, request WriteRequest, patch Patch) (Operation, error) {
	if !a.capabilities.UpdateNamespaces {
		return Operation{}, fail(CapabilityDisabled)
	}
	if len(patch.RemoveSearchAttributes) > 0 {
		return Operation{}, fail(SearchAttributeRemovalUnsupported)
	}
	if err := a.validatePatch(patch); err != nil {
		return Operation{}, err
	}
	receipt := a.receipt(request.NamespaceID, "update", request.OperationKey, fingerprint(patch))
	var result Operation
	err := a.mutate(ctx, request, func(call context.Context, ns *namespace.Namespace) error {
		if ns.State != resource.ResourceState_RESOURCE_STATE_ACTIVE {
			return &Error{Code: OperationPending, Retryable: true}
		}
		spec := proto.Clone(ns.Spec).(*namespace.NamespaceSpec)
		if patch.RetentionDays != nil {
			spec.RetentionDays = *patch.RetentionDays
		}
		if patch.FairnessEnabled != nil {
			spec.Fairness = &namespace.FairnessSpec{TaskQueueFairnessEnabled: *patch.FairnessEnabled}
		}
		if patch.DeleteProtection != nil {
			spec.Lifecycle = &namespace.LifecycleSpec{EnableDeleteProtection: *patch.DeleteProtection}
		}
		if patch.Description != nil {
			spec.Description = *patch.Description
		}
		if spec.SearchAttributes == nil {
			spec.SearchAttributes = map[string]namespace.NamespaceSpec_SearchAttributeType{}
		}
		for key, kind := range patch.SearchAttributes {
			target, _ := attributeType(kind)
			if current, exists := spec.SearchAttributes[key]; exists && current != target {
				return fail(SearchAttributeTypeConflict)
			}
			spec.SearchAttributes[key] = target
		}
		response, err := a.transport.UpdateNamespace(call, &cloudservice.UpdateNamespaceRequest{Namespace: request.NamespaceID, Spec: spec, ResourceVersion: ns.ResourceVersion, AsyncOperationId: receipt.ID})
		if err != nil {
			return providerError(err, receipt, true)
		}
		if response == nil {
			return invalidWriteResponse(receipt)
		}
		result, err = operationResult(receipt, response.AsyncOperation)
		return err
	})
	return result, err
}

func (a *Adapter) StartDelete(ctx context.Context, request WriteRequest) (Operation, error) {
	if !a.capabilities.DeleteNamespaces {
		return Operation{}, fail(CapabilityDisabled)
	}
	receipt := a.receipt(request.NamespaceID, "delete", request.OperationKey, fingerprint("delete"))
	var result Operation
	err := a.mutate(ctx, request, func(call context.Context, ns *namespace.Namespace) error {
		if ns.State == resource.ResourceState_RESOURCE_STATE_DELETING {
			return &Error{Code: OperationPending, Retryable: true}
		}
		if ns.Spec.GetLifecycle().GetEnableDeleteProtection() {
			return fail(DeleteProtectionEnabled)
		}
		response, err := a.transport.DeleteNamespace(call, &cloudservice.DeleteNamespaceRequest{Namespace: request.NamespaceID, ResourceVersion: ns.ResourceVersion, AsyncOperationId: receipt.ID})
		if err != nil {
			return providerError(err, receipt, true)
		}
		if response == nil {
			return invalidWriteResponse(receipt)
		}
		result, err = operationResult(receipt, response.AsyncOperation)
		return err
	})
	if typed, ok := err.(*Error); ok && typed.Code == NamespaceMissing {
		return Operation{Receipt: receipt, State: "AlreadyAbsent", Done: true, MutationProtection: "VerifiedAbsence"}, nil
	}
	return result, err
}

func (a *Adapter) StartTags(ctx context.Context, request WriteRequest, upsert map[string]string, remove []string) (Operation, error) {
	if !a.capabilities.GlobalTagWrites {
		return Operation{}, fail(CapabilityDisabled)
	}
	if len(upsert)+len(remove) == 0 {
		return Operation{}, fail(InvalidRequest)
	}
	for key, value := range upsert {
		if !validTag(key, value) || strings.HasPrefix(key, "registry-owner") {
			return Operation{}, fail(InvalidRequest)
		}
	}
	for _, key := range remove {
		if !text(key) || strings.HasPrefix(key, "registry-owner") {
			return Operation{}, fail(InvalidRequest)
		}
	}
	upsert = maps.Clone(upsert)
	remove = append([]string(nil), remove...)
	receipt := a.receipt(request.NamespaceID, "tags", request.OperationKey, fingerprint(struct {
		Upsert map[string]string
		Remove []string
	}{upsert, remove}))
	var result Operation
	err := a.mutate(ctx, request, func(call context.Context, _ *namespace.Namespace) error {
		// The SDK/API has NO resource_version field for this request. The
		// mandatory external guard is the serialization boundary here.
		response, err := a.transport.UpdateNamespaceTags(call, &cloudservice.UpdateNamespaceTagsRequest{Namespace: request.NamespaceID, TagsToUpsert: upsert, TagsToRemove: remove, AsyncOperationId: receipt.ID})
		if err != nil {
			return providerError(err, receipt, true)
		}
		if response == nil {
			return invalidWriteResponse(receipt)
		}
		result, err = operationResult(receipt, response.AsyncOperation)
		return err
	})
	return result, err
}

func (a *Adapter) mutate(ctx context.Context, request WriteRequest, apply func(context.Context, *namespace.Namespace) error) error {
	binding, ok := a.bindings[request.NamespaceID]
	if !ok || request.ExpectedIncarnationID != binding.Record.Ref.ProviderID {
		return fail(OwnershipConflict)
	}
	if !text(request.ExpectedVersion) || !text(request.ExpectedRevision) || !text(request.OperationKey) {
		return fail(InvalidRequest)
	}
	if request.ExpectedRevision != binding.Record.Revision {
		return fail(StaleSnapshot)
	}
	if err := a.validateWriter(request.Writer, binding.Record.WriterEpoch); err != nil {
		return err
	}
	return a.guarded(ctx, request.NamespaceID, request.Writer, func(guardContext context.Context) error {
		ns, err := a.read(guardContext, request.NamespaceID)
		if err != nil {
			return err
		}
		if ns == nil {
			return fail(NamespaceMissing)
		}
		if err := a.verifyBirth(binding, ns); err != nil {
			return err
		}
		if ns.State == resource.ResourceState_RESOURCE_STATE_DELETED {
			return fail(NamespaceMissing)
		}
		if ns.ResourceVersion != request.ExpectedVersion {
			return fail(StaleSnapshot)
		}
		// Owner/creation/reconcile metadata is attested by the trusted fixture
		// ledger, NOT claimed to be provider tags. Birth and version are read
		// independently; the external guard supplies active-writer authority.
		observed := ledgerObservation(binding.Record, ns.ResourceVersion)
		observed.Ref.ProviderID = IncarnationID(ns.Namespace, ns.CreatedTime.AsTime())
		observed.Ref.Name = ns.Spec.Name
		if _, err := a.owner.Authorize(ownership.MutationRequest{Action: ownership.Mutate, Record: binding.Record, Observed: observed, ExpectedRevision: request.ExpectedRevision, Writer: request.Writer, Now: a.now(), Fencing: ownership.ProviderConditional}); err != nil {
			return fail(OwnershipConflict)
		}
		call, cancel := context.WithTimeout(guardContext, a.timeout)
		defer cancel()
		return apply(call, ns)
	})
}

func (a *Adapter) guarded(ctx context.Context, id string, writer ownership.Writer, fn func(context.Context) error) error {
	if a.guard == nil {
		return fail(WriterGuardFailed)
	}
	called := false
	var operationError error
	err := a.guard.Run(ctx, id, writer, func(call context.Context) error {
		if called {
			return fail(WriterGuardFailed)
		}
		called = true
		if err := a.validateWriter(writer, writer.Epoch); err != nil {
			operationError = err
			return err
		}
		operationError = fn(call)
		return operationError
	})
	if operationError != nil {
		return operationError
	}
	if !called || err != nil {
		return fail(WriterGuardFailed)
	}
	return nil
}

func (a *Adapter) validateWriter(writer ownership.Writer, epoch uint64) error {
	if writer.Owner != a.owner.Owner() || writer.Epoch == 0 || writer.Epoch != epoch || !text(writer.ID) || !a.now().Before(writer.ExpiresAt) {
		return fail(OwnershipConflict)
	}
	return nil
}

func (a *Adapter) validatePatch(patch Patch) error {
	if patch.RetentionDays == nil && patch.FairnessEnabled == nil && patch.DeleteProtection == nil && patch.Description == nil && len(patch.SearchAttributes) == 0 {
		return fail(InvalidRequest)
	}
	if patch.RetentionDays != nil && (*patch.RetentionDays < 1 || *patch.RetentionDays > a.maxRetention) {
		return fail(InvalidRequest)
	}
	if patch.DeleteProtection != nil && !*patch.DeleteProtection && !a.capabilities.DisableDeleteProtection {
		return fail(CapabilityDisabled)
	}
	if patch.Description != nil && !description(*patch.Description) {
		return fail(InvalidRequest)
	}
	return validateAttributes(patch.SearchAttributes)
}

func ledgerObservation(record ownership.Record, version string) ownership.Observation {
	return ownership.Observation{Ref: record.Ref, Owner: record.Owner, CreationIdempotencyKey: record.CreationIdempotencyKey, Revision: record.Revision, SpecFingerprint: record.SpecFingerprint, WriterEpoch: record.WriterEpoch, Version: version}
}

func snapshot(ns *namespace.Namespace) Snapshot {
	attributes := map[string]SearchAttributeType{}
	for key, kind := range ns.Spec.SearchAttributes {
		attributes[key] = fromAttributeType(kind)
	}
	return Snapshot{NamespaceID: ns.Namespace, IncarnationID: IncarnationID(ns.Namespace, ns.CreatedTime.AsTime()), Name: ns.Spec.Name, CreatedAt: ns.CreatedTime.AsTime(), ResourceVersion: ns.ResourceVersion,
		State: ns.State.String(), Exists: ns.State != resource.ResourceState_RESOURCE_STATE_DELETED, APIKeyEnabled: ns.Spec.GetApiKeyAuth().GetEnabled(), MTLSEnabled: ns.Spec.GetMtlsAuth().GetEnabled(), DeleteProtected: ns.Spec.GetLifecycle().GetEnableDeleteProtection(),
		FairnessEnabled: ns.Spec.GetFairness().GetTaskQueueFairnessEnabled(), RetentionDays: ns.Spec.RetentionDays, SearchAttributes: attributes, Tags: maps.Clone(ns.Tags), GRPCAddress: ns.GetEndpoints().GetGrpcAddress()}
}

func NamespaceFingerprint(desired DesiredNamespace) (string, error) {
	if !text(desired.RegionID) || desired.RetentionDays < 1 || !description(desired.Description) {
		return "", fail(InvalidRequest)
	}
	if err := validateAttributes(desired.SearchAttributes); err != nil {
		return "", err
	}
	switch desired.AuthMode {
	case APIKeyAuth:
		if len(desired.MTLSCAPEM) > 0 {
			return "", fail(InvalidRequest)
		}
	case MTLSAuth:
		if !validCA(desired.MTLSCAPEM) {
			return "", fail(InvalidRequest)
		}
	default:
		return "", fail(InvalidRequest)
	}
	for key, value := range desired.Tags {
		if !validTag(key, value) || strings.HasPrefix(key, "registry-owner") {
			return "", fail(InvalidRequest)
		}
	}
	return fingerprint(desired), nil
}

func createSpec(name string, desired DesiredNamespace) *namespace.NamespaceSpec {
	spec := &namespace.NamespaceSpec{Name: name, Replicas: []*namespace.ReplicaSpec{{Region: desired.RegionID}}, RetentionDays: desired.RetentionDays, Lifecycle: &namespace.LifecycleSpec{EnableDeleteProtection: true}, Fairness: &namespace.FairnessSpec{TaskQueueFairnessEnabled: desired.FairnessEnabled}, SearchAttributes: map[string]namespace.NamespaceSpec_SearchAttributeType{}, Description: desired.Description}
	if desired.AuthMode == APIKeyAuth {
		spec.ApiKeyAuth = &namespace.ApiKeyAuthSpec{Enabled: true}
	} else {
		spec.MtlsAuth = &namespace.MtlsAuthSpec{Enabled: true, AcceptedClientCa: append([]byte(nil), desired.MTLSCAPEM...)}
	}
	for key, kind := range desired.SearchAttributes {
		spec.SearchAttributes[key], _ = attributeType(kind)
	}
	return spec
}

func validateAttributes(attributes map[string]SearchAttributeType) error {
	if len(attributes) > 1000 {
		return fail(InvalidRequest)
	}
	for key, kind := range attributes {
		if !text(key) || len(key) > 128 {
			return fail(InvalidRequest)
		}
		if _, ok := attributeType(kind); !ok {
			return fail(InvalidRequest)
		}
	}
	return nil
}

func attributeType(value SearchAttributeType) (namespace.NamespaceSpec_SearchAttributeType, bool) {
	values := map[SearchAttributeType]namespace.NamespaceSpec_SearchAttributeType{Text: namespace.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_TEXT, Keyword: namespace.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_KEYWORD, Int: namespace.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_INT, Double: namespace.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_DOUBLE, Bool: namespace.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_BOOL, Datetime: namespace.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_DATETIME, KeywordList: namespace.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_KEYWORD_LIST}
	result, ok := values[value]
	return result, ok
}

func fromAttributeType(value namespace.NamespaceSpec_SearchAttributeType) SearchAttributeType {
	for _, kind := range []SearchAttributeType{Text, Keyword, Int, Double, Bool, Datetime, KeywordList} {
		if candidate, _ := attributeType(kind); candidate == value {
			return kind
		}
	}
	return SearchAttributeType(fmt.Sprintf("unknown(%d)", value))
}

func validCA(data []byte) bool {
	found := false
	for len(strings.TrimSpace(string(data))) > 0 {
		if !strings.HasPrefix(strings.TrimSpace(string(data)), "-----BEGIN CERTIFICATE-----") {
			return false
		}
		block, rest := pem.Decode(data)
		if block == nil || block.Type != "CERTIFICATE" {
			return false
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !cert.IsCA {
			return false
		}
		found = true
		data = rest
	}
	return found
}

func validTag(key, value string) bool {
	return text(key) && len(key) <= 128 && len(value) <= 256 && !strings.ContainsAny(value, "\r\n\x00")
}
func description(value string) bool {
	if len(value) > 255 {
		return false
	}
	for _, r := range value {
		if r > 126 || r < 32 && r != '\t' && r != '\n' && r != '\r' {
			return false
		}
	}
	return true
}
func text(value string) bool {
	return value != "" && len(value) <= 256 && strings.TrimSpace(value) == value && !strings.ContainsAny(value, "\r\n\x00")
}
func fingerprint(value any) string {
	data, _ := json.Marshal(value)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func (a *Adapter) receipt(id, kind, key, inputFingerprint string) Receipt {
	seed := fingerprint(struct {
		Owner                                             ownership.Owner
		NamespaceID, Kind, OperationKey, InputFingerprint string
	}{a.owner.Owner(), id, kind, key, inputFingerprint})
	// RFC UUIDv5 under a fixed namespace, used only for idempotency. SHA-1
	// here is mandated by UUIDv5 and is never used for authentication.
	namespaceBytes := []byte{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}
	digest := sha1.Sum(append(namespaceBytes, []byte("registry-operator-v2/"+seed)...))
	bytes := append([]byte(nil), digest[:16]...)
	bytes[6] = (bytes[6] & 0x0f) | 0x50
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	hexID := hex.EncodeToString(bytes)
	return Receipt{ID: hexID[:8] + "-" + hexID[8:12] + "-" + hexID[12:16] + "-" + hexID[16:20] + "-" + hexID[20:], NamespaceID: id, Kind: kind, OperationKey: key, InputFingerprint: inputFingerprint}
}

func operationResult(receipt Receipt, current *operation.AsyncOperation) (Operation, error) {
	if current == nil || current.Id != receipt.ID {
		return Operation{}, invalidWriteResponse(receipt)
	}
	poll := time.Second
	if current.CheckDuration != nil && current.CheckDuration.CheckValid() == nil {
		poll = current.CheckDuration.AsDuration()
		if poll < 100*time.Millisecond {
			poll = 100 * time.Millisecond
		}
		if poll > 30*time.Second {
			poll = 30 * time.Second
		}
	}
	result := Operation{Receipt: receipt, State: current.State.String(), PollAfter: poll}
	switch receipt.Kind {
	case "tags":
		result.MutationProtection = "ExternalSerializationOnly"
	case "create":
		result.MutationProtection = "IdempotencyAndExternalGuard"
	default:
		result.MutationProtection = "ProviderVersionAndExternalGuard"
	}
	switch current.State {
	case operation.AsyncOperation_STATE_PENDING, operation.AsyncOperation_STATE_IN_PROGRESS:
	case operation.AsyncOperation_STATE_FULFILLED:
		result.Done = true
	case operation.AsyncOperation_STATE_FAILED, operation.AsyncOperation_STATE_REJECTED, operation.AsyncOperation_STATE_CANCELLED:
		result.Done = true
		result.Failed = true
		return result, &Error{Code: OperationFailed, ProviderCode: current.State.String(), Receipt: receipt}
	default:
		return Operation{}, invalidWriteResponse(receipt)
	}
	return result, nil
}

func invalidWriteResponse(receipt Receipt) *Error {
	return &Error{Code: ProviderResponseInvalid, Receipt: receipt, RecoveryRequired: true}
}

func (a *Adapter) Poll(ctx context.Context, receipt Receipt) (Operation, error) {
	_, bound := a.bindings[receipt.NamespaceID]
	_, planned := a.creates[receipt.NamespaceID]
	if !bound && !planned || !text(receipt.OperationKey) || !digestPattern.MatchString(receipt.InputFingerprint) || receipt != a.receipt(receipt.NamespaceID, receipt.Kind, receipt.OperationKey, receipt.InputFingerprint) {
		return Operation{}, fail(OwnershipConflict)
	}
	switch receipt.Kind {
	case "create", "update", "delete", "tags":
	default:
		return Operation{}, fail(InvalidRequest)
	}
	current, err := a.pollRaw(ctx, receipt)
	if err != nil {
		return Operation{}, err
	}
	return operationResult(receipt, current)
}

func (a *Adapter) pollRaw(ctx context.Context, receipt Receipt) (*operation.AsyncOperation, error) {
	call, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	response, err := a.transport.GetAsyncOperation(call, &cloudservice.GetAsyncOperationRequest{AsyncOperationId: receipt.ID})
	if err != nil {
		return nil, providerError(err, receipt, false)
	}
	if response == nil || response.AsyncOperation == nil || response.AsyncOperation.Id != receipt.ID {
		return nil, fail(ProviderResponseInvalid)
	}
	return proto.Clone(response.AsyncOperation).(*operation.AsyncOperation), nil
}
