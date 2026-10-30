package temporalcloud

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	namespace "go.temporal.io/cloud-sdk/api/namespace/v1"
	operation "go.temporal.io/cloud-sdk/api/operation/v1"
	resource "go.temporal.io/cloud-sdk/api/resource/v1"
	"go.temporal.io/cloud-sdk/cloudclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeTransport struct {
	ns             *namespace.Namespace
	calls          []string
	unbounded      bool
	createRequests []*cloudservice.CreateNamespaceRequest
	updateRequests []*cloudservice.UpdateNamespaceRequest
	deleteRequests []*cloudservice.DeleteNamespaceRequest
	tagRequests    []*cloudservice.UpdateNamespaceTagsRequest
	onGet          func(*cloudservice.GetNamespaceRequest) (*cloudservice.GetNamespaceResponse, error)
	onCreate       func(*cloudservice.CreateNamespaceRequest) (*cloudservice.CreateNamespaceResponse, error)
	onUpdate       func(*cloudservice.UpdateNamespaceRequest) (*cloudservice.UpdateNamespaceResponse, error)
	onOperation    func(*cloudservice.GetAsyncOperationRequest) (*cloudservice.GetAsyncOperationResponse, error)
}

func (f *fakeTransport) note(ctx context.Context, method string) {
	f.calls = append(f.calls, method)
	if _, ok := ctx.Deadline(); !ok {
		f.unbounded = true
	}
}
func (f *fakeTransport) GetNamespace(ctx context.Context, r *cloudservice.GetNamespaceRequest, _ ...grpc.CallOption) (*cloudservice.GetNamespaceResponse, error) {
	f.note(ctx, "GetNamespace")
	if f.onGet != nil {
		return f.onGet(r)
	}
	if f.ns == nil {
		return nil, status.Error(codes.NotFound, "missing")
	}
	return &cloudservice.GetNamespaceResponse{Namespace: f.ns}, nil
}
func (f *fakeTransport) CreateNamespace(ctx context.Context, r *cloudservice.CreateNamespaceRequest, _ ...grpc.CallOption) (*cloudservice.CreateNamespaceResponse, error) {
	f.note(ctx, "CreateNamespace")
	f.createRequests = append(f.createRequests, proto.Clone(r).(*cloudservice.CreateNamespaceRequest))
	if f.onCreate != nil {
		return f.onCreate(r)
	}
	return &cloudservice.CreateNamespaceResponse{Namespace: f.ns.Namespace, AsyncOperation: pending(r.AsyncOperationId)}, nil
}
func (f *fakeTransport) UpdateNamespace(ctx context.Context, r *cloudservice.UpdateNamespaceRequest, _ ...grpc.CallOption) (*cloudservice.UpdateNamespaceResponse, error) {
	f.note(ctx, "UpdateNamespace")
	f.updateRequests = append(f.updateRequests, proto.Clone(r).(*cloudservice.UpdateNamespaceRequest))
	if f.onUpdate != nil {
		return f.onUpdate(r)
	}
	return &cloudservice.UpdateNamespaceResponse{AsyncOperation: pending(r.AsyncOperationId)}, nil
}
func (f *fakeTransport) DeleteNamespace(ctx context.Context, r *cloudservice.DeleteNamespaceRequest, _ ...grpc.CallOption) (*cloudservice.DeleteNamespaceResponse, error) {
	f.note(ctx, "DeleteNamespace")
	f.deleteRequests = append(f.deleteRequests, proto.Clone(r).(*cloudservice.DeleteNamespaceRequest))
	return &cloudservice.DeleteNamespaceResponse{AsyncOperation: pending(r.AsyncOperationId)}, nil
}
func (f *fakeTransport) UpdateNamespaceTags(ctx context.Context, r *cloudservice.UpdateNamespaceTagsRequest, _ ...grpc.CallOption) (*cloudservice.UpdateNamespaceTagsResponse, error) {
	f.note(ctx, "UpdateNamespaceTags")
	f.tagRequests = append(f.tagRequests, proto.Clone(r).(*cloudservice.UpdateNamespaceTagsRequest))
	return &cloudservice.UpdateNamespaceTagsResponse{AsyncOperation: pending(r.AsyncOperationId)}, nil
}
func (f *fakeTransport) GetAsyncOperation(ctx context.Context, r *cloudservice.GetAsyncOperationRequest, _ ...grpc.CallOption) (*cloudservice.GetAsyncOperationResponse, error) {
	f.note(ctx, "GetAsyncOperation")
	if f.onOperation != nil {
		return f.onOperation(r)
	}
	return nil, status.Error(codes.NotFound, "missing operation")
}
func pending(id string) *operation.AsyncOperation {
	return &operation.AsyncOperation{Id: id, State: operation.AsyncOperation_STATE_PENDING}
}

type testGuard struct {
	calls int
	run   func(context.Context, func(context.Context) error) error
}

func (g *testGuard) Run(ctx context.Context, _ string, _ ownership.Writer, fn func(context.Context) error) error {
	g.calls++
	if g.run != nil {
		return g.run(ctx, fn)
	}
	return fn(ctx)
}

type testFixture struct {
	adapter *Adapter
	config  Config
	owner   ownership.Context
	intent  ownership.Intent
	desired DesiredNamespace
	binding Binding
	writer  ownership.Writer
	now     time.Time
	fake    *fakeTransport
	guard   *testGuard
}

func fixture(t *testing.T) *testFixture {
	t.Helper()
	now := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	owner, err := ownership.NewContext(ownership.Config{Owner: ownership.Owner{EnvironmentID: "lab", InstallationID: "installation-one", ProjectUID: "project-uid", Lifetime: ownership.Stable}, ResourceRoot: "lab", Scopes: []ownership.Scope{{Provider: ownership.Temporal, AccountID: "acct1", Namespace: "lab-control"}}})
	if err != nil {
		t.Fatal(err)
	}
	desired := DesiredNamespace{RegionID: "aws-ca-central-1", RetentionDays: 7, AuthMode: APIKeyAuth, SearchAttributes: map[string]SearchAttributeType{"Existing": Keyword}}
	fp, err := NamespaceFingerprint(desired)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := owner.Intent(ownership.Temporal, "namespace", "primary", "revision-one", "operation-create", fp, 39)
	if err != nil {
		t.Fatal(err)
	}
	id := intent.Name + ".acct1"
	born := now.Add(-time.Minute)
	writer := ownership.Writer{ID: "writer-one", Owner: owner.Owner(), Epoch: 3, ExpiresAt: now.Add(time.Hour)}
	observation := ownership.Observation{Ref: ownership.ResourceRef{Scope: intent.Scope, Purpose: intent.Purpose, Name: intent.Name, ProviderID: IncarnationID(id, born)}, Owner: owner.Owner(), CreationIdempotencyKey: intent.IdempotencyKey, Revision: intent.Revision, SpecFingerprint: intent.SpecFingerprint, WriterEpoch: writer.Epoch, Version: "version-one"}
	record, err := owner.BindCreated(intent, observation, writer, now)
	if err != nil {
		t.Fatal(err)
	}
	binding := Binding{NamespaceID: id, CreatedAt: born, ProvenanceID: "independently-verified-fixture-receipt", Record: record}
	spec := createSpec(intent.Name, desired)
	spec.CodecServer = &namespace.CodecServerSpec{Endpoint: "https://codec.example.test/"}
	fake := &fakeTransport{ns: &namespace.Namespace{Namespace: id, ResourceVersion: "version-one", Spec: spec, State: resource.ResourceState_RESOURCE_STATE_ACTIVE, CreatedTime: timestamppb.New(born), Tags: map[string]string{"team": "test"}}}
	guard := &testGuard{}
	config := Config{Ownership: owner, Bindings: []Binding{binding}, AllowedRegions: []string{desired.RegionID}, Capabilities: Capabilities{UpdateNamespaces: true, DeleteNamespaces: true, DisableDeleteProtection: true}, MaximumRetentionDays: 7, CallTimeout: 5 * time.Second, Guard: guard, Now: func() time.Time { return now }}
	adapter, err := New(config, fake)
	if err != nil {
		t.Fatal(err)
	}
	return &testFixture{adapter, config, owner, intent, desired, binding, writer, now, fake, guard}
}

func (f *testFixture) write() WriteRequest {
	return WriteRequest{NamespaceID: f.binding.NamespaceID, ExpectedIncarnationID: f.binding.Record.Ref.ProviderID, ExpectedVersion: f.fake.ns.ResourceVersion, ExpectedRevision: f.binding.Record.Revision, OperationKey: "operation-update", Writer: f.writer}
}
func (f *testFixture) creation(t *testing.T) *Adapter {
	t.Helper()
	cfg := f.config
	cfg.Bindings = nil
	cfg.PlannedCreates = []ownership.Intent{f.intent}
	cfg.CreationProjectID = "approved-test-project"
	cfg.Capabilities.CreateNamespaces = true
	a, err := New(cfg, f.fake)
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func pointer[T any](value T) *T { return &value }
func requireError(t *testing.T, err error, code ErrorCode) *Error {
	t.Helper()
	var typed *Error
	if !errors.As(err, &typed) || typed.Code != code {
		t.Fatalf("got %v; want %s", err, code)
	}
	return typed
}

func TestObservationPinsIncarnationAndCopiesProviderValues(t *testing.T) {
	f := fixture(t)
	view, err := f.adapter.Observe(context.Background(), f.binding.NamespaceID)
	if err != nil {
		t.Fatal(err)
	}
	if !view.Exists || view.IncarnationID != f.binding.Record.Ref.ProviderID || view.ResourceVersion != "version-one" {
		t.Fatalf("unverified observation: %+v", view)
	}
	view.Tags["team"] = "changed"
	view.SearchAttributes["Existing"] = Int
	if f.fake.ns.Tags["team"] != "test" || f.fake.ns.Spec.SearchAttributes["Existing"] != namespace.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_KEYWORD {
		t.Fatal("observation aliases provider response")
	}
	f.fake.ns.CreatedTime = timestamppb.New(f.binding.CreatedAt.Add(time.Second))
	_, err = f.adapter.Observe(context.Background(), f.binding.NamespaceID)
	requireError(t, err, OwnershipConflict)
}

func TestNamespaceUpdateUsesExactVersionsAndPreservesUnownedSpecFields(t *testing.T) {
	f := fixture(t)
	patch := Patch{RetentionDays: pointer(int32(5)), FairnessEnabled: pointer(true), SearchAttributes: map[string]SearchAttributeType{"OrderID": Keyword}}
	first, err := f.adapter.StartUpdate(context.Background(), f.write(), patch)
	if err != nil {
		t.Fatal(err)
	}
	second, err := f.adapter.StartUpdate(context.Background(), f.write(), patch)
	if err != nil {
		t.Fatal(err)
	}
	if first.Receipt.ID != second.Receipt.ID || first.Done || first.MutationProtection != "ProviderVersionAndExternalGuard" {
		t.Fatalf("bad mutation receipt: %+v", first)
	}
	request := f.fake.updateRequests[0]
	if request.Namespace != f.binding.NamespaceID || request.ResourceVersion != "version-one" || request.AsyncOperationId != first.Receipt.ID {
		t.Fatalf("missing exact target/version/idempotency: %+v", request)
	}
	if len(request.Spec.Replicas) != 1 || len(request.Spec.Regions) != 0 || request.Spec.CodecServer.Endpoint != "https://codec.example.test/" || request.Spec.SearchAttributes["Existing"] != namespace.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_KEYWORD || request.Spec.SearchAttributes["OrderID"] != namespace.NamespaceSpec_SEARCH_ATTRIBUTE_TYPE_KEYWORD {
		t.Fatal("patch destroyed unrelated provider state")
	}
	if f.fake.ns.Spec.RetentionDays != 7 || f.fake.ns.Spec.SearchAttributes["OrderID"] != 0 {
		t.Fatal("update modified the observation in place")
	}
	if f.fake.unbounded || f.guard.calls != 2 {
		t.Fatal("provider calls lack deadlines or external writer guard")
	}
}

func TestInvalidOwnershipAndStaleSnapshotsNeverMutate(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*testFixture, *WriteRequest)
		code   ErrorCode
	}{
		{"foreign target", func(f *testFixture, r *WriteRequest) { r.NamespaceID = "production.acct1" }, OwnershipConflict},
		{"foreign incarnation", func(f *testFixture, r *WriteRequest) { r.ExpectedIncarnationID = "foreign-id" }, OwnershipConflict},
		{"stale revision", func(f *testFixture, r *WriteRequest) { r.ExpectedRevision = "revision-zero" }, StaleSnapshot},
		{"stale version", func(f *testFixture, r *WriteRequest) { r.ExpectedVersion = "old-version" }, StaleSnapshot},
		{"foreign writer", func(f *testFixture, r *WriteRequest) { r.Writer.Owner.ProjectUID = "other" }, OwnershipConflict},
		{"stale epoch", func(f *testFixture, r *WriteRequest) { r.Writer.Epoch-- }, OwnershipConflict},
		{"expired writer", func(f *testFixture, r *WriteRequest) { r.Writer.ExpiresAt = f.now }, OwnershipConflict},
		{"reused name", func(f *testFixture, r *WriteRequest) {
			f.fake.ns.CreatedTime = timestamppb.New(f.binding.CreatedAt.Add(time.Minute))
		}, OwnershipConflict},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := fixture(t)
			request := f.write()
			test.change(f, &request)
			_, err := f.adapter.StartUpdate(context.Background(), request, Patch{FairnessEnabled: pointer(true)})
			requireError(t, err, test.code)
			if len(f.fake.updateRequests) != 0 {
				t.Fatal("invalid request mutated provider")
			}
		})
	}
	f := fixture(t)
	_, err := f.adapter.Observe(context.Background(), "production.acct1")
	requireError(t, err, OwnershipConflict)
	if len(f.fake.calls) != 0 {
		t.Fatal("unbound target caused a provider call")
	}
}

func TestSearchAttributeRemovalAndTypeChangesFailExplicitly(t *testing.T) {
	f := fixture(t)
	_, err := f.adapter.StartUpdate(context.Background(), f.write(), Patch{RemoveSearchAttributes: []string{"Existing"}})
	requireError(t, err, SearchAttributeRemovalUnsupported)
	if len(f.fake.calls) != 0 {
		t.Fatal("unsupported removal made a provider request")
	}
	_, err = f.adapter.StartUpdate(context.Background(), f.write(), Patch{SearchAttributes: map[string]SearchAttributeType{"Existing": Int}})
	requireError(t, err, SearchAttributeTypeConflict)
	if len(f.fake.updateRequests) != 0 {
		t.Fatal("type conflict mutated namespace")
	}
}

func TestTagsAreGloballyGatedAndNeverClaimProviderCAS(t *testing.T) {
	f := fixture(t)
	_, err := f.adapter.StartTags(context.Background(), f.write(), map[string]string{"team": "lab"}, nil)
	requireError(t, err, CapabilityDisabled)
	if len(f.fake.calls) != 0 {
		t.Fatal("disabled global tag authority made a call")
	}
	cfg := f.config
	cfg.Capabilities.GlobalTagWrites = true
	cfg.Guard = nil
	_, err = New(cfg, f.fake)
	requireError(t, err, InvalidConfiguration)
	cfg.Guard = f.guard
	a, err := New(cfg, f.fake)
	if err != nil {
		t.Fatal(err)
	}
	result, err := a.StartTags(context.Background(), f.write(), map[string]string{"team": "lab"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.MutationProtection != "ExternalSerializationOnly" || f.guard.calls != 1 || len(f.fake.tagRequests) != 1 {
		t.Fatal("tag request bypassed guard or misrepresented protection")
	}
	if f.fake.tagRequests[0].ProtoReflect().Descriptor().Fields().ByName("resource_version") != nil {
		t.Fatal("SDK contract changed: review tag version semantics")
	}
	_, err = a.StartTags(context.Background(), f.write(), map[string]string{"registry-owner": "forged"}, nil)
	requireError(t, err, InvalidRequest)
}

func TestExternalGuardMustRunAndCannotSmuggleRawErrors(t *testing.T) {
	f := fixture(t)
	f.guard.run = func(context.Context, func(context.Context) error) error { return nil }
	_, err := f.adapter.StartUpdate(context.Background(), f.write(), Patch{FairnessEnabled: pointer(true)})
	requireError(t, err, WriterGuardFailed)
	if len(f.fake.calls) != 0 {
		t.Fatal("unexecuted guard made provider call")
	}
	f.guard.run = func(context.Context, func(context.Context) error) error { return errors.New("Bearer very-secret-key") }
	_, err = f.adapter.StartUpdate(context.Background(), f.write(), Patch{FairnessEnabled: pointer(true)})
	if strings.Contains(err.Error(), "very-secret") {
		t.Fatal("guard error leaked credentials")
	}
}

func TestDeleteProtectionAndConfirmedAbsenceAreSeparate(t *testing.T) {
	f := fixture(t)
	_, err := f.adapter.StartDelete(context.Background(), f.write())
	requireError(t, err, DeleteProtectionEnabled)
	if len(f.fake.deleteRequests) != 0 {
		t.Fatal("delete implicitly disabled protection")
	}
	f.fake.ns.Spec.Lifecycle.EnableDeleteProtection = false
	request := f.write()
	result, err := f.adapter.StartDelete(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Done || f.fake.deleteRequests[0].ResourceVersion != "version-one" {
		t.Fatal("requested delete reported confirmed absence")
	}
	f.fake.ns = nil
	result, err = f.adapter.StartDelete(context.Background(), request)
	if err != nil || !result.Done || result.State != "AlreadyAbsent" || len(f.fake.deleteRequests) != 1 {
		t.Fatalf("absence not idempotent: %+v %v", result, err)
	}
}

func TestTypedProviderErrorsAreSafeAndDoNotRetryRejectedWritesBlindly(t *testing.T) {
	for _, test := range []struct {
		grpcCode       codes.Code
		want           ErrorCode
		retry, recover bool
	}{
		{codes.Unauthenticated, AuthenticationFailed, false, false},
		{codes.PermissionDenied, PermissionDenied, false, false},
		{codes.Aborted, StaleSnapshot, false, false},
		{codes.InvalidArgument, InvalidRequest, false, false},
		{codes.Unavailable, ProviderUnavailable, true, true},
		{codes.DeadlineExceeded, ProviderUnavailable, true, true},
		{codes.ResourceExhausted, ProviderThrottled, true, true},
	} {
		t.Run(test.grpcCode.String(), func(t *testing.T) {
			f := fixture(t)
			f.fake.onUpdate = func(*cloudservice.UpdateNamespaceRequest) (*cloudservice.UpdateNamespaceResponse, error) {
				return nil, status.Error(test.grpcCode, "Bearer very-secret-key payload")
			}
			_, err := f.adapter.StartUpdate(context.Background(), f.write(), Patch{FairnessEnabled: pointer(true)})
			typed := requireError(t, err, test.want)
			if typed.Retryable != test.retry || typed.RecoveryRequired != test.recover || typed.Receipt.ID == "" {
				t.Fatalf("wrong error semantics: %+v", typed)
			}
			safe, _ := json.Marshal(typed.Diagnostic("failure-id", "namespace-update", f.now))
			if strings.Contains(string(safe), "very-secret") || strings.Contains(err.Error(), "payload") {
				t.Fatal("raw provider message leaked")
			}
		})
	}
}

func TestCreateUsesCurrentReplicaAPIAndPersistedOperationIdentity(t *testing.T) {
	f := fixture(t)
	a := f.creation(t)
	first, err := a.StartCreate(context.Background(), f.intent, f.desired, f.writer)
	if err != nil {
		t.Fatal(err)
	}
	second, err := a.StartCreate(context.Background(), f.intent, f.desired, f.writer)
	if err != nil {
		t.Fatal(err)
	}
	request := f.fake.createRequests[0]
	if first.Receipt.ID != second.Receipt.ID || !proto.Equal(request, f.fake.createRequests[1]) || request.Spec.Name != f.intent.Name || request.ProjectId != "approved-test-project" || request.Spec.Replicas[0].Region != f.desired.RegionID || len(request.Spec.Regions) != 0 || !request.Spec.Lifecycle.EnableDeleteProtection || !request.Spec.ApiKeyAuth.Enabled || len(request.Tags) != 0 {
		t.Fatalf("unsafe create request: %+v", request)
	}
	if first.MutationProtection != "IdempotencyAndExternalGuard" || len(f.fake.calls) != 2 || f.fake.calls[0] != "CreateNamespace" {
		t.Fatal("create adopted by name or claimed provider fencing")
	}
}

func TestDefaultTestCapabilitiesDisableCreationBeforeRPC(t *testing.T) {
	f := fixture(t)
	cfg := f.config
	cfg.Bindings = nil
	cfg.PlannedCreates = []ownership.Intent{f.intent}
	cfg.Capabilities = Capabilities{}
	cfg.Guard = nil
	a, err := New(cfg, f.fake)
	if err != nil {
		t.Fatal(err)
	}
	_, err = a.StartCreate(context.Background(), f.intent, f.desired, f.writer)
	requireError(t, err, CapabilityDisabled)
	if len(f.fake.calls) != 0 {
		t.Fatal("test capabilities invoked account namespace creation")
	}
}

func TestUnknownCreateRecoveryRequiresExactProviderReceiptAndBirthWindow(t *testing.T) {
	f := fixture(t)
	a := f.creation(t)
	var accepted *cloudservice.CreateNamespaceRequest
	f.fake.onCreate = func(request *cloudservice.CreateNamespaceRequest) (*cloudservice.CreateNamespaceResponse, error) {
		accepted = proto.Clone(request).(*cloudservice.CreateNamespaceRequest)
		return nil, status.Error(codes.Unavailable, "connection lost after acceptance")
	}
	_, err := a.StartCreate(context.Background(), f.intent, f.desired, f.writer)
	typed := requireError(t, err, ProviderUnavailable)
	if !typed.RecoveryRequired || typed.Receipt.ID == "" {
		t.Fatal("unknown outcome lost recovery identity")
	}
	payload, err := anypb.New(accepted)
	if err != nil {
		t.Fatal(err)
	}
	f.fake.onOperation = func(request *cloudservice.GetAsyncOperationRequest) (*cloudservice.GetAsyncOperationResponse, error) {
		if request.AsyncOperationId != accepted.AsyncOperationId {
			t.Fatal("recovered another operation")
		}
		return &cloudservice.GetAsyncOperationResponse{AsyncOperation: &operation.AsyncOperation{Id: request.AsyncOperationId, State: operation.AsyncOperation_STATE_FULFILLED, OperationInput: payload, StartedTime: timestamppb.New(f.binding.CreatedAt.Add(-time.Second)), FinishedTime: timestamppb.New(f.binding.CreatedAt.Add(time.Second))}}, nil
	}
	recovered, err := a.RecoverCreate(context.Background(), f.intent, f.desired, f.writer)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Binding.Record.Ref.ProviderID != f.binding.Record.Ref.ProviderID || recovered.Binding.ProvenanceID != typed.Receipt.ID || !recovered.Operation.Done || len(f.fake.createRequests) != 1 {
		t.Fatal("unknown outcome recreated/adopted without receipt")
	}
	if !reflect.DeepEqual(f.fake.calls, []string{"CreateNamespace", "GetAsyncOperation", "GetNamespace"}) {
		t.Fatalf("unexpected recovery sequence %v", f.fake.calls)
	}
	f.fake.ns.CreatedTime = timestamppb.New(f.binding.CreatedAt.Add(-time.Hour))
	_, err = a.RecoverCreate(context.Background(), f.intent, f.desired, f.writer)
	requireError(t, err, UnprovenCreateOutcome)
}

func TestCreateRecoveryNeverAdoptsOnlyByNameOrMasksAuthentication(t *testing.T) {
	f := fixture(t)
	a := f.creation(t)
	_, err := a.RecoverCreate(context.Background(), f.intent, f.desired, f.writer)
	requireError(t, err, UnprovenCreateOutcome)
	if !reflect.DeepEqual(f.fake.calls, []string{"GetAsyncOperation"}) {
		t.Fatal("missing receipt fell back to name adoption")
	}
	f.fake.onOperation = func(*cloudservice.GetAsyncOperationRequest) (*cloudservice.GetAsyncOperationResponse, error) {
		return nil, status.Error(codes.PermissionDenied, "Bearer secret")
	}
	_, err = a.RecoverCreate(context.Background(), f.intent, f.desired, f.writer)
	typed := requireError(t, err, PermissionDenied)
	if typed.Retryable {
		t.Fatal("terminal auth failure converted to retryable recovery")
	}
}

func TestPollingIsBoundedAndRejectsForgedReceipts(t *testing.T) {
	f := fixture(t)
	result, err := f.adapter.StartUpdate(context.Background(), f.write(), Patch{FairnessEnabled: pointer(true)})
	if err != nil {
		t.Fatal(err)
	}
	f.fake.onOperation = func(r *cloudservice.GetAsyncOperationRequest) (*cloudservice.GetAsyncOperationResponse, error) {
		return &cloudservice.GetAsyncOperationResponse{AsyncOperation: &operation.AsyncOperation{Id: r.AsyncOperationId, State: operation.AsyncOperation_STATE_IN_PROGRESS, CheckDuration: durationpb.New(time.Hour)}}, nil
	}
	op, err := f.adapter.Poll(context.Background(), result.Receipt)
	if err != nil || op.PollAfter > 30*time.Second || op.Done {
		t.Fatalf("bad poll contract: %+v %v", op, err)
	}
	f.fake.calls = nil
	forged := result.Receipt
	forged.NamespaceID = "production.acct1"
	_, err = f.adapter.Poll(context.Background(), forged)
	requireError(t, err, OwnershipConflict)
	if len(f.fake.calls) != 0 {
		t.Fatal("foreign operation receipt reached provider")
	}
	f.fake.onOperation = func(r *cloudservice.GetAsyncOperationRequest) (*cloudservice.GetAsyncOperationResponse, error) {
		return &cloudservice.GetAsyncOperationResponse{AsyncOperation: &operation.AsyncOperation{Id: r.AsyncOperationId, State: operation.AsyncOperation_STATE_FAILED, FailureReason: "password=secret"}}, nil
	}
	_, err = f.adapter.Poll(context.Background(), result.Receipt)
	requireError(t, err, OperationFailed)
	if strings.Contains(err.Error(), "password") {
		t.Fatal("raw async failure leaked")
	}
}

func TestSDKConnectionRequiresExplicitSecureConfiguration(t *testing.T) {
	t.Setenv("TEMPORAL_API_KEY", "ambient-production-key")
	if _, err := NewCredentials(""); err == nil {
		t.Fatal("ambient credentials used")
	}
	credentials, err := NewCredentials("explicit-test-key")
	if err != nil {
		t.Fatal(err)
	}
	for _, rendered := range []string{fmt.Sprint(credentials), fmt.Sprintf("%#v", credentials), func() string { data, _ := json.Marshal(credentials); return string(data) }()} {
		if strings.Contains(rendered, "explicit-test-key") {
			t.Fatal("credentials formatting leaked key")
		}
	}
	if cloudclient.DefaultAPIVersion() != SupportedAPIVersion {
		t.Fatal("SDK API contract changed; review adapter")
	}
	for _, options := range []ConnectionOptions{
		{APIVersion: SupportedAPIVersion, Credentials: credentials},
		{Endpoint: "localhost:8443", Credentials: credentials},
		{Endpoint: "localhost:8443", APIVersion: SupportedAPIVersion},
		{Endpoint: "localhost:8443", APIVersion: SupportedAPIVersion, Credentials: credentials, TLSConfig: &tls.Config{InsecureSkipVerify: true}},
	} {
		_, _, err := NewSDKTransport(options)
		requireError(t, err, InvalidConfiguration)
	}
	// Official SDK construction is lazy: no RPC, DNS lookup or cloud endpoint
	// is used by this test. It only verifies the actual generated transport fits.
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS10}
	transport, close, err := NewSDKTransport(ConnectionOptions{Endpoint: "localhost:8443", APIVersion: SupportedAPIVersion, Credentials: credentials, TLSConfig: tlsConfig})
	if err != nil || transport == nil || close == nil {
		t.Fatalf("actual SDK transport did not build: %v", err)
	}
	if err := close(); err != nil {
		t.Fatal(err)
	}
	if tlsConfig.MinVersion != tls.VersionTLS10 {
		t.Fatal("connection mutated caller TLS configuration")
	}
}
