package projectbootstrap

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/testinfra/fixtureplan"
	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	namespace "go.temporal.io/cloud-sdk/api/namespace/v1"
	operation "go.temporal.io/cloud-sdk/api/operation/v1"
	project "go.temporal.io/cloud-sdk/api/project/v1"
	resource "go.temporal.io/cloud-sdk/api/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func fakeParent(t *testing.T) fixtureplan.NamespaceProjectBinding {
	t.Helper()
	raw, review, err := fixtureplan.ProjectCreateReview()
	if err != nil {
		t.Fatal("test fixture invalid")
	}
	hash := sha256.Sum256(raw)
	return fixtureplan.NamespaceProjectBinding{ProjectID: "11111111111111111111111111111111", CreatedTime: "2026-10-30T02:35:03.281016994Z", ResourceVersion: "5c1130d8-2094-4328-b550-4082f6a3662a", AsyncOperationID: review["asyncOperationID"].(string), ApprovedRequestSHA256: hex.EncodeToString(hash[:]), State: "RESOURCE_STATE_ACTIVE", AsyncState: "STATE_FULFILLED", OperationInputExactSpecAndAsyncVerified: true, DeleteProtectionEnabled: true}
}

type fakeNamespaceClient struct {
	fakeClient
	binding               fixtureplan.NamespaceProjectBinding
	parentBirthDrift      bool
	parentVersionDrift    bool
	parentProtectionDrift bool
	exists                bool
	responseID            string
	responseAsync         string
}

func (f *fakeNamespaceClient) GetNamespace(ctx context.Context, r *cloudservice.GetNamespaceRequest, opts ...grpc.CallOption) (*cloudservice.GetNamespaceResponse, error) {
	if r.Namespace == AnchorNamespace {
		return f.fakeClient.GetNamespace(ctx, r, opts...)
	}
	if f.exists {
		return &cloudservice.GetNamespaceResponse{Namespace: &namespace.Namespace{Namespace: r.Namespace}}, nil
	}
	return nil, status.Error(codes.NotFound, syntheticToken)
}
func (f *fakeNamespaceClient) GetProject(context.Context, *cloudservice.GetProjectRequest, ...grpc.CallOption) (*cloudservice.GetProjectResponse, error) {
	raw, _, _ := fixtureplan.ProjectCreateReview()
	req := &cloudservice.CreateProjectRequest{}
	_ = protojson.Unmarshal(raw, req)
	born, _ := time.Parse(time.RFC3339Nano, f.binding.CreatedTime)
	if f.parentBirthDrift {
		born = born.Add(time.Second)
	}
	version := f.binding.ResourceVersion
	if f.parentVersionDrift {
		version = "other-version"
	}
	if f.parentProtectionDrift {
		req.Spec.Lifecycle.EnableDeleteProtection = false
	}
	return &cloudservice.GetProjectResponse{Project: &project.Project{Id: f.binding.ProjectID, CreatedTime: timestamppb.New(born), ResourceVersion: version, State: resource.ResourceState_RESOURCE_STATE_ACTIVE, Spec: req.Spec}}, nil
}
func (f *fakeNamespaceClient) CreateNamespace(_ context.Context, req *cloudservice.CreateNamespaceRequest, _ ...grpc.CallOption) (*cloudservice.CreateNamespaceResponse, error) {
	f.creates++
	if f.beforeCreate != nil {
		f.beforeCreate()
	}
	if f.lost {
		return nil, status.Error(codes.Unavailable, syntheticToken)
	}
	id, async := req.Spec.Name+"."+AccountID, req.AsyncOperationId
	if f.responseID != "" {
		id = f.responseID
	}
	if f.responseAsync != "" {
		async = f.responseAsync
	}
	return &cloudservice.CreateNamespaceResponse{Namespace: id, AsyncOperation: &operation.AsyncOperation{Id: async, FailureReason: syntheticToken}}, nil
}

func namespaceFixture(t *testing.T, f *fakeNamespaceClient) (NamespaceOptions, func(NamespaceOptions) (NamespaceReceipt, error)) {
	t.Helper()
	dir, _ := filepath.EvalSymlinks(t.TempDir())
	_ = os.Chmod(dir, 0700)
	now := time.Now()
	f.binding = fakeParent(t)
	bindingRaw, _ := json.MarshalIndent(f.binding, "", "  ")
	bindingRaw = append(bindingRaw, '\n')
	bindingHash := sha256.Sum256(bindingRaw)
	drafts, _, err := fixtureplan.NamespaceCreateReview(f.binding)
	if err != nil {
		t.Fatal("test namespace drafts invalid")
	}
	draft := drafts[0]
	o := NamespaceOptions{Options: Options{OAuthConfigPath: oauthFile(t, dir, now.Add(time.Hour)), Profile: "default", RequestPath: privateFile(t, dir, "request.json", string(draft.RequestJSON)), ReceiptPath: filepath.Join(dir, "dispatch.json")}, ParentBindingPath: privateFile(t, dir, "parent.json", string(bindingRaw))}
	a := NamespaceApproval{Action: "CreateNamespaceOnce", RequestSHA256: draft.RequestSHA256, ParentBindingSHA256: hex.EncodeToString(bindingHash[:]), NamespaceID: draft.NamespaceID, AsyncID: draft.AsyncID, AccountID: AccountID, UserID: UserID, ExpiresAt: now.Add(time.Hour), DispatchReceiptPath: o.ReceiptPath}
	approvalJSON, _ := json.Marshal(a)
	o.ApprovalPath = privateFile(t, dir, "approval.json", string(approvalJSON))
	run := func(opts NamespaceOptions) (NamespaceReceipt, error) {
		return executeNamespace(context.Background(), opts, time.Now, func(s *session, _ *cloudservice.CreateNamespaceRequest, _ fixtureplan.NamespaceProjectBinding) (namespaceClient, *attemptCredentials, func() error, error) {
			return f, &attemptCredentials{session: s, now: time.Now, mutationMethod: cloudservice.CloudService_CreateNamespace_FullMethodName, projectRead: true}, func() error { return nil }, nil
		})
	}
	return o, run
}

func TestNamespaceDispatchDurableUnknownAndNeverReplay(t *testing.T) {
	for _, lost := range []bool{false, true} {
		f := &fakeNamespaceClient{fakeClient: fakeClient{lost: lost}}
		o, run := namespaceFixture(t, f)
		f.beforeCreate = func() {
			b, err := os.ReadFile(o.ReceiptPath)
			if err != nil || !strings.Contains(string(b), "DispatchMayHaveOccurred") {
				t.Fatal("namespace dispatched before durable intent")
			}
		}
		r, err := run(o)
		if f.creates != 1 || (err != nil) != lost || (r.Status == "UnknownCreateOutcome") != lost {
			t.Fatal("namespace create outcome incorrect")
		}
		b, _ := json.Marshal(r)
		if strings.Contains(string(b), syntheticToken) {
			t.Fatal("namespace result leaked bearer")
		}
		if _, err = run(o); err != Error("NamespaceDispatchPathRejectedOrRecorded") || f.creates != 1 {
			t.Fatal("namespace process replayed")
		}
		other := o
		other.ReceiptPath += "-different"
		if _, err = run(other); err != Error("NamespaceApprovalInvalid") || f.creates != 1 {
			t.Fatal("same approval bypassed process tombstone using new path")
		}
	}
}

func TestNamespacePreflightAndEchoFailClosed(t *testing.T) {
	for _, tc := range []struct {
		name    string
		f       fakeNamespaceClient
		creates int
	}{
		{"birth-drift", fakeNamespaceClient{parentBirthDrift: true}, 0},
		{"version-drift", fakeNamespaceClient{parentVersionDrift: true}, 0},
		{"protection-drift", fakeNamespaceClient{parentProtectionDrift: true}, 0},
		{"existing-namespace", fakeNamespaceClient{exists: true}, 0},
		{"foreign-response", fakeNamespaceClient{responseID: AnchorNamespace}, 1},
		{"token-response", fakeNamespaceClient{responseID: syntheticToken}, 1},
		{"foreign-async", fakeNamespaceClient{responseAsync: "another-operation"}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, run := namespaceFixture(t, &tc.f)
			r, err := run(o)
			b, _ := json.Marshal(r)
			if err == nil || tc.f.creates != tc.creates || strings.Contains(err.Error(), syntheticToken) || strings.Contains(string(b), syntheticToken) || r.Status == "SubmittedNamespaceUnattested" {
				t.Fatal("namespace ownership/preflight/echo accepted")
			}
		})
	}
}

func TestNamespaceRequestAndBindingCannotBeSwapped(t *testing.T) {
	for _, which := range []string{"request", "binding", "action"} {
		f := &fakeNamespaceClient{}
		o, run := namespaceFixture(t, f)
		switch which {
		case "request":
			raw, _ := os.ReadFile(o.RequestPath)
			_ = os.WriteFile(o.RequestPath, []byte(strings.ReplaceAll(string(raw), f.binding.ProjectID, ProtectedProject)), 0600)
		case "binding":
			raw, _ := os.ReadFile(o.ParentBindingPath)
			_ = os.WriteFile(o.ParentBindingPath, append(raw, ' '), 0600)
		case "action":
			raw, _ := os.ReadFile(o.ApprovalPath)
			_ = os.WriteFile(o.ApprovalPath, []byte(strings.ReplaceAll(string(raw), "CreateNamespaceOnce", "CreateProjectOnce")), 0600)
		}
		if _, err := run(o); err == nil || f.creates != 0 {
			t.Fatal("namespace request/binding/action substitution accepted")
		}
	}
}

func TestNamespaceTransportSealsExactPayloadTargetsWithoutNetwork(t *testing.T) {
	binding := fakeParent(t)
	drafts, _, _ := fixtureplan.NamespaceCreateReview(binding)
	req := &cloudservice.CreateNamespaceRequest{}
	_ = protojson.Unmarshal(drafts[0].RequestJSON, req)
	validate := namespaceRequestValidator(req, binding)
	// Later caller mutation must not change the captured create capability.
	foreign := proto.Clone(req).(*cloudservice.CreateNamespaceRequest)
	foreign.ProjectId = ProtectedProject
	req.Spec.RetentionDays = 90
	for _, tc := range []struct {
		method string
		req    any
	}{
		{cloudservice.CloudService_CreateNamespace_FullMethodName, foreign},
		{cloudservice.CloudService_CreateNamespace_FullMethodName, req},
		{cloudservice.CloudService_GetProject_FullMethodName, &cloudservice.GetProjectRequest{ProjectId: ProtectedProject}},
		{cloudservice.CloudService_GetNamespace_FullMethodName, &cloudservice.GetNamespaceRequest{Namespace: "other.a2dd6"}},
		{cloudservice.CloudService_GetAsyncOperation_FullMethodName, &cloudservice.GetAsyncOperationRequest{AsyncOperationId: "other-op"}},
		{cloudservice.CloudService_CreateProject_FullMethodName, &cloudservice.CreateProjectRequest{}},
		{cloudservice.CloudService_DeleteNamespace_FullMethodName, &cloudservice.DeleteNamespaceRequest{}},
	} {
		called := false
		err := validate(context.Background(), tc.method, tc.req, nil, nil, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			called = true
			return nil
		})
		if err != Error("NamespaceBootstrapRequestForbidden") || called {
			t.Fatal("namespace transport exposed foreign target/global method before auth")
		}
	}
	allowed := &cloudservice.CreateNamespaceRequest{}
	_ = protojson.Unmarshal(drafts[0].RequestJSON, allowed)
	called := false
	if validate(context.Background(), cloudservice.CloudService_CreateNamespace_FullMethodName, allowed, nil, nil, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
		called = true
		return nil
	}) != nil || !called {
		t.Fatal("exact immutable captured intent rejected")
	}
}
