package projectbootstrap

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/testinfra/fixtureplan"
	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	identity "go.temporal.io/cloud-sdk/api/identity/v1"
	resource "go.temporal.io/cloud-sdk/api/resource/v1"
	"go.temporal.io/cloud-sdk/cloudclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type NamespaceApproval struct {
	Action              string    `json:"action"`
	RequestSHA256       string    `json:"requestSHA256"`
	ParentBindingSHA256 string    `json:"parentBindingSHA256"`
	NamespaceID         string    `json:"namespaceID"`
	AsyncID             string    `json:"asyncID"`
	AccountID           string    `json:"accountID"`
	UserID              string    `json:"userID"`
	DispatchReceiptPath string    `json:"dispatchReceiptPath"`
	ExpiresAt           time.Time `json:"expiresAt"`
}

type NamespaceOptions struct {
	Options
	// Strict private reduced NamespaceProjectBinding JSON, whose bytes are
	// separately coordinator-approved. No live name lookup can produce it.
	ParentBindingPath string
}

type NamespaceReceipt struct {
	Schema              string    `json:"schema"`
	Status              string    `json:"status"`
	RequestSHA256       string    `json:"requestSHA256"`
	ParentBindingSHA256 string    `json:"parentBindingSHA256"`
	ProjectID           string    `json:"projectID"`
	ProjectCreatedTime  string    `json:"projectCreatedTime"`
	NamespaceID         string    `json:"namespaceID"`
	AsyncID             string    `json:"asyncID"`
	AccountID           string    `json:"accountID"`
	UserID              string    `json:"userID"`
	RecordedAt          time.Time `json:"recordedAt"`
	GRPCCode            string    `json:"grpcCode,omitempty"`
}

type namespaceClient interface {
	GetCurrentIdentity(context.Context, *cloudservice.GetCurrentIdentityRequest, ...grpc.CallOption) (*cloudservice.GetCurrentIdentityResponse, error)
	GetNamespace(context.Context, *cloudservice.GetNamespaceRequest, ...grpc.CallOption) (*cloudservice.GetNamespaceResponse, error)
	GetAsyncOperation(context.Context, *cloudservice.GetAsyncOperationRequest, ...grpc.CallOption) (*cloudservice.GetAsyncOperationResponse, error)
	GetProject(context.Context, *cloudservice.GetProjectRequest, ...grpc.CallOption) (*cloudservice.GetProjectResponse, error)
	CreateNamespace(context.Context, *cloudservice.CreateNamespaceRequest, ...grpc.CallOption) (*cloudservice.CreateNamespaceResponse, error)
}

// ExecuteNamespace is a separate human bootstrap action. It cannot be used by
// test principals or the Project-only command. Root approval must bind exact
// request AND immutable parent attestation bytes. No broad discovery, quota
// assumptions, region fallback, key creation or deletion is implemented here.
func ExecuteNamespace(ctx context.Context, o NamespaceOptions) (NamespaceReceipt, error) {
	if err := tracingDisabled(); err != nil {
		return NamespaceReceipt{}, err
	}
	return executeNamespace(ctx, o, time.Now, func(s *session, req *cloudservice.CreateNamespaceRequest, binding fixtureplan.NamespaceProjectBinding) (namespaceClient, *attemptCredentials, func() error, error) {
		c, budget, err := namespaceSDKClient(s, req, binding, endpoint, &tls.Config{MinVersion: tls.VersionTLS12}, true)
		if err != nil {
			return nil, nil, nil, err
		}
		return c.CloudService(), budget, c.Close, nil
	})
}

// The namespace client's interceptor seals not just methods but every read and
// write target before auth is requested. All req/binding data captured here is
// copied. The raw SDK client remains package-private.
func namespaceSDKClient(s *session, req *cloudservice.CreateNamespaceRequest, binding fixtureplan.NamespaceProjectBinding, host string, tc *tls.Config, zeroBuffer bool) (*cloudclient.Client, *attemptCredentials, error) {
	return scopedSDKClient(s, host, tc, zeroBuffer, cloudservice.CloudService_CreateNamespace_FullMethodName, namespaceRequestValidator(req, binding))
}

func namespaceRequestValidator(req *cloudservice.CreateNamespaceRequest, binding fixtureplan.NamespaceProjectBinding) grpc.UnaryClientInterceptor {
	expected := proto.Clone(req).(*cloudservice.CreateNamespaceRequest)
	nsID := expected.GetSpec().GetName() + "." + AccountID
	validate := func(ctx context.Context, method string, request, response any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		var allowed bool
		switch method {
		case cloudservice.CloudService_GetCurrentIdentity_FullMethodName:
			v, ok := request.(*cloudservice.GetCurrentIdentityRequest)
			allowed = ok && proto.Equal(v, &cloudservice.GetCurrentIdentityRequest{})
		case cloudservice.CloudService_GetNamespace_FullMethodName:
			v, ok := request.(*cloudservice.GetNamespaceRequest)
			allowed = ok && (proto.Equal(v, &cloudservice.GetNamespaceRequest{Namespace: AnchorNamespace}) || proto.Equal(v, &cloudservice.GetNamespaceRequest{Namespace: nsID}))
		case cloudservice.CloudService_GetProject_FullMethodName:
			v, ok := request.(*cloudservice.GetProjectRequest)
			allowed = ok && proto.Equal(v, &cloudservice.GetProjectRequest{ProjectId: binding.ProjectID})
		case cloudservice.CloudService_GetAsyncOperation_FullMethodName:
			v, ok := request.(*cloudservice.GetAsyncOperationRequest)
			allowed = ok && proto.Equal(v, &cloudservice.GetAsyncOperationRequest{AsyncOperationId: expected.AsyncOperationId})
		case cloudservice.CloudService_CreateNamespace_FullMethodName:
			v, ok := request.(*cloudservice.CreateNamespaceRequest)
			allowed = ok && proto.Equal(v, expected)
		}
		if !allowed {
			return Error("NamespaceBootstrapRequestForbidden")
		}
		return invoker(ctx, method, request, response, cc, opts...)
	}
	return validate
}

func executeNamespace(ctx context.Context, o NamespaceOptions, now func() time.Time, connect func(*session, *cloudservice.CreateNamespaceRequest, fixtureplan.NamespaceProjectBinding) (namespaceClient, *attemptCredentials, func() error, error)) (NamespaceReceipt, error) {
	var empty NamespaceReceipt
	var approval NamespaceApproval
	if err := readPrivateJSON(o.ApprovalPath, &approval); err != nil {
		return empty, err
	}
	start := now()
	if approval.Action != "CreateNamespaceOnce" || approval.AccountID != AccountID || approval.UserID != UserID || approval.DispatchReceiptPath != o.ReceiptPath || !approval.ExpiresAt.After(start) || approval.ExpiresAt.After(start.Add(24*time.Hour)) {
		return empty, Error("NamespaceApprovalInvalid")
	}
	bindingFile, err := openPrivate(o.ParentBindingPath)
	if err != nil {
		return empty, err
	}
	bindingRaw, readErr := boundedRead(bindingFile)
	_ = bindingFile.Close()
	if readErr != nil {
		return empty, readErr
	}
	bindingHash := sha256.Sum256(bindingRaw)
	if approval.ParentBindingSHA256 != hex.EncodeToString(bindingHash[:]) {
		return empty, Error("NamespaceParentBindingDigestMismatch")
	}
	var binding fixtureplan.NamespaceProjectBinding
	decoder := json.NewDecoder(bytes.NewReader(bindingRaw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&binding) != nil {
		return empty, Error("NamespaceParentBindingInvalid")
	}
	// Binding file must contain exactly one canonical approved JSON object.
	canonical, err := json.MarshalIndent(binding, "", "  ")
	if err != nil || !bytes.Equal(append(canonical, '\n'), bindingRaw) {
		return empty, Error("NamespaceParentBindingInvalid")
	}
	drafts, _, err := fixtureplan.NamespaceCreateReview(binding)
	if err != nil {
		return empty, Error("NamespaceParentBindingInvalid")
	}
	var selected *fixtureplan.NamespaceDraft
	for i := range drafts {
		if drafts[i].NamespaceID == approval.NamespaceID && drafts[i].AsyncID == approval.AsyncID && drafts[i].RequestSHA256 == approval.RequestSHA256 {
			selected = &drafts[i]
		}
	}
	if selected == nil {
		return empty, Error("NamespaceIntentNotApproved")
	}
	raw, err := readRequest(o.RequestPath)
	if err != nil || !bytes.Equal(raw, selected.RequestJSON) {
		return empty, Error("NamespaceRequestMismatch")
	}
	request := &cloudservice.CreateNamespaceRequest{}
	if protojson.Unmarshal(raw, request) != nil || request.ProjectId != binding.ProjectID {
		return empty, Error("NamespaceRequestMismatch")
	}
	if err := receiptAbsent(o.ReceiptPath); err != nil {
		return empty, Error("NamespaceDispatchPathRejectedOrRecorded")
	}
	s, err := loadSession(o.OAuthConfigPath, o.Profile, start)
	if err != nil {
		return empty, err
	}
	deadline := start.Add(20 * time.Second)
	for _, bound := range []time.Time{approval.ExpiresAt, s.private.expiry.Add(-30 * time.Second)} {
		if bound.Before(deadline) {
			deadline = bound
		}
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	c, budget, closeClient, err := connect(s, request, binding)
	if err != nil {
		return empty, Error("NamespaceTransportInvalid")
	}
	defer closeClient()
	budget.deadline = deadline
	current, err := c.GetCurrentIdentity(ctx, &cloudservice.GetCurrentIdentityRequest{})
	u := current.GetUser()
	if err != nil || current.GetPrincipalApiKey() != nil || u.GetId() != UserID || u.GetState() != resource.ResourceState_RESOURCE_STATE_ACTIVE || u.GetSpec().GetAccess().GetAccountAccess().GetRole() != identity.AccountAccess_ROLE_ADMIN {
		return empty, Error("NamespaceOAuthIdentityUnverified")
	}
	anchor, err := c.GetNamespace(ctx, &cloudservice.GetNamespaceRequest{Namespace: AnchorNamespace})
	if err != nil || anchor.GetNamespace().GetNamespace() != AnchorNamespace || anchor.GetNamespace().GetProjectId() != ProtectedProject || anchor.GetNamespace().GetState() != resource.ResourceState_RESOURCE_STATE_ACTIVE {
		return empty, Error("NamespaceAccountAnchorMismatch")
	}
	parent, err := c.GetProject(ctx, &cloudservice.GetProjectRequest{ProjectId: binding.ProjectID})
	expectedProjectRaw, _, renderErr := fixtureplan.ProjectCreateReview()
	expectedProject := &cloudservice.CreateProjectRequest{}
	if renderErr != nil || protojson.Unmarshal(expectedProjectRaw, expectedProject) != nil {
		return empty, Error("NamespaceParentIntentInvalid")
	}
	p := parent.GetProject()
	born, _ := time.Parse(time.RFC3339Nano, binding.CreatedTime)
	if err != nil || p.GetId() != binding.ProjectID || p.GetCreatedTime() == nil || p.CreatedTime.CheckValid() != nil || !p.CreatedTime.AsTime().Equal(born) || p.ResourceVersion != binding.ResourceVersion || p.State != resource.ResourceState_RESOURCE_STATE_ACTIVE || !proto.Equal(p.Spec, expectedProject.Spec) {
		return empty, Error("NamespaceParentObservationMismatch")
	}
	_, err = c.GetNamespace(ctx, &cloudservice.GetNamespaceRequest{Namespace: selected.NamespaceID})
	if status.Code(err) != codes.NotFound {
		return empty, Error("NamespaceAlreadyExistsOrUnproven")
	}
	_, err = c.GetAsyncOperation(ctx, &cloudservice.GetAsyncOperationRequest{AsyncOperationId: selected.AsyncID})
	if status.Code(err) != codes.NotFound {
		return empty, Error("NamespaceOperationAlreadyKnownOrUnproven")
	}
	if ctx.Err() != nil || !deadline.After(now()) || !s.private.expiry.After(now().Add(30*time.Second)) {
		return empty, Error("NamespaceBootstrapDeadlineExpired")
	}
	receipt := NamespaceReceipt{Schema: "registry-namespace-bootstrap-v1", Status: "DispatchMayHaveOccurred", RequestSHA256: selected.RequestSHA256, ParentBindingSHA256: approval.ParentBindingSHA256, ProjectID: binding.ProjectID, ProjectCreatedTime: binding.CreatedTime, NamespaceID: selected.NamespaceID, AsyncID: selected.AsyncID, AccountID: AccountID, UserID: UserID, RecordedAt: now().UTC()}
	initialJSON, _ := json.Marshal(receipt)
	if bytes.Contains(initialJSON, []byte(s.private.token)) || bytes.Contains(raw, []byte(s.private.token)) {
		return empty, Error("OAuthUnsafeMetadataOverlap")
	}
	if err := writeExclusiveReceipt(o.ReceiptPath, receipt); err != nil {
		return empty, err
	}
	budget.armed.Store(true)
	response, createErr := c.CreateNamespace(ctx, request)
	receipt.Status = "UnknownCreateOutcome"
	receipt.RecordedAt = now().UTC()
	code := status.Code(createErr)
	if code < codes.OK || code > codes.Unauthenticated {
		code = codes.Unknown
	}
	receipt.GRPCCode = code.String()
	if createErr == nil && response != nil && response.Namespace == selected.NamespaceID && response.GetAsyncOperation().GetId() == selected.AsyncID {
		receipt.Status = "SubmittedNamespaceUnattested"
	}
	encoded, _ := json.Marshal(receipt)
	if bytes.Contains(encoded, []byte(s.private.token)) {
		receipt.Status, receipt.GRPCCode = "UnknownCreateOutcome", ""
	}
	if writeExclusiveReceipt(o.ReceiptPath+".outcome.json", receipt) != nil {
		return receipt, Error("NamespaceOutcomePersistenceUnknown")
	}
	if receipt.Status != "SubmittedNamespaceUnattested" {
		return receipt, Error("NamespaceCreateOutcomeUnknown")
	}
	// This is not an owner binding. Exact async echoed input + observed birth
	// inside its operation window still require independent attestation. Parent
	// pre-read is not atomic provider fencing; bootstrap lifecycle is serialized.
	return receipt, nil
}
