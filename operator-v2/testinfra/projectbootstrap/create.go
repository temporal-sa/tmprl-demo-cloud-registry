package projectbootstrap

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/testinfra/fixtureplan"
	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	identity "go.temporal.io/cloud-sdk/api/identity/v1"
	resource "go.temporal.io/cloud-sdk/api/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	AccountID        = "a2dd6"
	UserID           = "6fc7d24d8fbe4e1588536c5130a6fc81"
	AnchorNamespace  = "tmprl-dem-cld-registry-operator.a2dd6"
	ProtectedProject = "eca773c382124db79bbfab0b429faa5a"
)

// Approval is written privately by the reviewing coordinator. It is a local
// review artifact, not a cryptographic signature or provider authorization.
type Approval struct {
	Action        string    `json:"action"`
	RequestSHA256 string    `json:"requestSHA256"`
	AccountID     string    `json:"accountID"`
	UserID        string    `json:"userID"`
	ExpiresAt     time.Time `json:"expiresAt"`
}

type Options struct {
	// Every path is explicit and absolute. The profile is never ambient.
	OAuthConfigPath string
	Profile         string
	ApprovalPath    string
	RequestPath     string
	ReceiptPath     string
}

// Receipt is safe to serialize. It never contains OAuth, provider error text,
// unknown proto fields, arbitrary resource versions or the returned full object.
type Receipt struct {
	Schema        string    `json:"schema"`
	Status        string    `json:"status"`
	RequestSHA256 string    `json:"requestSHA256"`
	AsyncID       string    `json:"asyncID"`
	AccountID     string    `json:"accountID"`
	UserID        string    `json:"userID"`
	RecordedAt    time.Time `json:"recordedAt"`
	ProjectID     string    `json:"projectID,omitempty"`
	GRPCCode      string    `json:"grpcCode,omitempty"`
}

type client interface {
	GetCurrentIdentity(context.Context, *cloudservice.GetCurrentIdentityRequest, ...grpc.CallOption) (*cloudservice.GetCurrentIdentityResponse, error)
	GetNamespace(context.Context, *cloudservice.GetNamespaceRequest, ...grpc.CallOption) (*cloudservice.GetNamespaceResponse, error)
	GetAsyncOperation(context.Context, *cloudservice.GetAsyncOperationRequest, ...grpc.CallOption) (*cloudservice.GetAsyncOperationResponse, error)
	CreateProject(context.Context, *cloudservice.CreateProjectRequest, ...grpc.CallOption) (*cloudservice.CreateProjectResponse, error)
}

// Execute performs exactly the reviewed fixture Project create after read-only
// identity/account checks. It never creates namespaces/keys or adopts by name.
// This is deliberately not a general operator/bootstrap API. No refresh flow,
// account key, environment credential fallback, logging or provider dump exists.
func Execute(ctx context.Context, o Options) (Receipt, error) {
	if err := tracingDisabled(); err != nil {
		return Receipt{}, err
	}
	return execute(ctx, o, time.Now, func(s *session) (client, *attemptCredentials, func() error, error) {
		c, budget, err := sdkClient(s, endpoint, &tls.Config{MinVersion: tls.VersionTLS12}, true)
		if err != nil {
			return nil, nil, nil, err
		}
		return c.CloudService(), budget, c.Close, nil
	})
}

func tracingDisabled() error {
	// gRPC may install environment-controlled binary/debug logging during
	// package initialization. Reject it before credentials are read. The
	// dedicated command also must not be embedded in an instrumented process.
	for _, key := range []string{"GRPC_BINARY_LOG_FILTER", "GRPC_GO_LOG_SEVERITY_LEVEL", "GRPC_GO_LOG_VERBOSITY_LEVEL", "GODEBUG", "SSLKEYLOGFILE"} {
		if value, ok := os.LookupEnv(key); ok && strings.TrimSpace(value) != "" {
			return Error("AmbientTransportTracingForbidden")
		}
	}
	return nil
}

func execute(ctx context.Context, o Options, now func() time.Time, connect func(*session) (client, *attemptCredentials, func() error, error)) (Receipt, error) {
	var empty Receipt
	var approval Approval
	if err := readPrivateJSON(o.ApprovalPath, &approval); err != nil {
		return empty, err
	}
	start := now()
	if approval.Action != "CreateProjectOnce" || approval.AccountID != AccountID || approval.UserID != UserID || !approval.ExpiresAt.After(start) || approval.ExpiresAt.After(start.Add(24*time.Hour)) {
		return empty, Error("ProjectApprovalInvalid")
	}
	raw, _, err := fixtureplan.ProjectCreateReview()
	if err != nil {
		return empty, Error("ProjectIntentInvalid")
	}
	hash := sha256.Sum256(raw)
	if approval.RequestSHA256 != hex.EncodeToString(hash[:]) {
		return empty, Error("ProjectRequestDigestMismatch")
	}
	// Public request is read only after comparing it to the generated exact
	// fixture bytes. It is never a generic caller-supplied resource specification.
	supplied, err := readRequest(o.RequestPath)
	if err != nil || !bytes.Equal(raw, supplied) {
		return empty, Error("ProjectRequestMismatch")
	}
	request := &cloudservice.CreateProjectRequest{}
	if protojson.Unmarshal(raw, request) != nil {
		return empty, Error("ProjectIntentInvalid")
	}
	// Check the receipt before loading credentials. Existing/unknown dispatch
	// markers are tombstones: even a previous pre-dispatch crash needs review.
	if err := receiptAbsent(o.ReceiptPath); err != nil {
		return empty, err
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
	c, budget, closeClient, err := connect(s)
	if err != nil {
		return empty, Error("BootstrapTransportInvalid")
	}
	defer closeClient()
	budget.deadline = deadline // Before any RPC/auth callback; immutable thereafter.
	identityResult, err := c.GetCurrentIdentity(ctx, &cloudservice.GetCurrentIdentityRequest{})
	if err != nil || identityResult.GetPrincipalApiKey() != nil {
		return empty, Error("OAuthIdentityUnverified")
	}
	u := identityResult.GetUser()
	if u.GetId() != UserID || u.GetState() != resource.ResourceState_RESOURCE_STATE_ACTIVE || u.GetSpec().GetAccess().GetAccountAccess().GetRole() != identity.AccountAccess_ROLE_ADMIN {
		return empty, Error("OAuthIdentityMismatch")
	}
	ns, err := c.GetNamespace(ctx, &cloudservice.GetNamespaceRequest{Namespace: AnchorNamespace})
	if err != nil || ns.GetNamespace().GetNamespace() != AnchorNamespace || ns.GetNamespace().GetProjectId() != ProtectedProject || ns.GetNamespace().GetState() != resource.ResourceState_RESOURCE_STATE_ACTIVE {
		return empty, Error("OAuthAccountAnchorMismatch")
	}
	_, err = c.GetAsyncOperation(ctx, &cloudservice.GetAsyncOperationRequest{AsyncOperationId: request.AsyncOperationId})
	if status.Code(err) != codes.NotFound {
		return empty, Error("ProjectOperationAlreadyKnownOrUnproven")
	}
	if ctx.Err() != nil || !approval.ExpiresAt.After(now()) || !s.private.expiry.After(now().Add(30*time.Second)) {
		return empty, Error("BootstrapDeadlineExpired")
	}
	receipt := Receipt{Schema: "registry-project-bootstrap-v1", Status: "DispatchMayHaveOccurred", RequestSHA256: approval.RequestSHA256, AsyncID: request.AsyncOperationId, AccountID: AccountID, UserID: UserID, RecordedAt: now().UTC()}
	initialJSON, _ := json.Marshal(receipt)
	if bytes.Contains(initialJSON, []byte(s.private.token)) || bytes.Contains(raw, []byte(s.private.token)) {
		return empty, Error("OAuthUnsafeMetadataOverlap")
	}
	if err := writeExclusiveReceipt(o.ReceiptPath, receipt); err != nil {
		return empty, err
	}
	budget.armed.Store(true) // Only after the receipt AND its parent directory fsync.
	response, createErr := c.CreateProject(ctx, request)
	// All failures after the durable marker are unknown; a stable async ID does
	// not establish provider replay semantics or recover a lost generated ID.
	receipt.Status = "UnknownCreateOutcome"
	receipt.RecordedAt = now().UTC()
	code := status.Code(createErr)
	if code < codes.OK || code > codes.Unauthenticated {
		code = codes.Unknown
	}
	receipt.GRPCCode = code.String() // Canonical enum only, never status.Message.
	if createErr == nil && response != nil && response.GetAsyncOperation().GetId() == request.AsyncOperationId && projectIDPattern.MatchString(response.GetProjectId()) && response.GetProjectId() != ProtectedProject {
		receipt.ProjectID = response.GetProjectId()
		receipt.Status = "SubmittedProjectUnattested"
	}
	// Reject a malformed provider echo before any persistence/output. Normal ID
	// syntax is also pinned; no provider error/unknown field enters the receipt.
	encoded, _ := json.Marshal(receipt)
	if bytes.Contains(encoded, []byte(s.private.token)) {
		receipt = Receipt{Schema: "registry-project-bootstrap-v1", Status: "UnknownCreateOutcome", RequestSHA256: approval.RequestSHA256, AsyncID: request.AsyncOperationId, AccountID: AccountID, UserID: UserID, RecordedAt: now().UTC()}
	}
	if err := writeExclusiveReceipt(o.ReceiptPath+".outcome.json", receipt); err != nil {
		// A valid new ID is returned to the coordinator for an independent backup
		// receipt. The original marker still forbids any future create attempt.
		return receipt, Error("ProjectOutcomePersistenceUnknown")
	}
	if receipt.Status != "SubmittedProjectUnattested" {
		return receipt, Error("ProjectCreateOutcomeUnknown")
	}
	return receipt, nil
}

var projectIDPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

func readRequest(path string) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, Error("ExplicitRequestPathRequired")
	}
	for p := path; ; p = filepath.Dir(p) {
		info, err := os.Lstat(p)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return nil, Error("ProjectRequestPathRejected")
		}
		if p == filepath.Dir(p) {
			break
		}
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, Error("ProjectRequestReadFailed")
	}
	defer f.Close()
	info, err := f.Stat()
	owner, ok := infoOwner(info)
	if err != nil || !ok || owner != uint32(os.Getuid()) || !info.Mode().IsRegular() || info.Size() > 1<<20 || info.Sys().(*syscall.Stat_t).Nlink != 1 {
		return nil, Error("ProjectRequestFileRejected")
	}
	return boundedRead(f)
}

func receiptAbsent(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return Error("ExplicitReceiptPathRequired")
	}
	for _, p := range []string{path, path + ".outcome.json"} {
		if _, err := os.Lstat(p); !os.IsNotExist(err) {
			return Error("ProjectDispatchAlreadyRecorded")
		}
	}
	for p := filepath.Dir(path); ; p = filepath.Dir(p) {
		info, err := os.Lstat(p)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return Error("ReceiptDirectoryInvalid")
		}
		if p == filepath.Dir(p) {
			break
		}
	}
	info, err := os.Stat(filepath.Dir(path))
	owner, ok := infoOwner(info)
	if err != nil || !ok || owner != uint32(os.Getuid()) || info.Mode().Perm()&0077 != 0 {
		return Error("PrivateReceiptDirectoryRequired")
	}
	return nil
}

func writeExclusiveReceipt(path string, receipt any) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return Error("ProjectReceiptWriteFailed")
	}
	raw, err := json.Marshal(receipt)
	if err == nil {
		_, err = f.Write(append(raw, '\n'))
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		return Error("ProjectReceiptWriteUnknown")
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return Error("ProjectReceiptDirectorySyncUnknown")
	}
	defer dir.Close()
	if dir.Sync() != nil {
		return Error("ProjectReceiptDirectorySyncUnknown")
	}
	return nil
}
