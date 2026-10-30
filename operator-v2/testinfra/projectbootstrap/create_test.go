package projectbootstrap

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/testinfra/fixtureplan"
	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	identity "go.temporal.io/cloud-sdk/api/identity/v1"
	namespace "go.temporal.io/cloud-sdk/api/namespace/v1"
	operation "go.temporal.io/cloud-sdk/api/operation/v1"
	resource "go.temporal.io/cloud-sdk/api/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const syntheticToken = "fixture-only-OPAQUE-OAUTH-SENTINEL-7b2e2fcc"

func privateFile(t *testing.T, dir, name, body string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if os.WriteFile(p, []byte(body), 0600) != nil {
		t.Fatal("test private file unavailable")
	}
	return p
}

func oauthFile(t *testing.T, dir string, expiry time.Time) string {
	return privateFile(t, dir, "oauth.toml", "[profile.default]\napi_key='MUST-NOT-BE-USED'\n[profile.default.oauth]\naccess_token='"+syntheticToken+"'\ntoken_type='Bearer'\nexpires_at='"+expiry.Format(time.RFC3339)+"'\nrefresh_token='MUST-NOT-BE-USED'\ntoken_url='http://untrusted.invalid'\n")
}

func TestOAuthPrivateAndOpaque(t *testing.T) {
	dir, _ := filepath.EvalSymlinks(t.TempDir())
	_ = os.Chmod(dir, 0700)
	now := time.Now()
	p := oauthFile(t, dir, now.Add(time.Hour))
	s, err := loadSession(p, "default", now)
	if err != nil || s.private.token != syntheticToken {
		t.Fatal("synthetic OAuth not loaded")
	}
	budget := &attemptCredentials{session: s, now: time.Now}
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%p", "%#p", "%d", "%T"} {
		for _, item := range []any{s, *s, budget, attemptCredentials{session: s, now: time.Now}, struct{ Session session }{*s}, struct{ Budget *attemptCredentials }{budget}} {
			view := fmt.Sprintf(format, item)
			if strings.Contains(view, syntheticToken) {
				t.Fatal("OAuth formatting leak")
			}
		}
	}
	if _, err := json.Marshal(s); err == nil {
		t.Fatal("OAuth serialized")
	}
	if _, err := loadSession(p, "", now); err == nil {
		t.Fatal("ambient profile accepted")
	}
	if _, err := loadSession(p, "absent", now); err == nil {
		t.Fatal("profile/APIkey fallback accepted")
	}
	_ = os.Chmod(p, 0644)
	if _, err := loadSession(p, "default", now); err == nil {
		t.Fatal("public credential file accepted")
	}
	_ = os.Chmod(p, 0600)
	link := filepath.Join(dir, "link")
	_ = os.Symlink(p, link)
	if _, err := loadSession(link, "default", now); err == nil {
		t.Fatal("symlink credential accepted")
	}
	p = oauthFile(t, dir, now.Add(time.Minute))
	if _, err := loadSession(p, "default", now); err != Error("OAuthSessionRefreshRequired") {
		t.Fatal("short-lived token must require external refresh")
	}
	p = privateFile(t, dir, "broken.toml", "[profile.default.oauth]\naccess_token = '"+syntheticToken)
	if _, err := loadSession(p, "default", now); err == nil || strings.Contains(err.Error(), syntheticToken) {
		t.Fatal("parser failure leaked config or was accepted")
	}
	_ = os.Link(p, filepath.Join(dir, "hardlink"))
	if _, err := loadSession(p, "default", now); err == nil {
		t.Fatal("multiply linked config accepted")
	}
}

func TestRequestReadIsBoundedRegularAndExact(t *testing.T) {
	dir, _ := filepath.EvalSymlinks(t.TempDir())
	_ = os.Chmod(dir, 0700)
	regular := privateFile(t, dir, "request", "safe")
	if b, err := readRequest(regular); err != nil || string(b) != "safe" {
		t.Fatal("regular bounded request rejected")
	}
	link := filepath.Join(dir, "symlink")
	_ = os.Symlink(regular, link)
	big := privateFile(t, dir, "oversized", strings.Repeat("x", (1<<20)+1))
	fifo := filepath.Join(dir, "fifo")
	if syscall.Mkfifo(fifo, 0600) != nil {
		t.Fatal("test fifo unavailable")
	}
	for _, path := range []string{link, big, fifo, dir, "relative"} {
		if _, err := readRequest(path); err == nil {
			t.Fatal("unbounded/symlink/nonregular request accepted")
		}
	}
	_ = os.Link(regular, filepath.Join(dir, "hardlink"))
	if _, err := readRequest(regular); err == nil {
		t.Fatal("multiply linked request accepted")
	}
}

func TestAmbientTracingRejectedBeforeCredentialRead(t *testing.T) {
	t.Setenv("GRPC_BINARY_LOG_FILTER", "*")
	if _, err := Execute(context.Background(), Options{OAuthConfigPath: "/must/not/be/read"}); err != Error("AmbientTransportTracingForbidden") {
		t.Fatal("ambient bearer logging accepted")
	}
}

type fakeClient struct {
	creates      int
	foreignUser  bool
	foreignScope bool
	knownOp      bool
	lost         bool
	badID        string
	beforeCreate func()
}

func (f *fakeClient) GetCurrentIdentity(context.Context, *cloudservice.GetCurrentIdentityRequest, ...grpc.CallOption) (*cloudservice.GetCurrentIdentityResponse, error) {
	id := UserID
	if f.foreignUser {
		id = "another-user"
	}
	return &cloudservice.GetCurrentIdentityResponse{Principal: &cloudservice.GetCurrentIdentityResponse_User{User: &identity.User{Id: id, State: resource.ResourceState_RESOURCE_STATE_ACTIVE, Spec: &identity.UserSpec{Access: &identity.Access{AccountAccess: &identity.AccountAccess{Role: identity.AccountAccess_ROLE_ADMIN}}}}}}, nil
}
func (f *fakeClient) GetNamespace(context.Context, *cloudservice.GetNamespaceRequest, ...grpc.CallOption) (*cloudservice.GetNamespaceResponse, error) {
	id := ProtectedProject
	if f.foreignScope {
		id = "different-project"
	}
	return &cloudservice.GetNamespaceResponse{Namespace: &namespace.Namespace{Namespace: AnchorNamespace, ProjectId: id, State: resource.ResourceState_RESOURCE_STATE_ACTIVE}}, nil
}
func (f *fakeClient) GetAsyncOperation(context.Context, *cloudservice.GetAsyncOperationRequest, ...grpc.CallOption) (*cloudservice.GetAsyncOperationResponse, error) {
	if f.knownOp {
		return &cloudservice.GetAsyncOperationResponse{}, nil
	}
	return nil, status.Error(codes.NotFound, syntheticToken) // Raw provider text must be ignored.
}
func (f *fakeClient) CreateProject(_ context.Context, r *cloudservice.CreateProjectRequest, _ ...grpc.CallOption) (*cloudservice.CreateProjectResponse, error) {
	f.creates++
	if f.beforeCreate != nil {
		f.beforeCreate()
	}
	if f.lost {
		return nil, status.Error(codes.Unavailable, syntheticToken)
	}
	id := "11111111111111111111111111111111"
	if f.badID != "" {
		id = f.badID
	}
	return &cloudservice.CreateProjectResponse{ProjectId: id, AsyncOperation: &operation.AsyncOperation{Id: r.AsyncOperationId, FailureReason: syntheticToken}}, nil
}

func executeFixture(t *testing.T, f *fakeClient) (Options, func() (Receipt, error)) {
	t.Helper()
	dir, _ := filepath.EvalSymlinks(t.TempDir())
	_ = os.Chmod(dir, 0700)
	now := time.Now()
	raw, _, err := fixtureplan.ProjectCreateReview()
	if err != nil {
		t.Fatal("fixture request invalid")
	}
	hash := sha256.Sum256(raw)
	approval, _ := json.Marshal(Approval{Action: "CreateProjectOnce", RequestSHA256: hex.EncodeToString(hash[:]), AccountID: AccountID, UserID: UserID, ExpiresAt: now.Add(time.Hour)})
	o := Options{OAuthConfigPath: oauthFile(t, dir, now.Add(time.Hour)), Profile: "default", ApprovalPath: privateFile(t, dir, "approval.json", string(approval)), RequestPath: privateFile(t, dir, "request.json", string(raw)), ReceiptPath: filepath.Join(dir, "dispatch.json")}
	run := func() (Receipt, error) {
		return execute(context.Background(), o, time.Now, func(s *session) (client, *attemptCredentials, func() error, error) {
			return f, &attemptCredentials{session: s, now: time.Now}, func() error { return nil }, nil
		})
	}
	return o, run
}

func TestDurableDispatchAndReplayTombstone(t *testing.T) {
	for _, lost := range []bool{false, true} {
		t.Run(fmt.Sprint(lost), func(t *testing.T) {
			f := &fakeClient{lost: lost}
			o, run := executeFixture(t, f)
			f.beforeCreate = func() {
				b, err := os.ReadFile(o.ReceiptPath)
				if err != nil || !strings.Contains(string(b), "DispatchMayHaveOccurred") {
					t.Fatal("RPC dispatched before durable intent")
				}
			}
			r, err := run()
			if f.creates != 1 || (err != nil) != lost || (r.Status == "UnknownCreateOutcome") != lost {
				t.Fatal("incorrect safe create outcome")
			}
			wantCode := "OK"
			if lost {
				wantCode = "Unavailable"
			}
			if r.GRPCCode != wantCode {
				t.Fatal("safe canonical provider code missing")
			}
			for _, p := range []string{o.ReceiptPath, o.ReceiptPath + ".outcome.json"} {
				b, e := os.ReadFile(p)
				info, _ := os.Stat(p)
				if e != nil || strings.Contains(string(b), syntheticToken) || info.Mode().Perm() != 0600 {
					t.Fatal("unsafe/missing private receipt")
				}
			}
			if _, e := run(); e != Error("ProjectDispatchAlreadyRecorded") || f.creates != 1 {
				t.Fatal("persisted dispatch was replayed")
			}
		})
	}
}

func TestPreflightAndResponseFailuresNeverAdopt(t *testing.T) {
	for _, test := range []struct {
		name string
		f    fakeClient
		want int
	}{
		{"foreign-user", fakeClient{foreignUser: true}, 0},
		{"foreign-account-anchor", fakeClient{foreignScope: true}, 0},
		{"known-operation", fakeClient{knownOp: true}, 0},
		{"protected-project", fakeClient{badID: ProtectedProject}, 1},
		{"token-project-echo", fakeClient{badID: syntheticToken}, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, run := executeFixture(t, &test.f)
			r, err := run()
			b, _ := json.Marshal(r)
			if err == nil || test.f.creates != test.want || strings.Contains(err.Error(), syntheticToken) || strings.Contains(string(b), syntheticToken) || r.ProjectID != "" {
				t.Fatal("unsafe preflight/create failure")
			}
		})
	}
}

func TestReceiptPersistenceUnknownDoesNotPermitReplay(t *testing.T) {
	f := &fakeClient{}
	o, run := executeFixture(t, f)
	f.beforeCreate = func() { _ = os.Mkdir(o.ReceiptPath+".outcome.json", 0700) }
	r, err := run()
	if err != Error("ProjectOutcomePersistenceUnknown") || r.ProjectID == "" || f.creates != 1 {
		t.Fatal("must retain safe returned ID when outcome persistence fails")
	}
	if _, err = run(); err != Error("ProjectDispatchAlreadyRecorded") || f.creates != 1 {
		t.Fatal("unknown outcome receipt allowed replay")
	}
}
