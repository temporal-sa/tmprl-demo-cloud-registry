package awspermission

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go/middleware"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/testinfra/fixtureplan"
)

func credentialBody(t *testing.T, f *fakeFixture) []byte {
	t.Helper()
	b, e := json.Marshal(struct {
		Version                                    int
		AccessKeyId, SecretAccessKey, SessionToken string
		Expiration                                 time.Time
	}{1, bootstrapKey, bootstrapSecret, bootstrapToken, f.now.Add(time.Hour)})
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func TestExplicitCredentialDecodeAndOpaqueBoundaries(t *testing.T) {
	f := fixture(t, Seed)
	body := credentialBody(t, f)
	m, e := DecodeBootstrapCredentials(bytes.NewReader(body), f.config.Now, f.config.ApprovedUntil)
	if e != nil {
		t.Fatal(e)
	}
	defer m.Close()
	for _, v := range []any{m, &m, struct{ Material Credentials }{m}} {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%p", "%T"} {
			out := fmt.Sprintf(verb, v)
			for _, raw := range []string{bootstrapKey, bootstrapSecret, bootstrapToken} {
				if strings.Contains(out, raw) {
					t.Fatal("formatter leaked credential")
				}
			}
		}
		if _, e := json.Marshal(v); e == nil {
			t.Fatal("credential JSON accepted")
		}
	}
	if _, e = m.MarshalText(); e == nil {
		t.Fatal("credential text accepted")
	}
	if _, e = m.Retrieve(context.Background()); e != Denied {
		t.Fatal("ambient credential access accepted")
	}
	ctx, p := operationContext(context.Background(), m, SourceIdentity)
	if _, e = m.Retrieve(ctx); e != nil {
		t.Fatal(e)
	}
	p.live.Store(false)
	if _, e = m.Retrieve(ctx); e != Denied {
		t.Fatal("spent operation credential access accepted")
	}
	ctx, _ = operationContext(context.Background(), m, SourceIdentity)
	m.Close()
	if _, e = m.Retrieve(ctx); e != Denied {
		t.Fatal("closed material usable")
	}
	for _, bad := range [][]byte{append(append([]byte{}, body...), body...), []byte(`{"Version":1,"Version":1}`), []byte(strings.Replace(string(body), `"Version":1`, `"Version":1,"version":1`, 1)), []byte(strings.Replace(string(body), `"Version":1`, `"Version":1,"Unexpected":0`, 1)), bytes.Repeat([]byte("x"), (1<<20)+1), []byte("null")} {
		if _, e := DecodeBootstrapCredentials(bytes.NewReader(bad), f.config.Now, f.config.ApprovedUntil); e == nil {
			t.Fatal("ambiguous credential input accepted")
		}
	}
}
func TestSafeMetadataCannotEchoCredentialIntoDurableReceipt(t *testing.T) {
	f := fixture(t, Seed)
	f.bootstrap.private.token = f.config.ApprovalSHA256
	if _, e := configured(f.config, f.bootstrap, f.log, f.factory); e != PrivacyDenied {
		t.Fatal("credential equal to durable digest accepted")
	}
	f = fixture(t, Seed)
	h, e := configured(f.config, f.bootstrap, f.log, f.factory)
	if e != nil {
		t.Fatal(e)
	}
	token := "11111111-2222-3333-4444-555555555555"
	f.bootstrap.private.token = token
	r, e := h.private.call(context.Background(), f.bootstrap, SourceIdentity, fixtureplan.ProvisionerRole, nil, func(context.Context) (callResult, error) { return callResult{"Allowed", "", token}, nil })
	if e != PrivacyDenied || len(f.log.finished) != 0 || r.RequestID != "" {
		t.Fatal("credential echo was persisted/returned")
	}
	// Public provider errors may contain arbitrary secrets. Only finite code and
	// a syntax-constrained nonmaterial request ID can survive providerResult.
	out, e := providerResult(fmt.Errorf("%s", bootstrapSecret), middleware.Metadata{}, false, f.bootstrap)
	if e != ProviderFailure || strings.Contains(fmt.Sprint(out), bootstrapSecret) {
		t.Fatal("provider text escaped")
	}
}

type roundTripper func(*http.Request) (*http.Response, error)

func (fn roundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return fn(r) }
func sdkRequest(ctx context.Context, m Credentials, service, action string) *http.Request {
	r, _ := http.NewRequestWithContext(ctx, "POST", "https://"+service+"."+fixtureplan.Region+".amazonaws.com/", strings.NewReader("Action="+action+"&Version=2011-06-15"))
	r.Header.Set("X-Amz-Security-Token", m.private.token)
	r.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+m.private.key+"/synthetic")
	if service != "sts" {
		prefix := "secretsmanager."
		if service == "kms" {
			prefix = "TrentService."
		}
		r.Header.Set("X-Amz-Target", prefix+action)
	}
	return r
}
func TestHTTPBoundaryFiniteMethodsIdentityAndSingleAttempt(t *testing.T) {
	for _, kind := range []string{"success", "ambient", "foreign-material", "wrong-host", "host-override", "plaintext", "wrong-action", "wrong-method", "canceled", "expired", "closed", "second-attempt"} {
		t.Run(kind, func(t *testing.T) {
			f := fixture(t, Seed)
			calls := 0
			g := &sdkHTTP{material: f.bootstrap, config: f.config, client: &http.Client{Transport: roundTripper(func(*http.Request) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("ok")), Header: http.Header{}}, nil
			})}}
			ctx, _ := operationContext(context.Background(), f.bootstrap, SourceIdentity)
			r := sdkRequest(ctx, f.bootstrap, "sts", "GetCallerIdentity")
			switch kind {
			case "ambient":
				r = r.WithContext(context.Background())
			case "foreign-material":
				other := fixture(t, Seed)
				ctx, _ = operationContext(context.Background(), other.bootstrap, SourceIdentity)
				r = r.WithContext(ctx)
			case "wrong-host":
				r.URL.Host = "sts.us-east-1.amazonaws.com"
			case "host-override":
				r.Host = "foreign.invalid"
			case "plaintext":
				r.URL.Scheme = "http"
			case "wrong-action":
				r.Body = io.NopCloser(strings.NewReader("Action=AssumeRole"))
			case "wrong-method":
				r.Method = "GET"
			case "canceled":
				c, cancel := context.WithCancel(ctx)
				cancel()
				r = r.WithContext(c)
			case "expired":
				f.now = f.config.ApprovedUntil
			case "closed":
				f.bootstrap.Close()
			}
			out, e := g.Do(r)
			if out != nil {
				out.Body.Close()
			}
			if kind == "success" || kind == "second-attempt" {
				if e != nil || calls != 1 {
					t.Fatal("allowed request failed")
				}
				if _, e = g.Do(sdkRequest(ctx, f.bootstrap, "sts", "GetCallerIdentity")); e != Denied || calls != 1 {
					t.Fatal("transparent replay not bounded")
				}
			} else if e != Denied || calls != 0 {
				t.Fatal("boundary denial reached HTTP client")
			}
		})
	}
}
func TestActualGeneratedSDKUsesExplicitSignerAndDoesNotReplay(t *testing.T) {
	f := fixture(t, Seed)
	calls := 0
	g := &sdkHTTP{material: f.bootstrap, config: f.config, client: &http.Client{Transport: roundTripper(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.Host != "sts."+fixtureplan.Region+".amazonaws.com" || !strings.Contains(r.Header.Get("Authorization"), bootstrapKey) || r.Header.Get("X-Amz-Security-Token") != bootstrapToken {
			t.Fatal("generated SDK changed signing scope")
		}
		return &http.Response{StatusCode: 503, Header: http.Header{"Content-Type": []string{"text/xml"}}, Body: io.NopCloser(strings.NewReader(`<ErrorResponse><Error><Code>ServiceUnavailable</Code><Message>inert</Message></Error></ErrorResponse>`)), Request: r}, nil
	})}}
	api := sdkClientsWithGuard(f.bootstrap, g)
	ctx, p := operationContext(context.Background(), f.bootstrap, SourceIdentity)
	if _, e := api.sts.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{}, noSTSRetry); e == nil || calls != 1 {
		t.Fatal("SDK unavailable response retried or succeeded")
	}
	p.live.Store(false)
	if _, e := api.sts.GetCallerIdentity(context.Background(), &sts.GetCallerIdentityInput{}, noSTSRetry); e == nil || calls != 1 {
		t.Fatal("SDK accessed ambient authority")
	}
}
func TestPrivateJournalReopenUncertaintyAndNoReset(t *testing.T) {
	dir := t.TempDir()
	if e := os.Chmod(dir, 0700); e != nil {
		t.Fatal(e)
	}
	root, e := os.OpenRoot(dir)
	if e != nil {
		t.Fatal(e)
	}
	defer root.Close()
	j, b, e := BootstrapJournal(root, hash("independent-bootstrap"), hash("approved-spec"))
	if e != nil {
		t.Fatal(e)
	}
	if _, _, e = BootstrapJournal(root, b.BootstrapUID, b.SpecSHA256); e == nil {
		t.Fatal("bootstrap reset existing journal")
	}
	r := Receipt{Step: SourceIdentity, SpecSHA256: b.SpecSHA256, RequestSHA256: hash("request"), TargetSHA256: hash("target"), Outcome: "Requested", At: time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)}
	if e = j.begin(context.Background(), r); e != nil {
		t.Fatal(e)
	}
	reopened, e := OpenJournal(root, b)
	if e != nil {
		t.Fatal(e)
	}
	if e = reopened.begin(context.Background(), r); e != AlreadyAttempted {
		t.Fatal("reopened request replay accepted")
	}
	r.Outcome = "Allowed"
	r.At = r.At.Add(time.Second)
	j.fault = func(string) error { return Unproven }
	if e = j.finish(context.Background(), r); e != Unproven {
		t.Fatal("lost ack reported success")
	}
	if e = reopened.finish(context.Background(), r); e != AlreadyAttempted {
		t.Fatal("uncertain finish replay accepted")
	}
	raw, e := os.ReadFile(filepath.Join(dir, string(SourceIdentity)+".receipt.json"))
	if e != nil || !bytes.Contains(raw, []byte(`"Allowed"`)) {
		t.Fatal("uncertain terminal data not retained")
	}
	bad := b
	bad.BootstrapUID = hash("other")
	if _, e = OpenJournal(root, bad); e == nil {
		t.Fatal("foreign pinned bootstrap adopted")
	}
	other := t.TempDir()
	os.Chmod(other, 0700)
	otherRoot, e := os.OpenRoot(other)
	if e != nil {
		t.Fatal(e)
	}
	defer otherRoot.Close()
	if _, e = OpenJournal(otherRoot, b); e == nil {
		t.Fatal("missing companion silently created")
	}
	if e = os.Remove(filepath.Join(dir, "identity.json")); e != nil {
		t.Fatal(e)
	}
	if e = os.Symlink(filepath.Join(dir, string(SourceIdentity)+".receipt.json"), filepath.Join(dir, "identity.json")); e != nil {
		t.Fatal(e)
	}
	if _, e = OpenJournal(root, b); e == nil {
		t.Fatal("symlink identity accepted")
	}
}
