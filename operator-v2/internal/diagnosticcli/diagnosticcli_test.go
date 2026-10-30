package diagnosticcli

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"testing"
	"time"

	c "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/catalogdiagnostics"
	d "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubejournal"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
)

func localBootstrap(t *testing.T) Bootstrap {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		t.Fatal(err)
	}
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	pk, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	key := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pk})
	// More than pipe capacity: only a started consumer may receive this wire.
	ca := bytes.Repeat(cert, 20)
	secrets, err := NewSecretInputs(ca, cert, key, []byte(strings.Repeat("PRIVATE_SESSION_KEY", 3)), "header.payload.signature")
	if err != nil {
		t.Fatal(err)
	}
	defer secrets.Close()
	owner := ownership.Owner{EnvironmentID: "lab", InstallationID: "test", ProjectUID: "control", Lifetime: ownership.Disposable, RunID: "owned-run"}
	scope := ownership.Scope{Provider: ownership.Kubernetes, ClusterID: "owned-cluster"}
	parent := ownership.Record{Ref: ownership.ResourceRef{Scope: scope, Purpose: "namespace", Name: "lab-control", ProviderID: "control-uid"}, Owner: owner, CreationOperationID: "control-bootstrap", CreationIdempotencyKey: "control-key", Revision: "control-v1", SpecFingerprint: strings.Repeat("a", 64), WriterEpoch: 1}
	m := Metadata{Mode: LocalMode, Target: c.Target{Environment: "lab", Installation: "test", ProjectUID: "project"}, Owner: owner, ResourceRoot: "lab", Cluster: kube.ClusterIdentity{ID: "owned-cluster", KubeSystemUID: "kubesystem-uid"}, Endpoint: "https://127.0.0.1:6443", ControlNamespace: parent, ControlAttempt: "control-bootstrap", Issuer: "https://operator.example.invalid", Audience: "operator-local", AllowedDomain: "example.invalid", SessionSeconds: 3600}
	ns := ownership.NamespaceParent{ClusterID: scope.ClusterID, Name: parent.Ref.Name, UID: parent.Ref.ProviderID, Owner: owner, CreationOperationID: parent.CreationOperationID, CreationIdempotencyKey: parent.CreationIdempotencyKey}
	child := func(name, revision string) ownership.NamespacedRecord {
		return ownership.NamespacedRecord{Parent: ns, Record: ownership.Record{Ref: ownership.ResourceRef{Scope: ownership.Scope{Provider: ownership.Kubernetes, ClusterID: scope.ClusterID, Namespace: parent.Ref.Name}, Purpose: "configmap", Name: name, ProviderID: name + "-uid"}, Owner: owner, CreationOperationID: "cell-bootstrap", CreationIdempotencyKey: "cell-key", Revision: revision, SpecFingerprint: strings.Repeat("b", 64), WriterEpoch: 1}}
	}
	m.Journal = child(kubejournal.Name(rolloutSubject(m.Target)), kubejournal.Revision)
	m.Desired = child(c.DesiredCellName(m.Target), c.DesiredRevision)
	m.Policy = child(c.PolicyCellName(m.Target), c.PolicyRevision)
	b, err := NewBootstrap(m, secrets)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)
	return b
}

func TestPrivateBootstrapRoundtripBoundsExactShapeAndFormatting(t *testing.T) {
	b := localBootstrap(t)
	var wire bytes.Buffer
	if err := b.WritePrivate(&wire); err != nil || wire.Len() <= 8192 {
		t.Fatal("private bounded bootstrap not produced", err)
	}
	copy, err := DecodeBootstrap(bytes.NewReader(wire.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	defer copy.Close()
	var again bytes.Buffer
	if err = copy.WritePrivate(&again); err != nil || !bytes.Equal(wire.Bytes(), again.Bytes()) {
		t.Fatal("private pipe roundtrip changed pins")
	}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%p"} {
		for _, value := range []any{b, &b, b.private.secrets, &b.private.secrets} {
			formatted := fmt.Sprintf(verb, value)
			if strings.Contains(formatted, "PRIVATE_SESSION_KEY") || strings.Contains(formatted, "header.payload.signature") {
				t.Fatal("bootstrap fmt leaked")
			}
		}
	}
	if _, err = json.Marshal(b); err == nil {
		t.Fatal("bootstrap JSON logging allowed")
	}
	for _, data := range [][]byte{bytes.Repeat([]byte("x"), MaxBootstrapBytes+1), append(append([]byte(nil), wire.Bytes()...), []byte("{}")...), bytes.Replace(wire.Bytes(), []byte(`"cookie":`), []byte(`"COOKIE":`), 1), bytes.Replace(wire.Bytes(), []byte(`"cookie":`), []byte(`"cookie":"private","cookie":`), 1), []byte(`{"unknown":"PRIVATE_ERROR"}`)} {
		if _, err := DecodeBootstrap(bytes.NewReader(data)); err == nil || strings.Contains(err.Error(), "PRIVATE_ERROR") {
			t.Fatal("invalid private wire accepted or leaked")
		}
	}
	for _, change := range []func(*Metadata){func(m *Metadata) { m.Mode = "production" }, func(m *Metadata) { m.Owner.Lifetime = ownership.Stable }, func(m *Metadata) { m.Target.ProjectUID = "../foreign" }, func(m *Metadata) { m.Policy.Parent.UID = "foreign" }, func(m *Metadata) { m.Endpoint = "https://foreign.invalid:443" }, func(m *Metadata) { m.ResourceRoot = "tmprl-dem-cld" }} {
		m := b.private.metadata
		change(&m)
		if _, err := NewBootstrap(m, b.private.secrets); err == nil {
			t.Fatal("unsafe scope accepted")
		}
	}
	b.Close()
	if err := b.WritePrivate(&bytes.Buffer{}); err == nil {
		t.Fatal("closed bootstrap reopened")
	}
	if err := copy.WritePrivate(&bytes.Buffer{}); err != nil {
		t.Fatal("closing source invalidated independent decoded copy")
	}
}

type authFunction func(context.Context, *http.Request, c.Target) (c.ReadGrant, error)

func (f authFunction) AuthorizeRead(ctx context.Context, r *http.Request, t c.Target) (c.ReadGrant, error) {
	return f(ctx, r, t)
}

type sourceFunction func(context.Context, c.ReadGrant) (c.Snapshot, error)

func (f sourceFunction) Read(ctx context.Context, g c.ReadGrant) (c.Snapshot, error) {
	return f(ctx, g)
}

func TestAuthorizationBeforeSourceAndSafeHumanJSONSeparation(t *testing.T) {
	now := time.Now().UTC()
	target := c.Target{Environment: "lab", Installation: "test", ProjectUID: "project"}
	calls := 0
	id := d.Identity{Environment: target.Environment, Installation: target.Installation, ProjectUID: target.ProjectUID, ProjectName: "demo", OperationID: "attempt-one", Generation: 1, ReconcileKey: "accepted-v1"}
	p, _ := d.NewProjection(id)
	p, _ = p.Apply(d.OperationEvent{Identity: id, Sequence: 1, OccurredAt: now, State: d.Failed, Step: "observe", Failure: &d.DomainFailure{ID: "cause", Code: "ChildUnavailable", Category: d.Unknown, Message: "The Kubernetes API denied access.", Step: "observe", OccurredAt: now, NextAction: "Restore the scoped read permission."}, Warnings: []d.DomainFailure{{ID: "warning", Code: "ChildMissing", Message: "The Kubernetes API reported the requested resource was not found.", OccurredAt: now}}})
	model, err := c.NewModel(sourceFunction(func(context.Context, c.ReadGrant) (c.Snapshot, error) {
		calls++
		return c.Snapshot{Target: target, Origin: c.Materialized, Operation: &p}, nil
	}), c.Config{Now: func() time.Time { return now }, MaxProgressAge: time.Minute, MaxDesiredAge: time.Minute, MaxAvailabilityAge: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	denied := authFunction(func(context.Context, *http.Request, c.Target) (c.ReadGrant, error) {
		return c.ReadGrant{}, c.ErrForbidden
	})
	if _, err := readAuthorized(context.Background(), denied, model, target, "header.payload.signature"); err == nil || err.Error() != "LocalDiagnosticAccessDenied" || calls != 0 {
		t.Fatal("denial disclosed source")
	}
	auth := authFunction(func(_ context.Context, r *http.Request, actual c.Target) (c.ReadGrant, error) {
		if actual != target || r.Header.Get("Cookie") != "temporal_demo_auth=header.payload.signature" {
			t.Fatal("session or target substituted")
		}
		return c.IssueReadGrant("synthetic-verified", "policy", target, now.Add(-time.Minute), now.Add(time.Minute))
	})
	view, err := readAuthorized(context.Background(), auth, model, target, "header.payload.signature")
	if err != nil || calls != 1 {
		t.Fatal(err)
	}
	view.CreateHistory = &c.CreateHistoryView{Scope: "HistoricalCreateRecords", Groups: []c.CreateHistoryGroup{{Kind: "Service", Outcome: "CanonicalReceiptRecorded", Count: 1}}}
	var human, out bytes.Buffer
	if WriteView(&human, view, false) != nil || WriteView(&out, view, true) != nil {
		t.Fatal("safe report failed")
	}
	for _, part := range []string{PrivilegeNotice, "Original cause [ChildUnavailable]", "Additional warning [ChildMissing]", "Next action:", "Historical create records (not current existence", "Automatic retry: Unknown", "Persistence time was not recorded"} {
		if !strings.Contains(human.String(), part) {
			t.Fatal("actionable independent fact omitted")
		}
	}
	exact, _ := view.JSON()
	if !bytes.Equal(exact, out.Bytes()) || bytes.Contains(out.Bytes(), []byte("SESSION_KEY")) {
		t.Fatal("JSON is not the exact safe read model")
	}
	if err := readError(errors.New("PRIVATE_PROVIDER_ERROR")); err.Error() != "LocalDiagnosticSourceUnavailable" {
		t.Fatal("raw error leaked")
	}
}
