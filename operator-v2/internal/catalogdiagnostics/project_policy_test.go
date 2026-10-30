package catalogdiagnostics

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"k8s.io/apimachinery/pkg/types"
)

func policyIdentity() SessionIdentity {
	return SessionIdentity{Issuer: "https://catalog.example.test", Subject: "google:123456789", Fingerprint: strings.Repeat("a", 64), IssuedAt: testNow.Add(-time.Minute), AuthenticatedAt: testNow.Add(-time.Minute), ExpiresAt: testNow.Add(time.Hour)}
}
func policyRecord() ProjectReadPolicy {
	return ProjectReadPolicy{SchemaVersion: 1, Target: target, Issuer: "https://catalog.example.test", IssuedAt: testNow.Add(-time.Hour), ExpiresAt: testNow.Add(time.Hour), Subjects: []PolicySubject{{Subject: policyIdentity().Subject, Allowed: true, AuthenticatedAtOrAfter: testNow.Add(-time.Hour)}}}
}

func policyFixture(t *testing.T) (PinnedPolicyConfig, *desiredTestBinding) {
	t.Helper()
	desired, binding := desiredFixture(t)
	bound := binding.bound
	object, err := PolicyBootstrapObject(bound.NamespaceName(), policyRecord())
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, _ := kube.ConfigMapFingerprint(object)
	intent, err := bound.Intent("configmap", object.Name, PolicyRevision, "bootstrap-policy", fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	parent := bound.Parent()
	parentObserved := ownership.Observation{Ref: ownership.ResourceRef{Scope: ownership.Scope{Provider: ownership.Kubernetes, ClusterID: parent.ClusterID}, Purpose: "namespace", Name: parent.Name, ProviderID: parent.UID}, Owner: bound.Owner(), CreationIdempotencyKey: parent.CreationIdempotencyKey, Revision: bound.ParentRevision(), SpecFingerprint: "namespace-spec", WriterEpoch: 1, Version: "1"}
	writer := ownership.Writer{ID: "control-writer", Owner: bound.Owner(), Epoch: 1, ExpiresAt: testNow.Add(time.Hour)}
	observed := ownership.NamespacedObservation{ParentUID: parent.UID, Observation: ownership.Observation{Ref: ownership.ResourceRef{Scope: bound.Scope(), Purpose: "configmap", Name: object.Name, ProviderID: "policy-uid"}, Owner: bound.Owner(), CreationIdempotencyKey: intent.Intent.IdempotencyKey, Revision: PolicyRevision, SpecFingerprint: fingerprint, WriterEpoch: 1, Version: "1"}}
	record, err := bound.BindCreated(intent, parentObserved, observed, writer, testNow)
	if err != nil {
		t.Fatal(err)
	}
	object.UID = types.UID(record.Record.Ref.ProviderID)
	object.ResourceVersion = "1"
	object.Annotations = kube.CreationAnnotations(intent.Intent, writer)
	binding.snapshot = kube.ConfigMapSnapshot{Object: object, Observation: observed}
	binding.calls = 0
	return PinnedPolicyConfig{Mode: LocalPinnedPolicy, Environment: target.Environment, Installation: target.Installation, Cells: []PolicyCell{{Target: target, Namespace: binding, Pinned: record}}, Now: desired.Now}, binding
}

func publishPolicy(t *testing.T, binding *desiredTestBinding, policy ProjectReadPolicy) {
	t.Helper()
	desired, err := PolicyBootstrapObject(binding.bound.NamespaceName(), policy)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := kube.ConfigMapFingerprint(desired)
	if err != nil {
		t.Fatal(err)
	}
	binding.snapshot.Object.Data = desired.Data
	binding.snapshot.Object.Annotations[kube.FingerprintAnnotation] = fingerprint
	binding.snapshot.Observation.Observation.SpecFingerprint = fingerprint
	n, _ := strconv.Atoi(binding.snapshot.Object.ResourceVersion)
	binding.snapshot.Object.ResourceVersion = strconv.Itoa(n + 1)
	// Fake metadata publisher, not a provider identity or CAS proof. Actual API
	// integration separately uses exact UID/resourceVersion conditional writes.
}

func TestPinnedPolicyMakesCurrentExplicitScopedDecisions(t *testing.T) {
	config, binding := policyFixture(t)
	policy, err := NewPinnedReadPolicy(config)
	if err != nil || binding.calls != 0 {
		t.Fatal("policy constructor accessed provider")
	}
	decision, err := policy.CheckDiagnosticRead(context.Background(), policyIdentity(), target)
	if err != nil || !decision.Allowed || len(decision.PolicyID) != 71 || decision.ValidUntil != testNow.Add(MaxGrantLifetime) || binding.calls != 1 {
		t.Fatalf("policy decision: %+v %v", decision, err)
	}
	foreign := target
	foreign.ProjectUID = "unknown-project"
	denied, err := policy.CheckDiagnosticRead(context.Background(), policyIdentity(), foreign)
	if err != nil || denied.Allowed || binding.calls != 1 {
		t.Fatal("unregistered project queried storage")
	}
	identity := policyIdentity()
	identity.Subject = "google:unlisted"
	if denied, err = policy.CheckDiagnosticRead(context.Background(), identity, target); err != nil || denied.Allowed {
		t.Fatal("implicit principal allow")
	}
	before := decision.PolicyID
	record := policyRecord()
	record.Subjects[0].Allowed = false
	publishPolicy(t, binding, record)
	if denied, err = policy.CheckDiagnosticRead(context.Background(), policyIdentity(), target); err != nil || denied.Allowed {
		t.Fatal("revoked principal retained cached permission")
	}
	record.Subjects[0].Allowed = true
	publishPolicy(t, binding, record)
	decision, err = policy.CheckDiagnosticRead(context.Background(), policyIdentity(), target)
	if err != nil || !decision.Allowed || before == decision.PolicyID {
		t.Fatal("provider version was not bound to new grant")
	}
}

func TestPolicyDenialAndUnavailableRemainDistinctSafeResults(t *testing.T) {
	for name, change := range map[string]func(*ProjectReadPolicy){
		"expired":         func(p *ProjectReadPolicy) { p.ExpiresAt = testNow },
		"future":          func(p *ProjectReadPolicy) { p.IssuedAt = testNow.Add(time.Minute) },
		"issuer":          func(p *ProjectReadPolicy) { p.Issuer = "https://foreign.example.test" },
		"session revoked": func(p *ProjectReadPolicy) { p.RevokedSessions = []string{policyIdentity().Fingerprint} },
		"reauth required": func(p *ProjectReadPolicy) { p.Subjects[0].AuthenticatedAtOrAfter = testNow },
	} {
		t.Run(name, func(t *testing.T) {
			config, binding := policyFixture(t)
			record := policyRecord()
			change(&record)
			publishPolicy(t, binding, record)
			p, err := NewPinnedReadPolicy(config)
			if err != nil {
				t.Fatal(err)
			}
			d, err := p.CheckDiagnosticRead(context.Background(), policyIdentity(), target)
			if err != nil || d.Allowed {
				t.Fatalf("denial: %+v %v", d, err)
			}
		})
	}
	for name, change := range map[string]func(*desiredTestBinding){
		"replaced": func(b *desiredTestBinding) { b.snapshot.Object.UID = "replacement" },
		"tampered": func(b *desiredTestBinding) {
			b.snapshot.Object.Data[PolicyDataKey] = strings.Replace(b.snapshot.Object.Data[PolicyDataKey], `"allowed":true`, `"allowed":false`, 1)
		},
		"duplicate": func(b *desiredTestBinding) {
			b.snapshot.Object.Data[PolicyDataKey] = strings.TrimSuffix(b.snapshot.Object.Data[PolicyDataKey], "}") + `,"issuer":"https://catalog.example.test"}`
		},
		"wrong parent": func(b *desiredTestBinding) { b.snapshot.Observation.ParentUID = "other" },
		"provider":     func(b *desiredTestBinding) { b.err = errors.New("PRIVATE-provider-error") },
	} {
		t.Run(name, func(t *testing.T) {
			config, b := policyFixture(t)
			p, err := NewPinnedReadPolicy(config)
			if err != nil {
				t.Fatal(err)
			}
			change(b)
			d, err := p.CheckDiagnosticRead(context.Background(), policyIdentity(), target)
			if d.Allowed || !errors.Is(err, ErrAuthorizationUnavailable) || strings.Contains(err.Error(), "PRIVATE") {
				t.Fatalf("unsafe/unproven decision: %+v %v", d, err)
			}
		})
	}
}

func TestVerifiedSessionGrantRechecksActualPolicyVersionAndRevocation(t *testing.T) {
	config, binding := policyFixture(t)
	policy, err := NewPinnedReadPolicy(config)
	if err != nil {
		t.Fatal(err)
	}
	a := sessionAuthorizer(t, policy)
	r := sessionRequest(tokenFor(sessionClaims()))
	grant, err := a.AuthorizeRead(context.Background(), r, target)
	if err != nil {
		t.Fatal(err)
	}
	reads := 0
	m := model(t, sourceFunc(func(context.Context, ReadGrant) (Snapshot, error) {
		reads++
		record := policyRecord()
		record.Subjects[0].Allowed = false
		publishPolicy(t, binding, record)
		return snapshot(t), nil
	}))
	if _, err = m.Read(context.Background(), grant, target); !errors.Is(err, ErrForbidden) || reads != 1 {
		t.Fatal("policy revocation during source read disclosed response")
	}
	if _, err = a.AuthorizeRead(context.Background(), r, target); !errors.Is(err, ErrForbidden) {
		t.Fatal("revoked policy reissued grant")
	}
}

func TestPolicyEncodingRejectsAmbiguousOrUnboundedRecords(t *testing.T) {
	for _, change := range []func(*ProjectReadPolicy){func(p *ProjectReadPolicy) { p.Subjects = append(p.Subjects, p.Subjects[0]) }, func(p *ProjectReadPolicy) { p.ExpiresAt = p.IssuedAt.Add(8 * 24 * time.Hour) }, func(p *ProjectReadPolicy) { p.RevokedSessions = []string{"bad"} }, func(p *ProjectReadPolicy) { p.Subjects[0].AuthenticatedAtOrAfter = time.Time{} }, func(p *ProjectReadPolicy) { p.SchemaVersion = 2 }} {
		record := policyRecord()
		change(&record)
		if _, err := record.JSON(); err == nil {
			t.Fatal("ambiguous policy encoded")
		}
	}
}
