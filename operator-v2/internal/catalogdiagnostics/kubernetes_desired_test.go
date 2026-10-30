package catalogdiagnostics

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"k8s.io/apimachinery/pkg/types"
)

type desiredTestBinding struct {
	bound    ownership.NamespacedContext
	snapshot kube.ConfigMapSnapshot
	calls    int
	err      error
}

func (b *desiredTestBinding) Context() ownership.NamespacedContext { return b.bound }
func (b *desiredTestBinding) ObserveConfigMap(context.Context, ownership.NamespacedRecord) (kube.ConfigMapSnapshot, error) {
	b.calls++
	copy := b.snapshot
	copy.Object = copy.Object.DeepCopy()
	return copy, b.err
}
func desiredFixture(t *testing.T) (PinnedDesiredConfig, *desiredTestBinding) {
	t.Helper()
	base, err := ownership.NewContext(ownership.Config{Owner: ownership.Owner{EnvironmentID: target.Environment, InstallationID: target.Installation, ProjectUID: "control", Lifetime: ownership.Disposable, RunID: "owned-run"}, ResourceRoot: "lab", Scopes: []ownership.Scope{{Provider: ownership.Kubernetes, ClusterID: "owned-cluster"}}})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := base.Intent(ownership.Kubernetes, "namespace", "control", "control-v1", "bootstrap-control", "namespace-spec", 63)
	if err != nil {
		t.Fatal(err)
	}
	writer := ownership.Writer{ID: "control-writer", Owner: base.Owner(), Epoch: 1, ExpiresAt: testNow.Add(time.Hour)}
	parentObserved := ownership.Observation{Ref: ownership.ResourceRef{Scope: intent.Scope, Purpose: intent.Purpose, Name: intent.Name, ProviderID: "parent-uid"}, Owner: intent.Owner, CreationIdempotencyKey: intent.IdempotencyKey, Revision: intent.Revision, SpecFingerprint: intent.SpecFingerprint, WriterEpoch: 1, Version: "1"}
	parent, err := base.BindCreated(intent, parentObserved, writer, testNow)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := base.BindNamespace(parent, parentObserved)
	if err != nil {
		t.Fatal(err)
	}
	accepted := AcceptedDesired{SchemaVersion: 1, Target: target, ReconcileKey: "independent-new-intent", Generation: 9, DecisionID: "decision-nine"}
	o, err := DesiredBootstrapObject(bound.NamespaceName(), accepted)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, _ := kube.ConfigMapFingerprint(o)
	childIntent, err := bound.Intent("configmap", o.Name, DesiredRevision, "bootstrap-desired", fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	childObserved := ownership.NamespacedObservation{ParentUID: bound.Parent().UID, Observation: ownership.Observation{Ref: ownership.ResourceRef{Scope: bound.Scope(), Purpose: "configmap", Name: o.Name, ProviderID: "desired-uid"}, Owner: bound.Owner(), CreationIdempotencyKey: childIntent.Intent.IdempotencyKey, Revision: DesiredRevision, SpecFingerprint: fingerprint, WriterEpoch: 1, Version: "2"}}
	pinned, err := bound.BindCreated(childIntent, parentObserved, childObserved, writer, testNow)
	if err != nil {
		t.Fatal(err)
	}
	o.UID = types.UID("desired-uid")
	o.ResourceVersion = "2"
	o.Annotations = kube.CreationAnnotations(childIntent.Intent, writer)
	binding := &desiredTestBinding{bound: bound, snapshot: kube.ConfigMapSnapshot{Object: o, Observation: childObserved}}
	return PinnedDesiredConfig{Mode: LocalPinnedDesired, Target: target, Namespace: binding, Pinned: pinned, Now: func() time.Time { return testNow }}, binding
}

func TestPinnedDesiredReadIsIndependentAndConstructorDoesNoIO(t *testing.T) {
	config, b := desiredFixture(t)
	s, err := NewPinnedDesired(config)
	if err != nil || b.calls != 0 {
		t.Fatal("constructor provider access")
	}
	observed, err := s.ObserveDesired(context.Background(), target)
	if err != nil || !observed.Known || observed.Desired.ReconcileKey != "independent-new-intent" || observed.Desired.Generation != 9 || observed.ObservedAt != testNow || b.calls != 1 {
		t.Fatalf("desired observation failed: %v", err)
	}
	foreign := target
	foreign.ProjectUID = "other"
	if _, err = s.ObserveDesired(context.Background(), foreign); !errors.Is(err, ErrForbidden) || b.calls != 1 {
		t.Fatal("foreign desired read reached provider")
	}
	b.err = errors.New("PRIVATE-provider-body")
	if _, err = s.ObserveDesired(context.Background(), target); !errors.Is(err, ErrSourceUnavailable) || strings.Contains(err.Error(), "PRIVATE") {
		t.Fatal("unsafe desired provider error")
	}
}

func TestPinnedDesiredRejectsReplacementPayloadDriftAndNoncanonicalClaims(t *testing.T) {
	changes := map[string]func(*desiredTestBinding){
		"replaced UID": func(b *desiredTestBinding) { b.snapshot.Object.UID = "replacement" },
		"parent":       func(b *desiredTestBinding) { b.snapshot.Observation.ParentUID = "other" },
		"creation":     func(b *desiredTestBinding) { b.snapshot.Object.Annotations[kube.CreationOperationAnnotation] = "other" },
		"body drift": func(b *desiredTestBinding) {
			b.snapshot.Object.Data[DesiredDataKey] = strings.Replace(b.snapshot.Object.Data[DesiredDataKey], `"generation":9`, `"generation":10`, 1)
		},
		"duplicate": func(b *desiredTestBinding) {
			b.snapshot.Object.Data[DesiredDataKey] = strings.TrimSuffix(b.snapshot.Object.Data[DesiredDataKey], "}") + `,"generation":9}`
		},
		"unknown": func(b *desiredTestBinding) {
			b.snapshot.Object.Data[DesiredDataKey] = strings.TrimSuffix(b.snapshot.Object.Data[DesiredDataKey], "}") + `,"credential":"PRIVATE"}`
		},
		"extra data":          func(b *desiredTestBinding) { b.snapshot.Object.Data["other"] = "PRIVATE" },
		"missing fingerprint": func(b *desiredTestBinding) { b.snapshot.Observation.Observation.SpecFingerprint = "" },
		"label drift":         func(b *desiredTestBinding) { b.snapshot.Object.Labels["other"] = "changed" },
		"oversized":           func(b *desiredTestBinding) { b.snapshot.Object.Data[DesiredDataKey] = strings.Repeat("x", 4097) },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			config, b := desiredFixture(t)
			s, err := NewPinnedDesired(config)
			if err != nil {
				t.Fatal(err)
			}
			change(b)
			if _, err = s.ObserveDesired(context.Background(), target); !errors.Is(err, ErrInvalidSource) {
				t.Fatalf("drift accepted: %v", err)
			}
		})
	}
}

func TestDesiredConstructorRefusesForeignOrUnpinnedScope(t *testing.T) {
	for _, change := range []func(*PinnedDesiredConfig){func(c *PinnedDesiredConfig) { c.Mode = "production" }, func(c *PinnedDesiredConfig) { c.Target.Environment = "prod" }, func(c *PinnedDesiredConfig) { c.Pinned.Record.Ref.ProviderID = "" }, func(c *PinnedDesiredConfig) { c.Pinned.Parent.UID = "other" }, func(c *PinnedDesiredConfig) { c.Pinned.Record.Ref.Name = "arbitrary" }, func(c *PinnedDesiredConfig) { c.Pinned.Record.Owner.ProjectUID = "other" }} {
		config, b := desiredFixture(t)
		change(&config)
		if _, err := NewPinnedDesired(config); !errors.Is(err, ErrInvalidSource) || b.calls != 0 {
			t.Fatal("invalid binding accepted or provider accessed")
		}
	}
}
