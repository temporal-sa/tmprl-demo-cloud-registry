package kubejournal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
)

type constructorBinding struct{ bound ownership.NamespacedContext }

func (b constructorBinding) Context() ownership.NamespacedContext { return b.bound }
func (constructorBinding) ObserveConfigMap(context.Context, ownership.NamespacedRecord) (kube.ConfigMapSnapshot, error) {
	panic("constructor must not read provider")
}
func (constructorBinding) UpdateConfigMap(context.Context, kube.ConfigMapUpdate) (ownership.NamespacedRecord, error) {
	panic("constructor must not write provider")
}
func (constructorBinding) ConfirmConfigMapUpdate(context.Context, kube.ConfigMapUpdate) (ownership.NamespacedRecord, error) {
	panic("constructor must not query provider")
}
func (constructorBinding) HasUnknownConfigMapUpdate(kube.ConfigMapUpdate) (bool, error) {
	panic("constructor must not query gate")
}

func configuration(t *testing.T, purpose string) Config {
	t.Helper()
	base, err := ownership.NewContext(ownership.Config{Owner: ownership.Owner{EnvironmentID: "lab", InstallationID: "test", ProjectUID: "control-plane", Lifetime: ownership.Stable}, ResourceRoot: "lab", Scopes: []ownership.Scope{{Provider: ownership.Kubernetes, ClusterID: "owned-cluster"}}})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := base.Intent(ownership.Kubernetes, purpose, "control", "control-v1", "bootstrap-control", "control-spec", 63)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	writer := ownership.Writer{ID: "control-writer", Owner: base.Owner(), Epoch: 1, ExpiresAt: now.Add(time.Hour)}
	parentObservation := ownership.Observation{Ref: ownership.ResourceRef{Scope: intent.Scope, Purpose: intent.Purpose, Name: intent.Name, ProviderID: "control-namespace-uid"}, Owner: intent.Owner, CreationIdempotencyKey: intent.IdempotencyKey, Revision: intent.Revision, SpecFingerprint: intent.SpecFingerprint, WriterEpoch: 1, Version: "1"}
	parent, err := base.BindCreated(intent, parentObservation, writer, now)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := base.BindNamespace(parent, parentObservation)
	if err != nil {
		t.Fatal(err)
	}
	subject := rollout.Subject{Environment: "lab", Installation: "test", ProjectUID: "user-project"}
	initial, _ := journal.Initial(subject, 1)
	object, err := BootstrapObject(bound.NamespaceName(), initial)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, _ := kube.ConfigMapFingerprint(object)
	childIntent, err := bound.Intent("configmap", object.Name, Revision, "bootstrap-cell", fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	child := ownership.NamespacedObservation{ParentUID: bound.Parent().UID, Observation: ownership.Observation{Ref: ownership.ResourceRef{Scope: bound.Scope(), Purpose: "configmap", Name: object.Name, ProviderID: "cell-uid"}, Owner: bound.Owner(), CreationIdempotencyKey: childIntent.Intent.IdempotencyKey, Revision: Revision, SpecFingerprint: fingerprint, WriterEpoch: 1, Version: "1"}}
	record, err := bound.BindCreated(childIntent, parentObservation, child, writer, now)
	if err != nil {
		t.Fatal(err)
	}
	return Config{Namespace: constructorBinding{bound}, Pinned: record, Subject: subject, Writer: func() ownership.Writer { return writer }}
}

func TestControlOwnerMayDifferFromProjectButNotEnvironmentOrInstallation(t *testing.T) {
	config := configuration(t, "namespace")
	if _, err := New(config); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*Config){func(c *Config) { c.Subject.Environment = "prod" }, func(c *Config) { c.Subject.Installation = "foreign" }} {
		foreign := config
		change(&foreign)
		// Even a matching new name/pinned field cannot grant cross-scope storage.
		foreign.Pinned.Record.Ref.Name = Name(foreign.Subject)
		if _, err := New(foreign); !errors.Is(err, journal.ErrInvalid) {
			t.Fatal("control namespace laundered foreign project scope")
		}
	}
	if _, err := New(configuration(t, "runtime-namespace")); !errors.Is(err, journal.ErrInvalid) {
		t.Fatal("candidate namespace accepted as durable control lifetime")
	}
}
