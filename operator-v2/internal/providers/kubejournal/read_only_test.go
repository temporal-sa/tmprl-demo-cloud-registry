package kubejournal

import (
	"context"
	"errors"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"k8s.io/apimachinery/pkg/types"
)

type onlyReader struct {
	bound ownership.NamespacedContext
	value kube.ConfigMapSnapshot
	reads int
}

func (r *onlyReader) Context() ownership.NamespacedContext { return r.bound }
func (r *onlyReader) ObserveConfigMap(context.Context, ownership.NamespacedRecord) (kube.ConfigMapSnapshot, error) {
	r.reads++
	return r.value, nil
}

func TestReadOnlyCellNeedsNoWriterAndAlwaysRejectsCASWithoutRPC(t *testing.T) {
	config := configuration(t, "namespace")
	initial, _ := journal.Initial(config.Subject, 1)
	object, _ := BootstrapObject(config.Namespace.Context().NamespaceName(), initial)
	r := config.Pinned.Record
	object.UID = types.UID(r.Ref.ProviderID)
	object.ResourceVersion = "1"
	object.Annotations = map[string]string{kube.CreationOperationAnnotation: r.CreationOperationID}
	reader := &onlyReader{bound: config.Namespace.Context(), value: kube.ConfigMapSnapshot{Object: object, Observation: ownership.NamespacedObservation{ParentUID: config.Pinned.Parent.UID, Observation: ownership.Observation{Ref: r.Ref, Owner: r.Owner, CreationIdempotencyKey: r.CreationIdempotencyKey, Revision: r.Revision, SpecFingerprint: r.SpecFingerprint}}}}
	cell, err := NewReadOnly(ReadConfig{Namespace: reader, Pinned: config.Pinned, Subject: config.Subject})
	if err != nil || reader.reads != 0 {
		t.Fatal("constructor invented authority or read", err)
	}
	if _, _, err = cell.Read(context.Background()); err != nil || reader.reads != 1 {
		t.Fatal("shared live read validation refused", err)
	}
	if err = cell.CompareAndSwap(context.Background(), journal.Version{}, []byte("PRIVATE_PAYLOAD"), "commit"); !errors.Is(err, journal.ErrInvalid) || reader.reads != 1 {
		t.Fatal("read-only CAS invoked provider")
	}
	object.UID = "replacement"
	if _, _, err = cell.Read(context.Background()); !errors.Is(err, journal.ErrMissing) {
		t.Fatal("replacement adopted")
	}
	wrong := config.Pinned
	wrong.Parent.UID = "foreign"
	if _, err = NewReadOnly(ReadConfig{Namespace: reader, Pinned: wrong, Subject: config.Subject}); !errors.Is(err, journal.ErrInvalid) {
		t.Fatal("foreign parent accepted")
	}
	if _, err = NewReadOnly(ReadConfig{}); !errors.Is(err, journal.ErrInvalid) {
		t.Fatal("missing binding accepted")
	}
}
