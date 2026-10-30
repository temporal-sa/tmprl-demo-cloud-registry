package kubernetes

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"sync/atomic"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func preparedFixture(t *testing.T, mutate func(http.ResponseWriter, *http.Request, *fixture), readFailure ...*atomic.Bool) (*fixture, *Client, *NamespaceBinding, *atomic.Int64) {
	t.Helper()
	f := guardFixture(t)
	f.namespace.Finalizers = []string{NamespaceLifecycleFinalizer, "other.example/keep"}
	f.namespace.Spec.Finalizers = []corev1.FinalizerName{"kubernetes", "other.example/spec"}
	reads := &atomic.Int64{}
	server, credentials := tlsServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != "GET" {
			f.writes.Add(1)
			mutate(w, r, f)
			return
		}
		reads.Add(1)
		if len(readFailure) > 0 && readFailure[0].Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusFailure, Reason: metav1.StatusReasonServiceUnavailable, Code: 503})
			return
		}
		if r.URL.Path == "/api/v1/namespaces/kube-system" {
			_ = json.NewEncoder(w).Encode(&corev1.Namespace{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"}, ObjectMeta: metav1.ObjectMeta{Name: "kube-system", UID: "owned-cluster-uid", ResourceVersion: "1"}})
			return
		}
		f.mu.RLock()
		defer f.mu.RUnlock()
		if f.namespace == nil {
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusFailure, Reason: metav1.StatusReasonNotFound, Code: 404})
			return
		}
		_ = json.NewEncoder(w).Encode(f.namespace)
	}))
	f.options.Endpoint, f.options.Credentials = server.URL, credentials
	client, err := New(context.Background(), f.options)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := client.BindNamespace(context.Background(), f.base, f.parent, "attempt-one")
	if err != nil {
		t.Fatal(err)
	}
	return f, client, binding, reads
}

func TestPreparedDeleteUsesFrozenUIDVersionBytesWithoutRefresh(t *testing.T) {
	var body []byte
	f, client, binding, reads := preparedFixture(t, func(w http.ResponseWriter, r *http.Request, f *fixture) {
		if r.Method != "DELETE" {
			t.Errorf("unexpected hidden mutation: %s", r.Method)
		}
		body, _ = io.ReadAll(r.Body)
		var options metav1.DeleteOptions
		_ = json.Unmarshal(body, &options)
		if options.Preconditions == nil || options.Preconditions.UID == nil || string(*options.Preconditions.UID) != "namespace-uid-one" || options.Preconditions.ResourceVersion == nil || *options.Preconditions.ResourceVersion != "100" {
			t.Error("frozen conditional delete changed")
		}
		w.WriteHeader(http.StatusConflict)
		_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusFailure, Reason: metav1.StatusReasonConflict, Code: 409})
	})
	ticket, err := binding.PrepareNamespaceDelete(context.Background(), f.writer, "delete-one")
	if err != nil {
		t.Fatal(err)
	}
	description := ticket.Description()
	if description.ResourceVersion != "100" || description.UID != string(f.namespace.UID) || len(description.ForeignFinalizers) != 2 || f.writes.Load() != 0 {
		t.Fatal("preparation changed resource or lost scope")
	}
	description.ForeignFinalizers[0] = "tampered"
	if ticket.Description().ForeignFinalizers[0] == "tampered" {
		t.Fatal("description aliases capability memory")
	}
	f.mu.Lock()
	f.namespace.ResourceVersion = "101"
	f.mu.Unlock()
	before := reads.Load()
	_, err = ticket.Execute(context.Background())
	requireCode(t, err, ConditionalConflict)
	if reads.Load() != before || f.writes.Load() != 1 {
		t.Fatal("execution refreshed or performed hidden writes")
	}
	digest := sha256.Sum256(body)
	if ticket.Description().BodyDigest != "sha256:"+hex.EncodeToString(digest[:]) {
		t.Fatal("journal fingerprint differs from dispatched bytes")
	}
	_, err = ticket.Execute(context.Background())
	requireCode(t, err, ConditionalConflict)
	if f.writes.Load() != 1 || !client.Lifecycle(f.parent.Ref.Name).Draining {
		t.Fatal("ticket reused or lifecycle reopened")
	}
}

func TestPreparedFinalizePreservesForeignStateAndReturnsCanonicalVersion(t *testing.T) {
	var body []byte
	f, _, binding, reads := preparedFixture(t, func(w http.ResponseWriter, r *http.Request, f *fixture) {
		if r.Method != "PUT" {
			t.Errorf("unexpected mutation: %s", r.Method)
		}
		body, _ = io.ReadAll(r.Body)
		var updated corev1.Namespace
		if err := json.Unmarshal(body, &updated); err != nil {
			t.Error(err)
		}
		if updated.UID != "namespace-uid-one" || updated.ResourceVersion != "100" || contains(updated.Finalizers, NamespaceLifecycleFinalizer) || !contains(updated.Finalizers, "other.example/keep") || len(updated.Spec.Finalizers) != 2 || updated.Labels["preserve"] != "yes" {
			t.Error("finalizer body changed another actor's state or UID/RV")
		}
		updated.ResourceVersion = "101"
		_ = json.NewEncoder(w).Encode(updated)
	})
	f.mu.Lock()
	now := metav1.Now()
	f.namespace.DeletionTimestamp = &now
	f.namespace.Status.Phase = corev1.NamespaceTerminating
	f.namespace.Labels = map[string]string{"preserve": "yes"}
	f.mu.Unlock()
	ticket, err := binding.PrepareOwnFinalizerRemoval(context.Background(), f.writer, "finalize-one")
	if err != nil {
		t.Fatal(err)
	}
	before := reads.Load()
	result, err := ticket.Execute(context.Background())
	if err != nil || !result.Acknowledged || result.ConfirmedAbsent || result.UID != "namespace-uid-one" || result.ResourceVersion != "101" || reads.Load() != before || f.writes.Load() != 1 {
		t.Fatalf("unexpected finalizer result: %+v %v", result, err)
	}
	digest := sha256.Sum256(body)
	if ticket.Description().BodyDigest != "sha256:"+hex.EncodeToString(digest[:]) {
		t.Fatal("finalizer body fingerprint not dispatched exactly")
	}
}

func TestPreparedFinalizeSuccessStatusConfirmsAbsenceWithoutInventingVersion(t *testing.T) {
	f, _, binding, _ := preparedFixture(t, func(w http.ResponseWriter, r *http.Request, f *fixture) {
		f.mu.Lock()
		f.namespace = nil
		f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusSuccess})
	})
	f.mu.Lock()
	now := metav1.Now()
	f.namespace.DeletionTimestamp = &now
	f.namespace.Status.Phase = corev1.NamespaceTerminating
	f.mu.Unlock()
	ticket, err := binding.PrepareOwnFinalizerRemoval(context.Background(), f.writer, "finalize-absent")
	if err != nil {
		t.Fatal(err)
	}
	result, err := ticket.Execute(context.Background())
	if err != nil || !result.Acknowledged || !result.ConfirmedAbsent || result.UID != "" || result.ResourceVersion != "" {
		t.Fatalf("absence fabricated canonical metadata: %+v %v", result, err)
	}
}

func TestUnknownPreparedCleanupRetainsTombstoneEvenAfterAbsence(t *testing.T) {
	f, client, binding, _ := preparedFixture(t, func(w http.ResponseWriter, r *http.Request, f *fixture) {
		f.mu.Lock()
		f.namespace = nil
		f.mu.Unlock()
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusFailure, Reason: metav1.StatusReasonInternalError, Code: 500})
	})
	ticket, err := binding.PrepareNamespaceDelete(context.Background(), f.writer, "delete-unknown")
	if err != nil {
		t.Fatal(err)
	}
	_, err = ticket.Execute(context.Background())
	requireCode(t, err, UnknownWriteOutcome)
	_, err = binding.ObserveDrainingNamespace(context.Background())
	requireCode(t, err, UnknownWriteOutcome)
	_, err = binding.PrepareNamespaceDelete(context.Background(), f.writer, "another-delete")
	requireCode(t, err, UnknownWriteOutcome)
	state := client.Lifecycle(f.parent.Ref.Name)
	if !state.Draining || len(state.Unresolved) != 1 || !state.Unresolved[0].Unknown || state.Unresolved[0].TargetFingerprint != ticket.Description().BodyDigest {
		t.Fatal("cleanup ambiguity lost exact immutable tombstone")
	}
}

func TestResumeDrainingPinsProvenanceAndDoesNotReopenChildLifecycle(t *testing.T) {
	f, client, _, _ := preparedFixture(t, func(w http.ResponseWriter, r *http.Request, f *fixture) { t.Error("resume dispatched a mutation") })
	f.mu.Lock()
	now := metav1.Now()
	f.namespace.DeletionTimestamp = &now
	f.namespace.Status.Phase = corev1.NamespaceTerminating
	f.mu.Unlock()
	binding, err := client.ResumeDrainingNamespace(context.Background(), f.base, f.parent, "attempt-one")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := binding.Refresh(context.Background()); err == nil {
		t.Fatal("terminating parent admitted normal child observation")
	}
	requireCode(t, client.lifecycle.run(f.parent.Ref.Name, f.parent.Ref.ProviderID, "attempt-one", OperationTombstone{ID: "new-child"}, func() error { t.Fatal("resumed cleanup allowed child callback"); return nil }), LifecycleDraining)
	f.mu.Lock()
	f.namespace.UID = "replacement"
	f.mu.Unlock()
	_, err = client.ResumeDrainingNamespace(context.Background(), f.base, f.parent, "attempt-one")
	requireCode(t, err, OwnershipRejected)
}

func TestEmptyPreparedCleanupHandlesFailClosedWithoutPanic(t *testing.T) {
	var nilDelete *PreparedNamespaceDelete
	var nilFinalize *PreparedNamespaceFinalize
	for _, ticket := range []*PreparedNamespaceDelete{nilDelete, {}} {
		if ticket.Description().UID != "" {
			t.Fatal("empty delete handle produced authority")
		}
		_, err := ticket.Execute(context.Background())
		requireCode(t, err, InvalidConfiguration)
	}
	for _, ticket := range []*PreparedNamespaceFinalize{nilFinalize, {}} {
		if ticket.Description().UID != "" {
			t.Fatal("empty finalize handle produced authority")
		}
		_, err := ticket.Execute(context.Background())
		requireCode(t, err, InvalidConfiguration)
	}
}

func TestImmediateFinalizeResponseCannotDropFrozenForeignFields(t *testing.T) {
	for _, test := range []struct {
		name  string
		forge func(*corev1.Namespace)
	}{
		{"metadata-finalizer", func(object *corev1.Namespace) { object.Finalizers = nil }},
		{"spec-finalizer", func(object *corev1.Namespace) { object.Spec.Finalizers = nil }},
		{"labels", func(object *corev1.Namespace) { object.Labels = map[string]string{"forged": "value"} }},
		{"annotations", func(object *corev1.Namespace) { object.Annotations["operator-test.local/foreign"] = "forged" }},
		{"owner-references", func(object *corev1.Namespace) { object.OwnerReferences = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, client, binding, _ := preparedFixture(t, func(w http.ResponseWriter, r *http.Request, f *fixture) {
				var object corev1.Namespace
				if err := json.NewDecoder(r.Body).Decode(&object); err != nil {
					t.Error(err)
				}
				object.ResourceVersion = "101"
				test.forge(&object)
				_ = json.NewEncoder(w).Encode(object)
			})
			f.mu.Lock()
			now := metav1.Now()
			f.namespace.DeletionTimestamp = &now
			f.namespace.Status.Phase = corev1.NamespaceTerminating
			f.namespace.Labels = map[string]string{"preserve": "yes"}
			f.namespace.Annotations["operator-test.local/foreign"] = "preserve"
			f.namespace.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "ConfigMap", Name: "external", UID: "external-uid"}}
			f.mu.Unlock()
			ticket, err := binding.PrepareOwnFinalizerRemoval(context.Background(), f.writer, "finalize-forged")
			if err != nil {
				t.Fatal(err)
			}
			_, err = ticket.Execute(context.Background())
			requireCode(t, err, UnknownWriteOutcome)
			if len(client.Lifecycle(f.parent.Ref.Name).Unresolved) != 1 {
				t.Fatal("forged 2xx lost uncertain operation")
			}
		})
	}
}

func TestAcknowledgedOldVersionFinalizeNamespaceRequiresSameCallbackAbsence(t *testing.T) {
	for _, mode := range []string{"absent", "still-present", "read-error", "wrong-uid"} {
		t.Run(mode, func(t *testing.T) {
			var unavailable atomic.Bool
			f, client, binding, _ := preparedFixture(t, func(w http.ResponseWriter, r *http.Request, f *fixture) {
				var updated corev1.Namespace
				if err := json.NewDecoder(r.Body).Decode(&updated); err != nil {
					t.Error(err)
				}
				if updated.ResourceVersion != "100" {
					t.Error("fixture lost old-version response")
				}
				f.mu.Lock()
				if mode == "still-present" {
					f.namespace = updated.DeepCopy()
				} else {
					f.namespace = nil
				}
				f.mu.Unlock()
				if mode == "read-error" {
					unavailable.Store(true)
				}
				if mode == "wrong-uid" {
					updated.UID = "foreign-incarnation"
				}
				_ = json.NewEncoder(w).Encode(updated)
			}, &unavailable)
			f.mu.Lock()
			now := metav1.Now()
			f.namespace.DeletionTimestamp = &now
			f.namespace.Status.Phase = corev1.NamespaceTerminating
			f.namespace.Finalizers = []string{NamespaceLifecycleFinalizer}
			f.namespace.Spec.Finalizers = nil
			f.mu.Unlock()
			ticket, err := binding.PrepareOwnFinalizerRemoval(context.Background(), f.writer, "finalize-old-version")
			if err != nil {
				t.Fatal(err)
			}
			result, err := ticket.Execute(context.Background())
			state := client.Lifecycle(f.parent.Ref.Name)
			if mode == "absent" {
				if err != nil || !result.Acknowledged || !result.ConfirmedAbsent || result.UID != "" || result.ResourceVersion != "" || len(state.Unresolved) != 0 {
					t.Fatalf("acknowledged absence fabricated metadata or retained false uncertainty: %+v %v", result, err)
				}
			} else {
				requireCode(t, err, UnknownWriteOutcome)
				if len(state.Unresolved) != 1 || !state.Unresolved[0].Unknown || result.ConfirmedAbsent {
					t.Fatal("nonabsence/read failure/forged UID cleared uncertainty")
				}
			}
		})
	}
}

func TestFinalizeSuccessStatusWithPresentForeignFieldDriftRemainsUnknown(t *testing.T) {
	for _, mode := range []string{"metadata-finalizer", "spec-finalizer", "labels"} {
		t.Run(mode, func(t *testing.T) {
			f, client, binding, _ := preparedFixture(t, func(w http.ResponseWriter, r *http.Request, f *fixture) {
				var updated corev1.Namespace
				if err := json.NewDecoder(r.Body).Decode(&updated); err != nil {
					t.Error(err)
				}
				updated.ResourceVersion = "101"
				switch mode {
				case "metadata-finalizer":
					updated.Finalizers = nil
				case "spec-finalizer":
					updated.Spec.Finalizers = nil
				case "labels":
					updated.Labels = map[string]string{"forged": "value"}
				}
				f.mu.Lock()
				f.namespace = updated.DeepCopy()
				f.mu.Unlock()
				_ = json.NewEncoder(w).Encode(metav1.Status{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"}, Status: metav1.StatusSuccess})
			})
			f.mu.Lock()
			now := metav1.Now()
			f.namespace.DeletionTimestamp = &now
			f.namespace.Status.Phase = corev1.NamespaceTerminating
			f.mu.Unlock()
			ticket, err := binding.PrepareOwnFinalizerRemoval(context.Background(), f.writer, "finalize-status-drift")
			if err != nil {
				t.Fatal(err)
			}
			result, err := ticket.Execute(context.Background())
			requireCode(t, err, UnknownWriteOutcome)
			if result.ConfirmedAbsent || len(client.Lifecycle(f.parent.Ref.Name).Unresolved) != 1 {
				t.Fatal("present drift relabeled committed from Status")
			}
		})
	}
}
