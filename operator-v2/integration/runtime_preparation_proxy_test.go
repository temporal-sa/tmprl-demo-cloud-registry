//go:build integration && preparation_integration

package integration

// Independent exact original-input/Pending/native-receipt witnesses. Raw bodies
// remain only in this owned proxy; no provider body or error reaches public DTOs.
import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubejournal"
	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimefault"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeinputs"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

type preparationCapture struct {
	mu               sync.Mutex
	created          map[string]preparationCreationMetadata
	posts, checks    map[string]int
	mutations, reads int
	denySA           bool
}

type preparationCreationMetadata struct {
	nativeCreationMetadata
	Revision, CreationKey, Owner string
}

func preparationNamespaceReceipt(row journal.Write, known preparationCreationMetadata) bool {
	p := row.Preconditions
	if known.UID == "" || known.Version == "" || known.Owner == "" || known.CreationKey == "" || known.Revision == "" || p.Kind != "Namespace" || p.Action != "Create" || p.Name != known.Name || p.Fingerprint != known.Fingerprint || p.ParentUID != "" || p.ProviderOperationID != known.Operation || p.TargetRevision != known.Revision {
		return false
	}
	return row.State == journal.Pending && row.ResultUID == "" && row.ResultVersion == "" || row.State == journal.Committed && row.ResultUID == known.UID && row.ResultVersion != ""
}

func preparationNamespaceWitnessContract(t *testing.T) {
	t.Helper()
	known := preparationCreationMetadata{nativeCreationMetadata: nativeCreationMetadata{Name: "owned-ns", UID: "known-post-uid", Version: "native-post-rv", Operation: "accepted-attempt", Fingerprint: "original-digest"}, Owner: "expected-owner", CreationKey: "expected-key", Revision: "accepted-revision"}
	pending := journal.Write{State: journal.Pending, Preconditions: journal.Preconditions{Kind: "Namespace", Action: "Create", Name: known.Name, Fingerprint: known.Fingerprint, ProviderOperationID: known.Operation, TargetRevision: known.Revision}}
	if !preparationNamespaceReceipt(pending, known) {
		t.Fatal("PreparationNamespaceWitnessInvariant")
	}
	committed := pending
	committed.State = journal.Committed
	committed.ResultUID = known.UID
	committed.ResultVersion = "independently-observed-rv"
	if !preparationNamespaceReceipt(committed, known) {
		t.Fatal("PreparationNamespaceObservedRVConflatedWithCreationRV")
	}
	for _, state := range []journal.WriteState{journal.Unknown, journal.Rejected} {
		row := pending
		row.State = state
		if preparationNamespaceReceipt(row, known) {
			t.Fatal("PreparationNamespaceAmbiguityAdopted")
		}
	}
	wrong := committed
	wrong.ResultUID = "replacement-uid"
	if preparationNamespaceReceipt(wrong, known) {
		t.Fatal("PreparationNamespaceReplacementAdopted")
	}
	missing := known
	missing.UID = ""
	if preparationNamespaceReceipt(pending, missing) {
		t.Fatal("PreparationNamespaceWithoutKnown201Adopted")
	}
	wrongPending := pending
	wrongPending.Preconditions.Fingerprint = "other-intent"
	if preparationNamespaceReceipt(wrongPending, known) {
		t.Fatal("PreparationNamespacePendingIntentChanged")
	}
}

func preparationEnvelope(t *testing.T, input preparationProcessInput) journal.Envelope {
	envelope, _ := preparationVersionedEnvelope(t, input)
	return envelope
}
func preparationVersionedEnvelope(t *testing.T, input preparationProcessInput) (journal.Envelope, journal.Version) {
	t.Helper()
	response, err := client.Get(local.Server + "/api/v1/namespaces/" + input.Base.Pin.Parent.Ref.Name + "/configmaps/" + input.Base.Pin.Cell.Record.Ref.Name)
	if err != nil {
		t.Fatal("PreparationWitnessJournalUnavailable")
	}
	defer response.Body.Close()
	var cell corev1.ConfigMap
	if response.StatusCode != 200 || json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&cell) != nil || string(cell.UID) != input.Base.Pin.Cell.Record.Ref.ProviderID {
		t.Fatal("PreparationWitnessCellChanged")
	}
	envelope, err := journal.Decode([]byte(cell.Data[kubejournal.DataKey]))
	if err != nil || envelope.Subject != input.Base.Pin.Subject {
		t.Fatal("PreparationWitnessJournalInvalid")
	}
	return envelope, journal.Version{UID: string(cell.UID), ResourceVersion: cell.ResourceVersion}
}

func preparationPending(t *testing.T, input preparationProcessInput, store *runtimeinputs.Store, kind string, raw []byte, birth preparationCreationMetadata) bool {
	t.Helper()
	plan, denied := acceptedPreparation(input)
	if denied != "" {
		t.Error("PreparationWitnessBootstrapInvalid")
		return false
	}
	namespace, err := plan.Namespace()
	desired, desiredErr := plan.Desired()
	envelope := preparationEnvelope(t, input)
	if err != nil || desiredErr != nil || envelope.Admission == nil || envelope.Admission.State != journal.AdmissionActive {
		t.Error("PreparationPOSTBeforeActiveAdmission")
		return false
	}
	var pending *journal.Write
	for _, row := range envelope.Writes {
		if row.Preconditions.Kind == kind && row.State == journal.Pending {
			copy := row
			if pending != nil {
				return false
			}
			pending = &copy
		}
	}
	if pending == nil {
		t.Error("PreparationExactPendingAbsent")
		return false
	}
	p := pending.Preconditions
	if pending.OperationID != plan.View().OperationID || p.Action != "Create" || p.ProviderOperationID != plan.View().OperationID || p.TargetRevision != desired.Revision || pending.ResultUID != "" || pending.ResultVersion != "" || pending.ResultSpecDigest != "" {
		return false
	}
	var object runtime.Object
	var intent ownership.Intent
	if kind == "Namespace" {
		object, intent = namespace.Object.DeepCopy(), namespace.Intent
		if pending.Input != (journal.InputReference{}) || p.ParentUID != "" || p.Fingerprint != journal.DesiredDigest(desired) {
			return false
		}
	} else {
		if !pending.Input.MatchesKind(kind) {
			return false
		}
		var parentWrite *journal.Write
		for _, row := range envelope.Writes {
			if row.Preconditions.Kind == "Namespace" && row.State == journal.Committed {
				copy := row
				parentWrite = &copy
			}
		}
		if parentWrite == nil || birth.UID == "" || parentWrite.ResultUID != birth.UID || parentWrite.ResultVersion == "" || parentWrite.Preconditions.ProviderOperationID != namespace.Intent.OperationID || parentWrite.Preconditions.TargetRevision != namespace.Intent.Revision || parentWrite.Preconditions.Name != namespace.Object.Name || parentWrite.ResultUID != p.ParentUID || parentWrite.Preconditions.Fingerprint != namespace.Intent.SpecFingerprint {
			return false
		}
		// Authority is reconstructed from the supervisor bytes, never the POST.
		var bootstrap preparationBootstrap
		if json.Unmarshal(input.Accepted, &bootstrap) != nil {
			return false
		}
		base, e := ownership.NewContext(bootstrap.Authority)
		if e != nil {
			return false
		}
		i := namespace.Intent
		parent := ownership.Record{Ref: ownership.ResourceRef{Scope: i.Scope, Purpose: i.Purpose, Name: i.Name, ProviderID: parentWrite.ResultUID}, Owner: i.Owner, CreationOperationID: i.OperationID, CreationIdempotencyKey: i.IdempotencyKey, Revision: i.Revision, SpecFingerprint: i.SpecFingerprint, WriterEpoch: parentWrite.WriterEpoch}
		adapter, _ := typedClient(t)
		binding, e := adapter.BindNamespace(context.Background(), base, parent, plan.View().OperationID)
		if e != nil {
			t.Error("PreparationIndependentParentUnverified")
			return false
		}
		target := runtimeinputs.Target{CellUID: input.Base.Pin.Cell.Record.Ref.ProviderID, Parent: binding.Parent(), AttemptID: plan.View().OperationID, Revision: desired.Revision, DesiredDigest: journal.DesiredDigest(desired), Name: p.Name, Fingerprint: p.Fingerprint}
		purpose := map[string]string{"ServiceAccount": "service-account", "Deployment": "deployment", "Service": "service"}[kind]
		namespacedIntent, e := binding.Context().Intent(purpose, p.Name, p.TargetRevision, p.ProviderOperationID, p.Fingerprint)
		if e != nil {
			return false
		}
		intent = namespacedIntent.Intent
		switch kind {
		case "ServiceAccount":
			payload, e := store.ResolveServiceAccount(context.Background(), pending.Input, target)
			if e != nil {
				return false
			}
			object = payload.ObjectForProvider()
		case "Deployment":
			payload, e := store.Resolve(context.Background(), pending.Input, target)
			if e != nil {
				return false
			}
			object = payload.ObjectForProvider()
		case "Service":
			payload, e := store.ResolveService(context.Background(), pending.Input, target)
			if e != nil {
				return false
			}
			object = payload.ObjectForProvider()
		default:
			return false
		}
		pair, e := plan.Render(binding.Context())
		if e != nil {
			return false
		}
		var accepted runtime.Object
		var acceptedIntent ownership.NamespacedIntent
		switch kind {
		case "ServiceAccount":
			rendered, e := pair.ServiceAccount()
			if e != nil {
				return false
			}
			accepted, acceptedIntent = rendered.Object, rendered.Intent
		case "Deployment":
			rendered, e := pair.Deployment()
			if e != nil {
				return false
			}
			accepted, acceptedIntent = rendered.Object, rendered.Intent
		case "Service":
			rendered, e := pair.Service()
			if e != nil {
				return false
			}
			accepted, acceptedIntent = rendered.Object, rendered.Intent
		}
		if acceptedIntent != namespacedIntent {
			t.Error("PreparationPendingIntentDiffersFromFrozenPlan")
			return false
		}
		acceptedBody, _ := json.Marshal(accepted)
		retainedBody, _ := json.Marshal(object)
		if !bytes.Equal(acceptedBody, retainedBody) {
			t.Error("PreparationRetainedInputDiffersFromFrozenPlan")
			return false
		}
		meta, ok := object.(metav1.Object)
		if !ok {
			return false
		}
		meta.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "v1", Kind: "Namespace", Name: binding.Parent().Name, UID: types.UID(binding.Parent().UID)}})
		annotations := meta.GetAnnotations()
		annotations[ownership.ParentNamespaceUIDAnnotation] = binding.Parent().UID
		meta.SetAnnotations(annotations)
	}
	meta, ok := object.(metav1.Object)
	if !ok || meta.GetName() != p.Name || intent.SpecFingerprint != p.Fingerprint {
		return false
	}
	annotations := meta.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	for key, value := range provider.CreationAnnotations(intent, ownership.Writer{Epoch: pending.WriterEpoch}) {
		annotations[key] = value
	}
	annotations[provider.OperationAnnotation] = plan.View().OperationID
	if kind == "Namespace" {
		annotations[provider.AttemptAnnotation] = plan.View().OperationID
		meta.SetFinalizers([]string{provider.NamespaceLifecycleFinalizer})
	}
	meta.SetAnnotations(annotations)
	expected, _ := json.Marshal(object)
	var expectedValue, actualValue any
	if json.Unmarshal(expected, &expectedValue) != nil || json.Unmarshal(raw, &actualValue) != nil {
		return false
	}
	expected, _ = json.Marshal(expectedValue)
	actual, _ := json.Marshal(actualValue)
	if !bytes.Equal(expected, actual) {
		t.Error("PreparationPOSTDiffersFromExactOriginalStampedBody")
		return false
	}
	return true
}

func preparationMetadata(kind string, raw []byte) (preparationCreationMetadata, bool) {
	var native nativeCreationMetadata
	if kind == "Deployment" || kind == "Service" {
		var err error
		native, err = nativeMetadata(runtimefault.Kind(kind), raw)
		if err != nil {
			return preparationCreationMetadata{}, false
		}
	}
	var meta metav1.ObjectMeta
	switch kind {
	case "Namespace":
		var object corev1.Namespace
		if json.Unmarshal(raw, &object) != nil {
			return preparationCreationMetadata{}, false
		}
		meta = object.ObjectMeta
	case "ServiceAccount":
		var object corev1.ServiceAccount
		if json.Unmarshal(raw, &object) != nil {
			return preparationCreationMetadata{}, false
		}
		meta = object.ObjectMeta
	case "Deployment":
		var object appsv1.Deployment
		if json.Unmarshal(raw, &object) != nil {
			return preparationCreationMetadata{}, false
		}
		meta = object.ObjectMeta
	case "Service":
		var object corev1.Service
		if json.Unmarshal(raw, &object) != nil {
			return preparationCreationMetadata{}, false
		}
		meta = object.ObjectMeta
	default:
		return preparationCreationMetadata{}, false
	}
	native.Name, native.Namespace, native.UID, native.Version = meta.Name, meta.Namespace, string(meta.UID), meta.ResourceVersion
	native.ParentUID, native.Operation, native.Fingerprint = meta.Annotations[ownership.ParentNamespaceUIDAnnotation], meta.Annotations[provider.CreationOperationAnnotation], meta.Annotations[provider.FingerprintAnnotation]
	return preparationCreationMetadata{native, meta.Annotations[provider.RevisionAnnotation], meta.Annotations[provider.CreationKeyAnnotation], meta.Annotations[provider.OwnerAnnotation]}, true
}

func preparationCreationProxy(t *testing.T, input preparationProcessInput, store *runtimeinputs.Store, denySA bool) (string, []byte, *preparationCapture) {
	t.Helper()
	target, _ := url.Parse(local.Server)
	forward := httputil.NewSingleHostReverseProxy(target)
	forward.Transport = client.Transport
	capture := &preparationCapture{created: map[string]preparationCreationMetadata{}, posts: map[string]int{}, checks: map[string]int{}, denySA: denySA}
	cellPath := "/api/v1/namespaces/" + input.Base.Pin.Parent.Ref.Name + "/configmaps/" + input.Base.Pin.Cell.Record.Ref.Name
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capture.mu.Lock()
		if r.Method == http.MethodGet {
			capture.reads++
		} else if r.URL.Path != cellPath {
			capture.mutations++
		}
		capture.mu.Unlock()
		if r.Method == http.MethodGet {
			capture.mu.Lock()
			created := map[string]preparationCreationMetadata{}
			for k, v := range capture.created {
				created[k] = v
			}
			capture.mu.Unlock()
			for kind, value := range created {
				path := "/api/v1/namespaces/" + value.Namespace + "/" + map[string]string{"ServiceAccount": "serviceaccounts", "Service": "services"}[kind] + "/" + value.Name
				if kind == "Deployment" {
					path = "/apis/apps/v1/namespaces/" + value.Namespace + "/deployments/" + value.Name
				}
				if kind == "Namespace" {
					path = "/api/v1/namespaces/" + value.Name
				}
				if r.URL.Path != path {
					continue
				}
				envelope := preparationEnvelope(t, input)
				matched := false
				for _, row := range envelope.Writes {
					p := row.Preconditions
					if p.Kind != kind || p.Name != value.Name || p.Fingerprint != value.Fingerprint || p.ParentUID != value.ParentUID || p.ProviderOperationID != value.Operation || p.TargetRevision != value.Revision {
						continue
					}
					if kind == "Namespace" {
						// Existing bootstrap validates its known201 binding then GETs
						// canonical namespace state before recording observed RV. This
						// is NOT the typed child creation-RV-before-GET contract.
						matched = preparationNamespaceReceipt(row, value)
					} else {
						matched = row.State == journal.Committed && row.ResultUID == value.UID && row.ResultVersion == value.Version && row.Input.MatchesKind(kind) && (kind != "Service" || row.ResultSpecDigest == value.AllocatedDigest && row.ResultSpecContract == value.SpecContract)
					}
				}
				if !matched {
					t.Error("PreparationGETBeforeCanonicalNativeReceiptCommit")
					http.Error(w, "fixture receipt unavailable", 409)
					return
				}
				capture.mu.Lock()
				capture.checks[kind]++
				deny := capture.denySA && kind == "ServiceAccount"
				capture.mu.Unlock()
				if deny {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(403)
					_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "Status", "status": "Failure", "reason": "Forbidden", "message": "owned observation denied", "code": 403})
					return
				}
			}
		}
		if r.Method != http.MethodPost {
			forward.ServeHTTP(w, r)
			return
		}
		kind := ""
		if r.URL.Path == "/api/v1/namespaces" {
			kind = "Namespace"
		}
		for suffix, k := range map[string]string{"/serviceaccounts": "ServiceAccount", "/deployments": "Deployment", "/services": "Service"} {
			if strings.HasSuffix(r.URL.Path, suffix) {
				kind = k
			}
		}
		if kind == "" {
			t.Error("PreparationUnexpectedPOST")
			http.Error(w, "fixture POST refused", 409)
			return
		}
		capture.mu.Lock()
		capture.posts[kind]++
		birth := capture.created["Namespace"]
		capture.mu.Unlock()
		body, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
		if err != nil || len(body) > 1<<20 || !preparationPending(t, input, store, kind, body, birth) {
			http.Error(w, "fixture Pending unproven", 409)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		out := r.Clone(r.Context())
		out.URL.Scheme, out.URL.Host = target.Scheme, target.Host
		out.Host = target.Host
		out.RequestURI = ""
		response, err := client.Transport.RoundTrip(out)
		if err != nil {
			t.Error("PreparationNativePOSTUnknown")
			http.Error(w, "fixture upstream unavailable", 502)
			return
		}
		defer response.Body.Close()
		body, err = io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
		if err != nil || len(body) > 1<<20 || response.StatusCode != 201 {
			t.Error("PreparationNativePOSTNotKnown201")
			http.Error(w, "fixture create unproven", 502)
			return
		}
		value, valid := preparationMetadata(kind, body)
		if !valid || value.UID == "" || value.Version == "" || value.Name == "" || value.Operation == "" || value.Fingerprint == "" {
			t.Error("PreparationNativeReceiptInvalid")
			http.Error(w, "fixture receipt invalid", 502)
			return
		}
		if kind == "Namespace" {
			plan, denied := acceptedPreparation(input)
			namespace, e := plan.Namespace()
			owner, _ := json.Marshal(namespace.Intent.Owner)
			if denied != "" || e != nil || value.Name != namespace.Intent.Name || value.Namespace != "" || value.Operation != namespace.Intent.OperationID || value.CreationKey != namespace.Intent.IdempotencyKey || value.Revision != namespace.Intent.Revision || value.Owner != string(owner) || value.Fingerprint != namespace.Intent.SpecFingerprint {
				t.Error("PreparationKnownNamespaceReceiptProvenanceInvalid")
				http.Error(w, "fixture namespace identity invalid", 502)
				return
			}
		}
		capture.mu.Lock()
		capture.created[kind] = value
		capture.mu.Unlock()
		for key, values := range response.Header {
			if key == "Content-Length" || key == "Content-Encoding" {
				continue
			}
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(201)
		_, _ = w.Write(body)
	})
	endpoint, ca := startRuntimeDispatchProxy(t, handler)
	return endpoint, ca, capture
}
