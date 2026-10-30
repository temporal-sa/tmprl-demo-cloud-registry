//go:build integration

package integration

// Independent real API witness for the TWO accepted retirement reasons. It
// verifies acknowledged exact prepared bytes/UID/RV before forwarding cleanup.
import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubejournal"
	corev1 "k8s.io/api/core/v1"
)

type retirementReasonWitness struct{ namespaces, configmaps, cleanup atomic.Int64 }

func retirementReasonRecoveryID(attempt string) string {
	encoded, _ := json.Marshal([]string{"retirement", attempt, "namespace"})
	sum := sha256.Sum256(encoded)
	return "retirement-" + hex.EncodeToString(sum[:])
}

func retirementReasonPending(t *testing.T, input retirementReasonInput, method, path string, body []byte) bool {
	t.Helper()
	response, err := client.Get(local.Server + "/api/v1/namespaces/" + input.Pin.Parent.Ref.Name + "/configmaps/" + input.Pin.Cell.Record.Ref.Name)
	if err != nil {
		t.Error("RetirementReasonWitnessReadFailed")
		return false
	}
	defer response.Body.Close()
	var cell corev1.ConfigMap
	if response.StatusCode != 200 || json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&cell) != nil || string(cell.UID) != input.Pin.Cell.Record.Ref.ProviderID {
		t.Error("RetirementReasonWitnessCellChanged")
		return false
	}
	envelope, err := journal.Decode([]byte(cell.Data[kubejournal.DataKey]))
	state, step := retirementReasonState(input.Reason)
	if err != nil || envelope.Subject != input.Pin.Subject || envelope.Admission == nil || envelope.Admission.State != journal.AdmissionActive || envelope.Projection == nil || envelope.Projection.Identity.OperationID != input.Attempt || envelope.Projection.State != state || envelope.Projection.Step != step || envelope.Projection.Failure != nil || envelope.Projection.Availability.RouteState != diagnostics.RouteUnknown {
		t.Error("RetirementReasonCleanupBeforeExpectedState")
		return false
	}
	var raw struct {
		Metadata      struct{ Name, UID, ResourceVersion string }
		Preconditions struct{ UID, ResourceVersion string }
	}
	if json.Unmarshal(body, &raw) != nil {
		return false
	}
	uid, version, action := raw.Metadata.UID, raw.Metadata.ResourceVersion, "Finalize"
	if method == http.MethodDelete {
		uid, version, action = raw.Preconditions.UID, raw.Preconditions.ResourceVersion, "Delete"
	}
	digest := sha256.Sum256(body)
	for _, attempt := range envelope.Attempts {
		if attempt.OperationID != input.Attempt || attempt.State != journal.Draining || attempt.DrainReason != input.Reason || attempt.NamespaceUID != uid || path != "/api/v1/namespaces/"+attempt.NamespaceName {
			continue
		}
		recovery := false
		for _, outcome := range envelope.Projection.Recovery {
			if outcome.ID == retirementReasonRecoveryID(input.Attempt) && outcome.Step == "namespace-cleanup" && outcome.State == diagnostics.OutcomeRunning {
				for _, resource := range outcome.RetainedResources {
					if resource.Provider == "kubernetes" && resource.Kind == "Namespace" && resource.ID == uid {
						recovery = true
					}
				}
			}
		}
		if !recovery {
			return false
		}
		for _, write := range envelope.Writes {
			p := write.Preconditions
			if write.State == journal.Pending && write.OperationID == attempt.OperationID && p.Kind == "Namespace" && p.Action == action && p.Name == attempt.NamespaceName && p.UID == uid && p.Version == version && p.Fingerprint == hex.EncodeToString(digest[:]) && p.TargetRevision == attempt.Revision && p.ProviderOperationID == write.ID {
				return true
			}
		}
	}
	t.Error("RetirementReasonExactCleanupPendingMissing")
	return false
}

func retirementReasonProxy(t *testing.T, input retirementReasonInput) (string, []byte, *retirementReasonWitness) {
	t.Helper()
	target, _ := url.Parse(local.Server)
	forward := httputil.NewSingleHostReverseProxy(target)
	forward.Transport = client.Transport
	forward.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		http.Error(w, "owned fixture upstream unavailable", 502)
	}
	proof := &retirementReasonWitness{}
	cellPath := "/api/v1/namespaces/" + input.Pin.Parent.Ref.Name + "/configmaps/" + input.Pin.Cell.Record.Ref.Name
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete || r.Method == http.MethodPut && r.URL.Path != cellPath {
			body, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
			if err != nil || len(body) > 1<<20 || !retirementReasonPending(t, input, r.Method, r.URL.Path, body) {
				http.Error(w, "owned fixture exact cleanup refused", 409)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			proof.cleanup.Add(1)
		}
		if r.Method == http.MethodPost {
			kind := "ConfigMap"
			if r.URL.Path == "/api/v1/namespaces" {
				kind = "Namespace"
			}
			body, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
			if err != nil || len(body) > 1<<20 || provePendingRuntimePost(input.Pin, cellPath, kind, body) != nil {
				t.Error("RetirementReasonExactCreatePendingMissing")
				http.Error(w, "owned fixture exact creation refused", 409)
				return
			}
			if kind == "ConfigMap" {
				var object corev1.ConfigMap
				if json.Unmarshal(body, &object) != nil || object.Name != "platform-project-metadata" || object.Namespace == "" || r.URL.Path != "/api/v1/namespaces/"+object.Namespace+"/configmaps" {
					t.Error("RetirementReasonCreateScopeInvalid")
					http.Error(w, "owned fixture creation scope refused", 409)
					return
				}
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			if kind == "Namespace" {
				proof.namespaces.Add(1)
			} else {
				proof.configmaps.Add(1)
			}
		}
		forward.ServeHTTP(w, r)
	})
	endpoint, ca := startRuntimeDispatchProxy(t, handler)
	return endpoint, ca, proof
}
