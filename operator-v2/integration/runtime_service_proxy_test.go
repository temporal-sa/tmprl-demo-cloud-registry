//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubejournal"
	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeinputs"
	corev1 "k8s.io/api/core/v1"
)

// Service-specific independent witness: neither provider guard return nor GET
// after dispatch substitutes for the pre-POST acknowledged exact private intent.
func serviceDispatchProxy(t *testing.T, input privateRuntimeProcessInput, store *runtimeinputs.Store, lose bool) (string, []byte, *atomic.Int64, *atomic.Int64) {
	t.Helper()
	target, _ := url.Parse(local.Server)
	forward := httputil.NewSingleHostReverseProxy(target)
	forward.Transport = client.Transport
	cellPath := "/api/v1/namespaces/" + input.Pin.Parent.Ref.Name + "/configmaps/" + input.Pin.Cell.Record.Ref.Name
	posts, gets := &atomic.Int64{}, &atomic.Int64{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/services/") {
			gets.Add(1)
		}
		if r.Method != http.MethodPost {
			forward.ServeHTTP(w, r)
			return
		}
		body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			http.Error(w, "owned fixture body unavailable", 500)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		if !strings.HasSuffix(r.URL.Path, "/services") {
			kind := "ConfigMap"
			if r.URL.Path == "/api/v1/namespaces" {
				kind = "Namespace"
			}
			if err := provePendingRuntimePost(input.Pin, cellPath, kind, body); err != nil {
				t.Error(err)
				http.Error(w, "pending fixture intent missing", 500)
				return
			}
			forward.ServeHTTP(w, r)
			return
		}
		var object corev1.Service
		if json.Unmarshal(body, &object) != nil {
			http.Error(w, "owned Service invalid", 500)
			return
		}
		response, err := client.Get(local.Server + cellPath)
		if err != nil {
			t.Error(err)
			http.Error(w, "journal unavailable", 500)
			return
		}
		var cell corev1.ConfigMap
		decodeErr := json.NewDecoder(response.Body).Decode(&cell)
		response.Body.Close()
		if response.StatusCode != 200 || decodeErr != nil || string(cell.UID) != input.Pin.Cell.Record.Ref.ProviderID {
			t.Error("pinned cell unproven")
			http.Error(w, "cell missing", 500)
			return
		}
		envelope, err := journal.Decode([]byte(cell.Data[kubejournal.DataKey]))
		if err != nil || envelope.Subject != input.Pin.Subject || envelope.Admission == nil || envelope.Admission.State != journal.AdmissionActive {
			t.Error("Service preceded exact active admission")
			http.Error(w, "admission missing", 500)
			return
		}
		verified := false
		for _, attempt := range envelope.Attempts {
			if attempt.State != journal.Observed || attempt.OperationID != runtimeAttempt(input.Pin) || attempt.NamespaceUID != object.Annotations[ownership.ParentNamespaceUIDAnnotation] {
				continue
			}
			for _, write := range envelope.Writes {
				p := write.Preconditions
				if write.State != journal.Pending || write.OperationID != attempt.OperationID || p.Kind != "Service" || p.Action != "Create" || p.Name != object.Name || p.ParentUID != attempt.NamespaceUID || p.Fingerprint != object.Annotations[provider.FingerprintAnnotation] || p.TargetRevision != attempt.Revision || p.ProviderOperationID != object.Annotations[provider.OperationAnnotation] || !write.Input.MatchesKind("Service") || write.ResultUID != "" || write.ResultSpecDigest != "" || write.ResultSpecContract != "" {
					continue
				}
				parent := request(t, "GET", "/api/v1/namespaces/"+attempt.NamespaceName, nil, 200)
				var owner ownership.Owner
				if json.Unmarshal([]byte(text(field(metadata(parent), "annotations"), provider.OwnerAnnotation)), &owner) != nil || owner != input.StoreConfig.Owner || text(metadata(parent), "uid") != attempt.NamespaceUID {
					t.Error("Service parent owner/UID unproven")
					continue
				}
				parentPin := ownership.NamespaceParent{ClusterID: local.RunID, Name: attempt.NamespaceName, UID: attempt.NamespaceUID, Owner: owner, CreationOperationID: text(field(metadata(parent), "annotations"), provider.CreationOperationAnnotation), CreationIdempotencyKey: text(field(metadata(parent), "annotations"), provider.CreationKeyAnnotation)}
				payload, resolveErr := store.ResolveService(context.Background(), write.Input, runtimeinputs.Target{CellUID: input.Pin.Cell.Record.Ref.ProviderID, Parent: parentPin, AttemptID: attempt.OperationID, Revision: attempt.Revision, DesiredDigest: attempt.DesiredDigest, Name: p.Name, Fingerprint: p.Fingerprint})
				original := payload.ObjectForProvider()
				if resolveErr != nil || original == nil {
					t.Error("private Service absent before POST", resolveErr)
					continue
				}
				fingerprint, fpErr := provider.ServiceFingerprint(original)
				if fpErr != nil || fingerprint != p.Fingerprint || original.Name != object.Name || original.Namespace != object.Namespace || original.Kind != object.Kind || original.APIVersion != object.APIVersion || !reflect.DeepEqual(original.Spec, object.Spec) || !reflect.DeepEqual(original.Labels, object.Labels) {
					t.Error("POST differs from retained original Service")
					continue
				}
				annotationsMatch := true
				for key, value := range original.Annotations {
					if object.Annotations[key] != value {
						annotationsMatch = false
					}
				}
				if !annotationsMatch {
					t.Error("retained Service annotations changed")
					continue
				}
				verified = true
			}
		}
		if !verified {
			http.Error(w, "exact referenced Pending Service unproven", 500)
			return
		}
		posts.Add(1)
		if !lose {
			forward.ServeHTTP(w, r)
			return
		}
		outbound := r.Clone(r.Context())
		outbound.URL.Scheme, outbound.URL.Host = target.Scheme, target.Host
		outbound.Host = target.Host
		outbound.RequestURI = ""
		response, err = client.Transport.RoundTrip(outbound)
		if err != nil {
			http.Error(w, "upstream unavailable", 502)
			return
		}
		_, _ = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusCreated {
			http.Error(w, "upstream rejected", response.StatusCode)
			return
		}
		connection, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			connection.Close()
		}
	})
	endpoint, ca := startRuntimeDispatchProxy(t, handler)
	return endpoint, ca, posts, gets
}
