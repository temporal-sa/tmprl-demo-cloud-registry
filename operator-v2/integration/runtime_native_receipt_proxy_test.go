//go:build integration

package integration

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Only immutable safe metadata/hash witnesses survive the response callback.
// Raw POST/GET objects remain inside this owned proxy, never activity/history.
type nativeCreationMetadata struct {
	Name, Namespace, UID, Version, ParentUID, Operation, Fingerprint string
	AllocatedDigest, SpecContract                                    string
}
type nativeCapture struct {
	mu            sync.Mutex
	created       nativeCreationMetadata
	posts, checks int
	injected      int
}

func (c *nativeCapture) snapshot() (nativeCreationMetadata, int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.created, c.posts, c.checks
}
func nativeMetadata(kind runtimefault.Kind, body []byte) (nativeCreationMetadata, error) {
	var meta metav1.ObjectMeta
	result := nativeCreationMetadata{}
	if kind == runtimefault.Deployment {
		var object appsv1.Deployment
		if e := json.Unmarshal(body, &object); e != nil {
			return result, e
		}
		meta = object.ObjectMeta
	} else {
		var object corev1.Service
		if e := json.Unmarshal(body, &object); e != nil {
			return result, e
		}
		meta = object.ObjectMeta
		raw, e := json.Marshal(object.Spec)
		if e != nil {
			return result, e
		}
		sum := sha256.Sum256(raw)
		result.AllocatedDigest, result.SpecContract = hex.EncodeToString(sum[:]), provider.ServiceSpecContract
	}
	result.Name, result.Namespace, result.UID, result.Version = meta.Name, meta.Namespace, string(meta.UID), meta.ResourceVersion
	result.ParentUID = meta.Annotations[ownership.ParentNamespaceUIDAnnotation]
	result.Operation = meta.Annotations[provider.CreationOperationAnnotation]
	result.Fingerprint = meta.Annotations[provider.FingerprintAnnotation]
	return result, nil
}
func nativeRowCommitted(t *testing.T, input nativeProcessInput, created nativeCreationMetadata, first bool) bool {
	t.Helper()
	path := "/api/v1/namespaces/" + input.Base.Pin.Parent.Ref.Name + "/configmaps/" + input.Base.Pin.Cell.Record.Ref.Name
	response, e := client.Get(local.Server + path)
	if e != nil {
		t.Error("owned receipt journal unavailable")
		return false
	}
	defer response.Body.Close()
	var object corev1.ConfigMap
	if response.StatusCode != 200 || json.NewDecoder(response.Body).Decode(&object) != nil || string(object.UID) != input.Base.Pin.Cell.Record.Ref.ProviderID {
		t.Error("receipt cell identity changed")
		return false
	}
	envelope, e := journal.Decode([]byte(object.Data[kubejournal.DataKey]))
	if e != nil || envelope.Subject != input.Base.Pin.Subject || envelope.Admission == nil || first && envelope.Admission.State != journal.AdmissionActive {
		t.Error("native GET preceded active exact journal")
		return false
	}
	for _, write := range envelope.Writes {
		p := write.Preconditions
		if p.Kind != string(input.Kind) || p.Name != created.Name || p.ParentUID != created.ParentUID || p.ProviderOperationID != created.Operation || p.Fingerprint != created.Fingerprint {
			continue
		}
		if write.State != journal.Committed || write.ResultUID != created.UID || write.ResultVersion != created.Version || !write.Input.MatchesKind(string(input.Kind)) {
			t.Error("first child GET preceded canonical native POST UID/RV commit")
			return false
		}
		if input.Kind == runtimefault.Service && (write.ResultSpecDigest != created.AllocatedDigest || write.ResultSpecContract != created.SpecContract) {
			t.Error("first Service GET preceded complete allocation receipt commit")
			return false
		}
		return true
	}
	t.Error("native GET lacks exact recorded creation")
	return false
}
func nativeCreationProxy(t *testing.T, input nativeProcessInput) (string, []byte, *nativeCapture) {
	t.Helper()
	target, _ := url.Parse(local.Server)
	forward := httputil.NewSingleHostReverseProxy(target)
	forward.Transport = client.Transport
	capture := &nativeCapture{}
	collection := "/services"
	if input.Kind == runtimefault.Deployment {
		collection = "/deployments"
	}
	cellPath := "/api/v1/namespaces/" + input.Base.Pin.Parent.Ref.Name + "/configmaps/" + input.Base.Pin.Cell.Record.Ref.Name
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			created, _, _ := capture.snapshot()
			prefix := "/api/v1/namespaces/"
			if input.Kind == runtimefault.Deployment {
				prefix = "/apis/apps/v1/namespaces/"
			}
			if created.UID != "" && r.URL.Path == prefix+created.Namespace+collection+"/"+created.Name {
				capture.mu.Lock()
				first := capture.checks == 0
				capture.mu.Unlock()
				if !nativeRowCommitted(t, input, created, first) {
					http.Error(w, "native receipt unproven before GET", 409)
					return
				}
				capture.mu.Lock()
				capture.checks++
				inject := capture.injected
				if capture.injected < 2 {
					capture.injected++
				}
				capture.mu.Unlock()
				if inject < 2 {
					// Two distinct nonretryable fixture observations, never a mutation.
					code, reason, message := 403, "Forbidden", "owned fixture observation denied"
					if inject == 1 {
						code, reason, message = 404, "NotFound", "owned fixture observation missing"
					}
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(code)
					_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "v1", "kind": "Status", "status": "Failure", "reason": reason, "message": message, "code": code})
					return
				}
			}
		}
		if r.Method != http.MethodPost {
			forward.ServeHTTP(w, r)
			return
		}
		if r.URL.Path == "/api/v1/namespaces" {
			body, e := io.ReadAll(io.LimitReader(r.Body, 1<<20))
			if e != nil {
				http.Error(w, "owned namespace body unavailable", 500)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			if e := provePendingRuntimePost(input.Base.Pin, cellPath, "Namespace", body); e != nil {
				t.Error(e)
				http.Error(w, "namespace Pending missing", 500)
				return
			}
			forward.ServeHTTP(w, r)
			return
		}
		if !strings.HasSuffix(r.URL.Path, collection) {
			forward.ServeHTTP(w, r)
			return
		}
		capture.mu.Lock()
		capture.posts++ // Count attempts, including any forbidden duplicate409.
		capture.mu.Unlock()
		outbound := r.Clone(r.Context())
		outbound.URL.Scheme, outbound.URL.Host = target.Scheme, target.Host
		outbound.Host = target.Host
		outbound.RequestURI = ""
		response, e := client.Transport.RoundTrip(outbound)
		if e != nil {
			t.Error("owned native create upstream unavailable")
			http.Error(w, "fixture upstream unavailable", 502)
			return
		}
		defer response.Body.Close()
		body, e := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
		if e != nil || len(body) > 1<<20 || response.StatusCode != http.StatusCreated {
			t.Error("native create was not exact known201")
			http.Error(w, "fixture native create unproven", 502)
			return
		}
		created, e := nativeMetadata(input.Kind, body)
		if e != nil || created.UID == "" || created.Version == "" || created.Name == "" || created.Namespace == "" || created.ParentUID == "" || created.Operation == "" || created.Fingerprint == "" {
			t.Error("canonical POST metadata unproven")
			http.Error(w, "fixture creation receipt missing", 502)
			return
		}
		capture.mu.Lock()
		capture.created = created
		capture.mu.Unlock()
		for key, values := range response.Header {
			if key == "Content-Length" || key == "Content-Encoding" {
				continue
			}
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(response.StatusCode)
		_, _ = w.Write(body)
	})
	endpoint, ca := startRuntimeDispatchProxy(t, handler)
	return endpoint, ca, capture
}
