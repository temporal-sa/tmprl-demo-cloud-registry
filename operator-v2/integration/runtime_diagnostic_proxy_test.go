//go:build integration

package integration

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubejournal"
	corev1 "k8s.io/api/core/v1"
)

// Only a known forwarded exact fault-projection PUT is hidden. This does not
// turn a later independent read into an acknowledgement received by the actor.
func diagnosticDispatchProxy(t *testing.T, input diagnosticProcessInput, lose bool) (string, []byte, *atomic.Int64, *atomic.Int64, *atomic.Bool) {
	t.Helper()
	target, _ := url.Parse(local.Server)
	forward := httputil.NewSingleHostReverseProxy(target)
	forward.Transport = client.Transport
	cellPath := "/api/v1/namespaces/" + input.Base.Pin.Parent.Ref.Name + "/configmaps/" + input.Base.Pin.Cell.Record.Ref.Name
	posts, updates, blocked := &atomic.Int64{}, &atomic.Int64{}, &atomic.Bool{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if blocked.Load() && r.Method == http.MethodGet && r.URL.Path == cellPath {
			http.Error(w, "owned fixture observation unavailable", 503)
			return
		}
		if r.Method == http.MethodPost {
			if strings.HasSuffix(r.URL.Path, "/services") {
				posts.Add(1)
			}
			if r.URL.Path == "/api/v1/namespaces" {
				body, e := io.ReadAll(io.LimitReader(r.Body, 1<<20))
				if e != nil {
					http.Error(w, "fixture body unavailable", 500)
					return
				}
				r.Body = io.NopCloser(bytes.NewReader(body))
				if e := provePendingRuntimePost(input.Base.Pin, cellPath, "Namespace", body); e != nil {
					t.Error(e)
					http.Error(w, "namespace lacks Pending acknowledgement", 500)
					return
				}
			}
			forward.ServeHTTP(w, r)
			return
		}
		if r.Method != http.MethodPut || r.URL.Path != cellPath {
			forward.ServeHTTP(w, r)
			return
		}
		body, e := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if e != nil {
			http.Error(w, "fixture body unavailable", 500)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		var cell corev1.ConfigMap
		if json.Unmarshal(body, &cell) != nil {
			http.Error(w, "fixture cell invalid", 500)
			return
		}
		envelope, e := journal.Decode([]byte(cell.Data[kubejournal.DataKey]))
		if e != nil || envelope.Subject != input.Base.Pin.Subject {
			http.Error(w, "fixture journal invalid", 500)
			return
		}
		if envelope.Projection == nil || envelope.Projection.Failure == nil || envelope.Projection.Failure.Code != input.ExpectedFailure || envelope.Admission == nil || envelope.Admission.State != journal.AdmissionActive {
			forward.ServeHTTP(w, r)
			return
		}
		if envelope.Admission == nil || envelope.Admission.State != journal.AdmissionActive || envelope.Projection.Identity != diagnosticIdentity(input) || len(envelope.Writes) != 1 || envelope.Writes[0].Preconditions.Kind != "Namespace" || envelope.Writes[0].State != journal.Committed {
			t.Error("pre-Pending diagnostic proposal changed accepted facts")
			http.Error(w, "diagnostic facts unproven", 500)
			return
		}
		updates.Add(1)
		if !lose {
			forward.ServeHTTP(w, r)
			return
		}
		outbound := r.Clone(r.Context())
		outbound.URL.Scheme, outbound.URL.Host = target.Scheme, target.Host
		outbound.Host = target.Host
		outbound.RequestURI = ""
		response, e := client.Transport.RoundTrip(outbound)
		if e != nil {
			t.Error("owned projection upstream unavailable")
			http.Error(w, "fixture upstream unavailable", 502)
			return
		}
		_, _ = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Error("exact projection CAS was not accepted")
			http.Error(w, "owned CAS rejected", response.StatusCode)
			return
		}
		blocked.Store(true)
		connection, _, e := w.(http.Hijacker).Hijack()
		if e == nil {
			connection.Close()
		}
	})
	endpoint, ca := startRuntimeDispatchProxy(t, handler)
	return endpoint, ca, posts, updates, blocked
}
