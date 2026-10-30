//go:build integration

package integration

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	renderer "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/render/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeconfig"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeinputs"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

type literalRuntimeFixture struct {
	config     runtimeconfig.Config
	repository *journal.Repository
	store      *runtimeinputs.Store
	pin        runtimePin
	worker     *runtimeconfig.Worker
	session    *runtimeconfig.Session
	objects    map[string]renderer.DesiredObject
	read       provider.LiteralWorkloadRead
	binding    *provider.NamespaceBinding
	posts      *literalCreateCounts
}

type literalCreateCounts struct {
	mu     sync.Mutex
	values map[string]int
}

func literalDispatchProxy(t *testing.T) (string, []byte, *literalCreateCounts) {
	t.Helper()
	target, err := url.Parse(local.Server)
	if err != nil {
		t.Fatal("LiteralOwnedAPIInvalid")
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.Transport = client.Transport
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) { w.WriteHeader(http.StatusBadGateway) }
	counts := &literalCreateCounts{values: map[string]int{}}
	endpoint, ca := startRuntimeDispatchProxy(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "POST" {
			kind := ""
			switch {
			case r.URL.Path == "/api/v1/namespaces":
				kind = "Namespace"
			case strings.HasSuffix(r.URL.Path, "/serviceaccounts"):
				kind = "ServiceAccount"
			case strings.HasSuffix(r.URL.Path, "/deployments"):
				kind = "Deployment"
			case strings.HasSuffix(r.URL.Path, "/services"):
				kind = "Service"
			}
			if kind != "" {
				counts.mu.Lock()
				counts.values[kind]++
				counts.mu.Unlock()
			}
		}
		proxy.ServeHTTP(w, r)
	}))
	return endpoint, ca, counts
}

func newLiteralRuntimeFixture(t *testing.T, ctx context.Context) *literalRuntimeFixture {
	t.Helper()
	pin := runtimeControlPin(t, "literal-workload")
	endpoint, ca, posts := literalDispatchProxy(t)
	config, repository := retirementConfig(t, pin, endpoint, ca)
	directory := t.TempDir()
	if os.Chmod(directory, 0700) != nil {
		t.Fatal("LiteralPrivateRootMode")
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal("LiteralPrivateRootUnavailable")
	}
	t.Cleanup(func() { root.Close() })
	var nonce [32]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		t.Fatal("LiteralPrivateNonceUnavailable")
	}
	store, _, err := runtimeinputs.Bootstrap(ctx, root, runtimeinputs.Config{BootstrapUID: hex.EncodeToString(nonce[:]), Owner: config.Project.Owner(), CellUID: pin.Cell.Record.Ref.ProviderID, MaximumEntries: 8, ServiceAccounts: true})
	if err != nil {
		t.Fatal("LiteralPrivateStoreUnavailable")
	}
	config.DeploymentInputs, config.ServiceInputs, config.ServiceAccountInputs = store, store, store
	worker, err := runtimeconfig.OpenInitial(ctx, config)
	if err != nil {
		t.Fatal("LiteralAdmissionUnavailable")
	}
	desired := desiredRuntimeDeployment(pin)
	session, err := worker.EnsureNamespace(ctx, desired, runtimeAttempt(pin))
	if err != nil {
		t.Fatal("LiteralNamespaceCreateFailed")
	}
	document, err := project.Decode([]byte(runtimeDeploymentDocument))
	if err != nil {
		t.Fatal("LiteralAcceptedDocumentInvalid")
	}
	bundle, err := renderer.RenderCandidate(document, renderer.Bindings{Namespace: session.Context(), Revision: desired.Revision, OperationID: runtimeAttempt(pin), Images: desired.Images})
	if err != nil {
		t.Fatal("LiteralAcceptedRenderFailed")
	}
	f := &literalRuntimeFixture{config: config, repository: repository, store: store, pin: pin, worker: worker, session: session, objects: map[string]renderer.DesiredObject{}, posts: posts}
	for _, o := range bundle.Objects {
		switch o.Object.(type) {
		case *corev1.ServiceAccount:
			f.objects["ServiceAccount"] = o
		case *appsv1.Deployment:
			f.objects["Deployment"] = o
		case *corev1.Service:
			f.objects["Service"] = o
		}
	}
	if len(f.objects) != 3 {
		t.Fatal("LiteralThreeTypedObjectsRequired")
	}
	if _, err = session.ApplyServiceAccount(ctx, f.objects["ServiceAccount"]); err != nil {
		t.Fatal("LiteralServiceAccountCreateFailed")
	}
	if _, err = session.ApplyDeployment(ctx, f.objects["Deployment"]); err != nil {
		t.Fatal("LiteralDeploymentCreateFailed")
	}
	if _, err = session.ApplyService(ctx, f.objects["Service"]); err != nil {
		t.Fatal("LiteralServiceCreateFailed")
	}
	f.restore(t, ctx)
	return f
}

// Restore all read authority from the actual independently acknowledged journal
// and private store, not the returned write DTO or current renderer/live body.
func (f *literalRuntimeFixture) restore(t *testing.T, ctx context.Context) {
	t.Helper()
	snapshot, err := f.repository.Load(ctx)
	if err != nil {
		t.Fatal("LiteralCanonicalJournalUnavailable")
	}
	e := snapshot.Envelope
	if len(e.Attempts) != 1 || e.Attempts[0].State != journal.Observed {
		t.Fatal("LiteralCanonicalAttemptMissing")
	}
	a := e.Attempts[0]
	var namespace journal.Write
	for _, w := range e.Writes {
		if w.Preconditions.Kind == "Namespace" {
			namespace = w
		}
	}
	if namespace.State != journal.Committed || namespace.ResultUID != a.NamespaceUID {
		t.Fatal("LiteralNamespaceReceiptMissing")
	}
	p := namespace.Preconditions
	// Namespace naming is produced by the real renderer; its accepted intent
	// must match that canonical name rather than guessed from a child label.
	rendered, renderErr := renderer.RenderNamespace(f.config.Project, renderer.NamespaceRequest{Revision: a.Revision, OperationID: a.OperationID, Fingerprint: a.DesiredDigest})
	if renderErr != nil || rendered.Intent.Name != p.Name || rendered.Intent.SpecFingerprint != p.Fingerprint || rendered.Intent.Revision != p.TargetRevision || rendered.Intent.OperationID != p.ProviderOperationID {
		t.Fatal("LiteralNamespaceIntentMismatch")
	}
	i := rendered.Intent
	parent := ownership.Record{Ref: ownership.ResourceRef{Scope: i.Scope, Purpose: i.Purpose, Name: p.Name, ProviderID: namespace.ResultUID}, Owner: i.Owner, CreationOperationID: i.OperationID, CreationIdempotencyKey: i.IdempotencyKey, Revision: i.Revision, SpecFingerprint: i.SpecFingerprint, WriterEpoch: namespace.WriterEpoch}
	f.binding, err = f.config.Client.BindNamespace(ctx, f.config.Project, parent, a.OperationID)
	if err != nil {
		t.Fatal("LiteralCanonicalParentUnavailable")
	}
	seen := map[string]bool{}
	for _, w := range e.Writes {
		if w.Preconditions.Kind == "Namespace" {
			continue
		}
		p := w.Preconditions
		if seen[p.Kind] || w.State != journal.Committed || w.ResultUID == "" || w.ResultVersion == "" || w.OperationID != a.OperationID || p.ParentUID != a.NamespaceUID || !w.Input.MatchesKind(p.Kind) {
			t.Fatal("LiteralCanonicalChildReceiptMissing")
		}
		seen[p.Kind] = true
		purpose := map[string]string{"ServiceAccount": "service-account", "Deployment": "deployment", "Service": "service"}[p.Kind]
		if purpose == "" {
			t.Fatal("LiteralUnsupportedChildHistory")
		}
		intent, e := f.binding.Context().Intent(purpose, p.Name, p.TargetRevision, p.ProviderOperationID, p.Fingerprint)
		if e != nil {
			t.Fatal("LiteralCanonicalChildIntentInvalid")
		}
		record := ownership.NamespacedRecord{Parent: f.binding.Parent(), Record: ownership.Record{Ref: ownership.ResourceRef{Scope: intent.Intent.Scope, Purpose: purpose, Name: p.Name, ProviderID: w.ResultUID}, Owner: intent.Intent.Owner, CreationOperationID: intent.Intent.OperationID, CreationIdempotencyKey: intent.Intent.IdempotencyKey, Revision: p.TargetRevision, SpecFingerprint: p.Fingerprint, WriterEpoch: w.WriterEpoch}}
		target := runtimeinputs.Target{CellUID: f.pin.Cell.Record.Ref.ProviderID, Parent: f.binding.Parent(), AttemptID: a.OperationID, Revision: a.Revision, DesiredDigest: a.DesiredDigest, Name: p.Name, Fingerprint: p.Fingerprint}
		switch p.Kind {
		case "ServiceAccount":
			payload, e := f.store.ResolveServiceAccount(ctx, w.Input, target)
			if e != nil {
				t.Fatal("LiteralOriginalServiceAccountMissing")
			}
			f.read.ServiceAccount = provider.ServiceAccountRead{Desired: payload.ObjectForProvider(), Intent: intent, Record: record}
		case "Deployment":
			payload, e := f.store.Resolve(ctx, w.Input, target)
			if e != nil {
				t.Fatal("LiteralOriginalDeploymentMissing")
			}
			f.read.Deployment = provider.DeploymentRead{Desired: payload.ObjectForProvider(), Intent: intent, Record: record}
		case "Service":
			payload, e := f.store.ResolveService(ctx, w.Input, target)
			if e != nil {
				t.Fatal("LiteralOriginalServiceMissing")
			}
			f.read.Service = provider.ServiceRead{Desired: payload.ObjectForProvider(), Intent: intent, Record: provider.ServiceRecord{Resource: record, SpecContract: w.ResultSpecContract, AllocatedSpecDigest: w.ResultSpecDigest}}
		}
	}
	if len(seen) != 3 {
		t.Fatal("LiteralTypedReceiptsIncomplete")
	}
	node := request(t, "GET", "/api/v1/nodes/"+local.RunID+"-control-plane", nil, 200)
	if text(metadata(node), "name") != local.RunID+"-control-plane" || text(metadata(node), "uid") == "" {
		t.Fatal("LiteralOwnedNodeMissing")
	}
	// The runner independently pins this sole owned Docker node/network/image.
	// This current API UID pin is not historical scheduling/runtime attestation.
	f.read.Node = provider.NodeBinding{Name: text(metadata(node), "name"), UID: text(metadata(node), "uid")}
}

func literalAPIReady(t *testing.T, ctx context.Context, namespace string) {
	t.Helper()
	// Fixed fixture capability only. No URL/path/header/redirect overrides.
	probe := &http.Client{Transport: client.Transport, Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	r, err := http.NewRequestWithContext(ctx, "GET", local.Server+"/api/v1/namespaces/"+namespace+"/services/http:app:8080/proxy/ready", nil)
	if err != nil {
		t.Fatal("LiteralFixedProbeInvalid")
	}
	response, err := probe.Do(r)
	if err != nil {
		t.Fatal("LiteralFixedProbeUnavailable")
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 65))
	if err != nil || response.StatusCode != 200 || len(body) > 64 || string(body) != "runtime-deployment" {
		t.Fatal("LiteralFixedProbeRevisionMismatch")
	}
}

func literalJournalBytes(t *testing.T, ctx context.Context, f *literalRuntimeFixture) ([]byte, journal.Version) {
	t.Helper()
	s, err := f.repository.Load(ctx)
	if err != nil {
		t.Fatal("LiteralJournalReadFailed")
	}
	data, err := json.Marshal(s.Envelope)
	if err != nil {
		t.Fatal("LiteralJournalEncodingFailed")
	}
	return data, s.Version
}
