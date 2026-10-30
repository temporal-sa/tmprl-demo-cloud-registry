//go:build integration

package integration

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	renderer "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/render/kubernetes"
)

func managedNamespacePlan(t *testing.T, suffix string) provider.NamespaceBootstrap {
	t.Helper()
	base, err := ownership.NewContext(ownership.Config{Owner: ownership.Owner{EnvironmentID: "lab", InstallationID: local.RunID, ProjectUID: "lifecycle-" + suffix, Lifetime: ownership.Disposable, RunID: local.RunID}, ResourceRoot: "lab", Scopes: []ownership.Scope{{Provider: ownership.Kubernetes, ClusterID: local.RunID}}})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := renderer.RenderNamespace(base, renderer.NamespaceRequest{Revision: "revision-one", OperationID: "accepted-attempt-" + suffix, Fingerprint: "namespace-desired"})
	if err != nil {
		t.Fatal(err)
	}
	return provider.NamespaceBootstrap{Context: base, Object: plan.Object, Intent: plan.Intent, Writer: ownership.Writer{ID: "fixture-writer", Owner: base.Owner(), Epoch: 1, ExpiresAt: time.Now().Add(time.Hour)}}
}
func bootstrapManaged(t *testing.T, adapter *provider.Client, plan provider.NamespaceBootstrap) *provider.NamespaceBinding {
	t.Helper()
	bound, err := adapter.BootstrapNamespace(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	return bound
}
func createManaged(t *testing.T, bound *provider.NamespaceBinding, writer ownership.Writer, name string) ownership.NamespacedRecord {
	t.Helper()
	desired := desiredConfigMap(bound, name, "one")
	intent := configMapIntent(t, bound, desired, "revision-one", "create-"+name)
	record, err := bound.CreateConfigMap(context.Background(), provider.ConfigMapCreate{Desired: desired, Intent: intent, Writer: writer})
	if err != nil {
		t.Fatal(err)
	}
	return record
}
func finishManaged(t *testing.T, bound *provider.NamespaceBinding, writer ownership.Writer, op string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		result, err := bound.DrainAndDeleteNamespace(ctx, writer, op)
		if err == nil {
			if !result.ConfirmedAbsent || result.Retained {
				t.Fatalf("teardown did not confirm absence: %+v", result)
			}
			return
		}
		var failure *provider.Error
		if !errors.As(err, &failure) || failure.Code != provider.ConditionalConflict || ctx.Err() != nil {
			t.Fatal(err)
		}
		// Only an explicit API conflict is retryable with a fresh independent read;
		// ambiguous transport results are never retried or cleared here.
	}
}

func TestManagedNamespaceBootstrapCreateDrainAndNeverReuse(t *testing.T) {
	if !boundaryVerified {
		t.Fatal("owned API/network proof required")
	}
	adapter, _ := typedClient(t)
	plan := managedNamespacePlan(t, "normal")
	bound := bootstrapManaged(t, adapter, plan)
	again := bootstrapManaged(t, adapter, plan)
	if again.Parent().UID != bound.Parent().UID {
		t.Fatal("live recovery changed parent UID")
	}
	record := createManaged(t, bound, plan.Writer, "settings")
	replay := createManaged(t, bound, plan.Writer, "settings")
	if replay.Record.Ref.ProviderID != record.Record.Ref.ProviderID {
		t.Fatal("idempotent create changed child UID")
	}
	finishManaged(t, bound, plan.Writer, "teardown-normal")
	_, err := adapter.BootstrapNamespace(context.Background(), plan)
	requireProviderCode(t, err, provider.AttemptConsumed)
	next, err := renderer.RenderNamespace(plan.Context, renderer.NamespaceRequest{Revision: plan.Intent.Revision, OperationID: "new-accepted-intent", Fingerprint: plan.Intent.SpecFingerprint})
	if err != nil {
		t.Fatal(err)
	}
	if next.Object.Name == plan.Object.Name {
		t.Fatal("new accepted attempt reused consumed name")
	}
	plan.Object, plan.Intent = next.Object, next.Intent
	replacement := bootstrapManaged(t, adapter, plan)
	finishManaged(t, replacement, plan.Writer, "teardown-next")
}

func TestManagedDrainWaitsForAlreadyDispatchedCreate(t *testing.T) {
	if !boundaryVerified {
		t.Fatal("owned API/network proof required")
	}
	plan := managedNamespacePlan(t, "drain-race")
	path := "/api/v1/namespaces/" + plan.Object.Name + "/configmaps"
	options, blocked, unblock := ownedMutationProxy(t, "POST", path)
	adapter, err := provider.New(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	bound := bootstrapManaged(t, adapter, plan)
	desired := desiredConfigMap(bound, "in-flight", "one")
	intent := configMapIntent(t, bound, desired, "revision-one", "in-flight-create")
	created := make(chan error, 1)
	go func() {
		_, err := bound.CreateConfigMap(context.Background(), provider.ConfigMapCreate{Desired: desired, Intent: intent, Writer: plan.Writer})
		created <- err
	}()
	waitForDispatch(t, blocked, created)
	drained := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	go func() { _, err := bound.DrainAndDeleteNamespace(ctx, plan.Writer, "drain-race"); drained <- err }()
	wait(t, func() bool { return adapter.Lifecycle(plan.Object.Name).Draining })
	late := desiredConfigMap(bound, "late", "no")
	_, err = bound.CreateConfigMap(context.Background(), provider.ConfigMapCreate{Desired: late, Intent: configMapIntent(t, bound, late, "revision-one", "late-create"), Writer: plan.Writer})
	requireProviderCode(t, err, provider.LifecycleDraining)
	parent := request(t, "GET", "/api/v1/namespaces/"+plan.Object.Name, nil, 200)
	if metadata(parent)["deletionTimestamp"] != nil {
		t.Fatal("namespace teardown dispatched before active creator drained")
	}
	select {
	case err := <-drained:
		t.Fatalf("drain finished before held create: %v", err)
	default:
	}
	unblock()
	if err := <-created; err != nil {
		t.Fatal(err)
	}
	err = <-drained
	if err != nil {
		requireProviderCode(t, err, provider.ConditionalConflict)
		finishManaged(t, bound, plan.Writer, "drain-race-retry")
	}
}

// acceptedButLostProxy forwards exactly the selected write to the independently
// verified owned API, consumes its real response, then closes the downstream
// connection. This models a committed API write whose result the client loses.
func acceptedButLostProxy(t *testing.T, method, path string) (provider.Connection, *atomic.Int64) {
	return uncertainWriteProxy(t, method, path, true)
}

func uncertainWriteProxy(t *testing.T, method, path string, commit bool, pauseReads ...*atomic.Bool) (provider.Connection, *atomic.Int64) {
	t.Helper()
	_, options := typedClient(t)
	options.CallTimeout = 5 * time.Second
	target, err := url.Parse(local.Server)
	if err != nil {
		t.Fatal(err)
	}
	forward := httputil.NewSingleHostReverseProxy(target)
	forward.Transport = client.Transport
	attempts := &atomic.Int64{}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(pauseReads) > 0 && pauseReads[0].Load() && attempts.Load() > 0 && r.Method == "GET" && r.URL.Path == path {
			http.Error(w, "owned fixture read temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		if r.Method != method || r.URL.Path != path {
			forward.ServeHTTP(w, r)
			return
		}
		attempts.Add(1)
		if !commit {
			http.Error(w, "fixture failed before dispatch; client cannot infer that", http.StatusBadGateway)
			return
		}
		outbound := r.Clone(r.Context())
		outbound.URL.Scheme, outbound.URL.Host = target.Scheme, target.Host
		outbound.Host = target.Host
		outbound.RequestURI = ""
		response, err := client.Transport.RoundTrip(outbound)
		if err != nil {
			http.Error(w, "owned upstream failure", http.StatusBadGateway)
			return
		}
		_, _ = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if response.StatusCode < 200 || response.StatusCode >= 300 {
			http.Error(w, "unexpected owned upstream rejection", response.StatusCode)
			return
		}
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "fixture cannot drop response", http.StatusBadGateway)
			return
		}
		connection, _, err := hijacker.Hijack()
		if err == nil {
			connection.Close()
		}
	}))
	ca, _ := base64.StdEncoding.DecodeString(local.CA)
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		t.Fatal("invalid fixture CA")
	}
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	server.StartTLS()
	t.Cleanup(server.Close)
	proxyCA := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	cert, _ := base64.StdEncoding.DecodeString(local.Certificate)
	key, _ := base64.StdEncoding.DecodeString(local.Key)
	options.Credentials, err = provider.NewCredentials(proxyCA, cert, key)
	if err != nil {
		t.Fatal(err)
	}
	options.Endpoint = server.URL
	return options, attempts
}

func TestManagedUnknownBootstrapRetainsProtectedNamespaceAndNoRetry(t *testing.T) {
	if !boundaryVerified {
		t.Fatal("owned API/network proof required")
	}
	plan := managedNamespacePlan(t, "unknown-bootstrap")
	options, attempts := acceptedButLostProxy(t, "POST", "/api/v1/namespaces")
	adapter, err := provider.New(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	_, err = adapter.BootstrapNamespace(context.Background(), plan)
	requireProviderCode(t, err, provider.UnknownWriteOutcome)
	parent := request(t, "GET", "/api/v1/namespaces/"+plan.Object.Name, nil, 200)
	if !hasFinalizer(parent, provider.NamespaceLifecycleFinalizer) {
		t.Fatal("unknown bootstrap did not retain namespace protection")
	}
	_, err = adapter.BootstrapNamespace(context.Background(), plan)
	requireProviderCode(t, err, provider.UnknownWriteOutcome)
	state := adapter.Lifecycle(plan.Object.Name)
	if attempts.Load() != 1 || len(state.Unresolved) != 1 || !state.Unresolved[0].Unknown {
		t.Fatalf("unknown bootstrap was retried or lost: attempts=%d state=%+v", attempts.Load(), state)
	}
}
func hasFinalizer(parent object, wanted string) bool {
	values, _ := metadata(parent)["finalizers"].([]any)
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
func TestManagedUnknownChildCreateBlocksTeardownAndRetry(t *testing.T) {
	if !boundaryVerified {
		t.Fatal("owned API/network proof required")
	}
	plan := managedNamespacePlan(t, "unknown-child")
	options, attempts := acceptedButLostProxy(t, "POST", "/api/v1/namespaces/"+plan.Object.Name+"/configmaps")
	adapter, err := provider.New(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	bound := bootstrapManaged(t, adapter, plan)
	desired := desiredConfigMap(bound, "uncertain", "one")
	create := provider.ConfigMapCreate{Desired: desired, Intent: configMapIntent(t, bound, desired, "revision-one", "unknown-create"), Writer: plan.Writer}
	_, err = bound.CreateConfigMap(context.Background(), create)
	requireProviderCode(t, err, provider.UnknownWriteOutcome)
	request(t, "GET", "/api/v1/namespaces/"+plan.Object.Name+"/configmaps/uncertain", nil, 200)
	_, err = bound.CreateConfigMap(context.Background(), create)
	requireProviderCode(t, err, provider.UnknownWriteOutcome)
	result, err := bound.DrainAndDeleteNamespace(context.Background(), plan.Writer, "blocked-teardown")
	requireProviderCode(t, err, provider.UnknownWriteOutcome)
	parent := request(t, "GET", "/api/v1/namespaces/"+plan.Object.Name, nil, 200)
	if attempts.Load() != 1 || !result.Retained || !hasFinalizer(parent, provider.NamespaceLifecycleFinalizer) || metadata(parent)["deletionTimestamp"] != nil {
		t.Fatal("unknown child write retried or permitted teardown")
	}
}
func TestManagedCreateRequiresProtectedLiveParentAndPreservesForeignFinalizers(t *testing.T) {
	if !boundaryVerified {
		t.Fatal("owned API/network proof required")
	}
	adapter, _ := typedClient(t)
	base, parent, attempt := ownedParent(t, "unprotected")
	bound, err := adapter.BindNamespace(context.Background(), base, parent, attempt)
	if err != nil {
		t.Fatal(err)
	}
	writer := ownership.Writer{ID: "fixture-writer", Owner: base.Owner(), Epoch: 1, ExpiresAt: time.Now().Add(time.Hour)}
	desired := desiredConfigMap(bound, "denied", "one")
	_, err = bound.CreateConfigMap(context.Background(), provider.ConfigMapCreate{Desired: desired, Intent: configMapIntent(t, bound, desired, "revision-one", "denied-create"), Writer: writer})
	requireProviderCode(t, err, provider.ParentUnprotected)
	foreignPlan := managedNamespacePlan(t, "foreign-parent-provenance")
	foreignBound := bootstrapManaged(t, adapter, foreignPlan)
	foreignPath := "/api/v1/namespaces/" + foreignPlan.Object.Name
	foreign := request(t, "GET", foreignPath, nil, 200)
	field(metadata(foreign), "annotations")[provider.CreationKeyAnnotation] = "foreign-creation-proof"
	request(t, "PUT", foreignPath, foreign, 200)
	foreignDesired := desiredConfigMap(foreignBound, "denied", "one")
	_, err = foreignBound.CreateConfigMap(context.Background(), provider.ConfigMapCreate{Desired: foreignDesired, Intent: configMapIntent(t, foreignBound, foreignDesired, "revision-one", "foreign-parent-create"), Writer: foreignPlan.Writer})
	requireProviderCode(t, err, provider.OwnershipRejected)
	plan := managedNamespacePlan(t, "foreign-finalizer")
	managed := bootstrapManaged(t, adapter, plan)
	path := "/api/v1/namespaces/" + plan.Object.Name
	current := request(t, "GET", path, nil, 200)
	metadata(current)["finalizers"] = []string{provider.NamespaceLifecycleFinalizer, "other-controller.local/hold"}
	request(t, "PUT", path, current, 200)
	request(t, "DELETE", path, object{"apiVersion": "v1", "kind": "DeleteOptions"}, 200)
	late := desiredConfigMap(managed, "denied", "one")
	_, err = managed.CreateConfigMap(context.Background(), provider.ConfigMapCreate{Desired: late, Intent: configMapIntent(t, managed, late, "revision-one", "terminating-create"), Writer: plan.Writer})
	requireProviderCode(t, err, provider.ParentTerminating)
	var result provider.DeletionResult
	for i := 0; i < 5; i++ {
		result, err = managed.DrainAndDeleteNamespace(context.Background(), plan.Writer, "foreign-finalizer-teardown")
		var code *provider.Error
		if !errors.As(err, &code) || code.Code != provider.ConditionalConflict {
			break
		}
	}
	requireProviderCode(t, err, provider.NamespaceRetained)
	after := request(t, "GET", path, nil, 200)
	if !result.Retained || result.ConfirmedAbsent || !hasFinalizer(after, "other-controller.local/hold") || hasFinalizer(after, provider.NamespaceLifecycleFinalizer) {
		t.Fatal("teardown stripped foreign finalizer or failed to remove only its own")
	}
}

func TestConfirmAmbiguousConfigMapUpdateRequiresExactCommittedEvidence(t *testing.T) {
	if !boundaryVerified {
		t.Fatal("owned API/network proof required")
	}
	for _, commit := range []bool{true, false} {
		name := "old-state"
		if commit {
			name = "committed"
		}
		t.Run(name, func(t *testing.T) {
			plan := managedNamespacePlan(t, "recover-"+name)
			path := "/api/v1/namespaces/" + plan.Object.Name + "/configmaps/settings"
			options, attempts := uncertainWriteProxy(t, "PUT", path, commit)
			adapter, err := provider.New(context.Background(), options)
			if err != nil {
				t.Fatal(err)
			}
			bound := bootstrapManaged(t, adapter, plan)
			record := createManaged(t, bound, plan.Writer, "settings")
			before, err := bound.ObserveConfigMap(context.Background(), record)
			if err != nil {
				t.Fatal(err)
			}
			desired := desiredConfigMap(bound, "settings", "committed-target")
			update := provider.ConfigMapUpdate{ConfigMapMutation: provider.ConfigMapMutation{OperationID: "uncertain-update", Record: record, ExpectedVersion: before.Object.ResourceVersion, Writer: plan.Writer}, Desired: desired, Intent: configMapIntent(t, bound, desired, "revision-two", "uncertain-update")}
			_, err = bound.UpdateConfigMap(context.Background(), update)
			requireProviderCode(t, err, provider.UnknownWriteOutcome)
			if attempts.Load() != 1 {
				t.Fatal("ambiguous update automatically retried")
			}
			wrong := update
			wrong.Desired = desiredConfigMap(bound, "settings", "different-target")
			wrong.Intent = configMapIntent(t, bound, wrong.Desired, "revision-two", "uncertain-update")
			_, err = bound.ConfirmConfigMapUpdate(context.Background(), wrong)
			requireProviderCode(t, err, provider.UnknownWriteOutcome)
			if len(adapter.Lifecycle(plan.Object.Name).Unresolved) != 1 {
				t.Fatal("wrong target cleared tombstone")
			}
			recovered, err := bound.ConfirmConfigMapUpdate(context.Background(), update)
			if commit {
				if err != nil {
					t.Fatal(err)
				}
				if recovered.Record.Ref.ProviderID != record.Record.Ref.ProviderID || recovered.Record.Revision != "revision-two" || len(adapter.Lifecycle(plan.Object.Name).Unresolved) != 0 {
					t.Fatal("committed recovery lost canonical identity or retained exact tombstone")
				}
				finishManaged(t, bound, plan.Writer, "recover-committed-teardown")
				return
			}
			requireProviderCode(t, err, provider.UnknownWriteOutcome)
			if len(adapter.Lifecycle(plan.Object.Name).Unresolved) != 1 {
				t.Fatal("old state cleared tombstone")
			}
			current := request(t, "GET", path, nil, 200)
			request(t, "DELETE", path, object{"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": object{"uid": record.Record.Ref.ProviderID}}, 200)
			_, err = bound.ConfirmConfigMapUpdate(context.Background(), update)
			requireProviderCode(t, err, provider.ChildUnavailable)
			metadata(current)["uid"] = nil
			delete(metadata(current), "uid")
			delete(metadata(current), "resourceVersion")
			delete(metadata(current), "creationTimestamp")
			delete(metadata(current), "managedFields")
			request(t, "POST", "/api/v1/namespaces/"+plan.Object.Name+"/configmaps", current, 201)
			_, err = bound.ConfirmConfigMapUpdate(context.Background(), update)
			requireProviderCode(t, err, provider.OwnershipRejected)
			if len(adapter.Lifecycle(plan.Object.Name).Unresolved) != 1 {
				t.Fatal("absence/replacement cleared tombstone")
			}
		})
	}
}
