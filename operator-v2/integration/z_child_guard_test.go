//go:build integration

package integration

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func desiredConfigMap(bound *provider.NamespaceBinding, name, value string) *corev1.ConfigMap {
	return &corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}, ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: bound.Parent().Name}, Data: map[string]string{"value": value}}
}
func configMapIntent(t *testing.T, bound *provider.NamespaceBinding, object *corev1.ConfigMap, revision, operation string) ownership.NamespacedIntent {
	t.Helper()
	fingerprint, err := provider.ConfigMapFingerprint(object)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := bound.Context().Intent("configmap", object.Name, revision, operation, fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	return intent
}
func fixtureConfigMap(t *testing.T, bound *provider.NamespaceBinding, writer ownership.Writer, name string) (ownership.NamespacedRecord, *corev1.ConfigMap) {
	t.Helper()
	desired := desiredConfigMap(bound, name, "one")
	intent := configMapIntent(t, bound, desired, "revision-one", "create-"+name)
	object := desired.DeepCopy()
	object.Annotations = provider.CreationAnnotations(intent.Intent, writer)
	object.Annotations[ownership.ParentNamespaceUIDAnnotation] = bound.Parent().UID
	object.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Namespace", Name: bound.Parent().Name, UID: types.UID(bound.Parent().UID)}}
	created := request(t, "POST", "/api/v1/namespaces/"+bound.Parent().Name+"/configmaps", object, 201)
	encoded, _ := json.Marshal(created)
	var actual corev1.ConfigMap
	if err := json.Unmarshal(encoded, &actual); err != nil {
		t.Fatal(err)
	}
	record := ownership.NamespacedRecord{Parent: bound.Parent(), Record: ownership.Record{Ref: ownership.ResourceRef{Scope: bound.Context().Scope(), Purpose: "configmap", Name: name, ProviderID: string(actual.UID)}, Owner: writer.Owner, CreationOperationID: intent.Intent.OperationID, CreationIdempotencyKey: intent.Intent.IdempotencyKey, Revision: intent.Intent.Revision, SpecFingerprint: intent.Intent.SpecFingerprint, WriterEpoch: writer.Epoch}}
	return record, &actual
}

// ownedMutationProxy is an mTLS-only, in-container loopback fixture. Reads and
// writes go only to the independently pinned owned API transport. It can pause
// one dispatch to place a real UID/version race after the adapter's pre-read.
func ownedMutationProxy(t *testing.T, method, path string) (provider.Connection, <-chan struct{}, func()) {
	t.Helper()
	_, options := typedClient(t)
	options.CallTimeout = 10 * time.Second
	target, err := url.Parse(local.Server)
	if err != nil {
		t.Fatal(err)
	}
	forward := httputil.NewSingleHostReverseProxy(target)
	forward.Transport = client.Transport
	blocked, release := make(chan struct{}), make(chan struct{})
	var held atomic.Bool
	var once sync.Once
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == method && r.URL.Path == path && held.CompareAndSwap(false, true) {
			close(blocked)
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		forward.ServeHTTP(w, r)
	}))
	ca, err := base64.StdEncoding.DecodeString(local.CA)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca)
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	server.StartTLS()
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() { unblock(); server.Close() })
	proxyCA := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	certificate, _ := base64.StdEncoding.DecodeString(local.Certificate)
	key, _ := base64.StdEncoding.DecodeString(local.Key)
	options.Credentials, err = provider.NewCredentials(proxyCA, certificate, key)
	if err != nil {
		t.Fatal(err)
	}
	options.Endpoint = server.URL
	return options, blocked, unblock
}

func waitForDispatch(t *testing.T, blocked <-chan struct{}, result <-chan error) {
	t.Helper()
	select {
	case <-blocked:
		return
	case err := <-result:
		t.Fatalf("operation stopped before controlled dispatch: %v", err)
	case <-time.After(15 * time.Second):
		t.Fatal("controlled dispatch did not start")
	}
}

func TestTypedConfigMapMutationsEnforceVersionUIDAndParentMarkers(t *testing.T) {
	if !boundaryVerified {
		t.Fatal("owned API/network proof required")
	}
	adapter, _ := typedClient(t)
	base, parent, attempt := ownedParent(t, "configmap-conditional")
	bound, err := adapter.BindNamespace(context.Background(), base, parent, attempt)
	if err != nil {
		t.Fatal(err)
	}
	writer := ownership.Writer{ID: "fixture-writer", Owner: base.Owner(), Epoch: parent.WriterEpoch, ExpiresAt: time.Now().Add(time.Hour)}
	record, _ := fixtureConfigMap(t, bound, writer, "settings")
	snapshot, err := bound.ObserveConfigMap(context.Background(), record)
	if err != nil {
		t.Fatal(err)
	}
	desired := desiredConfigMap(bound, "settings", "two")
	intent := configMapIntent(t, bound, desired, "revision-two", "update-two")
	updated, err := bound.UpdateConfigMap(context.Background(), provider.ConfigMapUpdate{ConfigMapMutation: provider.ConfigMapMutation{OperationID: "update-two", Record: record, ExpectedVersion: snapshot.Object.ResourceVersion, Writer: writer}, Desired: desired, Intent: intent})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Record.CreationIdempotencyKey != record.Record.CreationIdempotencyKey || updated.Record.Revision != "revision-two" {
		t.Fatal("mutation lost immutable creation provenance or target revision")
	}
	current, err := bound.ObserveConfigMap(context.Background(), updated)
	if err != nil {
		t.Fatal(err)
	}
	if current.Object.Data["value"] != "two" {
		t.Fatal("typed update did not apply expected data")
	}
	staleDesired := desiredConfigMap(bound, "settings", "stale")
	staleIntent := configMapIntent(t, bound, staleDesired, "revision-three", "stale-update")
	_, err = bound.UpdateConfigMap(context.Background(), provider.ConfigMapUpdate{ConfigMapMutation: provider.ConfigMapMutation{OperationID: "stale-update", Record: updated, ExpectedVersion: snapshot.Object.ResourceVersion, Writer: writer}, Desired: staleDesired, Intent: staleIntent})
	requireProviderCode(t, err, provider.ConditionalConflict)
	path := "/api/v1/namespaces/" + bound.Parent().Name + "/configmaps/settings"
	object := request(t, "GET", path, nil, 200)
	field(metadata(object), "annotations")[ownership.ParentNamespaceUIDAnnotation] = "foreign-parent-uid"
	request(t, "PUT", path, object, 200)
	_, err = bound.ObserveConfigMap(context.Background(), updated)
	requireProviderCode(t, err, provider.OwnershipRejected)
}

func TestTypedConfigMapUpdateAndDeleteRejectReplacementsAfterPreRead(t *testing.T) {
	if !boundaryVerified {
		t.Fatal("owned API/network proof required")
	}
	for _, method := range []string{"PUT", "DELETE"} {
		t.Run(method, func(t *testing.T) {
			base, parent, attempt := ownedParent(t, "child-race-"+method)
			path := "/api/v1/namespaces/" + parent.Ref.Name + "/configmaps/settings"
			options, blocked, unblock := ownedMutationProxy(t, method, path)
			adapter, err := provider.New(context.Background(), options)
			if err != nil {
				t.Fatal(err)
			}
			bound, err := adapter.BindNamespace(context.Background(), base, parent, attempt)
			if err != nil {
				t.Fatal(err)
			}
			writer := ownership.Writer{ID: "fixture-writer", Owner: base.Owner(), Epoch: parent.WriterEpoch, ExpiresAt: time.Now().Add(time.Hour)}
			record, original := fixtureConfigMap(t, bound, writer, "settings")
			result := make(chan error, 1)
			go func() {
				mutation := provider.ConfigMapMutation{OperationID: "raced-operation", Record: record, ExpectedVersion: original.ResourceVersion, Writer: writer}
				if method == "DELETE" {
					_, err := bound.DeleteConfigMap(context.Background(), mutation)
					result <- err
					return
				}
				desired := desiredConfigMap(bound, "settings", "must-not-apply")
				intent := configMapIntent(t, bound, desired, "revision-two", "raced-operation")
				_, err := bound.UpdateConfigMap(context.Background(), provider.ConfigMapUpdate{ConfigMapMutation: mutation, Desired: desired, Intent: intent})
				result <- err
			}()
			waitForDispatch(t, blocked, result)
			request(t, "DELETE", path, object{"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": object{"uid": record.Record.Ref.ProviderID}}, 200)
			replacement := original.DeepCopy()
			replacement.UID = ""
			replacement.ResourceVersion = ""
			replacement.CreationTimestamp = metav1.Time{}
			replacement.ManagedFields = nil
			replacement.Data = map[string]string{"value": "replacement"}
			created := request(t, "POST", "/api/v1/namespaces/"+parent.Ref.Name+"/configmaps", replacement, 201)
			unblock()
			err = <-result
			var rejected *provider.Error
			if !errors.As(err, &rejected) || (rejected.Code != provider.ConditionalConflict && rejected.Code != provider.OwnershipRejected) {
				t.Fatalf("replacement race was not conditionally rejected: %v", err)
			}
			after := request(t, "GET", path, nil, 200)
			if text(metadata(after), "uid") != text(metadata(created), "uid") || text(field(after, "data"), "value") != "replacement" {
				t.Fatal("stale operation modified/deleted replacement child")
			}
			if len(adapter.Lifecycle(parent.Ref.Name).Unresolved) != 0 {
				t.Fatal("deterministic API rejection was mislabeled as unknown write")
			}
		})
	}
}
