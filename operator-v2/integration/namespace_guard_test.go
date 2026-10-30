//go:build integration

package integration

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
)

func typedClient(t *testing.T) (*provider.Client, provider.Connection) {
	t.Helper()
	decode := func(value string) []byte {
		result, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	credentials, err := provider.NewCredentials(decode(local.CA), decode(local.Certificate), decode(local.Key))
	if err != nil {
		t.Fatal(err)
	}
	options := provider.Connection{Mode: provider.LocalSerialized, Endpoint: local.Server, Identity: provider.ClusterIdentity{ID: local.RunID, KubeSystemUID: local.NamespaceUID}, Credentials: credentials, CallTimeout: 3 * time.Second}
	adapter, err := provider.New(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	return adapter, options
}

func requireProviderCode(t *testing.T, err error, expected provider.Code) {
	t.Helper()
	var typed *provider.Error
	if !errors.As(err, &typed) || typed.Code != expected {
		t.Fatalf("expected %s, got %v", expected, err)
	}
}

func ownedParent(t *testing.T, suffix string) (ownership.Context, ownership.Record, string) {
	t.Helper()
	base, err := ownership.NewContext(ownership.Config{Owner: ownership.Owner{EnvironmentID: "lab", InstallationID: local.RunID, ProjectUID: "guard-" + suffix, Lifetime: ownership.Disposable, RunID: local.RunID}, ResourceRoot: "lab", Scopes: []ownership.Scope{{Provider: ownership.Kubernetes, ClusterID: local.RunID}}})
	if err != nil {
		t.Fatal(err)
	}
	attempt := "attempt-" + suffix
	intent, err := base.Intent(ownership.Kubernetes, "runtime-namespace", attempt, "revision-one", "create-"+suffix, "namespace-spec-"+suffix, 63)
	if err != nil {
		t.Fatal(err)
	}
	writer := ownership.Writer{ID: "fixture-writer", Owner: base.Owner(), Epoch: 1, ExpiresAt: time.Now().Add(time.Hour)}
	annotations := provider.CreationAnnotations(intent, writer)
	annotations[provider.AttemptAnnotation] = attempt
	created := request(t, "POST", "/api/v1/namespaces", object{"apiVersion": "v1", "kind": "Namespace", "metadata": object{"name": intent.Name, "annotations": annotations}}, 201)
	record := ownership.Record{Ref: ownership.ResourceRef{Scope: intent.Scope, Purpose: intent.Purpose, Name: intent.Name, ProviderID: text(metadata(created), "uid")}, Owner: intent.Owner, CreationOperationID: intent.OperationID, CreationIdempotencyKey: intent.IdempotencyKey, Revision: intent.Revision, SpecFingerprint: intent.SpecFingerprint, WriterEpoch: writer.Epoch}
	return base, record, attempt
}

func TestTypedNamespaceGuardPinsClusterProvenanceUIDAndDeletionState(t *testing.T) {
	if !boundaryVerified {
		t.Fatal("owned API/network proof required")
	}
	adapter, options := typedClient(t)
	options.Identity.KubeSystemUID = "foreign-cluster-uid"
	_, err := provider.New(context.Background(), options)
	requireProviderCode(t, err, provider.ClusterMismatch)
	base, parent, attempt := ownedParent(t, "refresh")
	bound, err := adapter.BindNamespace(context.Background(), base, parent, attempt)
	if err != nil {
		t.Fatal(err)
	}
	path := "/api/v1/namespaces/" + parent.Ref.Name
	current := request(t, "GET", path, nil, 200)
	field(metadata(current), "annotations")["fixture-note"] = "benign-update"
	current = request(t, "PUT", path, current, 200)
	observation, err := bound.Refresh(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if observation.Version != text(metadata(current), "resourceVersion") {
		t.Fatal("namespace observation did not refresh resourceVersion")
	}
	metadata(current)["finalizers"] = []string{"operator-test.local/hold"}
	request(t, "PUT", path, current, 200)
	request(t, "DELETE", path, object{"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": object{"uid": parent.Ref.ProviderID}}, 200)
	_, err = bound.Refresh(context.Background())
	requireProviderCode(t, err, provider.ParentTerminating)

	foreignBase, foreignParent, foreignAttempt := ownedParent(t, "foreign")
	foreignPath := "/api/v1/namespaces/" + foreignParent.Ref.Name
	foreign := request(t, "GET", foreignPath, nil, 200)
	field(metadata(foreign), "annotations")[provider.CreationKeyAnnotation] = "foreign-create-operation"
	request(t, "PUT", foreignPath, foreign, 200)
	_, err = adapter.BindNamespace(context.Background(), foreignBase, foreignParent, foreignAttempt)
	requireProviderCode(t, err, provider.OwnershipRejected)
}

func TestTypedNamespaceGuardRejectsReusedNamespaceName(t *testing.T) {
	if !boundaryVerified {
		t.Fatal("owned API/network proof required")
	}
	adapter, _ := typedClient(t)
	base, parent, attempt := ownedParent(t, "replacement")
	bound, err := adapter.BindNamespace(context.Background(), base, parent, attempt)
	if err != nil {
		t.Fatal(err)
	}
	path := "/api/v1/namespaces/" + parent.Ref.Name
	before := request(t, "GET", path, nil, 200)
	request(t, "DELETE", path, object{"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": object{"uid": parent.Ref.ProviderID}}, 200)
	wait(t, func() bool {
		response, err := client.Get(local.Server + path)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		return response.StatusCode == 404
	})
	// Simulate an outside fixture administrator bypassing the managed allocator.
	// The adapter itself never offers recreation of an observed namespace name.
	recreated := request(t, "POST", "/api/v1/namespaces", object{"apiVersion": "v1", "kind": "Namespace", "metadata": object{"name": parent.Ref.Name, "annotations": field(metadata(before), "annotations")}}, 201)
	if text(metadata(recreated), "uid") == parent.Ref.ProviderID {
		t.Fatal("fixture did not replace namespace incarnation")
	}
	_, err = bound.Refresh(context.Background())
	requireProviderCode(t, err, provider.OwnershipRejected)
	desired := desiredConfigMap(bound, "stale-parent-child", "must-not-create")
	writer := ownership.Writer{ID: "fixture-writer", Owner: base.Owner(), Epoch: parent.WriterEpoch, ExpiresAt: time.Now().Add(time.Hour)}
	_, err = bound.CreateConfigMap(context.Background(), provider.ConfigMapCreate{Desired: desired, Intent: configMapIntent(t, bound, desired, "revision-one", "stale-parent-create"), Writer: writer})
	requireProviderCode(t, err, provider.OwnershipRejected)
	replacement := parent
	replacement.Ref.ProviderID = text(metadata(recreated), "uid")
	_, err = adapter.BindNamespace(context.Background(), base, replacement, attempt)
	requireProviderCode(t, err, provider.AttemptConsumed)
}
