package integration

import (
	"strings"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	renderer "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/render/kubernetes"
	appsv1 "k8s.io/api/apps/v1"
)

// No clients, subprocesses, credentials or live observations are used here.
// This validates the exact authored fixture before expensive owned API setup.
func TestRuntimeDeploymentFixtureDecodesAndRendersOffline(t *testing.T) {
	document, err := project.Decode([]byte(runtimeDeploymentDocument))
	if err != nil {
		t.Fatal(err)
	}
	base, err := ownership.NewContext(ownership.Config{Owner: ownership.Owner{EnvironmentID: "lab", InstallationID: "fixture", ProjectUID: "fixture-project", Lifetime: ownership.Disposable, RunID: "fixture-run"}, ResourceRoot: "lab", Scopes: []ownership.Scope{{Provider: ownership.Kubernetes, ClusterID: "fixture-cluster"}}})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := renderer.RenderNamespace(base, renderer.NamespaceRequest{Revision: "git-abcdef1", OperationID: "accepted-fixture", Fingerprint: strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	intent := plan.Intent
	ref := ownership.ResourceRef{Scope: intent.Scope, Purpose: intent.Purpose, Name: intent.Name, ProviderID: "synthetic-canonical-uid"}
	parent := ownership.Record{Ref: ref, Owner: intent.Owner, CreationOperationID: intent.OperationID, CreationIdempotencyKey: intent.IdempotencyKey, Revision: intent.Revision, SpecFingerprint: intent.SpecFingerprint, WriterEpoch: 1}
	observation := ownership.Observation{Ref: ref, Owner: intent.Owner, CreationIdempotencyKey: intent.IdempotencyKey, Revision: intent.Revision, SpecFingerprint: intent.SpecFingerprint, WriterEpoch: 1, Version: "synthetic-version"}
	bound, err := base.BindNamespace(parent, observation)
	if err != nil {
		t.Fatal(err)
	}
	image := "example.invalid/fixture@sha256:" + strings.Repeat("b", 64)
	bundle, err := renderer.RenderCandidate(document, renderer.Bindings{Namespace: bound, Revision: intent.Revision, OperationID: intent.OperationID, Images: map[string]string{"app": image}})
	if err != nil {
		t.Fatal(err)
	}
	deployments := 0
	for _, object := range bundle.Objects {
		if deployment, ok := object.Object.(*appsv1.Deployment); ok {
			deployments++
			fingerprint, err := provider.DeploymentFingerprint(deployment)
			if err != nil || fingerprint != object.Intent.Intent.SpecFingerprint || deployment.Spec.Template.Spec.Containers[0].Image != image {
				t.Fatal("rendered fixture does not bind exact Deployment body/image", err)
			}
		}
	}
	if deployments != 1 {
		t.Fatal("fixture must render exactly one Deployment")
	}
}
