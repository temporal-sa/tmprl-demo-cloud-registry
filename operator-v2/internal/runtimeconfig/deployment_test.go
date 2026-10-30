package runtimeconfig

import (
	"context"
	"strings"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	renderer "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/render/kubernetes"
	appsv1 "k8s.io/api/apps/v1"
)

func TestDeploymentHistoryRequiresHistoricalDesiredBeforeAnyProviderRead(t *testing.T) {
	// No client is supplied: missing independent desired inputs must reject
	// before attempting to read or admit the existing provider resource.
	w := &Worker{snapshot: journal.Snapshot{Envelope: journal.Envelope{
		Attempts: []journal.Attempt{{OperationID: "accepted", State: journal.Observed}},
		Writes:   []journal.Write{{OperationID: "accepted", State: journal.Committed, Preconditions: journal.Preconditions{Kind: "Deployment", Action: "Create"}}},
	}}}
	requireCode(t, w.preflight(context.Background()), Invalid)
}

func TestReferencedInputHistoryCannotUseZeroReferencePrototypeResolver(t *testing.T) {
	called := false
	w := &Worker{config: Config{DeploymentDesired: func(context.Context, DeploymentRestore) (*appsv1.Deployment, error) { called = true; return nil, nil }}, snapshot: journal.Snapshot{Envelope: journal.Envelope{
		Attempts: []journal.Attempt{{OperationID: "accepted", State: journal.Observed}},
		Writes:   []journal.Write{{OperationID: "accepted", State: journal.Committed, Preconditions: journal.Preconditions{Kind: "Deployment", Action: "Create"}, Input: journal.InputReference{Kind: "DeploymentRender", UID: "render-" + strings.Repeat("a", 64), Version: "1", Digest: strings.Repeat("b", 64), Contract: journal.DeploymentInputContract}}},
	}}}
	requireCode(t, w.preflight(context.Background()), Invalid)
	if called {
		t.Fatal("old resolver was asked to interpret unsupported retained inputs")
	}
}

func TestZeroDeploymentSessionDoesNotDispatchAndSpecDriftIsChanged(t *testing.T) {
	for _, session := range []*Session{nil, {}} {
		_, err := session.ApplyDeployment(context.Background(), renderer.DesiredObject{})
		requireCode(t, err, Invalid)
		_, err = session.ObserveDeployment(context.Background(), renderer.DesiredObject{})
		requireCode(t, err, Invalid)
	}
	requireCode(t, classify(&kube.Error{Code: kube.DeploymentSpecMismatch}), Changed)
}
