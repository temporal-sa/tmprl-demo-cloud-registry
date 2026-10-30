package runtimeconfig

import (
	"context"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	renderer "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/render/kubernetes"
	"testing"
)

func TestServiceHistoryRequiresOriginalInputBeforeProviderAdmission(t *testing.T) {
	w := &Worker{snapshot: journal.Snapshot{Envelope: journal.Envelope{Attempts: []journal.Attempt{{OperationID: "accepted", State: journal.Observed}}, Writes: []journal.Write{{OperationID: "accepted", State: journal.Committed, Preconditions: journal.Preconditions{Kind: "Service", Action: "Create"}}}}}}
	requireCode(t, w.preflight(context.Background()), Invalid)
}
func TestZeroServiceSessionAndSpecDriftFailClosed(t *testing.T) {
	for _, s := range []*Session{nil, {}} {
		_, e := s.ApplyService(context.Background(), renderer.DesiredObject{})
		requireCode(t, e, Invalid)
		_, e = s.ObserveService(context.Background(), renderer.DesiredObject{})
		requireCode(t, e, Invalid)
	}
	requireCode(t, classify(&kube.Error{Code: kube.ServiceSpecMismatch}), Changed)
}
