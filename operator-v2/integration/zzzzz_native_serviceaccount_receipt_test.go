//go:build integration && preparation_integration

package integration

import (
	"os"
	"testing"
)

func TestRuntimeServiceAccountReceiptBeforeFailedObservation(t *testing.T) {
	if os.Getenv("OPERATOR_LOCAL_PREPARATION_FIXTURE") != "1" {
		t.Fatal("PreparationExplicitOwnedRunnerRequired")
	}
	if phase := os.Getenv("OPERATOR_PREPARATION_PHASE"); phase != "" {
		preparationProcess(t, phase)
		return
	}
	TestOwnedAPIAndNetworkBoundary(t)
	input, _, store := newPreparationProcessFixture(t, "sa403")
	endpoint, ca, capture := preparationCreationProxy(t, input, store, true)
	input.Base.Endpoint, input.Base.ProxyCA = endpoint, ca
	first := runPreparationChild(t, "TestRuntimeServiceAccountReceiptBeforeFailedObservation", "sa-fault", input)
	if first.State != "Forbidden" || len(first.Checkpoint) == 0 || first.Primary == nil || len(first.Writes) != 2 || first.Writes["Deployment"].ID != "" || first.Writes["Service"].ID != "" {
		t.Fatal("PreparationSA403DidNotRetainOnlyCanonicalPrerequisite")
	}
	preparationAssertCounters(t, capture, []string{"Namespace", "ServiceAccount"})
	// The prior writer is exited/reaped. Only read-only troubleshooting resumes;
	// applying the rest of the Plan remains blocked by the original failure.
	capture.mu.Lock()
	capture.denySA = false
	capture.mu.Unlock()
	input.Base.Checkpoint = first.Checkpoint
	next := runPreparationChild(t, "TestRuntimeServiceAccountReceiptBeforeFailedObservation", "sa-inspect", input)
	if next.State != "Inspected" || next.Primary == nil || !canonicalFailureWireEqual(*first.Primary, *next.Primary) {
		t.Fatal("PreparationSAInspectionLostOriginalForbiddenCause")
	}
	preparationRequireCanonical(t, first, next, 2)
	preparationAssertCounters(t, capture, []string{"Namespace", "ServiceAccount"})
	preparationPublic(t, "sa403")
}
