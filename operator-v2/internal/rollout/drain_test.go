package rollout

import (
	"errors"
	"testing"
)

func TestDrainOldAttemptAfterDesiredGenerationAdvances(t *testing.T) {
	d, o := intent()
	o.Candidate = candidateFor(d)
	o.Generation = d.Generation + 1
	if _, err := Compute(d, o); !errors.Is(err, ErrStaleIntent) {
		t.Fatal("normal stale intent accepted")
	}
	p, err := ComputeDrain(d, o)
	if err != nil || p.Next == nil || p.Next.Kind != CaptureEvidence {
		t.Fatal("generation advance prevented exact old-attempt cleanup")
	}
	o.Candidate.ReconcileKey = "foreign-attempt"
	if _, err = ComputeDrain(d, o); !errors.Is(err, ErrInvalidObservation) {
		t.Fatal("drain lost old intent binding")
	}
}
