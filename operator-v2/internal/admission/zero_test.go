package admission

import (
	"context"
	"errors"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
)

func TestUnconstructedHandlesCannotAuthorizeOrPanic(t *testing.T) {
	for _, claim := range []*Claim{nil, {}} {
		if gate, err := claim.Confirm(context.Background()); gate != nil || !errors.Is(err, ErrInvalid) {
			t.Fatal("unconstructed claim accepted")
		}
	}
	for _, gate := range []*Gate{nil, {}} {
		if _, err := gate.Snapshot(context.Background()); !errors.Is(err, ErrInvalid) {
			t.Fatal("unconstructed snapshot")
		}
		if _, err := gate.Commit(context.Background(), journal.Envelope{}); !errors.Is(err, ErrInvalid) {
			t.Fatal("unconstructed commit")
		}
		if _, err := gate.RecoverCommit(context.Background()); !errors.Is(err, ErrInvalid) {
			t.Fatal("unconstructed recovery")
		}
		if _, err := gate.Ready(context.Background()); !errors.Is(err, ErrInvalid) {
			t.Fatal("unconstructed handoff")
		}
		if _, err := gate.RecoverReady(context.Background()); !errors.Is(err, ErrInvalid) {
			t.Fatal("unconstructed handoff recovery")
		}
	}
}
