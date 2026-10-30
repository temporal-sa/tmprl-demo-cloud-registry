package journal

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
)

func observedFixture(t *testing.T) (*Repository, Snapshot) {
	t.Helper()
	r, _, s := fixture(t)
	s = commit(t, r, s, reserve(s.Envelope))
	n := clone(s.Envelope)
	n.Writes = []Write{namespaceWrite()}
	s = commit(t, r, s, n)
	n = clone(s.Envelope)
	n.Attempts[0].State = Observed
	n.Attempts[0].NamespaceUID = "namespace-uid"
	n.Writes[0].State = Committed
	n.Writes[0].ResultUID = "namespace-uid"
	n.Writes[0].ResultVersion = "2"
	return r, commit(t, r, s, n)
}
func beginDrain(t *testing.T, r *Repository, s Snapshot) Snapshot {
	t.Helper()
	n := clone(s.Envelope)
	n.Attempts[0].State = Draining
	n.Attempts[0].DrainReason = DrainProjectFinalization
	return commit(t, r, s, n)
}
func cleanupWrite(action string) Write {
	return Write{ID: "cleanup-namespace", OperationID: "rollout-one", WriterEpoch: 1, State: Pending, Preconditions: Preconditions{Kind: "Namespace", Action: action, Name: "rgtest-candidate-one", UID: "namespace-uid", Version: "4", Fingerprint: strings.Repeat("f", 64), TargetRevision: "revision-one", ProviderOperationID: "cleanup-provider-call"}}
}

func TestDrainingIsMonotoneAndBlocksNormalWritesAndNextAttempt(t *testing.T) {
	r, s := observedFixture(t)
	n := clone(s.Envelope)
	n.Attempts[0].State = Consumed
	n.Attempts[0].CanonicalAbsent = true
	n.Attempts[0].DrainReason = DrainProjectFinalization
	if _, err := r.Commit(context.Background(), s, n); !errors.Is(err, ErrInvalid) {
		t.Fatal("observed attempt skipped durable drain")
	}
	s = beginDrain(t, r, s)
	for _, change := range []func(*Envelope){
		func(n *Envelope) { n.Attempts[0].State = Observed; n.Attempts[0].DrainReason = "" },
		func(n *Envelope) { n.Attempts[0].DrainReason = DrainSuperseded },
		func(n *Envelope) { n.Attempts[0].CanonicalAbsent = true },
		func(n *Envelope) {
			n.Attempts = append(n.Attempts, Attempt{Number: 2, OperationID: "rollout-two", NamespaceName: "candidate-two", Revision: "revision-two", DesiredDigest: strings.Repeat("c", 64), State: Reserved})
		},
		func(n *Envelope) {
			n.Writes = append(n.Writes, Write{ID: "normal-child", OperationID: "rollout-one", WriterEpoch: 1, State: Pending, Preconditions: Preconditions{Kind: "Deployment", Action: "Create", Name: "app", ParentUID: "namespace-uid", Fingerprint: strings.Repeat("c", 64), TargetRevision: "revision-one", ProviderOperationID: "create-app"}})
		},
	} {
		n = clone(s.Envelope)
		change(&n)
		if _, err := r.Commit(context.Background(), s, n); !errors.Is(err, ErrInvalid) {
			t.Fatal("draining state reopened normal work")
		}
	}
}

func TestFailedCandidateCauseMustBePersistedBeforeDrainingAndStaysSticky(t *testing.T) {
	r, s := observedFixture(t)
	n := clone(s.Envelope)
	n.Attempts[0].State = Draining
	n.Attempts[0].DrainReason = DrainCandidateFailed
	if _, err := r.Commit(context.Background(), s, n); !errors.Is(err, ErrInvalid) {
		t.Fatal("failure drain had no original cause")
	}
	id := diagnostics.Identity{Environment: s.Envelope.Subject.Environment, Installation: s.Envelope.Subject.Installation, ProjectUID: s.Envelope.Subject.ProjectUID, ProjectName: "example", OperationID: "rollout-one", ReconcileKey: "revision-one", Generation: 1}
	p, err := diagnostics.NewProjection(id)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	failure := diagnostics.DomainFailure{ID: "smoke-one", Code: "SmokeFailed", Category: diagnostics.Smoke, Message: "Candidate smoke failed", OccurredAt: at}
	p, err = p.Apply(diagnostics.OperationEvent{Identity: id, Sequence: 1, OccurredAt: at, State: diagnostics.Failed, Step: "smoke", Failure: &failure})
	if err != nil {
		t.Fatal(err)
	}
	n.Projection = &p
	s = commit(t, r, s, n)
	n = clone(s.Envelope)
	n.Writes = append(n.Writes, cleanupWrite("Delete"))
	s = commit(t, r, s, n)
	n = clone(s.Envelope)
	n.Projection.Sequence++
	n.Projection.Failure.Message = "Cleanup failed instead"
	if _, err = r.Commit(context.Background(), s, n); !errors.Is(err, ErrInvalid) {
		t.Fatal("cleanup replaced original candidate cause")
	}
}

func TestFinalizeRecordsKnownVersionOrAtomicConfirmedAbsence(t *testing.T) {
	for _, absent := range []bool{false, true} {
		t.Run(map[bool]string{false: "updated-version", true: "confirmed-absent"}[absent], func(t *testing.T) {
			r, s := observedFixture(t)
			s = beginDrain(t, r, s)
			n := clone(s.Envelope)
			n.Writes = append(n.Writes, cleanupWrite("Finalize"))
			s = commit(t, r, s, n)
			n = clone(s.Envelope)
			n.Writes[1].State = Committed
			if absent {
				n.Writes[1].ConfirmedAbsent = true
				n.Attempts[0].State = Consumed
				n.Attempts[0].CanonicalAbsent = true
			} else {
				n.Writes[1].ResultUID = "namespace-uid"
				n.Writes[1].ResultVersion = "5"
			}
			s = commit(t, r, s, n)
			if !absent {
				n = clone(s.Envelope)
				n.Attempts[0].State = Consumed
				n.Attempts[0].CanonicalAbsent = true
				s = commit(t, r, s, n)
			}
			if s.Envelope.Attempts[0].State != Consumed || !s.Envelope.Attempts[0].CanonicalAbsent {
				t.Fatal("canonical absence was not durable")
			}
		})
	}
}

func TestUnknownCleanupCannotBeRelabeledByAbsenceOrRetried(t *testing.T) {
	for _, action := range []string{"Delete", "Finalize"} {
		t.Run(action, func(t *testing.T) {
			r, s := observedFixture(t)
			s = beginDrain(t, r, s)
			n := clone(s.Envelope)
			n.Writes = append(n.Writes, cleanupWrite(action))
			s = commit(t, r, s, n)
			n = clone(s.Envelope)
			n.Writes[1].State = Unknown
			s = commit(t, r, s, n)
			for _, change := range []func(*Envelope){
				func(n *Envelope) { n.Attempts[0].State = Consumed; n.Attempts[0].CanonicalAbsent = true },
				func(n *Envelope) {
					n.Writes[1].State = Committed
					n.Writes[1].ConfirmedAbsent = true
					n.Attempts[0].State = Consumed
					n.Attempts[0].CanonicalAbsent = true
				},
				func(n *Envelope) { n.Writes[1].State = Rejected },
				func(n *Envelope) {
					w := cleanupWrite(action)
					w.ID = "cleanup-retry"
					w.Preconditions.Version = "9"
					n.Writes = append(n.Writes, w)
				},
			} {
				n = clone(s.Envelope)
				change(&n)
				if _, err := r.Commit(context.Background(), s, n); !errors.Is(err, ErrInvalid) {
					t.Fatal("unknown cleanup was closed or retried")
				}
			}
		})
	}
}

func TestKnownDeleteAbsenceMustConsumeAtomically(t *testing.T) {
	r, s := observedFixture(t)
	s = beginDrain(t, r, s)
	n := clone(s.Envelope)
	n.Writes = append(n.Writes, cleanupWrite("Delete"))
	s = commit(t, r, s, n)
	n = clone(s.Envelope)
	n.Writes[1].State = Committed
	n.Writes[1].ConfirmedAbsent = true
	if _, err := r.Commit(context.Background(), s, n); !errors.Is(err, ErrInvalid) {
		t.Fatal("delete absence did not consume attempt atomically")
	}
	n.Attempts[0].State = Consumed
	n.Attempts[0].CanonicalAbsent = true
	commit(t, r, s, n)
}

func TestNewAttemptArchivesTheExactOriginalFailure(t *testing.T) {
	r, s := observedFixture(t)
	id := diagnostics.Identity{Environment: s.Envelope.Subject.Environment, Installation: s.Envelope.Subject.Installation, ProjectUID: s.Envelope.Subject.ProjectUID, ProjectName: "example", OperationID: "rollout-one", ReconcileKey: "revision-one", Generation: 1}
	p, err := diagnostics.NewProjection(id)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Now().UTC()
	cause := diagnostics.DomainFailure{ID: "original-smoke", Code: "SmokeFailed", Category: diagnostics.Smoke, Message: "Original candidate failed", OccurredAt: at}
	p, err = p.Apply(diagnostics.OperationEvent{Identity: id, Sequence: 1, OccurredAt: at, State: diagnostics.Failed, Failure: &cause})
	if err != nil {
		t.Fatal(err)
	}
	n := clone(s.Envelope)
	n.Projection = &p
	s = commit(t, r, s, n)
	n = clone(s.Envelope)
	n.Attempts = append(n.Attempts, Attempt{Number: 2, OperationID: "rollout-two", NamespaceName: "candidate-two", Revision: "revision-two", DesiredDigest: strings.Repeat("c", 64), State: Reserved})
	n.Projection = nil
	forged := clone(s.Envelope).Projection
	forged.Failure.Code = "CleanupFailed"
	n.Archive = append(n.Archive, *forged)
	if _, err = r.Commit(context.Background(), s, n); !errors.Is(err, ErrInvalid) {
		t.Fatal("caller archive replaced original cause")
	}
	n.Archive = nil
	s = commit(t, r, s, n)
	if len(s.Envelope.Archive) != 1 || s.Envelope.Archive[0].Failure.Code != "SmokeFailed" || s.Envelope.Archive[0].Failure.Message != "Original candidate failed" {
		t.Fatal("automatic archive did not preserve exact cause")
	}
}
