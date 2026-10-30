package runtimeconfig

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
)

type retirementTestCell struct {
	body    []byte
	version journal.Version
}

func (c *retirementTestCell) Read(context.Context) ([]byte, journal.Version, error) {
	return append([]byte(nil), c.body...), c.version, nil
}
func (c *retirementTestCell) CompareAndSwap(_ context.Context, expected journal.Version, body []byte, _ string) error {
	if expected != c.version {
		return journal.ErrConflict
	}
	version, _ := strconv.Atoi(c.version.ResourceVersion)
	c.body = append([]byte(nil), body...)
	c.version.ResourceVersion = strconv.Itoa(version + 1)
	return nil
}

func TestPreparedTicketSHAEncodingCommitsThroughRealJournalContract(t *testing.T) {
	subject := rollout.Subject{Environment: "lab", Installation: "test", ProjectUID: "project-one"}
	initial, err := journal.Initial(subject, 1)
	if err != nil {
		t.Fatal(err)
	}
	body, err := initial.JSON()
	if err != nil {
		t.Fatal(err)
	}
	cell := &retirementTestCell{body: body, version: journal.Version{UID: "cell-one", ResourceVersion: "1"}}
	repository, err := journal.New(cell, subject, "cell-one")
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := repository.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	commit := func(next journal.Envelope) {
		t.Helper()
		var err error
		snapshot, err = repository.Commit(context.Background(), snapshot, next)
		if err != nil {
			t.Fatal(err)
		}
	}
	attempt := journal.Attempt{Number: 1, OperationID: "accepted-one", Revision: "revision-one", DesiredDigest: strings.Repeat("a", 64), NamespaceName: "candidate-one", State: journal.Reserved}
	next := clone(snapshot.Envelope)
	next.Attempts = []journal.Attempt{attempt}
	next.Writes = []journal.Write{{ID: "birth-one", OperationID: attempt.OperationID, WriterEpoch: 1, State: journal.Pending, Preconditions: journal.Preconditions{Kind: "Namespace", Action: "Create", Name: attempt.NamespaceName, Fingerprint: strings.Repeat("b", 64), TargetRevision: attempt.Revision, ProviderOperationID: attempt.OperationID}}}
	commit(next)
	next = clone(snapshot.Envelope)
	next.Attempts[0].State, next.Attempts[0].NamespaceUID = journal.Observed, "namespace-one"
	next.Writes[0].State, next.Writes[0].ResultUID, next.Writes[0].ResultVersion = journal.Committed, "namespace-one", "rv-one"
	commit(next)
	next = clone(snapshot.Envelope)
	next.Attempts[0].State, next.Attempts[0].DrainReason = journal.Draining, journal.DrainProjectFinalization
	commit(next)
	for _, action := range []string{"Delete", "Finalize"} {
		description := kube.NamespaceCleanupDescription{Namespace: attempt.NamespaceName, UID: "namespace-one", ResourceVersion: "rv-two", OperationID: "cleanup-" + action, BodyDigest: "sha256:" + strings.Repeat("c", 64)}
		preconditions, err := cleanupPreconditions(snapshot.Envelope.Attempts[0], action, description)
		if err != nil {
			t.Fatal(err)
		}
		next = clone(snapshot.Envelope)
		next.Writes = append(next.Writes, journal.Write{ID: description.OperationID, OperationID: attempt.OperationID, WriterEpoch: 1, State: journal.Pending, Preconditions: preconditions})
		commit(next)
	}
	for _, bad := range []string{strings.Repeat("c", 64), "SHA256:" + strings.Repeat("c", 64), "sha256:" + strings.Repeat("C", 64), "sha256:" + strings.Repeat("c", 63), "sha512:" + strings.Repeat("c", 64)} {
		_, err := cleanupPreconditions(attempt, "Delete", kube.NamespaceCleanupDescription{BodyDigest: bad})
		requireCode(t, err, Invalid)
	}
}

func TestEmptyRetirementHandlesRejectWithoutPanic(t *testing.T) {
	var absent *Retirement
	for _, handle := range []*Retirement{absent, {}} {
		_, err := handle.Advance(context.Background())
		requireCode(t, err, Invalid)
	}
}
