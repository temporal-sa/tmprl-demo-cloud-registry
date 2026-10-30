package localmetadata

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	awsstore "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/awssecrets"
	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
)

var ctx = context.Background()
var testNow = time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)

type noIO struct {
	tc.Transport
	tc.CredentialTransport
	tc.SecureCandidateStore
}

// Intents come from the actual manager's pure Prepare contract, not a fixture
// that merely restates this backend's hash/name validation implementation.
func fixtureConfig(t *testing.T, max int) (Config, tc.CredentialIntent) {
	t.Helper()
	c, err := ownership.NewContext(ownership.Config{Owner: ownership.Owner{EnvironmentID: "local-lab", InstallationID: "metadata-tests", ProjectUID: "test-project-uid", Lifetime: ownership.Disposable, RunID: "test-run-0001"}, ResourceRoot: "rv2", Scopes: []ownership.Scope{{Provider: ownership.Temporal, AccountID: "testaccount", Namespace: "fixture-authority"}, {Provider: ownership.AWS, Partition: "aws", AccountID: "123456789012", Region: "us-west-2"}}})
	if err != nil {
		t.Fatal(err)
	}
	intent, err := c.Intent(ownership.Temporal, "namespace", "metadata-target", "fixture-v1", "namespace-create", "fixture-fingerprint", 39)
	if err != nil {
		t.Fatal(err)
	}
	id := intent.Name + ".testaccount"
	born := testNow.Add(-time.Hour)
	inc := tc.IncarnationID(id, born)
	writer := ownership.Writer{ID: "fixture-writer", Owner: c.Owner(), Epoch: 1, ExpiresAt: testNow.Add(time.Hour)}
	record, err := c.BindCreated(intent, ownership.Observation{Ref: ownership.ResourceRef{Scope: intent.Scope, Purpose: intent.Purpose, Name: intent.Name, ProviderID: inc}, Owner: c.Owner(), CreationIdempotencyKey: intent.IdempotencyKey, Revision: intent.Revision, SpecFingerprint: intent.SpecFingerprint, WriterEpoch: 1, Version: "namespace-v1"}, writer, testNow)
	if err != nil {
		t.Fatal(err)
	}
	io := &noIO{}
	adapter, err := tc.New(tc.Config{Ownership: c, Bindings: []tc.Binding{{NamespaceID: id, CreatedAt: born, ProvenanceID: "fixture-create-provenance", Record: record}}, AllowedRegions: []string{"aws-us-east-2"}, MaximumRetentionDays: 7, CallTimeout: time.Second, Now: func() time.Time { return testNow }}, io)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := tc.NewCredentialManager(tc.CredentialConfig{MaximumKeyTTL: 24 * time.Hour, MaximumRecoveryPages: 1, StoreID: "local-test-store"}, adapter, io, nilJournal{}, io, nil)
	if err != nil {
		t.Fatal(err)
	}
	i, err := manager.Prepare(tc.StageRequest{Namespace: tc.WriteRequest{NamespaceID: id, ExpectedIncarnationID: inc, ExpectedRevision: record.Revision, OperationKey: "fixed-stage-operation", Writer: writer}, AttemptID: "attempt-one", ExpiresAt: testNow.Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	sa := tc.ServiceAccountRef{ID: "sa-owned", Name: i.ServiceAccountName, CreatedAt: testNow.Add(-time.Minute), VersionDigest: tc.ProviderVersionDigest("sa-v1"), ProvenanceID: "fixture-sa-create"}
	return Config{BootstrapUID: "metadata-unit-bootstrap-0001", Ownership: c, NamespaceID: id, NamespaceCreatedAt: born, NamespaceIncarnationID: inc, StoreID: "local-test-store", PreprovisionedServiceAccount: sa, MaximumRevisions: max, MaximumRecordBytes: 32768}, i
}

type nilJournal struct{}

func (nilJournal) Load(context.Context, string) (tc.CredentialState, bool, error) {
	panic("Prepare must not perform I/O")
}
func (nilJournal) CompareAndSwap(context.Context, string, string, tc.CredentialState) (tc.CredentialState, error) {
	panic("Prepare must not perform I/O")
}

func testStore(t *testing.T, max int) (*Store, Binding, *os.Root, tc.CredentialIntent) {
	t.Helper()
	cfg, i := fixtureConfig(t, max)
	dir := t.TempDir()
	_ = os.Chmod(dir, 0700)
	r, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	s, b, err := Bootstrap(ctx, r, cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s, b, r, i
}
func mustSave(t *testing.T, s *Store, previous, next tc.CredentialState) tc.CredentialState {
	t.Helper()
	next.Version = ""
	got, err := s.CredentialJournal().CompareAndSwap(ctx, s.slot, previous.Version, next)
	if err != nil {
		t.Fatal(err)
	}
	return got
}
func toStoreRequested(t *testing.T, s *Store, i tc.CredentialIntent) tc.CredentialState {
	t.Helper()
	v := mustSave(t, s, tc.CredentialState{}, tc.CredentialState{Intent: i, Phase: tc.CredentialRecorded})
	n := v
	n.Phase = tc.CredentialSAReady
	n.ServiceAccount = s.config.PreprovisionedServiceAccount
	v = mustSave(t, s, v, n)
	n = v
	n.Phase = tc.CredentialKeyRequested
	n.KeyReceipt = s.expectedReceipt("api-key-create", i.StageID, keyFingerprint(n))
	v = mustSave(t, s, v, n)
	n = v
	n.Phase = tc.CredentialStoreRequested
	n.Key = tc.KeyRef{ID: "key-owned", ServiceAccountID: n.ServiceAccount.ID, CreatedAt: testNow, VersionDigest: tc.ProviderVersionDigest("key-v1"), CreationReceiptID: n.KeyReceipt.ID, Verified: true, State: "RESOURCE_STATE_ACTIVE"}
	n.CandidateWriteID = writeID(n)
	return mustSave(t, s, v, n)
}
func writeFor(v tc.CredentialState) tc.CandidateWrite {
	key := v.Key
	key.State = ""
	key.VersionDigest = ""
	return tc.CandidateWrite{Intent: v.Intent, Key: key, WriteID: v.CandidateWriteID}
}
func saveOutcome(t *testing.T, s *Store, previous, next awsstore.Outcome) awsstore.Outcome {
	t.Helper()
	next.Version = ""
	out, err := s.OutcomeLedger().CompareAndSwap(ctx, next.Write.WriteID, previous.Version, next)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func canonicalARN(s *Store, name string) string {
	a, _ := s.config.Ownership.Scope(ownership.AWS)
	return "arn:" + a.Partition + ":secretsmanager:" + a.Region + ":" + a.AccountID + ":secret:" + name + "-AbC123"
}

func TestIndependentVersionsReopenHistoryAndStaleCAS(t *testing.T) {
	s, b, r, i := testStore(t, 32)
	v := toStoreRequested(t, s, i)
	journalVersion := v.Version
	w := writeFor(v)
	o := saveOutcome(t, s, awsstore.Outcome{}, awsstore.Outcome{Write: w, Phase: awsstore.CreateRequested})
	n := o
	n.Phase = awsstore.CreateReceipted
	n.ARN = canonicalARN(s, i.CandidateID)
	o = saveOutcome(t, s, o, n)
	n = o
	n.Phase = awsstore.MetadataVerified
	n.CreatedAt = testNow.Add(time.Second)
	o = saveOutcome(t, s, o, n)
	loaded, found, err := s.CredentialJournal().Load(ctx, s.slot)
	if err != nil || !found || loaded.Version != journalVersion {
		t.Fatal("outcome CAS invalidated manager journal version")
	}
	before := v
	next := v
	next.Phase = tc.CredentialStaged
	next.Candidate = tc.SecretVersionRef{StoreID: s.config.StoreID, CandidateID: i.CandidateID, VersionID: w.WriteID, WriteID: w.WriteID, Owner: i.Owner, NamespaceIncarnationID: i.NamespaceIncarnationID, ServiceAccountID: w.Key.ServiceAccountID, KeyID: w.Key.ID, Encrypted: true, Candidate: true}
	v = mustSave(t, s, v, next)
	if _, err = s.CredentialJournal().CompareAndSwap(ctx, s.slot, before.Version, tc.CredentialState{Intent: i, Phase: tc.CredentialRecorded}); err != CASConflict {
		t.Fatal("stale CAS accepted")
	}
	next = v
	next.Phase = tc.CredentialVerified
	v = mustSave(t, s, v, next)
	reopened, err := Open(r, b, s.config)
	if err != nil {
		t.Fatal(err)
	}
	loaded, found, err = reopened.CredentialJournal().Load(ctx, s.slot)
	if err != nil || !found || loaded.Version != v.Version || loaded.Phase != tc.CredentialVerified {
		t.Fatal("reopen lost exact durable credential state")
	}
	h, err := reopened.History(ctx)
	if err != nil || len(h) != 9 {
		t.Fatal("immutable bounded history missing")
	}
	h[0].Journal.Intent.AttemptID = "mutated-copy"
	fresh, _ := reopened.History(ctx)
	if fresh[0].Journal.Intent.AttemptID == "mutated-copy" {
		t.Fatal("history aliases mutable caller data")
	}
	if _, found, err = s.OutcomeLedger().Load(ctx, w.WriteID); err != nil || !found {
		t.Fatal("outcome metadata lost")
	}
}

func TestUnknownAppendRetainedAndExplicitRecovery(t *testing.T) {
	for _, point := range []string{"after-intent", "after-commit"} {
		t.Run(point, func(t *testing.T) {
			s, b, r, i := testStore(t, 8)
			s.fault = func(at string) error {
				if at == point {
					return Uncertain
				}
				return nil
			}
			if _, err := s.CredentialJournal().CompareAndSwap(ctx, s.slot, "", tc.CredentialState{Intent: i, Phase: tc.CredentialRecorded}); err != Uncertain {
				t.Fatal("append uncertainty hidden")
			}
			if _, found, err := s.CredentialJournal().Load(ctx, s.slot); err != Uncertain || found {
				t.Fatal("unknown append returned stale journal")
			}
			reopened, err := Open(r, b, s.config)
			if err != nil {
				t.Fatal(err)
			}
			if point == "after-intent" {
				if _, found, err := reopened.CredentialJournal().Load(ctx, s.slot); err != Uncertain || found {
					t.Fatal("new process ignored pending tail")
				}
			}
			pending, err := s.Pending(ctx)
			if err != nil || pending.Digest == "" {
				t.Fatal("retained intent unavailable")
			}
			if err = s.Recover(ctx, strings64("0")); err != CASConflict && err != Uncertain {
				t.Fatal("foreign recovery digest accepted")
			}
			s.fault = nil
			if err = s.Recover(ctx, pending.Digest); err != nil {
				t.Fatal(err)
			}
			got, found, err := s.CredentialJournal().Load(ctx, s.slot)
			if err != nil || !found || got.Phase != tc.CredentialRecorded {
				t.Fatal("commit-only recovery lost intent")
			}
		})
	}
}
func strings64(ch string) string {
	s := ""
	for len(s) < 64 {
		s += ch
	}
	return s
}

func TestPartialTailAndReplacedProvenanceNeverReset(t *testing.T) {
	s, b, r, i := testStore(t, 8)
	s.fault = func(string) error { return Uncertain }
	_, _ = s.CredentialJournal().CompareAndSwap(ctx, s.slot, "", tc.CredentialState{Intent: i, Phase: tc.CredentialRecorded})
	f, err := r.OpenFile(frameName(1, "intent"), os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte("{"))
	_ = f.Close()
	other, err := Open(r, b, s.config)
	if err != nil {
		t.Fatal(err)
	}
	if _, found, err := other.CredentialJournal().Load(ctx, s.slot); err != Corrupt || found {
		t.Fatal("partial tail became stale state")
	}
	if _, err = other.Pending(ctx); err != Corrupt {
		t.Fatal("partial tail was silently discarded")
	}
	if _, err = r.Stat(frameName(1, "intent")); err != nil {
		t.Fatal("uncertain evidence removed")
	}
	bad := b
	bad.BootstrapUID = "different-bootstrap-uid"
	if _, err = Open(r, bad, s.config); err != ScopeConflict {
		t.Fatal("foreign bootstrap binding accepted")
	}
	_ = r.Rename(lockName, "old-lock")
	f, err = r.OpenFile(lockName, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if _, err = other.History(ctx); err != ScopeConflict {
		t.Fatal("replacement lock inode authorized")
	}
}

func TestConcurrentOpenCASAndCapacity(t *testing.T) {
	s, b, r, i := testStore(t, 2)
	other, err := Open(r, b, s.config)
	if err != nil {
		t.Fatal(err)
	}
	initial := tc.CredentialState{Intent: i, Phase: tc.CredentialRecorded}
	v := mustSave(t, s, tc.CredentialState{}, initial)
	nextA := v
	nextA.Phase = tc.CredentialSAReady
	nextA.ServiceAccount = s.config.PreprovisionedServiceAccount
	nextA.Version = ""
	nextB := v
	nextB.Phase = tc.CredentialSARequested
	nextB.SAReceipt = s.expectedReceipt("service-account-create", i.StageID, saFingerprint(i))
	nextB.Version = ""
	var wg sync.WaitGroup
	wg.Add(2)
	results := make(chan error, 2)
	go func() {
		defer wg.Done()
		_, err := s.CredentialJournal().CompareAndSwap(ctx, s.slot, v.Version, nextA)
		results <- err
	}()
	go func() {
		defer wg.Done()
		_, err := other.CredentialJournal().CompareAndSwap(ctx, s.slot, v.Version, nextB)
		results <- err
	}()
	wg.Wait()
	close(results)
	wins, conflicts := 0, 0
	for err := range results {
		if err == nil {
			wins++
		} else if err == CASConflict {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if wins != 1 || conflicts != 1 {
		t.Fatal("two filesystem writers bypassed CAS")
	}
	got, _, _ := s.CredentialJournal().Load(ctx, s.slot)
	next := got
	next.CleanupFailureCode = tc.CredentialCleanupBlocked
	next.Version = ""
	if _, err = s.CredentialJournal().CompareAndSwap(ctx, s.slot, got.Version, next); err != Capacity {
		t.Fatal("history exhausted without bounded stop")
	}
}

func TestInvalidScopesTransitionsAndUnknownStoreCannotCompensate(t *testing.T) {
	s, _, _, i := testStore(t, 32)
	initial := tc.CredentialState{Intent: i, Phase: tc.CredentialVerified}
	if _, err := s.CredentialJournal().CompareAndSwap(ctx, s.slot, "", initial); err != IllegalTransition {
		t.Fatal("initial state bypassed staging")
	}
	v := toStoreRequested(t, s, i)
	next := v
	next.Version = ""
	next.Phase = tc.CredentialCompensating
	next.FailureCode = tc.CredentialStoreRejected
	if _, err := s.CredentialJournal().CompareAndSwap(ctx, s.slot, v.Version, next); err != IllegalTransition {
		t.Fatal("unknown store became compensation authority")
	}
	for _, mutate := range []func(*tc.CredentialState){func(v *tc.CredentialState) { v.Intent.Owner.ProjectUID = "foreign" }, func(v *tc.CredentialState) { v.Intent.NamespaceIncarnationID = "reused-name" }, func(v *tc.CredentialState) { v.Intent.AttemptID = "bypass-pending" }, func(v *tc.CredentialState) { v.Key.ID = "different-key" }} {
		n := v
		n.Version = ""
		mutate(&n)
		if _, err := s.CredentialJournal().CompareAndSwap(ctx, s.slot, v.Version, n); err != IllegalTransition {
			t.Fatal("scope/identity rewrite accepted")
		}
	}
	w := writeFor(v)
	out := awsstore.Outcome{Write: w, Phase: awsstore.MetadataVerified, ARN: canonicalARN(s, i.CandidateID), CreatedAt: testNow}
	if _, err := s.OutcomeLedger().CompareAndSwap(ctx, w.WriteID, "", out); err != IllegalTransition {
		t.Fatal("outcome skipped durable create request")
	}
}

func TestNoImplicitBootstrapAndCopiedRootRejected(t *testing.T) {
	s, b, _, _ := testStore(t, 4)
	newDir := t.TempDir()
	_ = os.Chmod(newDir, 0700)
	root, err := os.OpenRoot(newDir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if _, err = Open(root, b, s.config); err != ScopeConflict {
		t.Fatal("missing root implicitly initialized")
	}
	for _, name := range []string{identityName, lockName} {
		raw, err := s.root.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		if root.WriteFile(name, raw, 0600) != nil {
			t.Fatal("test copy failed")
		}
	}
	if _, err = Open(root, b, s.config); err != ScopeConflict {
		t.Fatal("copied manifest/root laundered provenance")
	}
	if _, err = os.Stat(filepath.Join(newDir, "tx-000001.intent.json")); !os.IsNotExist(err) {
		t.Fatal("Open unexpectedly created history")
	}
}

func TestAttemptTombstonesRetainedSAAndLatestOnlyReplay(t *testing.T) {
	s, _, _, i := testStore(t, 64)
	v := mustSave(t, s, tc.CredentialState{}, tc.CredentialState{Intent: i, Phase: tc.CredentialRecorded})
	n := v
	n.Phase = tc.CredentialSAReady
	n.ServiceAccount = s.config.PreprovisionedServiceAccount
	v = mustSave(t, s, v, n)
	n = v
	n.Phase = tc.CredentialKeyRequested
	n.KeyReceipt = s.expectedReceipt("api-key-create", i.StageID, keyFingerprint(n))
	v = mustSave(t, s, v, n)
	n = v
	n.Phase = tc.CredentialCompensating
	n.Key = tc.KeyRef{ID: "lost-key", ServiceAccountID: n.ServiceAccount.ID, CreatedAt: testNow, VersionDigest: tc.ProviderVersionDigest("key-v1"), CreationReceiptID: n.KeyReceipt.ID, Verified: true}
	n.FailureCode = tc.CredentialMaterialLost
	v = mustSave(t, s, v, n)
	n = v
	n.Phase = tc.CredentialRevoked
	v = mustSave(t, s, v, n)
	n = v
	n.Phase = tc.CredentialAborted
	v = mustSave(t, s, v, n)
	firstAbortedVersion := v.Version
	makeNext := func(previous tc.CredentialState, attempt string) tc.CredentialState {
		intent := previous.Intent
		intent.AttemptID = attempt
		intent.StageID = digest(struct {
			Slot, Operation, Attempt string
			Expiry                   time.Time
		}{intent.SlotID, intent.OperationKey, attempt, intent.ExpiresAt})
		intent.KeyDisplayName, _ = s.config.Ownership.Name(ownership.Temporal, "api-key", intent.StageID, 63)
		intent.KeyMarker = "registry-key:" + intent.StageID
		intent.CandidateID, _ = s.config.Ownership.Path(ownership.Temporal, "api-key-candidate", intent.StageID)
		return tc.CredentialState{Intent: intent, Phase: tc.CredentialSAKnown, ServiceAccount: previous.ServiceAccount, SAReceipt: previous.SAReceipt}
	}
	n = makeNext(v, "attempt-two")
	v = mustSave(t, s, v, n)
	replay, err := s.CredentialJournal().CompareAndSwap(ctx, s.slot, firstAbortedVersion, n)
	if err != nil || replay.Version != v.Version {
		t.Fatal("latest accepted retry changed result")
	}
	n = v
	n.Phase = tc.CredentialSAReady
	v = mustSave(t, s, v, n)
	if _, err = s.CredentialJournal().CompareAndSwap(ctx, s.slot, firstAbortedVersion, makeNext(v, "attempt-two")); err != CASConflict {
		t.Fatal("historical success authorized stale provider work")
	}
	n = v
	n.Phase = tc.CredentialKeyRequested
	n.KeyReceipt = s.expectedReceipt("api-key-create", n.Intent.StageID, keyFingerprint(n))
	v = mustSave(t, s, v, n)
	n = v
	n.Phase = tc.CredentialCompensating
	n.Key = tc.KeyRef{ID: "second-lost-key", ServiceAccountID: n.ServiceAccount.ID, CreatedAt: testNow.Add(time.Second), VersionDigest: tc.ProviderVersionDigest("key-v1"), CreationReceiptID: n.KeyReceipt.ID, Verified: true}
	n.FailureCode = tc.CredentialMaterialLost
	v = mustSave(t, s, v, n)
	n = v
	n.Phase = tc.CredentialRevoked
	v = mustSave(t, s, v, n)
	n = v
	n.Phase = tc.CredentialAborted
	v = mustSave(t, s, v, n)
	old := tc.CredentialState{Intent: i, Phase: tc.CredentialSAKnown, ServiceAccount: v.ServiceAccount, SAReceipt: v.SAReceipt}
	if _, err = s.CredentialJournal().CompareAndSwap(ctx, s.slot, v.Version, old); err != IllegalTransition {
		t.Fatal("old attempt/stage tombstone reused")
	}
}

func TestForeignARNReceiptPartialAndUnsafeDataRejected(t *testing.T) {
	s, _, _, i := testStore(t, 32)
	v := toStoreRequested(t, s, i)
	w := writeFor(v)
	o := saveOutcome(t, s, awsstore.Outcome{}, awsstore.Outcome{Write: w, Phase: awsstore.CreateRequested})
	n := o
	n.Version = ""
	n.Phase = awsstore.CreateReceipted
	n.ARN = "arn:aws:secretsmanager:us-east-1:123456789012:secret:" + i.CandidateID + "-AbC123"
	if _, err := s.OutcomeLedger().CompareAndSwap(ctx, w.WriteID, o.Version, n); err != IllegalTransition {
		t.Fatal("foreign provider scope ARN accepted")
	}
	n.ARN = canonicalARN(s, "foreign-name")
	if _, err := s.OutcomeLedger().CompareAndSwap(ctx, w.WriteID, o.Version, n); err != IllegalTransition {
		t.Fatal("ARN/name incoherence accepted")
	}
	bad := v
	bad.Version = ""
	bad.KeyReceipt.ID = "opaque-secret-sentinel"
	if _, err := s.CredentialJournal().CompareAndSwap(ctx, s.slot, v.Version, bad); err != IllegalTransition {
		t.Fatal("fabricated receipt persisted")
	}
	bad = v
	bad.Version = ""
	bad.Key.VersionDigest = "bearer token\nsecret"
	if _, err := s.CredentialJournal().CompareAndSwap(ctx, s.slot, v.Version, bad); err != IllegalTransition {
		t.Fatal("unbounded/unsafe metadata persisted")
	}
	bad = v
	bad.Version = ""
	bad.Key.State = "999999"
	if _, err := s.CredentialJournal().CompareAndSwap(ctx, s.slot, v.Version, bad); err != IllegalTransition {
		t.Fatal("unknown numeric provider state accepted")
	}
	bad = v
	bad.Version = ""
	bad.Key.State = "RESOURCE_STATE_UNRECOGNIZED"
	if _, err := s.CredentialJournal().CompareAndSwap(ctx, s.slot, v.Version, bad); err != IllegalTransition {
		t.Fatal("unknown named provider state accepted")
	}
	bad = v
	bad.Version = ""
	bad.Intent.ExpiresAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := s.CredentialJournal().CompareAndSwap(ctx, s.slot, v.Version, bad); err != IllegalTransition {
		t.Fatal("unserializable time silently hashed/written")
	}
	h, _ := s.History(ctx)
	if len(h) != 5 {
		t.Fatal("rejected payload altered history")
	}
}
