package localmetadata

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	observation "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/credentialobservation"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	awsstore "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/awssecrets"
	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
)

func receiptPolicy() *AuthObservationPolicy {
	return &AuthObservationPolicy{Contract: AuthObservationReceiptContract, ObservationContract: observation.Contract, ObservationBindingDigest: digest("owned-actor-settings-and-kms"), EndpointBindingDigest: digest("owned-observer-endpoint-settings"), MaximumPerStage: 8, MaximumDuration: 5 * time.Second}
}

// Actual manager/store composition prepares the Staged and Outcome facts.
// Receipt facts below are deliberately synthetic backend fixtures, not proof
// of real authentication. The separate actor TLS tier owns that future proof.
func receiptFixture(t *testing.T, maximum int) (*composition, AuthObservationReceipt) {
	t.Helper()
	c := composed(t)
	cfg := c.backend.config
	cfg.BootstrapUID = "auth-receipt-bootstrap-0001"
	cfg.AuthObservationPolicy = receiptPolicy()
	cfg.MaximumRevisions = maximum
	dir := t.TempDir()
	if os.Chmod(dir, 0700) != nil {
		t.Fatal("private fixture directory unavailable")
	}
	root, e := os.OpenRoot(dir)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = root.Close() })
	c.backend, c.binding, e = Bootstrap(ctx, root, cfg)
	if e != nil {
		t.Fatal(e)
	}
	c.root = root
	c.manager, e = retainManager(t, c, nil)
	if e != nil {
		t.Fatal(e)
	}
	c.advance(t)
	c.advance(t)
	j := c.advance(t)
	if j.Phase != tc.CredentialStaged {
		t.Fatal("fixture did not reach Staged")
	}
	o, found, e := c.backend.OutcomeLedger().Load(ctx, j.CandidateWriteID)
	if e != nil || !found {
		t.Fatal("fixture missing verified candidate outcome")
	}
	q := AuthObservationRequest{AttemptID: digest("owned-auth-observation-one"), SlotID: j.Intent.SlotID, StageID: j.Intent.StageID, CredentialAttemptID: j.Intent.AttemptID, Owner: j.Intent.Owner, NamespaceID: cfg.NamespaceID, NamespaceIncarnationID: cfg.NamespaceIncarnationID, NamespaceCreatedAt: cfg.NamespaceCreatedAt, StoreID: cfg.StoreID, CandidateID: j.Candidate.CandidateID, CandidateARN: o.ARN, CandidateCreatedAt: o.CreatedAt, WriteID: j.CandidateWriteID, ServiceAccountID: j.ServiceAccount.ID, KeyID: j.Key.ID, CredentialJournalVersion: j.Version, OutcomeLedgerVersion: o.Version, ObservationBindingDigest: cfg.AuthObservationPolicy.ObservationBindingDigest}
	start := testNow.Add(2 * time.Second)
	proof := &observation.Observation{Schema: 1, Contract: observation.Contract, Owner: q.Owner, SlotID: q.SlotID, StageID: q.StageID, AttemptID: q.CredentialAttemptID, NamespaceID: q.NamespaceID, NamespaceIncarnationID: q.NamespaceIncarnationID, StoreID: q.StoreID, CandidateID: q.CandidateID, WriteID: q.WriteID, VersionID: q.WriteID, ServiceAccountID: q.ServiceAccountID, KeyID: q.KeyID, CredentialJournalVersion: q.CredentialJournalVersion, OutcomeLedgerVersion: q.OutcomeLedgerVersion, EndpointBindingDigest: cfg.AuthObservationPolicy.EndpointBindingDigest, DataPlaneIDHash: digest("independently-observed-data-plane-id"), KeyVersionDigest: tc.ProviderVersionDigest("new-observed-active-version"), ServiceAccountVersionDigest: tc.ProviderVersionDigest("observed-sa-version"), NamespaceVersionDigest: tc.ProviderVersionDigest("observed-namespace-version"), StartedAt: start, ObservedAt: start.Add(time.Millisecond), ExpiresAt: j.Intent.ExpiresAt}
	r := AuthObservationReceipt{Contract: AuthObservationReceiptContract, Request: q, Authority: AuthObservationAuthority{ControlCellUID: "owned-control-cell-uid", ControlVersionDigest: digest("opaque-control-version"), SessionID: digest("original-admission-session"), AdmissionGeneration: 1, WriterEpoch: 1, WriterID: "fixture-writer"}, StartedAt: start, RecordedAt: start.Add(2 * time.Millisecond), Kind: AuthObserved, Observation: proof}
	return c, r
}
func insertReceipt(t *testing.T, s *Store, r AuthObservationReceipt) AuthObservationReceipt {
	t.Helper()
	saved, e := s.AuthObservations().Insert(ctx, r, r.RecordedAt)
	if e != nil {
		t.Fatal(e)
	}
	return saved
}

func TestAuthReceiptChannelHistoricalReplayAndIndependentVersions(t *testing.T) {
	c, r := receiptFixture(t, 32)
	j, _, _ := c.backend.CredentialJournal().Load(ctx, c.backend.slot)
	o, _, _ := c.backend.OutcomeLedger().Load(ctx, r.Request.WriteID)
	before, _ := c.backend.History(ctx)
	if _, found, e := c.backend.AuthObservations().Preflight(ctx, r.Request); e != nil || found {
		t.Fatal("new read preflight unexpectedly persisted a record")
	}
	still, _ := c.backend.History(ctx)
	if !equal(before, still) {
		t.Fatal("read capacity preflight mutated history")
	}
	saved := insertReceipt(t, c.backend, r)
	j2, _, _ := c.backend.CredentialJournal().Load(ctx, c.backend.slot)
	o2, _, _ := c.backend.OutcomeLedger().Load(ctx, r.Request.WriteID)
	if !equal(j, j2) || !equal(o, o2) || saved.Version == j.Version || saved.Version == o.Version || saved.Observation.KeyVersionDigest == j.Key.VersionDigest {
		t.Fatal("receipt changed credential/outcome CAS or refreshed staged key")
	}
	// An expired exact duplicate is historical, not a new authentication claim.
	duplicate, e := c.backend.AuthObservations().Insert(ctx, r, testNow.Add(48*time.Hour))
	if e != nil || !equal(saved, duplicate) {
		t.Fatal("exact historical durable replay failed")
	}
	reopened, e := Open(c.root, c.binding, c.backend.config)
	if e != nil {
		t.Fatal(e)
	}
	historical, found, e := reopened.AuthObservations().Preflight(ctx, r.Request)
	if e != nil || !found || !equal(historical, saved) || historical.Authority.SessionID != r.Authority.SessionID || !historical.RecordedAt.Equal(r.RecordedAt) {
		t.Fatal("clean successor lost historical refs or original session/time")
	}
	// No current session is an input to historical lookup. It belongs to the
	// actor's separate current-authority check, not record equality.
	historical.Authority.SessionID = digest("new-actor-session")
	historical.Observation.KeyID = "mutated-copy"
	loaded, found, e := reopened.AuthObservations().Load(ctx, r.Request.AttemptID)
	if e != nil || !found || !equal(loaded, saved) {
		t.Fatal("historical result aliases caller data")
	}
	h, e := reopened.History(ctx)
	if e != nil || len(h) != len(before)+1 || h[len(h)-1].AuthObservation == nil {
		t.Fatal("receipt missing from global high-water history")
	}
	h[len(h)-1].AuthObservation.Observation.KeyID = "mutated-history"
	loaded, _, _ = reopened.AuthObservations().Load(ctx, r.Request.AttemptID)
	if !equal(loaded, saved) {
		t.Fatal("history aliases mutable receipt")
	}
	c.privacy(t)
}

func TestAuthReceiptInsertOnlyAttemptTombstoneAndCanonicalReplay(t *testing.T) {
	c, r := receiptFixture(t, 32)
	saved := insertReceipt(t, c.backend, r)
	for name, change := range map[string]func(*AuthObservationReceipt){
		"session":         func(r *AuthObservationReceipt) { r.Authority.SessionID = digest("new-session") },
		"result":          func(r *AuthObservationReceipt) { r.Observation.DataPlaneIDHash = digest("other-result") },
		"time":            func(r *AuthObservationReceipt) { r.RecordedAt = r.RecordedAt.Add(time.Nanosecond) },
		"stage":           func(r *AuthObservationReceipt) { r.Request.StageID = digest("other-stage") },
		"outcome-version": func(r *AuthObservationReceipt) { r.Request.OutcomeLedgerVersion = digest("other-version") },
	} {
		t.Run(name, func(t *testing.T) {
			next := copyAuthReceipt(r)
			change(&next)
			if _, e := c.backend.AuthObservations().Insert(ctx, next, next.RecordedAt); e != CASConflict {
				t.Fatal("stable attempt was rebound")
			}
		})
	}
	request := r.Request
	request.OutcomeLedgerVersion = digest("other-version")
	if _, _, e := c.backend.AuthObservations().Preflight(ctx, request); e != CASConflict {
		t.Fatal("historical input mismatch ignored")
	}
	// Time-zone presentation is normalized only to canonical UTC instants; no
	// substantive result, session, or timestamp is silently replaced.
	zone := time.FixedZone("owned-offset", -4*60*60)
	canonical := copyAuthReceipt(r)
	canonical.StartedAt = canonical.StartedAt.In(zone)
	canonical.RecordedAt = canonical.RecordedAt.In(zone)
	canonical.Request.NamespaceCreatedAt = canonical.Request.NamespaceCreatedAt.In(zone)
	canonical.Request.CandidateCreatedAt = canonical.Request.CandidateCreatedAt.In(zone)
	canonical.Observation.StartedAt = canonical.Observation.StartedAt.In(zone)
	canonical.Observation.ObservedAt = canonical.Observation.ObservedAt.In(zone)
	canonical.Observation.ExpiresAt = canonical.Observation.ExpiresAt.In(zone)
	got, e := c.backend.AuthObservations().Insert(ctx, canonical, canonical.RecordedAt)
	if e != nil || !equal(got, saved) {
		t.Fatal("canonical durable equality broke legitimate replay")
	}
}

func TestAuthReceiptDeniesForeignAndUnsupportedEvidenceWithoutAppending(t *testing.T) {
	c, r := receiptFixture(t, 128)
	before, _ := c.backend.History(ctx)
	for name, change := range map[string]func(*AuthObservationReceipt){
		"contract":        func(r *AuthObservationReceipt) { r.Contract = "future" },
		"requested-phase": func(r *AuthObservationReceipt) { r.Kind = "Requested" },
		"owner":           func(r *AuthObservationReceipt) { r.Request.Owner.RunID = "other-run" },
		"namespace-birth": func(r *AuthObservationReceipt) {
			r.Request.NamespaceCreatedAt = r.Request.NamespaceCreatedAt.Add(time.Nanosecond)
		},
		"secret-birth": func(r *AuthObservationReceipt) {
			r.Request.CandidateCreatedAt = r.Request.CandidateCreatedAt.Add(time.Nanosecond)
		},
		"arn":               func(r *AuthObservationReceipt) { r.Request.CandidateARN += "Other" },
		"write":             func(r *AuthObservationReceipt) { r.Request.WriteID = digest("foreign-write") },
		"journal-version":   func(r *AuthObservationReceipt) { r.Request.CredentialJournalVersion = digest("other-version") },
		"settings":          func(r *AuthObservationReceipt) { r.Request.ObservationBindingDigest = digest("other-settings") },
		"endpoint":          func(r *AuthObservationReceipt) { r.Observation.EndpointBindingDigest = digest("other-endpoint") },
		"proof-contract":    func(r *AuthObservationReceipt) { r.Observation.Contract = "future" },
		"proof-version":     func(r *AuthObservationReceipt) { r.Observation.VersionID = digest("other-version") },
		"proof-expiry":      func(r *AuthObservationReceipt) { r.Observation.ExpiresAt = r.Observation.ExpiresAt.Add(time.Second) },
		"raw-version":       func(r *AuthObservationReceipt) { r.Observation.KeyVersionDigest = compositionMaterial },
		"unbounded-date":    func(r *AuthObservationReceipt) { r.RecordedAt = r.StartedAt.Add(time.Hour) },
		"future-completion": func(r *AuthObservationReceipt) { r.Observation.ObservedAt = r.RecordedAt.Add(time.Nanosecond) },
		"missing-authority": func(r *AuthObservationReceipt) { r.Authority = AuthObservationAuthority{} },
		"raw-session":       func(r *AuthObservationReceipt) { r.Authority.SessionID = compositionMaterial },
		"mixed-result": func(r *AuthObservationReceipt) {
			r.Failure = &AuthObservationFailure{Code: observation.Denied, Step: observation.Identity}
		},
	} {
		t.Run(name, func(t *testing.T) {
			next := copyAuthReceipt(r)
			change(&next)
			if _, e := c.backend.AuthObservations().Insert(ctx, next, next.RecordedAt); e == nil || bytes.Contains([]byte(fmt.Sprint(e)), []byte(compositionMaterial)) {
				t.Fatal("foreign or unsafe evidence admitted/disclosed")
			}
		})
	}
	after, e := c.backend.History(ctx)
	if e != nil || !equal(before, after) {
		t.Fatal("rejected receipt changed durable history")
	}
	c.privacy(t)
}

func TestAuthReceiptFailureIsFiniteAndExpiredHistoryRemainsReadable(t *testing.T) {
	c, r := receiptFixture(t, 32)
	success := copyAuthReceipt(r)
	r.Kind, r.Observation = AuthNotEstablished, nil
	r.Failure = &AuthObservationFailure{Code: observation.AuthenticationRejected, Step: observation.Identity}
	saved := insertReceipt(t, c.backend, r)
	for _, change := range []func(*AuthObservationReceipt){
		func(r *AuthObservationReceipt) { r.Failure.Code = observation.Code(compositionMaterial) },
		func(r *AuthObservationReceipt) { r.Failure.Step = observation.Step(compositionMaterial) },
		func(r *AuthObservationReceipt) { r.Failure = nil },
	} {
		next := copyAuthReceipt(r)
		next.Request.AttemptID = digest(fmt.Sprint(len(encoded(next)), "new-attempt"))
		change(&next)
		if _, e := c.backend.AuthObservations().Insert(ctx, next, next.RecordedAt); e == nil {
			t.Fatal("nonfinite failure persisted")
		}
	}
	late := copyAuthReceipt(r)
	late.Request.AttemptID = digest("expired-new-insert")
	late.StartedAt, late.RecordedAt = testNow.Add(2*time.Hour), testNow.Add(2*time.Hour)
	late.Failure.Code = observation.Expired
	expiredFailure := insertReceipt(t, c.backend, late)
	success.Request.AttemptID = digest("expired-success")
	success.StartedAt, success.RecordedAt = late.StartedAt, late.RecordedAt
	success.Observation.StartedAt, success.Observation.ObservedAt = late.StartedAt, late.RecordedAt
	if _, e := c.backend.AuthObservations().Insert(ctx, success, success.RecordedAt); e != IllegalTransition {
		t.Fatal("new expired authentication success was recorded")
	}
	late.Request.AttemptID = digest("stale-recording-point")
	if _, e := c.backend.AuthObservations().Insert(ctx, late, late.RecordedAt.Add(time.Second)); e != IllegalTransition {
		t.Fatal("stale caller recording time was accepted")
	}
	// No clock input to scan/Load: old expiration is a historical fact, never
	// a retrospective corruption test as a process's wall clock advances.
	reopened, e := Open(c.root, c.binding, c.backend.config)
	if e != nil {
		t.Fatal(e)
	}
	loaded, found, e := reopened.AuthObservations().Load(ctx, r.Request.AttemptID)
	if e != nil || !found || !equal(loaded, saved) {
		t.Fatal("historical finite failure lost")
	}
	loaded, found, e = reopened.AuthObservations().Load(ctx, expiredFailure.Request.AttemptID)
	if e != nil || !found || !equal(loaded, expiredFailure) || loaded.Failure.Code != observation.Expired {
		t.Fatal("expired failure history became retrospectively unreadable")
	}
}

func TestAuthReceiptCapacityAndConcurrentOpenInsert(t *testing.T) {
	c, r := receiptFixture(t, 32)
	other, e := Open(c.root, c.binding, c.backend.config)
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, store := range []*Store{c.backend, other} {
		wg.Add(1)
		go func(s *Store) {
			defer wg.Done()
			_, e := s.AuthObservations().Insert(ctx, r, r.RecordedAt)
			results <- e
		}(store)
	}
	wg.Wait()
	close(results)
	for e := range results {
		if e != nil {
			t.Fatal("concurrent identical inserts were not idempotent")
		}
	}
	h, _ := c.backend.History(ctx)
	count := 0
	for _, revision := range h {
		if revision.AuthObservation != nil {
			count++
		}
	}
	if count != 1 {
		t.Fatal("concurrent insert duplicated terminal receipt")
	}
	for n := 1; n < c.backend.config.AuthObservationPolicy.MaximumPerStage; n++ {
		next := copyAuthReceipt(r)
		next.Request.AttemptID = digest(n)
		insertReceipt(t, c.backend, next)
	}
	next := copyAuthReceipt(r)
	next.Request.AttemptID = digest("over-stage-capacity")
	if _, _, e := c.backend.AuthObservations().Preflight(ctx, next.Request); e != Capacity {
		t.Fatal("capacity preflight did not refuse before read")
	}
	if _, e := c.backend.AuthObservations().Insert(ctx, next, next.RecordedAt); e != Capacity {
		t.Fatal("receipt stage capacity bypassed")
	}
	// Global history capacity has its own gate, independent of per-stage limit.
	full, proposed := receiptFixture(t, 8)
	if _, _, e := full.backend.AuthObservations().Preflight(ctx, proposed.Request); e != Capacity {
		t.Fatal("global capacity preflight bypassed")
	}
}

func TestAuthReceiptUnknownAppendExactRecoveryAndPartialTail(t *testing.T) {
	for _, point := range []string{"after-intent", "after-commit", "partial-intent"} {
		t.Run(point, func(t *testing.T) {
			c, r := receiptFixture(t, 32)
			c.backend.fault = func(at string) error {
				if at == point || point == "partial-intent" && at == "after-intent" {
					return Uncertain
				}
				return nil
			}
			if saved, e := c.backend.AuthObservations().Insert(ctx, r, r.RecordedAt); e != Uncertain || saved.Version != "" {
				t.Fatal("uncertain receipt was acknowledged")
			}
			if _, _, e := c.backend.AuthObservations().Load(ctx, r.Request.AttemptID); e != Uncertain {
				t.Fatal("uncertain append exposed stale facts")
			}
			pending, e := c.backend.Pending(ctx)
			if e != nil || pending.Kind != "auth-observation" || pending.Key != r.Request.AttemptID {
				t.Fatal("exact retained safe receipt reference missing")
			}
			if point == "partial-intent" {
				f, e := c.root.OpenFile(frameName(pending.Sequence, "intent"), os.O_WRONLY|os.O_TRUNC, 0600)
				if e != nil {
					t.Fatal(e)
				}
				_, _ = f.Write([]byte("{"))
				_ = f.Sync()
				_ = f.Close()
				if e := c.backend.Recover(ctx, pending.Digest); e != Corrupt {
					t.Fatal("partial record was silently repaired")
				}
				return
			}
			if e := c.backend.Recover(ctx, digest("wrong-recovery-frame")); e == nil {
				t.Fatal("recovery accepted foreign receipt digest")
			}
			c.backend.fault = nil
			if e := c.backend.Recover(ctx, pending.Digest); e != nil {
				t.Fatal(e)
			}
			reopened, e := Open(c.root, c.binding, c.backend.config)
			if e != nil {
				t.Fatal(e)
			}
			loaded, found, e := reopened.AuthObservations().Load(ctx, r.Request.AttemptID)
			if e != nil || !found || loaded.Version != pending.Digest || loaded.Kind != AuthObserved {
				t.Fatal("commit-only recovery lost terminal evidence")
			}
			c.privacy(t)
		})
	}
}

func TestAuthReceiptFreshSchemaZeroBytesAndBindingDiscrimination(t *testing.T) {
	old, binding, root, _ := testStore(t, 16)
	// These frozen legacy wire structures have no knowledge of new optional
	// fields: this is a byte compatibility contract, including null fields.
	type legacyFrame struct {
		Schema, BootstrapUID                       string
		Sequence                                   uint64
		PreviousDigest, Kind, Key, ExpectedVersion string
		Journal                                    *tc.CredentialState
		Outcome                                    *awsstore.Outcome
	}
	legacy := legacyFrame{Schema: "credential-metadata-intent-v1", BootstrapUID: "fixture", Sequence: 1, Kind: "journal", Key: "slot"}
	current := frame{Schema: legacy.Schema, BootstrapUID: legacy.BootstrapUID, Sequence: legacy.Sequence, Kind: legacy.Kind, Key: legacy.Key}
	if !bytes.Equal(encoded(legacy), encoded(current)) || old.identitySchema() != "credential-local-metadata-v1" || old.intentSchema() != "credential-metadata-intent-v1" || old.commitSchema() != "credential-metadata-commit-v1" {
		t.Fatal("zero-policy legacy schema/frame bytes changed")
	}
	type legacyRevision struct {
		Sequence                                           uint64
		Digest, PreviousDigest, Kind, Key, ExpectedVersion string
		Journal                                            *tc.CredentialState
		Outcome                                            *awsstore.Outcome
	}
	if !bytes.Equal(encoded(legacyRevision{}), encoded(Revision{})) {
		t.Fatal("zero receipt changed legacy high-water serialization")
	}
	if bytes.Contains(encoded(old.config), []byte("AuthObservationPolicy")) {
		t.Fatal("zero policy added a serialized field")
	}
	// Independently freeze the original config digest domain, not a digest of
	// the new branch. Identity bytes bind its exact original schema+digest.
	cfg := old.config
	temporal, _ := cfg.Ownership.Scope(ownership.Temporal)
	aws, _ := cfg.Ownership.Scope(ownership.AWS)
	seal, _ := cfg.Ownership.Name(ownership.Temporal, "metadata-scope", cfg.BootstrapUID, 63)
	legacyDigest := digest(struct {
		UID, Store, Namespace, Incarnation, Slot, ScopeSeal string
		Owner                                               ownership.Owner
		Temporal, AWS                                       ownership.Scope
		Born                                                time.Time
		SA                                                  tc.ServiceAccountRef
		MaxRevisions, MaxBytes                              int
	}{cfg.BootstrapUID, cfg.StoreID, cfg.NamespaceID, cfg.NamespaceIncarnationID, old.slot, seal, cfg.Ownership.Owner(), temporal, aws, cfg.NamespaceCreatedAt, cfg.PreprovisionedServiceAccount, cfg.MaximumRevisions, cfg.MaximumRecordBytes})
	raw, _ := root.ReadFile(identityName)
	expected := encoded(identity{Schema: "credential-local-metadata-v1", BootstrapUID: cfg.BootstrapUID, ConfigSHA256: legacyDigest, Directory: binding.Directory, Lock: binding.Lock})
	if binding.ConfigSHA256 != legacyDigest || !bytes.Equal(raw, expected) {
		t.Fatal("zero-policy legacy config/identity bytes changed")
	}
	if _, _, e := old.AuthObservations().Load(ctx, digest("attempt")); e != Invalid {
		t.Fatal("old store implicitly enabled receipt writes")
	}
	enable := cfg
	enable.AuthObservationPolicy = receiptPolicy()
	if _, e := Open(root, binding, enable); e != ScopeConflict {
		t.Fatal("existing root implicitly upgraded")
	}
	c, _ := receiptFixture(t, 32)
	if c.backend.identitySchema() == old.identitySchema() || c.backend.intentSchema() == old.intentSchema() || c.backend.commitSchema() == old.commitSchema() {
		t.Fatal("new lane did not discriminate every durable schema")
	}
	disable := c.backend.config
	disable.AuthObservationPolicy = nil
	if _, e := Open(c.root, c.binding, disable); e != ScopeConflict {
		t.Fatal("new root implicitly downgraded")
	}
	changed := c.backend.config
	copyPolicy := *changed.AuthObservationPolicy
	copyPolicy.EndpointBindingDigest = digest("different-endpoints")
	changed.AuthObservationPolicy = &copyPolicy
	if _, e := Open(c.root, c.binding, changed); e != ScopeConflict {
		t.Fatal("new root accepted changed trusted policy")
	}
}

func TestAuthReceiptPolicyIsFiniteCopiedAndFreshOnly(t *testing.T) {
	for name, change := range map[string]func(*AuthObservationPolicy){
		"contract":          func(p *AuthObservationPolicy) { p.Contract = "future" },
		"observer-contract": func(p *AuthObservationPolicy) { p.ObservationContract = "future" },
		"settings":          func(p *AuthObservationPolicy) { p.ObservationBindingDigest = "" },
		"endpoints":         func(p *AuthObservationPolicy) { p.EndpointBindingDigest = "" },
		"capacity":          func(p *AuthObservationPolicy) { p.MaximumPerStage = 9 },
		"duration":          func(p *AuthObservationPolicy) { p.MaximumDuration = 11 * time.Second },
	} {
		t.Run(name, func(t *testing.T) {
			old, _, root, _ := testStore(t, 16)
			cfg := old.config
			cfg.AuthObservationPolicy = receiptPolicy()
			change(cfg.AuthObservationPolicy)
			if _, e := configured(root, cfg); e != Invalid {
				t.Fatal("nonfinite receipt policy accepted")
			}
		})
	}
	c, _ := receiptFixture(t, 32)
	cfg := c.backend.config
	s, e := configured(c.root, cfg)
	if e != nil {
		t.Fatal(e)
	}
	before := s.configDigest()
	cfg.AuthObservationPolicy.EndpointBindingDigest = digest("external-mutation")
	if s.configDigest() != before {
		t.Fatal("caller can mutate stored policy")
	}
}
