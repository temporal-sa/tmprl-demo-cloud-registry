package localmetadata

import (
	"bytes"
	"context"
	"io"
	"strconv"

	awsstore "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/awssecrets"
	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
)

type frame struct {
	Schema          string
	BootstrapUID    string
	Sequence        uint64
	PreviousDigest  string
	Kind            string
	Key             string
	ExpectedVersion string
	Journal         *tc.CredentialState
	Outcome         *awsstore.Outcome
	AuthObservation *AuthObservationReceipt `json:",omitempty"`
}
type commit struct {
	Schema, BootstrapUID string
	Sequence             uint64
	Digest               string
}
type view struct {
	history          []Revision
	journal          *tc.CredentialState
	outcomes         map[string]awsstore.Outcome
	usedStages       map[string]bool
	authObservations map[string]AuthObservationReceipt
	lastDigest       string
	pending          *frame
	pendingDigest    string
}

func (s *Store) scan(ctx context.Context, allowPending bool) (*view, error) {
	f, err := s.root.Open(".")
	if err != nil {
		return nil, Corrupt
	}
	entries, readErr := f.ReadDir(2*s.config.MaximumRevisions + 3)
	_ = f.Close()
	if readErr != nil && readErr != io.EOF {
		return nil, Uncertain
	}
	if len(entries) > 2*s.config.MaximumRevisions+2 {
		return nil, Capacity
	}
	intents, commits := map[uint64]bool{}, map[uint64]bool{}
	var max uint64
	for _, e := range entries {
		if e.Name() == identityName || e.Name() == lockName {
			continue
		}
		m := framePattern.FindStringSubmatch(e.Name())
		if m == nil || e.IsDir() {
			return nil, Corrupt
		}
		n, _ := strconv.ParseUint(m[1], 10, 64)
		if n == 0 || n > uint64(s.config.MaximumRevisions) {
			return nil, Capacity
		}
		if n > max {
			max = n
		}
		if m[2] == "intent" {
			intents[n] = true
		} else {
			commits[n] = true
		}
	}
	v := &view{outcomes: map[string]awsstore.Outcome{}, usedStages: map[string]bool{}, authObservations: map[string]AuthObservationReceipt{}}
	for n := uint64(1); n <= max; n++ {
		if ctx.Err() != nil {
			return nil, Uncertain
		}
		if !intents[n] {
			return nil, Corrupt
		}
		raw, err := s.readPrivate(frameName(n, "intent"), true)
		if err != nil {
			return nil, err
		}
		var next frame
		if decode(raw, &next) != nil || !bytes.Equal(encoded(next), raw) || next.Schema != s.intentSchema() || next.BootstrapUID != s.binding.BootstrapUID || next.Sequence != n || next.PreviousDigest != v.lastDigest {
			return nil, Corrupt
		}
		if s.validateFrame(v, next) != nil {
			return nil, Corrupt
		}
		d := rawDigest(raw)
		if !commits[n] {
			if n != max {
				return nil, Corrupt
			}
			v.pending = &next
			v.pendingDigest = d
			if !allowPending {
				return nil, Uncertain
			}
			return v, nil
		}
		commitRaw, err := s.readPrivate(frameName(n, "commit"), true)
		if err != nil {
			return nil, err
		}
		var accepted commit
		if decode(commitRaw, &accepted) != nil || !bytes.Equal(encoded(accepted), commitRaw) || accepted.Schema != s.commitSchema() || accepted.BootstrapUID != s.binding.BootstrapUID || accepted.Sequence != n || accepted.Digest != d {
			return nil, Corrupt
		}
		v.apply(next, d)
	}
	if s.syncDirectory() != nil {
		return nil, Uncertain
	}
	return v, nil
}

func (v *view) apply(f frame, d string) {
	r := Revision{Sequence: f.Sequence, Digest: d, PreviousDigest: f.PreviousDigest, Kind: f.Kind, Key: f.Key, ExpectedVersion: f.ExpectedVersion}
	if f.Journal != nil {
		next := *f.Journal
		next.Version = d
		v.journal = &next
		copy := next
		r.Journal = &copy
		v.usedStages[next.Intent.StageID] = true
	}
	if f.Outcome != nil {
		next := *f.Outcome
		next.Version = d
		v.outcomes[f.Key] = next
		copy := next
		r.Outcome = &copy
	}
	if f.AuthObservation != nil {
		next := copyAuthReceipt(*f.AuthObservation)
		next.Version = d
		v.authObservations[f.Key] = next
		copy := copyAuthReceipt(next)
		r.AuthObservation = &copy
	}
	v.history = append(v.history, r)
	v.lastDigest = d
}

func (s *Store) append(ctx context.Context, v *view, f frame) (string, error) {
	if len(v.history) >= s.config.MaximumRevisions {
		return "", Capacity
	}
	f.Schema = s.intentSchema()
	f.BootstrapUID = s.binding.BootstrapUID
	f.Sequence = uint64(len(v.history) + 1)
	f.PreviousDigest = v.lastDigest
	if s.validateFrame(v, f) != nil {
		return "", IllegalTransition
	}
	if ctx.Err() != nil {
		return "", Uncertain
	}
	raw := encoded(f)
	if len(raw) == 0 {
		return "", IllegalTransition
	}
	if len(raw) > s.config.MaximumRecordBytes {
		return "", Capacity
	}
	d := rawDigest(raw)
	s.uncertain = true // Never clear on an error or canceled/late append.
	if s.writeNew(frameName(f.Sequence, "intent"), raw) != nil || s.syncDirectory() != nil {
		return "", Uncertain
	}
	if s.fault != nil && s.fault("after-intent") != nil {
		return "", Uncertain
	}
	if ctx.Err() != nil {
		return "", Uncertain
	}
	c := commit{Schema: s.commitSchema(), BootstrapUID: s.binding.BootstrapUID, Sequence: f.Sequence, Digest: d}
	if s.writeNew(frameName(f.Sequence, "commit"), encoded(c)) != nil || s.syncDirectory() != nil {
		return "", Uncertain
	}
	if s.fault != nil && s.fault("after-commit") != nil {
		return "", Uncertain
	}
	check, err := s.scan(ctx, false)
	if err != nil || check.lastDigest != d || ctx.Err() != nil {
		return "", Uncertain
	}
	s.uncertain = false
	return d, nil
}

// Pending inspects safe intent identifiers without acknowledging a stale state.
// A malformed/partial tail remains Corrupt; it is never removed or reset here.
func (s *Store) Pending(ctx context.Context) (Pending, error) {
	var pending Pending
	err := s.locked(ctx, func() error {
		v, err := s.scan(ctx, true)
		if err != nil {
			return err
		}
		if v.pending != nil {
			pending = Pending{Sequence: v.pending.Sequence, Digest: v.pendingDigest, Kind: v.pending.Kind, Key: v.pending.Key}
			return nil
		}
		if s.uncertain && len(v.history) > 0 {
			r := v.history[len(v.history)-1]
			pending = Pending{Sequence: r.Sequence, Digest: r.Digest, Kind: r.Kind, Key: r.Key}
			return nil
		}
		return Uncertain
	})
	return pending, err
}

// Recover is explicit and commit-only. It confirms an already accepted record,
// or finishes the exact valid retained metadata intent named by its digest. It
// cannot abandon an intent, clear a partial record, roll back or claim a cloud
// secret write FinalNotCommitted. Inspect safe intent/history before using it.
func (s *Store) Recover(ctx context.Context, expectedDigest string) error {
	if !hex64.MatchString(expectedDigest) {
		return Invalid
	}
	return s.locked(ctx, func() error {
		v, err := s.scan(ctx, true)
		if err != nil {
			return err
		}
		if v.pending == nil {
			if v.lastDigest != expectedDigest {
				return Uncertain
			}
			s.uncertain = true
			if s.syncDirectory() != nil {
				return Uncertain
			}
			s.uncertain = false
			return nil
		}
		if v.pendingDigest != expectedDigest {
			return CASConflict
		}
		s.uncertain = true
		c := commit{Schema: s.commitSchema(), BootstrapUID: s.binding.BootstrapUID, Sequence: v.pending.Sequence, Digest: v.pendingDigest}
		if s.writeNew(frameName(c.Sequence, "commit"), encoded(c)) != nil || s.syncDirectory() != nil {
			return Uncertain
		}
		checked, err := s.scan(ctx, false)
		if err != nil || checked.lastDigest != expectedDigest {
			return Uncertain
		}
		s.uncertain = false
		return nil
	})
}

func (j *Journal) Load(ctx context.Context, slot string) (tc.CredentialState, bool, error) {
	var state tc.CredentialState
	var found bool
	if slot != j.store.slot {
		return state, false, ScopeConflict
	}
	err := j.store.locked(ctx, func() error {
		if j.store.uncertain {
			return Uncertain
		}
		v, err := j.store.scan(ctx, false)
		if err != nil {
			return err
		}
		if v.journal != nil {
			state = *v.journal
			found = true
		}
		return nil
	})
	return state, found, err
}
func (j *Journal) CompareAndSwap(ctx context.Context, slot, expected string, next tc.CredentialState) (tc.CredentialState, error) {
	s := j.store
	var saved tc.CredentialState
	if slot != s.slot || (next.Version != "" && next.Version != expected) {
		return saved, ScopeConflict
	}
	next.Version = ""
	next = normalizeState(next)
	err := s.locked(ctx, func() error {
		if s.uncertain {
			return Uncertain
		}
		v, err := s.scan(ctx, false)
		if err != nil {
			return err
		}
		current := ""
		if v.journal != nil {
			current = v.journal.Version
		}
		if current != expected {
			if v.journal != nil {
				comparison := *v.journal
				comparison.Version = ""
				for n := len(v.history) - 1; n >= 0; n-- {
					r := v.history[n]
					if r.Journal != nil {
						if r.ExpectedVersion == expected && equal(comparison, next) {
							saved = *v.journal
							return nil
						}
						break
					}
				}
			}
			return CASConflict
		}
		if v.journal != nil {
			copy := *v.journal
			copy.Version = ""
			if equal(copy, next) {
				saved = *v.journal
				return nil
			}
		}
		d, err := s.append(ctx, v, frame{Kind: "journal", Key: slot, ExpectedVersion: expected, Journal: &next})
		if err != nil {
			return err
		}
		next.Version = d
		saved = next
		return nil
	})
	return saved, err
}
func (l *Ledger) Load(ctx context.Context, writeID string) (awsstore.Outcome, bool, error) {
	var out awsstore.Outcome
	var found bool
	if !hex64.MatchString(writeID) {
		return out, false, ScopeConflict
	}
	err := l.store.locked(ctx, func() error {
		if l.store.uncertain {
			return Uncertain
		}
		v, err := l.store.scan(ctx, false)
		if err != nil {
			return err
		}
		out, found = v.outcomes[writeID]
		return nil
	})
	return out, found, err
}
func (l *Ledger) CompareAndSwap(ctx context.Context, key, expected string, next awsstore.Outcome) (awsstore.Outcome, error) {
	s := l.store
	var saved awsstore.Outcome
	if !hex64.MatchString(key) || (next.Version != "" && next.Version != expected) {
		return saved, ScopeConflict
	}
	next.Version = ""
	next = normalizeOutcome(next)
	err := s.locked(ctx, func() error {
		if s.uncertain {
			return Uncertain
		}
		v, err := s.scan(ctx, false)
		if err != nil {
			return err
		}
		current, found := v.outcomes[key]
		version := ""
		if found {
			version = current.Version
		}
		if version != expected {
			copy := current
			copy.Version = ""
			if found && equal(copy, next) {
				for n := len(v.history) - 1; n >= 0; n-- {
					r := v.history[n]
					if r.Outcome != nil && r.Key == key {
						if r.ExpectedVersion == expected {
							saved = current
							return nil
						}
						break
					}
				}
			}
			return CASConflict
		}
		if found {
			copy := current
			copy.Version = ""
			if equal(copy, next) {
				saved = current
				return nil
			}
		}
		d, err := s.append(ctx, v, frame{Kind: "outcome", Key: key, ExpectedVersion: expected, Outcome: &next})
		if err != nil {
			return err
		}
		next.Version = d
		saved = next
		return nil
	})
	return saved, err
}
