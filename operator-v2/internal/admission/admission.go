// Package admission provides one-use CAS admission for a controlled clean
// handoff. The caller must close/drain provider dispatch and observe old worker
// exit before a successor starts. This is not a lease, process-death detector,
// distributed lock, or provider RPC fence. Only local clean-handoff mode exists.
package admission

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"sync"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
)

type Mode string

const LocalCleanHandoff Mode = "local_clean_handoff"

var (
	ErrInvalid   = errors.New("invalid clean-handoff admission contract")
	ErrChanged   = errors.New("clean-handoff admission changed or was already consumed")
	ErrUncertain = errors.New("clean-handoff admission write is uncertain; no dispatch authorized")
	ErrBlocked   = errors.New("clean-handoff admission blocked by unresolved state")
	ErrClosed    = errors.New("clean-handoff session is closed")
)

type Config struct {
	Mode       Mode
	Repository *journal.Repository
}
type handoffValue struct {
	Schema    int               `json:"schema"`
	Subject   rollout.Subject   `json:"subject"`
	Version   journal.Version   `json:"version"`
	Sequence  uint64            `json:"sequence"`
	Digest    string            `json:"digest"`
	Admission journal.Admission `json:"admission"`
}

// Handoff contains trusted supervisor data, not a bearer credential. Its exact
// version/digest/nonce is consumed by provider CAS, so replay cannot admit two
// successors even if the bytes are copied. It does not prove old process exit.
type Handoff struct{ value handoffValue }

var hash = regexp.MustCompile(`^[a-f0-9]{64}$`)

func (h Handoff) JSON() ([]byte, error) {
	v := h.value
	if v.Schema != 1 || v.Version.UID == "" || v.Version.ResourceVersion == "" || v.Sequence == 0 || !hash.MatchString(v.Digest) || v.Admission.Generation == 0 || v.Admission.State != journal.AdmissionReady || !hash.MatchString(v.Admission.SessionID) || !hash.MatchString(v.Admission.HandoffID) {
		return nil, ErrInvalid
	}
	if _, err := journal.Initial(v.Subject, 1); err != nil {
		return nil, ErrInvalid
	}
	data, err := json.Marshal(v)
	if err != nil || len(data) > 8192 {
		return nil, ErrInvalid
	}
	return data, nil
}
func Parse(data []byte) (Handoff, error) {
	if len(data) > 8192 {
		return Handoff{}, ErrInvalid
	}
	var h Handoff
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(&h.value) != nil || d.Decode(new(any)) != io.EOF {
		return Handoff{}, ErrInvalid
	}
	if _, err := h.JSON(); err != nil {
		return Handoff{}, err
	}
	return h, nil
}

type Claim struct {
	mu         sync.Mutex
	repository *journal.Repository
	previous   journal.Snapshot
	next       journal.Envelope
	admitted   *Gate
}
type Gate struct {
	mu             sync.Mutex
	repository     *journal.Repository
	snapshot       journal.Snapshot
	session        journal.Admission
	closed         bool
	pendingHandoff bool
	pendingReady   *journal.Envelope
	readyPrevious  journal.Snapshot
	handoff        *Handoff
	pendingCommit  *journal.Envelope
	commitPrevious journal.Snapshot
}

func BeginInitial(ctx context.Context, c Config) (*Claim, error) {
	if c.Mode != LocalCleanHandoff || c.Repository == nil {
		return nil, ErrInvalid
	}
	s, err := c.Repository.Load(ctx)
	if err != nil {
		return nil, safe(err)
	}
	e := s.Envelope
	if e.Sequence != 1 || e.Admission != nil || len(e.Attempts) != 0 || len(e.Writes) != 0 || e.Projection != nil || len(e.Archive) != 0 {
		return nil, ErrChanged
	}
	id, err := randomID()
	if err != nil {
		return nil, ErrInvalid
	}
	n := copyEnvelope(e)
	n.Admission = &journal.Admission{Generation: 1, SessionID: id, State: journal.AdmissionActive}
	return &Claim{repository: c.Repository, previous: s, next: n}, nil
}

func BeginResume(ctx context.Context, c Config, h Handoff) (*Claim, error) {
	if c.Mode != LocalCleanHandoff || c.Repository == nil {
		return nil, ErrInvalid
	}
	if _, err := h.JSON(); err != nil {
		return nil, err
	}
	s, err := c.Repository.Load(ctx)
	if err != nil {
		return nil, safe(err)
	}
	v := h.value
	if s.Envelope.Subject != v.Subject || s.Version != v.Version || s.Envelope.Sequence != v.Sequence || s.Envelope.Digest != v.Digest || s.Envelope.Admission == nil || *s.Envelope.Admission != v.Admission {
		return nil, ErrChanged
	}
	if unresolved(s.Envelope) || v.Admission.Generation == ^uint64(0) {
		return nil, ErrBlocked
	}
	id, err := randomID()
	if err != nil {
		return nil, ErrInvalid
	}
	n := copyEnvelope(s.Envelope)
	n.Admission = &journal.Admission{Generation: v.Admission.Generation + 1, SessionID: id, State: journal.AdmissionActive}
	return &Claim{repository: c.Repository, previous: s, next: n}, nil
}

// Confirm is repeatable only for this exact claim. An unknown or conflicting
// claim never returns a Gate; late conditional writes cannot authorize losers.
// The same successful Claim object returns the same Gate, never a second one.
func (c *Claim) Confirm(ctx context.Context) (*Gate, error) {
	if c == nil || c.repository == nil {
		return nil, ErrInvalid
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.admitted != nil {
		return c.admitted, nil
	}
	s, err := c.repository.Commit(ctx, c.previous, c.next)
	if err != nil {
		return nil, safe(err)
	}
	c.admitted = &Gate{repository: c.repository, snapshot: s, session: *s.Envelope.Admission}
	return c.admitted, nil
}

func (g *Gate) current(ctx context.Context) (journal.Snapshot, error) {
	if g.repository == nil {
		return journal.Snapshot{}, ErrInvalid
	}
	if g.closed {
		return journal.Snapshot{}, ErrClosed
	}
	if g.pendingCommit != nil {
		return journal.Snapshot{}, ErrUncertain
	}
	s, err := g.repository.Load(ctx)
	if err != nil {
		return journal.Snapshot{}, safe(err)
	}
	if s.Envelope.Admission == nil || *s.Envelope.Admission != g.session || s.Version != g.snapshot.Version || s.Envelope.Digest != g.snapshot.Envelope.Digest {
		g.closed = true
		return journal.Snapshot{}, ErrChanged
	}
	return s, nil
}
func (g *Gate) Snapshot(ctx context.Context) (journal.Snapshot, error) {
	if g == nil {
		return journal.Snapshot{}, ErrInvalid
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	s, err := g.current(ctx)
	if err != nil {
		return journal.Snapshot{}, err
	}
	s.Envelope = copyEnvelope(s.Envelope)
	return s, nil
}

// Commit retains this session's admission. Runtime intent/result changes still
// obey journal transitions. It grants no permission to call a provider itself.
func (g *Gate) Commit(ctx context.Context, next journal.Envelope) (journal.Snapshot, error) {
	if g == nil {
		return journal.Snapshot{}, ErrInvalid
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	previous, err := g.current(ctx)
	if err != nil {
		return journal.Snapshot{}, err
	}
	if next.Admission == nil || *next.Admission != g.session {
		return journal.Snapshot{}, ErrInvalid
	}
	s, err := g.repository.Commit(ctx, previous, next)
	if err != nil {
		if errors.Is(err, journal.ErrUnknown) {
			copy := copyEnvelope(next)
			g.pendingCommit = &copy
			g.commitPrevious = previous
		} else {
			g.closed = true
		}
		return journal.Snapshot{}, safe(err)
	}
	g.snapshot = s
	copy := s
	copy.Envelope = copyEnvelope(s.Envelope)
	return copy, nil
}

// RecoverCommit confirms only this Gate's exact interrupted journal mutation.
// No provider dispatch may occur while it is pending. Recovery of a proven
// commit can resume the same still-active admission; it cannot create a handoff.
func (g *Gate) RecoverCommit(ctx context.Context) (journal.Snapshot, error) {
	if g == nil {
		return journal.Snapshot{}, ErrInvalid
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.pendingCommit == nil || (g.closed && !g.pendingHandoff) {
		return journal.Snapshot{}, ErrInvalid
	}
	s, err := g.repository.Commit(ctx, g.commitPrevious, *g.pendingCommit)
	if err != nil {
		return journal.Snapshot{}, safe(err)
	}
	if s.Envelope.Admission == nil || *s.Envelope.Admission != g.session {
		g.closed = true
		return journal.Snapshot{}, ErrChanged
	}
	g.snapshot = s
	g.pendingCommit = nil
	copy := s
	copy.Envelope = copyEnvelope(s.Envelope)
	return copy, nil
}

// Ready is called only AFTER actual provider writers have drained. It closes
// this Gate before CAS; failure never reopens it. A lost acknowledgement keeps
// the exact proposal for RecoverReady instead of inventing a new handoff.
func (g *Gate) Ready(ctx context.Context) (Handoff, error) {
	if g == nil {
		return Handoff{}, ErrInvalid
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	s, err := g.current(ctx)
	g.closed = true
	if err != nil {
		// Closing after an ambiguous intent/result CAS must retain a path to
		// acknowledge that exact proposal. Recovery cannot reopen this Gate.
		if (errors.Is(err, ErrUncertain) && g.pendingCommit != nil) || errors.Is(err, ErrBlocked) {
			g.pendingHandoff = true
		}
		return Handoff{}, err
	}
	return g.prepareReady(ctx, s)
}
func (g *Gate) prepareReady(ctx context.Context, s journal.Snapshot) (Handoff, error) {
	if unresolved(s.Envelope) {
		return Handoff{}, ErrBlocked
	}
	id, err := randomID()
	if err != nil {
		return Handoff{}, ErrInvalid
	}
	n := copyEnvelope(s.Envelope)
	a := g.session
	a.State = journal.AdmissionReady
	a.HandoffID = id
	n.Admission = &a
	g.readyPrevious = s
	g.pendingReady = &n
	g.pendingHandoff = false
	return g.confirmReady(ctx)
}
func (g *Gate) RecoverReady(ctx context.Context) (Handoff, error) {
	if g == nil {
		return Handoff{}, ErrInvalid
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.closed || (g.pendingReady == nil && !g.pendingHandoff) {
		return Handoff{}, ErrInvalid
	}
	if g.pendingCommit != nil {
		return Handoff{}, ErrUncertain
	}
	if g.pendingReady == nil {
		return g.prepareReady(ctx, g.snapshot)
	}
	return g.confirmReady(ctx)
}
func (g *Gate) confirmReady(ctx context.Context) (Handoff, error) {
	if g.handoff != nil {
		return *g.handoff, nil
	}
	s, err := g.repository.Commit(ctx, g.readyPrevious, *g.pendingReady)
	if err != nil {
		return Handoff{}, safe(err)
	}
	h := Handoff{handoffValue{Schema: 1, Subject: s.Envelope.Subject, Version: s.Version, Sequence: s.Envelope.Sequence, Digest: s.Envelope.Digest, Admission: *s.Envelope.Admission}}
	if _, err = h.JSON(); err != nil {
		return Handoff{}, err
	}
	g.handoff = &h
	g.snapshot = s
	return h, nil
}

func randomID() (string, error) {
	var bytes [32]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes[:]), nil
}
func unresolved(e journal.Envelope) bool {
	for _, w := range e.Writes {
		if w.State == journal.Pending || w.State == journal.Unknown {
			return true
		}
	}
	return false
}
func copyEnvelope(e journal.Envelope) journal.Envelope {
	data, _ := json.Marshal(e)
	var copy journal.Envelope
	_ = json.Unmarshal(data, &copy)
	return copy
}
func safe(err error) error {
	if errors.Is(err, journal.ErrUnknown) {
		return ErrUncertain
	}
	if errors.Is(err, journal.ErrConflict) || errors.Is(err, journal.ErrMissing) {
		return ErrChanged
	}
	if errors.Is(err, journal.ErrInvalid) {
		return ErrInvalid
	}
	return ErrBlocked
}
