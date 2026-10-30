// Package simulation supplies explicitly fake providers and an in-memory
// journal for local contract demonstrations. It proves neither real Kubernetes
// semantics nor durable storage, Temporal replay, IAM or account isolation.
package simulation

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/engine"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
)

type Scenario string

const (
	Success          Scenario = "success"
	ReadinessFailure Scenario = "readiness-failure"
	SmokeFailure     Scenario = "smoke-failure"
	EvidenceFailure  Scenario = "evidence-failure"
	CleanupFailure   Scenario = "cleanup-failure"
)

var ErrConflict = diagnostics.NewError(diagnostics.DomainFailure{ID: "conditional-write-conflict", Code: "ConditionalWriteConflict", Category: diagnostics.Configuration, Message: "The independent provider observation changed; its conditional write was rejected.", NextAction: "Re-read the current object owner, UID and version before retrying."})
var fixedTime = time.Date(2026, 10, 29, 14, 32, 0, 0, time.UTC)

type resource struct{ observation ownership.Observation }
type outcomeRecord struct {
	revision, reconcileKey string
	operation              string
	outcome                engine.CandidateOutcome
}

type World struct {
	clock              atomic.Int64
	mu                 sync.Mutex
	owner              ownership.Context
	writer             ownership.Writer
	scenario           Scenario
	candidate          *resource
	candidates         map[string]*resource
	currentOperation   string
	settledByOperation map[string]*rollout.FailureOutcome
	failureByOperation map[string]*diagnostics.DomainFailure
	supersessions      map[string]engine.SupersessionOutcome
	unknownWrites      map[string]bool
	attemptBindings    map[string]rollout.Desired
	drainingOperations map[string]bool
	route              *resource
	active             *rollout.Revision
	version            uint64
	records            map[string]ownership.Record
	intents            map[string]ownership.Intent
	mutations          map[string]engine.MutationIntent
	results            map[string]outcomeRecord
	settled            *rollout.FailureOutcome
	failure            *diagnostics.DomainFailure
	trace              []string
	// Fault switches are configured before use. Hooks deliberately expose a race
	// seam for tests; mutation methods still lock and check exact UID/version.
	BeforePublish       func(*World)
	FailSaveIntent      bool
	FailFailureOutcome  bool
	FailNextRecord      bool
	FailSupersessionAck bool
}

func New(scenario Scenario, withActive bool) (*World, error) {
	switch scenario {
	case Success, ReadinessFailure, SmokeFailure, EvidenceFailure, CleanupFailure:
	default:
		return nil, fmt.Errorf("unknown local simulation scenario")
	}
	owner, err := ownership.NewContext(ownership.Config{Owner: ownership.Owner{EnvironmentID: "local", InstallationID: "demo", ProjectUID: "banking", Lifetime: ownership.Disposable, RunID: "local-run"}, ResourceRoot: "rgtest", Scopes: []ownership.Scope{{Provider: ownership.Kubernetes, ClusterID: "fake-cluster", Namespace: "simulation"}}})
	if err != nil {
		return nil, err
	}
	writer := ownership.Writer{ID: "local-writer", Owner: owner.Owner(), Epoch: 1, ExpiresAt: fixedTime.Add(time.Hour)}
	w := &World{owner: owner, writer: writer, scenario: scenario, version: 1, records: map[string]ownership.Record{}, intents: map[string]ownership.Intent{}, mutations: map[string]engine.MutationIntent{}, results: map[string]outcomeRecord{}, candidates: map[string]*resource{}, settledByOperation: map[string]*rollout.FailureOutcome{}, failureByOperation: map[string]*diagnostics.DomainFailure{}, supersessions: map[string]engine.SupersessionOutcome{}, unknownWrites: map[string]bool{}, attemptBindings: map[string]rollout.Desired{}}
	if withActive {
		intent, err := owner.Intent(ownership.Kubernetes, "route", "primary", "git-aaaaaaa", "seed-old", "sha-old", 63)
		if err != nil {
			return nil, err
		}
		observed := observation(intent, "route-uid", "1", writer.Epoch)
		record, err := owner.BindCreated(intent, observed, writer, fixedTime)
		if err != nil {
			return nil, err
		}
		w.route = &resource{observed}
		w.records[observed.Ref.ProviderID] = record
		w.active = &rollout.Revision{ID: "git-aaaaaaa", ReconcileKey: "sha-old", ResourceID: "active-uid", ResourceVersion: "1"}
	}
	return w, nil
}

func (w *World) Context() ownership.Context { return w.owner }
func (w *World) Writer() ownership.Writer   { w.mu.Lock(); defer w.mu.Unlock(); return w.writer }
func (w *World) Now() time.Time {
	if value := w.clock.Load(); value != 0 {
		return time.Unix(0, value).UTC()
	}
	return fixedTime
}

// SetTime explicitly advances the fake observation clock and fake lease. This
// is a test capability, never evidence of real writer-lease renewal authority.
func (w *World) SetTime(at time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.clock.Store(at.UnixNano())
	w.writer.ExpiresAt = at.Add(time.Hour)
}
func (w *World) Capabilities() engine.Capabilities {
	return engine.Capabilities{Observer: w, Candidates: w, Routes: w, Evidence: w, Journal: w, Now: w.Now}
}
func (w *World) Trace() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.trace...)
}

func Request() engine.Request {
	return engine.Request{ProjectName: "banking", OperationID: "rollout-local", Desired: rollout.Desired{Subject: rollout.Subject{Environment: "local", Installation: "demo", ProjectUID: "banking"}, Generation: 2, ReconcileKey: "sha-new", Revision: "git-bbbbbbb", RequiredBuilds: []string{"worker"}, Images: map[string]string{"worker": "example.invalid/banking/worker:git-bbbbbbb"}, RequireSmoke: true}}
}

func observation(intent ownership.Intent, id, version string, epoch uint64) ownership.Observation {
	return ownership.Observation{Ref: ownership.ResourceRef{Scope: intent.Scope, Purpose: intent.Purpose, Name: intent.Name, ProviderID: id}, Owner: intent.Owner, CreationIdempotencyKey: intent.IdempotencyKey, Revision: intent.Revision, SpecFingerprint: intent.SpecFingerprint, WriterEpoch: epoch, Version: version}
}

func (w *World) token() string { return fmt.Sprint(w.version) }
func (w *World) owned(r *resource) *engine.Owned {
	if r == nil {
		return nil
	}
	o := &engine.Owned{Observation: r.observation}
	if record, ok := w.records[r.observation.Ref.ProviderID]; ok {
		copy := record
		o.Record = &copy
	}
	return o
}

func (w *World) Snapshot(_ context.Context, request engine.Request) (engine.Snapshot, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	desired := request.Desired
	if request.OperationID != w.currentOperation && w.currentOperation != "" {
		return engine.Snapshot{}, ErrConflict
	}
	w.candidate = w.candidates[request.OperationID]
	w.settled = w.settledByOperation[request.OperationID]
	w.failure = w.failureByOperation[request.OperationID]
	state := rollout.Observed{Subject: desired.Subject, Generation: desired.Generation, ObservationToken: w.token()}
	if w.active != nil {
		copy := *w.active
		state.Active = &copy
	}
	if w.route != nil {
		o := w.route.observation
		state.Route = &rollout.Route{Subject: rollout.Subject{Environment: o.Owner.EnvironmentID, Installation: o.Owner.InstallationID, ProjectUID: o.Owner.ProjectUID}, ResourceID: o.Ref.ProviderID, ResourceVersion: o.Version, Revision: o.Revision, ConfirmedAt: w.Now()}
	}
	if w.candidate != nil {
		o := w.candidate.observation
		c := &rollout.Candidate{Revision: rollout.Revision{ID: o.Revision, ReconcileKey: o.SpecFingerprint, ResourceID: o.Ref.ProviderID, ResourceVersion: o.Version}, Evidence: rollout.EvidencePending}
		for _, record := range w.results {
			if record.operation != request.OperationID || record.revision != desired.Revision || record.reconcileKey != desired.ReconcileKey {
				continue
			}
			outcome := record.outcome
			if outcome.Readiness != nil {
				c.Ready = outcome.Readiness.Ready
				c.FailureCode = outcome.Readiness.FailureCode
			}
			if outcome.Smoke != nil {
				copy := *outcome.Smoke
				c.Smoke = &copy
			}
			if outcome.Evidence != nil {
				c.Evidence = *outcome.Evidence
			}
			c.CleanupFailure = c.CleanupFailure || outcome.CleanupFailed
		}
		state.Candidate = c
	}
	if w.settled != nil {
		copy := *w.settled
		state.SettledFailure = &copy
	}
	var failure *diagnostics.DomainFailure
	if w.failure != nil && w.settled != nil && w.settled.Revision == desired.Revision && w.settled.ReconcileKey == desired.ReconcileKey {
		f := diagnostics.SafeFailure(*w.failure)
		failure = &f
	}
	return engine.Snapshot{State: state, Candidate: w.owned(w.candidate), Route: w.owned(w.route), Failure: failure}, nil
}

func (w *World) SaveIntent(_ context.Context, intent ownership.Intent) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.trace = append(w.trace, "persist-intent:"+intent.Purpose)
	if w.FailSaveIntent {
		return fault("IntentStorageUnavailable", "simulated intent storage unavailable", diagnostics.Publication)
	}
	if w.drainingOperations[intent.OperationID] {
		return ErrConflict
	}
	if intent.Purpose == "candidate" {
		if w.currentOperation == "" {
			w.currentOperation = intent.OperationID
		}
		if w.currentOperation != intent.OperationID {
			return ErrConflict
		}
	}
	if previous, ok := w.intents[intent.IdempotencyKey]; ok && previous != intent {
		return ErrConflict
	}
	w.intents[intent.IdempotencyKey] = intent
	return nil
}
func (w *World) LoadIntent(_ context.Context, key string) (ownership.Intent, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	v, ok := w.intents[key]
	return v, ok, nil
}
func (w *World) SaveRecord(_ context.Context, record ownership.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.trace = append(w.trace, "bind-record:"+record.Ref.Purpose)
	if w.FailNextRecord {
		w.FailNextRecord = false
		return fault("LedgerAcknowledgmentUnavailable", "simulated crash before ledger acknowledgment", diagnostics.Publication)
	}
	if old, ok := w.records[record.Ref.ProviderID]; ok && (old.Owner != record.Owner || old.Ref != record.Ref || old.CreationIdempotencyKey != record.CreationIdempotencyKey) {
		return ErrConflict
	}
	var current *resource
	if w.candidate != nil && w.candidate.observation.Ref.ProviderID == record.Ref.ProviderID {
		current = w.candidate
	}
	if w.route != nil && w.route.observation.Ref.ProviderID == record.Ref.ProviderID {
		current = w.route
	}
	if current == nil {
		return ErrConflict
	}
	if _, err := w.owner.Authorize(ownership.MutationRequest{Action: ownership.Mutate, Record: record, Observed: current.observation, ExpectedRevision: record.Revision, Writer: w.writer, Now: w.Now(), Fencing: ownership.ProviderConditional}); err != nil {
		return err
	}
	w.records[record.Ref.ProviderID] = record
	return nil
}
func mutationKey(operation, purpose string) string { return operation + "/" + purpose }
func (w *World) SaveMutation(_ context.Context, intent engine.MutationIntent) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.drainingOperations[intent.OperationID] {
		return ErrConflict
	}
	w.trace = append(w.trace, "persist-mutation:"+intent.Purpose)
	key := mutationKey(intent.OperationID, intent.Purpose)
	if previous, ok := w.mutations[key]; ok {
		previous.ExpectedVersion = intent.ExpectedVersion
		if previous != intent {
			return ErrConflict
		}
	}
	w.mutations[key] = intent
	return nil
}
func (w *World) LoadMutation(_ context.Context, operation, purpose string) (engine.MutationIntent, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	v, ok := w.mutations[mutationKey(operation, purpose)]
	return v, ok, nil
}

func (w *World) verifyAction(action rollout.Action) error {
	p := action.Preconditions
	if p.ObservationToken != w.token() {
		return ErrConflict
	}
	if p.CandidateResourceID != "" && (w.candidate == nil || w.candidate.observation.Ref.ProviderID != p.CandidateResourceID || w.candidate.observation.Version != p.CandidateResourceVersion) {
		return ErrConflict
	}
	if p.RouteMustBeAbsent {
		if w.route != nil {
			return ErrConflict
		}
	} else if w.route == nil || w.route.observation.Ref.ProviderID != p.RouteResourceID || w.route.observation.Version != p.RouteResourceVersion || w.route.observation.Revision != p.RouteRevision {
		return ErrConflict
	}
	return nil
}

func (w *World) verifyResource(r *resource, supplied ownership.Preconditions, action ownership.Action) error {
	if r == nil {
		return ErrConflict
	}
	record, ok := w.records[r.observation.Ref.ProviderID]
	if !ok {
		return errors.New("fake provider has no trusted ledger record")
	}
	pre, err := w.owner.Authorize(ownership.MutationRequest{Action: action, Record: record, Observed: r.observation, ExpectedRevision: record.Revision, Writer: w.writer, Now: w.Now(), Fencing: ownership.ProviderConditional})
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(pre, supplied) {
		return ErrConflict
	}
	return nil
}

func (w *World) verifyCandidate(desired rollout.Desired, supplied ownership.Preconditions, action ownership.Action) error {
	if w.candidate == nil || w.candidate.observation.Revision != desired.Revision || w.candidate.observation.SpecFingerprint != desired.ReconcileKey {
		return ErrConflict
	}
	return w.verifyResource(w.candidate, supplied, action)
}

func (w *World) Ensure(_ context.Context, desired rollout.Desired, action rollout.Action, permit engine.CreationPermit) (ownership.Observation, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	intent := permit.Preconditions.Intent
	if w.drainingOperations[intent.OperationID] {
		return ownership.Observation{}, ErrConflict
	}
	if intent.Purpose != "candidate" || intent.Revision != desired.Revision || intent.SpecFingerprint != desired.ReconcileKey {
		return ownership.Observation{}, ErrConflict
	}
	pre, err := w.owner.AuthorizeCreate(intent, w.writer, w.Now())
	if err != nil {
		return ownership.Observation{}, err
	}
	if pre != permit.Preconditions {
		return ownership.Observation{}, ErrConflict
	}
	if persisted, ok := w.intents[intent.IdempotencyKey]; !ok || persisted != intent {
		return ownership.Observation{}, errors.New("create preceded durable intent")
	}
	if w.candidate != nil {
		if _, err = w.owner.BindCreated(intent, w.candidate.observation, w.writer, w.Now()); err != nil {
			return ownership.Observation{}, err
		}
		w.trace = append(w.trace, "ensure-existing:candidate")
		return w.candidate.observation, nil
	}
	if err = w.verifyAction(action); err != nil {
		return ownership.Observation{}, err
	}
	w.trace = append(w.trace, "create:candidate")
	w.version++
	w.candidate = &resource{observation(intent, "candidate-uid-"+intent.IdempotencyKey[:12], w.token(), w.writer.Epoch)}
	w.candidates[intent.OperationID] = w.candidate
	return w.candidate.observation, nil
}

func (w *World) Readiness(_ context.Context, desired rollout.Desired, action rollout.Action, permit engine.CandidatePermit) (engine.ReadinessResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.verifyAction(action); err != nil {
		return engine.ReadinessResult{}, err
	}
	if err := w.verifyCandidate(desired, permit.Preconditions, ownership.Mutate); err != nil {
		return engine.ReadinessResult{}, err
	}
	w.trace = append(w.trace, "observe:readiness")
	if w.scenario == ReadinessFailure || w.scenario == EvidenceFailure || w.scenario == CleanupFailure {
		return engine.ReadinessResult{FailureCode: "ImagePullUnauthorized"}, nil
	}
	return engine.ReadinessResult{Ready: true}, nil
}
func (w *World) Smoke(_ context.Context, desired rollout.Desired, action rollout.Action, permit engine.CandidatePermit) (rollout.SmokeResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.verifyAction(action); err != nil {
		return rollout.SmokeResult{}, err
	}
	if err := w.verifyCandidate(desired, permit.Preconditions, ownership.Mutate); err != nil {
		return rollout.SmokeResult{}, err
	}
	w.trace = append(w.trace, "check:smoke")
	if w.scenario == SmokeFailure {
		return rollout.SmokeResult{FailureCode: "SmokeCheckFailed"}, nil
	}
	return rollout.SmokeResult{Passed: true}, nil
}
func (w *World) Capture(_ context.Context, desired rollout.Desired, action rollout.Action, permit engine.CandidatePermit) (diagnostics.EvidenceRef, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.verifyAction(action); err != nil {
		return diagnostics.EvidenceRef{}, err
	}
	if err := w.verifyCandidate(desired, permit.Preconditions, ownership.Mutate); err != nil {
		return diagnostics.EvidenceRef{}, err
	}
	w.trace = append(w.trace, "capture:evidence")
	if w.scenario == EvidenceFailure {
		return diagnostics.EvidenceRef{}, fault("EvidenceUploadDenied", "simulated evidence upload denied", diagnostics.Cleanup)
	}
	return diagnostics.EvidenceRef{ID: "candidate-evidence", Kind: "local-simulation", Locator: "simulation:candidate-evidence", State: "Captured"}, nil
}

func fault(code, message string, category diagnostics.Category) error {
	return diagnostics.NewError(diagnostics.DomainFailure{ID: "local-" + code, Code: code, Category: category, Message: message})
}
