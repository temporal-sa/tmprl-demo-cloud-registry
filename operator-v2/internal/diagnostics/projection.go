package diagnostics

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrInvalidEvent      = errors.New("invalid diagnostic event")
	ErrIdentityMismatch  = errors.New("diagnostic operation identity mismatch")
	ErrStaleEvent        = errors.New("diagnostic event is stale or out of order")
	ErrFuturePublication = errors.New("publication refers to an unobserved sequence")
)

func validateIdentity(v Identity) error {
	for _, value := range []string{v.Environment, v.Installation, v.ProjectUID, v.ProjectName, v.OperationID, v.ReconcileKey} {
		if strings.TrimSpace(value) == "" || len(value) > 128 {
			return fmt.Errorf("%w: identity fields must be nonempty and at most 128 bytes", ErrInvalidEvent)
		}
		for _, c := range value {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("-_.:/@+", c)) {
				return fmt.Errorf("%w: identity must use stable ASCII identifiers", ErrInvalidEvent)
			}
		}
	}
	if v.Generation < 0 {
		return fmt.Errorf("%w: negative generation", ErrInvalidEvent)
	}
	return nil
}

func NewProjection(identity Identity) (OperationProjection, error) {
	if err := validateIdentity(identity); err != nil {
		return OperationProjection{}, err
	}
	return OperationProjection{SchemaVersion: SchemaVersion, Identity: identity, State: Pending, Retry: RetryInfo{Status: RetryUnknown}, Availability: Availability{RouteState: RouteUnknown}}, nil
}

func validState(state State) bool {
	switch state {
	case Pending, Running, Waiting, Retrying, Succeeded, Failed, Cancelled, Finalizing, Finalized:
		return true
	}
	return false
}

// Apply folds an observation without executing any provider action. Stale events
// are explicit errors; callers may acknowledge duplicates without mutating state.
// A projection never takes ownership of an event's slices or pointed values.
func (p OperationProjection) Apply(event OperationEvent) (OperationProjection, error) {
	if event.SchemaVersion != 0 && event.SchemaVersion != SchemaVersion {
		return p, fmt.Errorf("%w: unsupported schema version", ErrInvalidEvent)
	}
	if err := validateIdentity(event.Identity); err != nil {
		return p, err
	}
	if event.Identity != p.Identity {
		return p, ErrIdentityMismatch
	}
	if event.Sequence <= p.Sequence || event.OccurredAt.Before(p.ProgressedAt) {
		return p, ErrStaleEvent
	}
	if event.OccurredAt.IsZero() || !validState(event.State) {
		return p, fmt.Errorf("%w: state and observation time are required", ErrInvalidEvent)
	}
	p = safeProjection(p)
	event = SafeEvent(event)
	p.Sequence, p.ProgressedAt, p.State, p.Step = event.Sequence, event.OccurredAt, event.State, event.Step
	if p.Failure == nil && event.Failure != nil {
		p.Failure = event.Failure
	}
	p.Retry = RetryInfo{Status: RetryUnknown}
	if event.Retry != nil {
		p.Retry = *event.Retry
	}
	if event.Availability != nil {
		p.Availability = *event.Availability
	}
	p.Recovery = mergeRecovery(p.Recovery, event.Recovery)
	p.Evidence = mergeEvidence(p.Evidence, event.Evidence)
	p.Warnings = append(p.Warnings, event.Warnings...)
	p.Partial = p.Partial || event.Partial
	return safeProjection(p), nil
}

func mergeRecovery(previous, next []RecoveryOutcome) []RecoveryOutcome {
	for _, value := range next {
		found := false
		for i := range previous {
			if previous[i].ID == value.ID {
				previous[i] = value
				found = true
				break
			}
		}
		if !found {
			previous = append(previous, value)
		}
	}
	return previous
}

func mergeEvidence(previous, next []EvidenceRef) []EvidenceRef {
	for _, value := range next {
		found := false
		for i := range previous {
			if previous[i].ID == value.ID {
				previous[i] = value
				found = true
				break
			}
		}
		if !found {
			previous = append(previous, value)
		}
	}
	return previous
}

// WithPublication repairs delivery state only. It cannot modify the operation's
// event sequence, root failure, retry policy or resource availability, and has no
// callback through which provisioning could accidentally be repeated.
func (p OperationProjection) WithPublication(report SinkHealth) (OperationProjection, error) {
	if report.SourceSequence > p.Sequence {
		return p, ErrFuturePublication
	}
	if report.Sink == "" || report.ObservedAt.IsZero() || (report.State != DeliveryPending && report.State != DeliverySucceeded && report.State != DeliveryFailed && report.State != DeliveryUnknown) {
		return p, fmt.Errorf("%w: invalid publication report", ErrInvalidEvent)
	}
	p = safeProjection(p)
	s := newSanitizer(nil)
	report.Sink = s.text(report.Sink, 64)
	if report.Failure != nil {
		f := s.failure(*report.Failure)
		report.Failure = &f
	}
	if report.State == DeliverySucceeded {
		report.Failure = nil
	}
	for i, existing := range p.Publication {
		if existing.Sink == report.Sink {
			if report.SourceSequence < existing.SourceSequence || report.ObservedAt.Before(existing.ObservedAt) || report.SourceSequence == existing.SourceSequence && existing.State == DeliverySucceeded && report.State != DeliverySucceeded {
				return p, ErrStaleEvent
			}
			p.Publication[i] = report
			return safeProjection(p), nil
		}
	}
	// A bounded sink table cannot silently drop a new delivery report.
	if len(p.Publication) >= 4 {
		return p, fmt.Errorf("%w: publication sink capacity reached", ErrInvalidEvent)
	}
	p.Publication = append(p.Publication, report)
	return safeProjection(p), nil
}

func terminal(state State) bool {
	return state == Succeeded || state == Failed || state == Cancelled || state == Finalized
}

// AssessFreshness compares desired intent with actual operation progress.
// Projection delivery time is deliberately not an input. Terminal observations
// remain current for matching intent; old terminal failures do not imply liveness.
func (p OperationProjection) AssessFreshness(desired DesiredState, now time.Time, maxProgressAge time.Duration) Freshness {
	f := Freshness{State: FreshnessUnknown, ObservedReconcileKey: p.Identity.ReconcileKey, DesiredReconcileKey: Redact(desired.ReconcileKey), ObservedGeneration: p.Identity.Generation, DesiredGeneration: desired.Generation, LastProgressAt: p.ProgressedAt, EvaluatedAt: now}
	if p.Sequence == 0 || p.ProgressedAt.IsZero() || now.IsZero() || p.ProgressedAt.After(now) || desired.ReconcileKey == "" || desired.Generation < p.Identity.Generation {
		return f
	}
	if desired.ReconcileKey != p.Identity.ReconcileKey || desired.Generation != p.Identity.Generation {
		f.State = FreshnessReconciling
		return f
	}
	if !terminal(p.State) && maxProgressAge > 0 && now.Sub(p.ProgressedAt) > maxProgressAge {
		f.State = FreshnessStale
		return f
	}
	f.State = FreshnessCurrent
	return f
}

func safeProjection(value OperationProjection) OperationProjection {
	s := newSanitizer(nil)
	if value.SchemaVersion == 0 {
		value.SchemaVersion = SchemaVersion
	}
	// Ownership identifiers are validated at construction/Apply. They must not
	// be silently shortened while folding observations.
	value.State = State(s.text(string(value.State), 48))
	value.Step = s.text(value.Step, 128)
	if value.Failure != nil {
		f := s.failure(*value.Failure)
		value.Failure = &f
	}
	value.Retry = safeRetry(value.Retry)
	value.Availability = s.availability(value.Availability)
	value.Recovery = s.recovery(value.Recovery)
	value.Evidence = s.evidence(value.Evidence)
	value.Warnings = s.warnings(value.Warnings)
	publication := value.Publication
	if len(publication) > 4 {
		publication = publication[:4]
		s.partial = true
	}
	value.Publication = make([]SinkHealth, len(publication))
	for i, report := range publication {
		report.Sink = s.text(report.Sink, 64)
		if report.Failure != nil {
			f := s.failure(*report.Failure)
			report.Failure = &f
		}
		value.Publication[i] = report
	}
	value.Partial = value.Partial || s.partial
	return value
}

// JSON returns a sanitized bounded wire view. Known sensitive values can be
// supplied for defense in depth, but producers should sanitize with SafeEvent
// before an observation ever reaches durable history or logs.
func (p OperationProjection) JSON(sensitiveValues ...string) ([]byte, error) {
	if p.SchemaVersion != 0 && p.SchemaVersion != SchemaVersion {
		return nil, fmt.Errorf("%w: unsupported projection schema", ErrInvalidEvent)
	}
	if err := validateIdentity(p.Identity); err != nil {
		return nil, err
	}
	p = safeProjection(p)
	s := newSanitizer(sensitiveValues)
	p.Identity = s.identity(p.Identity)
	if p.Failure != nil {
		f := s.failure(*p.Failure)
		p.Failure = &f
	}
	p.Recovery = s.recovery(p.Recovery)
	p.Evidence = s.evidence(p.Evidence)
	p.Warnings = s.warnings(p.Warnings)
	p.Step = s.text(p.Step, 128)
	p.Availability = s.availability(p.Availability)
	for i := range p.Publication {
		if p.Publication[i].Failure != nil {
			f := s.failure(*p.Publication[i].Failure)
			p.Publication[i].Failure = &f
		}
		p.Publication[i].Sink = s.text(p.Publication[i].Sink, 64)
	}
	p.Partial = p.Partial || s.partial
	type raw OperationProjection
	for {
		encoded, err := json.Marshal(raw(p))
		if err != nil || len(encoded) <= MaxJSONBytes {
			return encoded, err
		}
		p.Partial = true
		switch {
		case len(p.Warnings) > 0:
			p.Warnings = p.Warnings[:len(p.Warnings)-1]
		case len(p.Evidence) > 0:
			p.Evidence = p.Evidence[:len(p.Evidence)-1]
		case len(p.Recovery) > 1:
			p.Recovery = removeRecovery(p.Recovery)
		case len(p.Publication) > 1:
			p.Publication = p.Publication[:len(p.Publication)-1]
		default:
			// The sanitized original cause is independently bounded to2KiB.
			// Adding cleanup/publication must never rewrite it after the journal
			// has checked cause immutability. Only secondary summaries shrink.
			for i := range p.Recovery {
				p.Recovery[i].Step = truncate(p.Recovery[i].Step, 32)
				p.Recovery[i].RetainedResources = nil
				if p.Recovery[i].Failure != nil {
					f := tinyFailure(*p.Recovery[i].Failure)
					p.Recovery[i].Failure = &f
				}
			}
			for i := range p.Publication {
				if p.Publication[i].Failure != nil {
					f := tinyFailure(*p.Publication[i].Failure)
					p.Publication[i].Failure = &f
				}
			}
			encoded, err = json.Marshal(raw(p))
			if err != nil {
				return nil, err
			}
			if len(encoded) > MaxJSONBytes {
				return nil, fmt.Errorf("diagnostic envelope exceeds %d bytes after compaction", MaxJSONBytes)
			}
			return encoded, nil
		}
	}
}

func (p OperationProjection) MarshalJSON() ([]byte, error) { return p.JSON() }
