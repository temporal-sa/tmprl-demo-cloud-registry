package diagnostics

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

var (
	privateKey           = regexp.MustCompile(`(?s)-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----`)
	credentialAssignment = regexp.MustCompile(`(?i)(authorization|api[_-]?key|access[_-]?token|refresh[_-]?token|session[_-]?token|token|password|passwd|secret[_-]?access[_-]?key|client[_-]?secret|secret)["']?\s*[:=]\s*(?:"[^"]*"|'[^']*'|(?:(?:bearer|basic)\s+)?[^\s,;"'}]+)`)
	bearer               = regexp.MustCompile(`(?i)\b(?:bearer|basic)\s+[A-Za-z0-9_+/=.-]+`)
	awsKey               = regexp.MustCompile(`\b(?:AKIA|ASIA)[A-Z0-9]{16}\b`)
	apiKey               = regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{8,}`)
	jwt                  = regexp.MustCompile(`\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+`)
	link                 = regexp.MustCompile(`(?:https?|s3)://[^\s<>"']+`)
)

type sanitizer struct {
	secrets []string
	partial bool
}

func newSanitizer(secrets []string) *sanitizer {
	copy := append([]string(nil), secrets...)
	sort.SliceStable(copy, func(i, j int) bool { return len(copy[i]) > len(copy[j]) })
	return &sanitizer{secrets: copy}
}

// Redact removes explicitly supplied sensitive values and common credential
// forms. It cannot identify arbitrary secrets in application logs; callers must
// supply known sensitive values and keep raw logs behind separate authorization.
func Redact(value string, sensitiveValues ...string) string {
	return newSanitizer(sensitiveValues).redact(value)
}

func (s *sanitizer) redact(value string) string {
	for _, secret := range s.secrets {
		if secret != "" {
			value = strings.ReplaceAll(value, secret, "[REDACTED]")
		}
	}
	value = privateKey.ReplaceAllString(value, "[REDACTED PRIVATE KEY]")
	value = credentialAssignment.ReplaceAllString(value, "$1=[REDACTED]")
	value = bearer.ReplaceAllString(value, "[REDACTED AUTHORIZATION]")
	value = awsKey.ReplaceAllString(value, "[REDACTED AWS KEY]")
	value = apiKey.ReplaceAllString(value, "[REDACTED API KEY]")
	value = jwt.ReplaceAllString(value, "[REDACTED TOKEN]")
	return link.ReplaceAllStringFunc(value, func(raw string) string {
		u, err := url.Parse(raw)
		if err != nil {
			return "[REDACTED URL]"
		}
		u.User, u.RawQuery, u.Fragment = nil, "", ""
		u.ForceQuery = false
		return u.String()
	})
}

func (s *sanitizer) text(value string, limit int) string {
	value = s.redact(value)
	if len(value) <= limit {
		return value
	}
	s.partial = true
	return truncate(value, limit)
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	const suffix = "...[truncated]"
	end := limit - len(suffix)
	if end < 0 {
		return suffix[:limit]
	}
	for end > 0 && !utf8.RuneStart(value[end]) {
		end--
	}
	return value[:end] + suffix
}

func (s *sanitizer) identity(value Identity) Identity {
	value.Environment = s.text(value.Environment, 128)
	value.Installation = s.text(value.Installation, 128)
	value.ProjectUID = s.text(value.ProjectUID, 128)
	value.ProjectName = s.text(value.ProjectName, 128)
	value.OperationID = s.text(value.OperationID, 128)
	value.ReconcileKey = s.text(value.ReconcileKey, 128)
	return value
}

func (s *sanitizer) resources(values []ResourceRef) []ResourceRef {
	if len(values) > 4 {
		values = values[:4]
		s.partial = true
	}
	result := make([]ResourceRef, len(values))
	for i, value := range values {
		result[i] = ResourceRef{s.text(value.Provider, 48), s.text(value.Kind, 64), s.text(value.ID, 128)}
	}
	return result
}

func (s *sanitizer) failure(value DomainFailure) DomainFailure {
	value.ID, value.Code = s.text(value.ID, 96), s.text(value.Code, 96)
	value.Category = Category(s.text(string(value.Category), 48))
	if value.Category == "" {
		value.Category = Unknown
	}
	value.Message, value.Step = s.text(value.Message, 768), s.text(value.Step, 96)
	value.NextAction = s.text(value.NextAction, 512)
	causes := value.Causes
	if len(causes) > 4 {
		causes = causes[:4]
		s.partial = true
	}
	value.Causes = make([]Cause, len(causes))
	for i, cause := range causes {
		value.Causes[i] = Cause{s.text(cause.Type, 64), s.text(cause.Message, 192)}
	}
	value.Execution.Namespace = s.text(value.Execution.Namespace, 96)
	value.Execution.WorkflowID = s.text(value.Execution.WorkflowID, 128)
	value.Execution.RunID = s.text(value.Execution.RunID, 96)
	value.Execution.ActivityID = s.text(value.Execution.ActivityID, 96)
	value.Execution.ActivityType = s.text(value.Execution.ActivityType, 96)
	value.Provider.Service = s.text(value.Provider.Service, 64)
	value.Provider.Code = s.text(value.Provider.Code, 96)
	value.Provider.RequestID = s.text(value.Provider.RequestID, 128)
	value.Resources = s.resources(value.Resources)
	ids := value.EvidenceIDs
	if len(ids) > 4 {
		ids = ids[:4]
		s.partial = true
	}
	value.EvidenceIDs = make([]string, len(ids))
	for i, id := range ids {
		value.EvidenceIDs[i] = s.text(id, 96)
	}
	value.Partial = value.Partial || s.partial
	return boundFailure(value)
}

// SafeFailure returns an independent sanitized copy, ready for persistence.
func SafeFailure(value DomainFailure, sensitiveValues ...string) DomainFailure {
	return newSanitizer(sensitiveValues).failure(value)
}

func compactFailure(value DomainFailure) DomainFailure {
	value.Causes, value.Resources, value.EvidenceIDs = nil, nil, nil
	value.Message, value.NextAction = truncate(value.Message, 192), truncate(value.NextAction, 128)
	value.Partial = true
	return value
}

func boundFailure(value DomainFailure) DomainFailure {
	type raw DomainFailure
	encoded, _ := json.Marshal(raw(value))
	if len(encoded) <= MaxFailureBytes {
		return value
	}
	value = compactFailure(value)
	encoded, _ = json.Marshal(raw(value))
	if len(encoded) <= MaxFailureBytes {
		return value
	}
	// A small fallback also bounds pathological strings that expand sixfold
	// under JSON escaping. Its partial marker directs readers to source evidence.
	return DomainFailure{ID: truncate(value.ID, 32), Code: truncate(value.Code, 32), Category: Unknown, Message: truncate(value.Message, 64), OccurredAt: value.OccurredAt, Partial: true}
}

func (value DomainFailure) MarshalJSON() ([]byte, error) {
	type raw DomainFailure
	return json.Marshal(raw(SafeFailure(value)))
}

func safeRetry(value RetryInfo) RetryInfo {
	if !value.Authoritative || value.ObservedAt.IsZero() || (value.Status != RetryScheduled && value.Status != RetryExhausted && value.Status != RetryNotRetryable) {
		return RetryInfo{Status: RetryUnknown}
	}
	if value.Attempt < 0 {
		value.Attempt = 0
	}
	if value.MaximumAttempts < 0 {
		value.MaximumAttempts = 0
	}
	if value.Status != RetryScheduled {
		value.NextAttemptAt = nil
	}
	if value.NextAttemptAt != nil {
		t := *value.NextAttemptAt
		if t.Before(value.ObservedAt) {
			value.NextAttemptAt = nil
		} else {
			value.NextAttemptAt = &t
		}
	}
	return value
}

func (s *sanitizer) availability(value Availability) Availability {
	value.ActiveRevision = s.text(value.ActiveRevision, 128)
	value.CandidateRevision = s.text(value.CandidateRevision, 128)
	// Unconfirmed explicitly records the absence of valid current confirmation;
	// it does not need a fabricated confirmation timestamp or active revision.
	// Positive serving claims require both pieces of independent evidence.
	if (value.RouteState != RouteActivePreserved && value.RouteState != RoutePromoted && value.RouteState != RouteUnconfirmed) || ((value.RouteState == RouteActivePreserved || value.RouteState == RoutePromoted) && (value.ConfirmedAt.IsZero() || value.ActiveRevision == "")) {
		value.RouteState = RouteUnknown
	}
	return value
}

func (s *sanitizer) recovery(values []RecoveryOutcome) []RecoveryOutcome {
	if len(values) > 8 {
		values = values[:8]
		s.partial = true
	}
	result := make([]RecoveryOutcome, len(values))
	for i, value := range values {
		value.ID, value.Step = s.text(value.ID, 96), s.text(value.Step, 128)
		value.State = OutcomeState(s.text(string(value.State), 48))
		value.RetainedResources = s.resources(value.RetainedResources)
		if value.Failure != nil {
			f := s.failure(*value.Failure)
			value.Failure = &f
		}
		result[i] = value
	}
	return result
}

func (s *sanitizer) evidence(values []EvidenceRef) []EvidenceRef {
	if len(values) > 8 {
		values = values[:8]
		s.partial = true
	}
	result := make([]EvidenceRef, len(values))
	for i, value := range values {
		value.ID, value.Kind = s.text(value.ID, 96), s.text(value.Kind, 48)
		value.Locator, value.State = s.text(value.Locator, 512), s.text(value.State, 48)
		result[i] = value
	}
	return result
}

func (s *sanitizer) warnings(values []DomainFailure) []DomainFailure {
	if len(values) > 4 {
		values = values[:4]
		s.partial = true
	}
	result := make([]DomainFailure, len(values))
	for i, value := range values {
		result[i] = s.failure(value)
	}
	return result
}

// SafeEvent returns a bounded independent copy and redacts before a producer
// writes the event to Temporal history, a log, or another durable destination.
func SafeEvent(value OperationEvent, sensitiveValues ...string) OperationEvent {
	s := newSanitizer(sensitiveValues)
	if value.SchemaVersion == 0 {
		value.SchemaVersion = SchemaVersion
	}
	value.Identity = s.identity(value.Identity)
	value.State = State(s.text(string(value.State), 48))
	value.Step = s.text(value.Step, 128)
	if value.Failure != nil {
		f := s.failure(*value.Failure)
		value.Failure = &f
	}
	if value.Retry != nil {
		r := safeRetry(*value.Retry)
		if r.ObservedAt.After(value.OccurredAt) {
			r = RetryInfo{Status: RetryUnknown}
		}
		value.Retry = &r
	}
	if value.Availability != nil {
		a := s.availability(*value.Availability)
		if a.ConfirmedAt.After(value.OccurredAt) {
			a.RouteState = RouteUnknown
		}
		value.Availability = &a
	}
	value.Recovery = s.recovery(value.Recovery)
	value.Evidence = s.evidence(value.Evidence)
	value.Warnings = s.warnings(value.Warnings)
	value.Partial = value.Partial || s.partial
	return boundEvent(value)
}

func boundEvent(value OperationEvent) OperationEvent {
	type raw OperationEvent
	for {
		encoded, _ := json.Marshal(raw(value))
		if len(encoded) <= MaxJSONBytes {
			return value
		}
		value.Partial = true
		switch {
		case len(value.Warnings) > 0:
			value.Warnings = value.Warnings[:len(value.Warnings)-1]
		case len(value.Evidence) > 0:
			value.Evidence = value.Evidence[:len(value.Evidence)-1]
		case len(value.Recovery) > 1:
			value.Recovery = removeRecovery(value.Recovery)
		default:
			if value.Failure != nil {
				f := compactFailure(*value.Failure)
				value.Failure = &f
			}
			for i := range value.Recovery {
				if value.Recovery[i].Failure != nil {
					f := compactFailure(*value.Recovery[i].Failure)
					value.Recovery[i].Failure = &f
				}
				value.Recovery[i].RetainedResources = nil
			}
			// With one compact recovery and the field bounds above, the envelope
			// is below 8 KiB even when every character needs JSON escaping.
			value.Step = truncate(value.Step, 64)
			return minimalEvent(value)
		}
	}
}

func minimalEvent(value OperationEvent) OperationEvent {
	type raw OperationEvent
	encoded, _ := json.Marshal(raw(value))
	if len(encoded) <= MaxJSONBytes {
		return value
	}
	// Worst-case escaping is handled by shortening every remaining string. Root
	// cause identity/code and ownership are retained; omitted detail is explicit.
	if value.Failure != nil {
		f := tinyFailure(*value.Failure)
		value.Failure = &f
	}
	for i := range value.Recovery {
		value.Recovery[i].ID = truncate(value.Recovery[i].ID, 32)
		value.Recovery[i].Step = truncate(value.Recovery[i].Step, 32)
		if value.Recovery[i].Failure != nil {
			f := tinyFailure(*value.Recovery[i].Failure)
			value.Recovery[i].Failure = &f
		}
	}
	return value
}

func tinyFailure(v DomainFailure) DomainFailure {
	v = compactFailure(v)
	v.ID = truncate(v.ID, 32)
	v.Code = truncate(v.Code, 48)
	v.Category = Category(truncate(string(v.Category), 32))
	v.Message = truncate(v.Message, 96)
	v.Step = truncate(v.Step, 32)
	v.NextAction = truncate(v.NextAction, 64)
	v.Execution = ExecutionRef{ActivityID: truncate(v.Execution.ActivityID, 32), ScheduledEventID: v.Execution.ScheduledEventID}
	v.Provider = ProviderRef{Service: truncate(v.Provider.Service, 32), Code: truncate(v.Provider.Code, 32)}
	return boundFailure(v)
}

func removeRecovery(values []RecoveryOutcome) []RecoveryOutcome {
	for i, value := range values {
		if value.State == OutcomeCompleted {
			return append(values[:i:i], values[i+1:]...)
		}
	}
	return values[:len(values)-1]
}

func (value OperationEvent) MarshalJSON() ([]byte, error) {
	type raw OperationEvent
	if value.SchemaVersion != 0 && value.SchemaVersion != SchemaVersion {
		return nil, fmt.Errorf("%w: unsupported event schema", ErrInvalidEvent)
	}
	if err := validateIdentity(value.Identity); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(raw(SafeEvent(value)))
	if err == nil && len(encoded) > MaxJSONBytes {
		return nil, fmt.Errorf("diagnostic event exceeds %d bytes", MaxJSONBytes)
	}
	return encoded, err
}
