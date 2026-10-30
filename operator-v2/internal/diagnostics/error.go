package diagnostics

// FailureError is a producer-sanitized error for a capability boundary. It does
// not retain an unsafe raw exception or an automatically serializable cause.
type FailureError struct{ failure DomainFailure }

func NewError(failure DomainFailure, sensitiveValues ...string) *FailureError {
	return &FailureError{failure: SafeFailure(failure, sensitiveValues...)}
}

func (e *FailureError) Error() string             { return e.failure.Message }
func (e *FailureError) Diagnostic() DomainFailure { return SafeFailure(e.failure) }
