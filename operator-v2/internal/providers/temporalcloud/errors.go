package temporalcloud

import (
	"context"
	"errors"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ErrorCode string

const (
	InvalidConfiguration              ErrorCode = "TemporalCloudInvalidConfiguration"
	InvalidRequest                    ErrorCode = "TemporalCloudInvalidRequest"
	CapabilityDisabled                ErrorCode = "TemporalCloudCapabilityDisabled"
	OwnershipConflict                 ErrorCode = "TemporalCloudOwnershipConflict"
	StaleSnapshot                     ErrorCode = "TemporalCloudStaleSnapshot"
	AuthenticationFailed              ErrorCode = "TemporalCloudAuthenticationFailed"
	PermissionDenied                  ErrorCode = "TemporalCloudPermissionDenied"
	ProviderUnavailable               ErrorCode = "TemporalCloudProviderUnavailable"
	ProviderThrottled                 ErrorCode = "TemporalCloudProviderThrottled"
	ProviderRejected                  ErrorCode = "TemporalCloudProviderRejected"
	ProviderResponseInvalid           ErrorCode = "TemporalCloudProviderResponseInvalid"
	NamespaceMissing                  ErrorCode = "TemporalCloudNamespaceMissing"
	DeleteProtectionEnabled           ErrorCode = "TemporalCloudDeleteProtectionEnabled"
	SearchAttributeRemovalUnsupported ErrorCode = "TemporalCloudSearchAttributeRemovalUnsupported"
	SearchAttributeTypeConflict       ErrorCode = "TemporalCloudSearchAttributeTypeConflict"
	OperationPending                  ErrorCode = "TemporalCloudOperationPending"
	OperationFailed                   ErrorCode = "TemporalCloudOperationFailed"
	UnprovenCreateOutcome             ErrorCode = "TemporalCloudUnprovenCreateOutcome"
	WriterGuardFailed                 ErrorCode = "TemporalCloudWriterGuardFailed"
	RequestCancelled                  ErrorCode = "TemporalCloudRequestCancelled"
)

// Error never retains raw provider messages/causes: they can contain credentials
// or echoed payloads. Receipt enables exact unknown-outcome recovery.
type Error struct {
	Code             ErrorCode
	Retryable        bool
	ProviderCode     string
	Receipt          Receipt
	RecoveryRequired bool
}

func (e *Error) Error() string { return string(e.Code) }

func (e *Error) Diagnostic(id, step string, at time.Time) diagnostics.DomainFailure {
	category := diagnostics.Configuration
	switch e.Code {
	case AuthenticationFailed, PermissionDenied:
		category = diagnostics.Authentication
	case ProviderUnavailable, ProviderThrottled, OperationPending:
		category = diagnostics.Transient
	case InvalidRequest, SearchAttributeRemovalUnsupported, SearchAttributeTypeConflict:
		category = diagnostics.Validation
	case OwnershipConflict, StaleSnapshot, UnprovenCreateOutcome:
		category = diagnostics.Configuration
	case OperationFailed, ProviderRejected, ProviderResponseInvalid:
		category = diagnostics.Unknown
	}
	message := "Temporal Cloud operation did not complete."
	next := "Inspect the operation code and verified namespace binding."
	if e.RecoveryRequired {
		message = "Temporal Cloud may have accepted the request before the response was lost."
		next = "Poll the recorded operation receipt before planning another mutation."
	}
	if e.Code == SearchAttributeRemovalUnsupported {
		message = "Search attribute removal is not supported by this adapter's verified Cloud API contract."
		next = "Retain the attribute and resolve the explicit API compatibility decision."
	}
	return diagnostics.SafeFailure(diagnostics.DomainFailure{ID: id, Code: string(e.Code), Category: category, Message: message, Step: step, OccurredAt: at,
		Provider: diagnostics.ProviderRef{Service: "temporal-cloud", Code: e.ProviderCode, RequestID: e.Receipt.ID}, NextAction: next})
}

func fail(code ErrorCode) *Error { return &Error{Code: code} }

func providerError(err error, receipt Receipt, write bool) *Error {
	if typed := new(Error); errors.As(err, &typed) {
		return typed
	}
	code := status.Code(err)
	result := &Error{Code: ProviderRejected, ProviderCode: code.String(), Receipt: receipt}
	if errors.Is(err, context.Canceled) {
		result.Code = RequestCancelled
		result.RecoveryRequired = write
		return result
	}
	switch code {
	case codes.Unauthenticated:
		result.Code = AuthenticationFailed
	case codes.PermissionDenied:
		result.Code = PermissionDenied
	case codes.NotFound:
		result.Code = NamespaceMissing
	case codes.AlreadyExists:
		result.Code = OwnershipConflict
	case codes.Aborted, codes.FailedPrecondition:
		result.Code = StaleSnapshot
	case codes.InvalidArgument, codes.OutOfRange:
		result.Code = InvalidRequest
	case codes.ResourceExhausted:
		result.Code = ProviderThrottled
		result.Retryable = true
	case codes.Unavailable, codes.DeadlineExceeded, codes.Internal, codes.Unknown:
		result.Code = ProviderUnavailable
		result.Retryable = true
	case codes.Canceled:
		result.Code = RequestCancelled
	}
	result.RecoveryRequired = write && (result.Retryable || code == codes.Canceled)
	return result
}
