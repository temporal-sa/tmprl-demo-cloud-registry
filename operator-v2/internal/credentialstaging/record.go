package credentialstaging

import (
	"context"
	"errors"
	"reflect"
	"time"

	observation "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/credentialobservation"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	awsstore "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/awssecrets"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/localmetadata"
	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
	"google.golang.org/grpc/metadata"
)

const (
	AuthReceiptDisabled       Error = "CredentialStagingAuthReceiptDisabled"
	AuthReceiptOutcomeUnknown Error = "CredentialStagingAuthReceiptOutcomeUnknown"
	AuthReceiptRejected       Error = "CredentialStagingAuthReceiptRejected"
	AuthReceiptCapacity       Error = "CredentialStagingAuthReceiptCapacity"
	AuthReceiptPolicyContract       = "temporal-candidate-auth-observation-policy-binding-v1"
	// A bounded opportunity, never a promise that filesystem I/O completes in1s.
	AuthReceiptPersistenceReserve = time.Second
)

type RecordObservationRequest struct {
	ObserveRequest
	AttemptID string
}
type ReceiptAcknowledgement string

const (
	ReceiptNotAttempted ReceiptAcknowledgement = "NotAttempted"
	ReceiptRecorded     ReceiptAcknowledgement = "Recorded"
	ReceiptHistorical   ReceiptAcknowledgement = "Historical"
	ReceiptUnknown      ReceiptAcknowledgement = "Unknown"
	ReceiptRejected     ReceiptAcknowledgement = "Rejected"
)

type recordObservationState struct {
	AttemptID          string
	Acknowledgement    ReceiptAcknowledgement
	Receipt            *localmetadata.AuthObservationReceipt `json:",omitempty"`
	ObservationFailure *localmetadata.AuthObservationFailure `json:",omitempty"`
}

// AuthObservationPolicyFingerprint is public bootstrap computation, never a
// privilege grant. Its explicit domain is separate from the existing observer
// v1 endpoint digest and actor read-settings/KMS binding.
func AuthObservationPolicyFingerprint(p localmetadata.AuthObservationPolicy, metadataConfigDigest, observationBindingDigest string) (string, error) {
	if p.Contract != localmetadata.AuthObservationReceiptContract || p.ObservationContract != observation.Contract || !hexDigest.MatchString(p.ObservationBindingDigest) || !hexDigest.MatchString(p.EndpointBindingDigest) || p.ObservationBindingDigest != observationBindingDigest || !hexDigest.MatchString(metadataConfigDigest) || p.MaximumPerStage < 1 || p.MaximumPerStage > 8 || p.MaximumDuration <= 0 || p.MaximumDuration > 10*time.Second {
		return "", Invalid
	}
	return sum(struct {
		Contract, Metadata, Observation string
		Policy                          localmetadata.AuthObservationPolicy
	}{AuthReceiptPolicyContract, metadataConfigDigest, observationBindingDigest, p}), nil
}

func (a *Actor) configureAuthReceipts() error {
	if a.config.Mode != LocalDisposableStagingAuthReceipts {
		if a.config.MetadataConfig.AuthObservationPolicy != nil || a.registry.config.AuthObservationPolicyDigest != "" {
			return Invalid
		}
		return nil
	}
	if a.observer == nil || a.readSettings == nil || a.config.MetadataConfig.AuthObservationPolicy == nil {
		return Invalid
	}
	p := *a.config.MetadataConfig.AuthObservationPolicy
	if a.readSettings.Timeout+AuthReceiptPersistenceReserve > p.MaximumDuration {
		return Invalid
	}
	binding, e := observation.ExpectedEndpointBinding(observation.EndpointBindingInput{Cloud: a.readSettings.Cloud, Data: a.readSettings.Data, TrustBindingID: a.readSettings.TrustBindingID, ProjectID: a.readSettings.ProjectID})
	if e != nil || binding.Schema != 1 || binding.Contract != observation.EndpointBindingContract || binding.Digest != p.EndpointBindingDigest {
		return Invalid
	}
	digest, e := AuthObservationPolicyFingerprint(p, a.config.MetadataBinding.ConfigSHA256, a.registry.config.ObservationBindingDigest)
	if e != nil || digest != a.registry.config.AuthObservationPolicyDigest {
		return Invalid
	}
	a.authReceiptPolicy = &p
	return nil
}

func finiteObservationFailure(err error) *localmetadata.AuthObservationFailure {
	var e *observation.Error
	if !errors.As(err, &e) || e == nil {
		return nil
	}
	switch e.Step {
	case observation.Metadata, observation.Secret, observation.Identity, observation.Namespace, observation.DataPlane, observation.Completion:
	default:
		return nil
	}
	switch e.Code {
	case observation.Invalid, observation.Denied, observation.MetadataChanged, observation.SecretUnproven, observation.ReadUnknown, observation.IdentityUnproven, observation.NamespaceUnproven, observation.Expired, observation.PrivacyDenied, observation.AuthenticationRejected, observation.AuthorizationRejected, observation.EvidenceUnsupported, observation.Canceled, observation.Deadline:
		return &localmetadata.AuthObservationFailure{Code: e.Code, Step: e.Step}
	}
	return nil
}

// Receipt errors have a finite whitelist. Only known pre-append refusal paths
// claim Rejected; unknown backend errors retain uncertainty, never raw text.
func receiptPersistenceError(err error) (ReceiptAcknowledgement, error) {
	switch err {
	case localmetadata.Capacity:
		return ReceiptRejected, AuthReceiptCapacity
	case localmetadata.Invalid, localmetadata.ScopeConflict, localmetadata.CASConflict, localmetadata.IllegalTransition:
		return ReceiptRejected, AuthReceiptRejected
	default:
		return ReceiptUnknown, AuthReceiptOutcomeUnknown
	}
}

// A pure finite result boundary, also used for fault-composition tests without
// exporting a runtime store hook/factory. It never replaces the original
// observation failure with a persistence failure or an unacknowledged receipt.
func recordReceiptOutcome(result *recordObservationState, saved localmetadata.AuthObservationReceipt, err error) error {
	result.Receipt = nil
	if err != nil {
		result.Acknowledgement, err = receiptPersistenceError(err)
		return err
	}
	if !hexDigest.MatchString(saved.ID) || !hexDigest.MatchString(saved.Version) || saved.Contract != localmetadata.AuthObservationReceiptContract {
		result.Acknowledgement = ReceiptUnknown
		return AuthReceiptOutcomeUnknown
	}
	result.Acknowledgement, result.Receipt = ReceiptRecorded, &saved
	return nil
}

func (a *Actor) authReceiptRequest(r RecordObservationRequest, s tc.CredentialState, o awsstore.Outcome) localmetadata.AuthObservationRequest {
	return localmetadata.AuthObservationRequest{AttemptID: r.AttemptID, SlotID: s.Intent.SlotID, StageID: s.Intent.StageID, CredentialAttemptID: s.Intent.AttemptID, Owner: s.Intent.Owner, NamespaceID: a.config.Namespace.NamespaceID, NamespaceIncarnationID: a.config.Namespace.Record.Ref.ProviderID, NamespaceCreatedAt: a.config.Namespace.CreatedAt, StoreID: s.Candidate.StoreID, CandidateID: s.Candidate.CandidateID, CandidateARN: o.ARN, CandidateCreatedAt: o.CreatedAt, WriteID: s.CandidateWriteID, ServiceAccountID: s.ServiceAccount.ID, KeyID: s.Key.ID, CredentialJournalVersion: s.Version, OutcomeLedgerVersion: o.Version, ObservationBindingDigest: a.registry.config.ObservationBindingDigest}
}

// RecordAuthObservation optionally records one terminal auth-only diagnostic.
// There is no Requested state, manager verification, mutation of Staged, or
// activation. Exact historical replay performs NO provider/secret/TLS calls.
// This method owns the same private actor/registry lifecycle as ObserveStaged.
func (a *Actor) RecordAuthObservation(ctx context.Context, r RecordObservationRequest) (RecordObservationResult, error) {
	control := &recordObservationState{Acknowledgement: ReceiptNotAttempted}
	result := RecordObservationResult{private: control}
	if a == nil {
		return result, Invalid
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return result, Closed
	}
	if a.config.Mode != LocalDisposableStagingAuthReceipts || a.authReceiptPolicy == nil || a.observer == nil || a.readSettings == nil {
		return result, AuthReceiptDisabled
	}
	if !hexDigest.MatchString(r.AttemptID) || !hexDigest.MatchString(r.StageID) || !hexDigest.MatchString(r.WriteID) || !hexDigest.MatchString(r.CredentialJournalVersion) || inheritedReadAuthority(ctx) {
		return result, NotAuthorized
	}
	control.AttemptID = r.AttemptID
	if ctx.Err() != nil {
		return result, readContextError(ctx)
	}
	call, cancel := context.WithTimeout(metadata.NewOutgoingContext(ctx, metadata.MD{}), a.authReceiptPolicy.MaximumDuration)
	defer cancel()
	err := a.registry.locked(call, func() error {
		writer, e := a.current(call)
		if e != nil {
			return e
		}
		state, outcome, e := a.stagedMetadataSnapshot(call, r.ObserveRequest)
		if e != nil {
			return e
		}
		request := a.authReceiptRequest(r, state, outcome)
		ledger := a.metadata.AuthObservations()
		existing, found, e := ledger.Preflight(call, request)
		if e != nil {
			control.Acknowledgement, e = receiptPersistenceError(e)
			return e
		}
		if found {
			if _, e := a.current(call); e != nil {
				return e
			}
			control.Acknowledgement, control.Receipt = ReceiptHistorical, &existing
			if existing.Failure != nil {
				copy := *existing.Failure
				control.ObservationFailure = &copy
			}
			return nil
		}
		if a.snapshot.Envelope.Admission == nil {
			return NotAuthorized
		}
		session, cell := *a.snapshot.Envelope.Admission, a.snapshot.Version
		started := a.config.Now()
		var proof observation.Observation
		var observationErr error
		if !state.Intent.ExpiresAt.After(started.Add(5 * time.Second)) {
			// This branch records a known local validity failure only. It does
			// NOT bypass the fresh private read guard or perform any SDK call.
			observationErr = &observation.Error{Code: observation.Expired, Step: observation.Metadata}
		} else {
			read, stopRead := context.WithTimeout(call, a.readSettings.Timeout)
			proof, observationErr = a.observeLocked(read, r.ObserveRequest, state, outcome, writer)
			stopRead()
		}
		control.ObservationFailure = finiteObservationFailure(observationErr)
		if call.Err() != nil {
			return readContextError(call)
		}
		if e := a.recheckAuthRecording(call, r.ObserveRequest, state, outcome, writer, session.SessionID, session.Generation, cell.UID, cell.ResourceVersion); e != nil {
			return e
		}
		if observationErr != nil && control.ObservationFailure == nil {
			return observationErr
		}
		recorded := a.config.Now()
		receipt := localmetadata.AuthObservationReceipt{Contract: localmetadata.AuthObservationReceiptContract, Request: request, Authority: localmetadata.AuthObservationAuthority{ControlCellUID: cell.UID, ControlVersionDigest: sum(cell), SessionID: session.SessionID, AdmissionGeneration: session.Generation, WriterID: writer.ID, WriterEpoch: writer.Epoch}, StartedAt: started, RecordedAt: recorded}
		if observationErr == nil {
			receipt.Kind, receipt.Observation, receipt.StartedAt = localmetadata.AuthObserved, &proof, proof.StartedAt
		} else {
			receipt.Kind, receipt.Failure = localmetadata.AuthNotEstablished, control.ObservationFailure
		}
		saved, saveErr := ledger.Insert(call, receipt, recorded)
		if saveErr = recordReceiptOutcome(control, saved, saveErr); saveErr != nil {
			return saveErr
		}
		return observationErr
	})
	return result, err
}

func (a *Actor) recheckAuthRecording(ctx context.Context, request ObserveRequest, state tc.CredentialState, outcome awsstore.Outcome, expectedWriter ownership.Writer, session string, generation uint64, cellUID, cellVersion string) error {
	writer, e := a.current(ctx)
	if e != nil {
		return e
	}
	if writer != expectedWriter || a.snapshot.Envelope.Admission == nil || a.snapshot.Envelope.Admission.SessionID != session || a.snapshot.Envelope.Admission.Generation != generation || a.snapshot.Version.UID != cellUID || a.snapshot.Version.ResourceVersion != cellVersion {
		return Changed
	}
	if e := a.registry.check(); e != nil {
		return e
	}
	current, receipt, e := a.stagedMetadataSnapshot(ctx, request)
	if e != nil {
		return e
	}
	if !reflect.DeepEqual(state, current) || !reflect.DeepEqual(outcome, receipt) {
		return Changed
	}
	return nil
}
