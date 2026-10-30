package credentialstaging

import (
	"fmt"
	"io"
	"time"

	observation "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/credentialobservation"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/localmetadata"
)

const AuthReceiptPrivacyDenied Error = "CredentialStagingAuthReceiptControlOnly"
const AuthReceiptViewContract = "temporal-candidate-auth-observation-record-view-v1"

// RecordObservationResult is LOCAL CONTROL state, never an activity/history/
// ordinary diagnostic payload. One private pointer protects even invalid fmt
// verbs on a value. There is deliberately no public raw-receipt accessor.
// Use View for the bounded safe DTO. The private backend keeps its exact bytes.
type RecordObservationResult struct{ private *recordObservationState }

func (RecordObservationResult) String() string               { return "AuthObservationRecordResult{private}" }
func (r RecordObservationResult) GoString() string           { return r.String() }
func (r RecordObservationResult) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, r.String()) }
func (RecordObservationResult) MarshalJSON() ([]byte, error) { return nil, AuthReceiptPrivacyDenied }
func (RecordObservationResult) MarshalText() ([]byte, error) { return nil, AuthReceiptPrivacyDenied }

// Acknowledgement describes LOCAL RECEIPT persistence, not network dispatch or
// authentication. Historical is never a fresh observation. Original finite auth
// failure remains separate from receipt persistence acknowledgement.
type RecordObservationView struct {
	Contract                  string
	AttemptID                 string
	Acknowledgement           ReceiptAcknowledgement
	ReceiptID, ReceiptVersion string                                `json:",omitempty"`
	Kind                      localmetadata.AuthObservationKind     `json:",omitempty"`
	RecordedAt                time.Time                             `json:",omitzero"`
	ObservationFailure        *localmetadata.AuthObservationFailure `json:",omitempty"`
}

func (r RecordObservationResult) View() RecordObservationView {
	v := RecordObservationView{Contract: AuthReceiptViewContract, Acknowledgement: ReceiptNotAttempted}
	if r.private == nil {
		return v
	}
	s := r.private
	if hexDigest.MatchString(s.AttemptID) {
		v.AttemptID = s.AttemptID
	}
	switch s.Acknowledgement {
	case ReceiptNotAttempted, ReceiptRecorded, ReceiptHistorical, ReceiptUnknown, ReceiptRejected:
		v.Acknowledgement = s.Acknowledgement
	default:
		v.Acknowledgement = ReceiptUnknown
	}
	if s.ObservationFailure != nil {
		v.ObservationFailure = finiteObservationFailure(&observation.Error{Code: s.ObservationFailure.Code, Step: s.ObservationFailure.Step})
	}
	if p := s.Receipt; (v.Acknowledgement == ReceiptRecorded || v.Acknowledgement == ReceiptHistorical) && p != nil && hexDigest.MatchString(p.ID) && hexDigest.MatchString(p.Version) && p.Contract == localmetadata.AuthObservationReceiptContract && (p.Kind == localmetadata.AuthObserved || p.Kind == localmetadata.AuthNotEstablished) && !p.RecordedAt.IsZero() && p.RecordedAt.Year() >= 1 && p.RecordedAt.Year() <= 9999 {
		v.ReceiptID, v.ReceiptVersion, v.Kind, v.RecordedAt = p.ID, p.Version, p.Kind, p.RecordedAt.UTC()
	} else if v.Acknowledgement == ReceiptRecorded || v.Acknowledgement == ReceiptHistorical {
		v.Acknowledgement = ReceiptUnknown
	}
	return v
}
