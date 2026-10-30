package localmetadata

import (
	"context"
	"time"

	observation "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/credentialobservation"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	awsstore "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/awssecrets"
	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
)

const AuthObservationReceiptContract = "temporal-candidate-auth-observation-receipt-v1"

// AuthObservationPolicy is independently pinned bootstrap configuration. The
// two binding digests have different domains: actor settings/KMS versus the
// observer's exact Cloud/data endpoint and public trust settings. Neither is
// evidence of actual SDK identity, IAM permission, or a network observation.
type AuthObservationPolicy struct {
	Contract, ObservationContract                   string
	ObservationBindingDigest, EndpointBindingDigest string
	MaximumPerStage                                 int
	MaximumDuration                                 time.Duration
}

type AuthObservationRequest struct {
	AttemptID, SlotID, StageID, CredentialAttemptID string
	Owner                                           ownership.Owner
	NamespaceID, NamespaceIncarnationID             string
	NamespaceCreatedAt                              time.Time
	StoreID, CandidateID, CandidateARN              string
	CandidateCreatedAt                              time.Time
	WriteID, ServiceAccountID, KeyID                string
	CredentialJournalVersion, OutcomeLedgerVersion  string
	ObservationBindingDigest                        string
}

// AuthObservationAuthority is original actor provenance, not a transferable
// capability. A clean successor checks its current authority independently;
// it must not replace this historical session with its newly created session.
type AuthObservationAuthority struct {
	ControlCellUID, ControlVersionDigest, SessionID string
	AdmissionGeneration, WriterEpoch                uint64
	WriterID                                        string
}

type AuthObservationKind string

const (
	AuthObserved       AuthObservationKind = "Observed"
	AuthNotEstablished AuthObservationKind = "NotEstablished"
)

type AuthObservationFailure struct {
	Code observation.Code
	Step observation.Step
}

// AuthObservationReceipt contains terminal historical read facts only. It
// never changes Staged, refreshes a deletion precondition, or authorizes
// CredentialVerified, workflow/worker readiness, activation or cleanup.
type AuthObservationReceipt struct {
	ID, Version, Contract string
	Request               AuthObservationRequest
	Authority             AuthObservationAuthority
	StartedAt, RecordedAt time.Time
	Kind                  AuthObservationKind
	Observation           *observation.Observation `json:",omitempty"`
	Failure               *AuthObservationFailure  `json:",omitempty"`
}

type AuthObservations struct{ store *Store }

func (s *Store) AuthObservations() *AuthObservations { return &AuthObservations{store: s} }

func validAuthPolicy(p AuthObservationPolicy) bool {
	return p.Contract == AuthObservationReceiptContract && p.ObservationContract == observation.Contract && hex64.MatchString(p.ObservationBindingDigest) && hex64.MatchString(p.EndpointBindingDigest) && p.MaximumPerStage >= 1 && p.MaximumPerStage <= 8 && p.MaximumDuration > 0 && p.MaximumDuration <= 10*time.Second
}

func (s *Store) identitySchema() string {
	if s.config.AuthObservationPolicy != nil {
		return "credential-local-auth-observation-metadata-v1"
	}
	return "credential-local-metadata-v1"
}
func (s *Store) intentSchema() string {
	if s.config.AuthObservationPolicy != nil {
		return "credential-auth-observation-metadata-intent-v1"
	}
	return "credential-metadata-intent-v1"
}
func (s *Store) commitSchema() string {
	if s.config.AuthObservationPolicy != nil {
		return "credential-auth-observation-metadata-commit-v1"
	}
	return "credential-metadata-commit-v1"
}

func copyAuthReceipt(r AuthObservationReceipt) AuthObservationReceipt {
	if r.Observation != nil {
		copy := *r.Observation
		r.Observation = &copy
	}
	if r.Failure != nil {
		copy := *r.Failure
		r.Failure = &copy
	}
	return r
}
func normalizeAuthReceipt(r AuthObservationReceipt) AuthObservationReceipt {
	r = copyAuthReceipt(r)
	r.Request = normalizeAuthRequest(r.Request)
	r.StartedAt = r.StartedAt.UTC()
	r.RecordedAt = r.RecordedAt.UTC()
	if r.Observation != nil {
		r.Observation.StartedAt = r.Observation.StartedAt.UTC()
		r.Observation.ObservedAt = r.Observation.ObservedAt.UTC()
		r.Observation.ExpiresAt = r.Observation.ExpiresAt.UTC()
	}
	return r
}
func normalizeAuthRequest(r AuthObservationRequest) AuthObservationRequest {
	r.NamespaceCreatedAt = r.NamespaceCreatedAt.UTC()
	r.CandidateCreatedAt = r.CandidateCreatedAt.UTC()
	return r
}
func (s *Store) authRecordID(attemptID string) string {
	return digest(struct{ Contract, Config, Attempt string }{AuthObservationReceiptContract, s.configDigest(), attemptID})
}
func validAuthFailure(f AuthObservationFailure) bool {
	switch f.Step {
	case observation.Metadata, observation.Secret, observation.Identity, observation.Namespace, observation.DataPlane, observation.Completion:
	default:
		return false
	}
	switch f.Code {
	case observation.Invalid, observation.Denied, observation.MetadataChanged, observation.SecretUnproven, observation.ReadUnknown, observation.IdentityUnproven, observation.NamespaceUnproven, observation.Expired, observation.PrivacyDenied, observation.AuthenticationRejected, observation.AuthorizationRejected, observation.EvidenceUnsupported, observation.Canceled, observation.Deadline:
		return true
	}
	return false
}

func (s *Store) validAuthRequest(v *view, r AuthObservationRequest) bool {
	if s.config.AuthObservationPolicy == nil || !hex64.MatchString(r.AttemptID) || r.ObservationBindingDigest != s.config.AuthObservationPolicy.ObservationBindingDigest || r.Owner != s.config.Ownership.Owner() || r.SlotID != s.slot || r.NamespaceID != s.config.NamespaceID || r.NamespaceIncarnationID != s.config.NamespaceIncarnationID || !r.NamespaceCreatedAt.Equal(s.config.NamespaceCreatedAt) || r.StoreID != s.config.StoreID {
		return false
	}
	j := v.journal
	o, found := v.outcomes[r.WriteID]
	if j == nil || !found || j.Phase != tc.CredentialStaged || j.Intent.FailurePolicy != tc.DisposableRetainOnFailure || j.Version != r.CredentialJournalVersion || o.Version != r.OutcomeLedgerVersion || o.Phase != awsstore.MetadataVerified || j.Intent.StageID != r.StageID || j.Intent.AttemptID != r.CredentialAttemptID || j.CandidateWriteID != r.WriteID || j.Candidate.WriteID != r.WriteID || j.Candidate.VersionID != r.WriteID || j.Candidate.StoreID != r.StoreID || j.Candidate.CandidateID != r.CandidateID || j.Candidate.Owner != r.Owner || j.Candidate.NamespaceIncarnationID != r.NamespaceIncarnationID || j.Candidate.ServiceAccountID != r.ServiceAccountID || j.Candidate.KeyID != r.KeyID || !j.Candidate.Candidate || !j.Candidate.Encrypted || j.Candidate.Active || j.ServiceAccount.ID != r.ServiceAccountID || j.Key.ID != r.KeyID || j.Key.ServiceAccountID != r.ServiceAccountID || !j.Key.Verified || j.FailureCode != "" || j.CleanupFailureCode != "" || j.DeleteReceipt != (tc.Receipt{}) {
		return false
	}
	return o.ARN == r.CandidateARN && s.validARN(r.CandidateARN, r.CandidateID) && !o.CreatedAt.IsZero() && o.CreatedAt.Equal(r.CandidateCreatedAt) && equal(o.Write.Intent, j.Intent) && immutableKey(j.Key, o.Write.Key)
}

func (s *Store) validAuthReceipt(v *view, r AuthObservationReceipt) bool {
	p := s.config.AuthObservationPolicy
	if p == nil || r.Contract != p.Contract || r.Version != "" || r.ID != s.authRecordID(r.Request.AttemptID) || !s.validAuthRequest(v, r.Request) || !identifier.MatchString(r.Authority.ControlCellUID) || !hex64.MatchString(r.Authority.ControlVersionDigest) || !hex64.MatchString(r.Authority.SessionID) || r.Authority.AdmissionGeneration == 0 || r.Authority.WriterEpoch == 0 || !identifier.MatchString(r.Authority.WriterID) || !validTime(r.StartedAt, false) || !validTime(r.RecordedAt, false) || r.RecordedAt.Before(r.StartedAt) || r.RecordedAt.Sub(r.StartedAt) > p.MaximumDuration || r.StartedAt.Before(v.journal.Key.CreatedAt) {
		return false
	}
	if r.Kind == AuthNotEstablished {
		return r.Observation == nil && r.Failure != nil && validAuthFailure(*r.Failure)
	}
	// Expired is a useful finite historical failure. Recording it does not
	// authorize another TLS call with an expired candidate. Successful evidence
	// must have completed and been recorded before the pinned key expiry.
	if r.Kind != AuthObserved || r.Observation == nil || r.Failure != nil || !v.journal.Intent.ExpiresAt.After(r.RecordedAt) {
		return false
	}
	o, q, i := r.Observation, r.Request, v.journal.Intent
	return o.Schema == 1 && o.Contract == p.ObservationContract && o.Owner == q.Owner && o.SlotID == q.SlotID && o.StageID == q.StageID && o.AttemptID == q.CredentialAttemptID && o.NamespaceID == q.NamespaceID && o.NamespaceIncarnationID == q.NamespaceIncarnationID && o.StoreID == q.StoreID && o.CandidateID == q.CandidateID && o.WriteID == q.WriteID && o.VersionID == q.WriteID && o.ServiceAccountID == q.ServiceAccountID && o.KeyID == q.KeyID && o.CredentialJournalVersion == q.CredentialJournalVersion && o.OutcomeLedgerVersion == q.OutcomeLedgerVersion && o.EndpointBindingDigest == p.EndpointBindingDigest && hex64.MatchString(o.DataPlaneIDHash) && hex64.MatchString(o.KeyVersionDigest) && hex64.MatchString(o.ServiceAccountVersionDigest) && hex64.MatchString(o.NamespaceVersionDigest) && o.StartedAt.Equal(r.StartedAt) && validTime(o.ObservedAt, false) && !o.ObservedAt.Before(o.StartedAt) && !o.ObservedAt.After(r.RecordedAt) && o.ExpiresAt.Equal(i.ExpiresAt)
}

func (s *Store) authCapacity(v *view, stage string) error {
	if len(v.history) >= s.config.MaximumRevisions {
		return Capacity
	}
	count := 0
	for _, r := range v.authObservations {
		if r.Request.StageID == stage {
			count++
		}
	}
	if count >= s.config.AuthObservationPolicy.MaximumPerStage {
		return Capacity
	}
	return nil
}
func (s *Store) validateAuthFrame(v *view, f frame) error {
	if f.Journal != nil || f.Outcome != nil || f.AuthObservation == nil || f.ExpectedVersion != "" || f.Key != f.AuthObservation.Request.AttemptID || !s.validAuthReceipt(v, *f.AuthObservation) {
		return IllegalTransition
	}
	if _, found := v.authObservations[f.Key]; found {
		return CASConflict
	}
	return s.authCapacity(v, f.AuthObservation.Request.StageID)
}
func (l *AuthObservations) enabled() bool {
	return l != nil && l.store != nil && l.store.config.AuthObservationPolicy != nil
}

// Load returns historical facts with their ORIGINAL timestamp and authority.
// It deliberately has no clock argument and never promises current freshness.
// Actual current admission checks belong to the owning actor, not this ledger.
func (l *AuthObservations) Load(ctx context.Context, attemptID string) (AuthObservationReceipt, bool, error) {
	var result AuthObservationReceipt
	var found bool
	if !l.enabled() || !hex64.MatchString(attemptID) {
		return result, false, Invalid
	}
	err := l.store.locked(ctx, func() error {
		if l.store.uncertain {
			return Uncertain
		}
		v, e := l.store.scan(ctx, false)
		if e != nil {
			return e
		}
		result, found = v.authObservations[attemptID]
		result = copyAuthReceipt(result)
		return nil
	})
	return result, found, err
}

// Preflight checks exact stable request refs and capacity without reserving or
// writing anything. Found means an immutable HISTORICAL record; a successor's
// current session is intentionally not an input. A later insert rechecks all
// facts/capacity under the lock. No observation happens in this backend.
func (l *AuthObservations) Preflight(ctx context.Context, request AuthObservationRequest) (AuthObservationReceipt, bool, error) {
	var result AuthObservationReceipt
	var found bool
	if !l.enabled() {
		return result, false, Invalid
	}
	request = normalizeAuthRequest(request)
	err := l.store.locked(ctx, func() error {
		if l.store.uncertain {
			return Uncertain
		}
		v, e := l.store.scan(ctx, false)
		if e != nil {
			return e
		}
		if existing, ok := v.authObservations[request.AttemptID]; ok {
			if !equal(existing.Request, request) {
				return CASConflict
			}
			result, found = copyAuthReceipt(existing), true
			return nil
		}
		if !l.store.validAuthRequest(v, request) {
			return ScopeConflict
		}
		return l.store.authCapacity(v, request.StageID)
	})
	return result, found, err
}

// Insert is terminal-only and insert-only. now is the trusted actor's exact
// recording time. Freshness is checked ONLY for a new insert, never a scan or
// historical duplicate. Repeating the exact canonical durable proposal is
// idempotent; changed evidence/authority/timestamps conflict. Caller inputs are
// copied. No raw material or arbitrary serialized data is accepted here.
func (l *AuthObservations) Insert(ctx context.Context, next AuthObservationReceipt, now time.Time) (AuthObservationReceipt, error) {
	var saved AuthObservationReceipt
	if !l.enabled() || next.Version != "" {
		return saved, Invalid
	}
	s := l.store
	next = normalizeAuthReceipt(next)
	id := s.authRecordID(next.Request.AttemptID)
	if next.ID != "" && next.ID != id {
		return saved, ScopeConflict
	}
	next.ID = id
	err := s.locked(ctx, func() error {
		if s.uncertain {
			return Uncertain
		}
		v, e := s.scan(ctx, false)
		if e != nil {
			return e
		}
		if existing, found := v.authObservations[next.Request.AttemptID]; found {
			comparison := copyAuthReceipt(existing)
			comparison.Version = ""
			if !equal(comparison, next) {
				return CASConflict
			}
			saved = copyAuthReceipt(existing)
			return nil
		}
		if !validTime(now, false) || !next.RecordedAt.Equal(now) || !s.validAuthReceipt(v, next) {
			return IllegalTransition
		}
		if e := s.authCapacity(v, next.Request.StageID); e != nil {
			return e
		}
		d, e := s.append(ctx, v, frame{Kind: "auth-observation", Key: next.Request.AttemptID, AuthObservation: &next})
		if e != nil {
			return e
		}
		next.Version = d
		saved = copyAuthReceipt(next)
		return nil
	})
	return saved, err
}
