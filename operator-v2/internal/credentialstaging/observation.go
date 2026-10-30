package credentialstaging

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"time"

	observation "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/credentialobservation"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	awsstore "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/awssecrets"
	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
	"google.golang.org/grpc/metadata"
)

const ObservationDisabled Error = "CredentialStagingObservationDisabled"

// ObservationSettings contains public trusted bootstrap inputs, never project
// authoring or credentials. ReaderBindingID attests coordinator configuration;
// it does NOT prove the injected SDK client's actual IAM identity/permissions.
type ObservationSettings struct {
	ProjectID, TrustBindingID, ReaderBindingID, ExpectedDataPlaneID string
	Cloud, Data                                                     observation.Endpoint
	Timeout                                                         time.Duration
}
type ObservationRuntime struct {
	Settings     ObservationSettings
	SecretReader observation.SecretClient
}
type ObserveRequest struct{ StageID, WriteID, CredentialJournalVersion string }

// ObservationSettingsFingerprint grants no permission. Bootstrap must retain it
// independently in a new read-enabled registry. Exact KMS ARN/provenance/owner
// are included: the older stage-only registry did not pin those settings.
func ObservationSettingsFingerprint(s ObservationSettings, kms awsstore.KMSBinding) (string, error) {
	if !safeID.MatchString(s.ReaderBindingID) || !safeID.MatchString(s.ProjectID) || !safeID.MatchString(s.TrustBindingID) || s.Timeout <= 0 || s.Timeout > 10*time.Second || kms.ARN == "" || kms.ProvenanceID == "" {
		return "", Invalid
	}
	return sum(struct {
		Contract string
		Settings ObservationSettings
		KMS      awsstore.KMSBinding
	}{observation.Contract, s, kms}), nil
}
func copyObservationSettings(s ObservationSettings) ObservationSettings {
	s.Cloud.RootCAPEM = append([]byte(nil), s.Cloud.RootCAPEM...)
	s.Data.RootCAPEM = append([]byte(nil), s.Data.RootCAPEM...)
	return s
}
func (a *Actor) configureObservation() error {
	runtime := a.config.Observation
	if runtime == nil {
		if a.registry.config.ObservationBindingDigest != "" {
			return Invalid
		}
		return nil
	}
	settings := copyObservationSettings(runtime.Settings)
	digest, e := ObservationSettingsFingerprint(settings, a.config.KMS)
	if e != nil || digest != a.registry.config.ObservationBindingDigest || runtime.SecretReader == nil {
		return Invalid
	}
	c := a.config
	a.observer, e = observation.New(observation.Config{Ownership: c.Project, Namespace: c.Namespace, ProjectID: settings.ProjectID, StoreID: a.registry.config.StoreID, KMSARN: c.KMS.ARN, TrustBindingID: settings.TrustBindingID, Cloud: settings.Cloud, Data: settings.Data, ExpectedDataPlaneID: settings.ExpectedDataPlaneID, Timeout: settings.Timeout, Now: c.Now}, a.metadata.CredentialJournal(), a.metadata.OutcomeLedger(), runtime.SecretReader, actorReadGuard{actor: a})
	if e != nil {
		return Invalid
	}
	a.readSettings = &settings
	// The explicit reader client is retained only by Observer's opaque private
	// state. Do not add another printable long-lived SDK credential holder.
	a.config.Observation = &ObservationRuntime{Settings: copyObservationSettings(settings)}
	return nil
}

func inheritedReadAuthority(ctx context.Context) bool {
	md, _ := metadata.FromOutgoingContext(ctx)
	for key, values := range md {
		if len(values) == 0 {
			continue
		}
		switch strings.ToLower(key) {
		case "authorization", "proxy-authorization", "x-api-key", "x-temporal-api-key", "temporal-namespace", "temporal-cloud-api-version":
			return true
		}
	}
	return false
}
func readContextError(ctx context.Context) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return &observation.Error{Code: observation.Deadline, Step: observation.Completion}
	}
	return &observation.Error{Code: observation.Canceled, Step: observation.Completion}
}

// ObserveStaged is a diagnostic read, not manager verification or promotion.
// The same actor/registry lifecycle locks cover namespace pre-read, actual
// candidate TLS calls, connection/material close and authority rechecks.
func (a *Actor) ObserveStaged(ctx context.Context, r ObserveRequest) (observation.Observation, error) {
	if a == nil {
		return observation.Observation{}, Invalid
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return observation.Observation{}, Closed
	}
	if a.observer == nil || a.readSettings == nil {
		return observation.Observation{}, ObservationDisabled
	}
	if !hexDigest.MatchString(r.StageID) || !hexDigest.MatchString(r.WriteID) || !hexDigest.MatchString(r.CredentialJournalVersion) || inheritedReadAuthority(ctx) {
		return observation.Observation{}, NotAuthorized
	}
	if ctx.Err() != nil {
		return observation.Observation{}, readContextError(ctx)
	}
	call, cancel := context.WithTimeout(metadata.NewOutgoingContext(ctx, metadata.MD{}), a.readSettings.Timeout)
	defer cancel()
	var result observation.Observation
	err := a.registry.locked(call, func() error {
		writer, e := a.current(call)
		if e != nil {
			return e
		}
		state, receipt, e := a.stagedReadSnapshot(call, r)
		if e != nil {
			return e
		}
		result, e = a.observeLocked(call, r, state, receipt, writer)
		return e
	})
	if err != nil {
		return observation.Observation{}, err
	}
	return result, nil
}

// Private shared read core. Callers hold mu+registry lock and establish the
// exact fresh staged snapshot first; no public permit/fake factory is added.
func (a *Actor) observeLocked(call context.Context, r ObserveRequest, state tc.CredentialState, receipt awsstore.Outcome, writer ownership.Writer) (observation.Observation, error) {
	ns, e := a.namespaces.Observe(call, a.config.Namespace.NamespaceID)
	if call.Err() != nil {
		return observation.Observation{}, readContextError(call)
	}
	if e != nil || !ns.Exists || ns.IncarnationID != a.config.Namespace.Record.Ref.ProviderID || ns.State != "RESOURCE_STATE_ACTIVE" || !ns.APIKeyEnabled {
		return observation.Observation{}, Changed
	}
	if a.snapshot.Envelope.Admission == nil {
		return observation.Observation{}, NotAuthorized
	}
	invocation := &readInvocation{actor: a, request: r, state: state, outcome: receipt, writer: writer, session: *a.snapshot.Envelope.Admission, cell: a.snapshot.Version, live: true}
	a.reading = invocation
	defer func() { invocation.live = false; a.reading = nil }()
	result, e := a.observer.Observe(context.WithValue(call, readInvocationKey{}, invocation), state.Candidate)
	if call.Err() != nil {
		return observation.Observation{}, readContextError(call)
	}
	if invocation.guardError != nil {
		return observation.Observation{}, invocation.guardError
	}
	if check := a.checkReadInvocation(call, invocation); check != nil {
		return observation.Observation{}, check
	}
	return result, e
}

// A single atomic localmetadata history read pairs journal/outcome facts. It
// never acknowledges/reconstructs an uncertain append or provider operation.
func (a *Actor) stagedReadSnapshot(ctx context.Context, r ObserveRequest) (tc.CredentialState, awsstore.Outcome, error) {
	state, receipt, e := a.stagedMetadataSnapshot(ctx, r)
	if e != nil {
		return tc.CredentialState{}, awsstore.Outcome{}, e
	}
	if !state.Intent.ExpiresAt.After(a.config.Now().Add(5 * time.Second)) {
		return tc.CredentialState{}, awsstore.Outcome{}, NotAuthorized
	}
	return state, receipt, nil
}

// Stable refs permit historical receipt lookup/known-expiry recording only.
// Actual secret/TLS calls still require stagedReadSnapshot and private guard.
func (a *Actor) stagedMetadataSnapshot(ctx context.Context, r ObserveRequest) (tc.CredentialState, awsstore.Outcome, error) {
	history, e := a.metadata.History(ctx)
	if e != nil {
		return tc.CredentialState{}, awsstore.Outcome{}, Blocked
	}
	if e = a.historySafe(history); e != nil {
		return tc.CredentialState{}, awsstore.Outcome{}, e
	}
	var state tc.CredentialState
	var outcome awsstore.Outcome
	for _, revision := range history {
		if revision.Journal != nil {
			state = *revision.Journal
		}
		if revision.Outcome != nil && revision.Outcome.Write.WriteID == r.WriteID {
			outcome = *revision.Outcome
		}
	}
	i, k, c := state.Intent, state.Key, state.Candidate
	slot := sum(struct {
		Owner       any
		Incarnation string
	}{a.config.Project.Owner(), a.config.Namespace.Record.Ref.ProviderID})
	if state.Phase != tc.CredentialStaged || state.Version != r.CredentialJournalVersion || i.StageID != r.StageID || i.SlotID != slot || i.Owner != a.config.Project.Owner() || i.NamespaceID != a.config.Namespace.NamespaceID || i.NamespaceIncarnationID != a.config.Namespace.Record.Ref.ProviderID || i.FailurePolicy != tc.DisposableRetainOnFailure || state.CandidateWriteID != r.WriteID || c.WriteID != r.WriteID || c.VersionID != r.WriteID || c.StoreID != a.registry.config.StoreID || c.Owner != i.Owner || c.NamespaceIncarnationID != i.NamespaceIncarnationID || c.CandidateID != i.CandidateID || c.KeyID != k.ID || c.ServiceAccountID != state.ServiceAccount.ID || !c.Encrypted || !c.Candidate || c.Active || !k.Verified || k.CreatedAt.IsZero() || k.ServiceAccountID != state.ServiceAccount.ID || state.FailureCode != "" || state.CleanupFailureCode != "" || state.DeleteReceipt != (tc.Receipt{}) {
		return tc.CredentialState{}, awsstore.Outcome{}, NotAuthorized
	}
	key := k
	key.VersionDigest = ""
	key.State = ""
	if outcome.Phase != awsstore.MetadataVerified || !hexDigest.MatchString(outcome.Version) || outcome.ARN == "" || outcome.CreatedAt.IsZero() || outcome.Write.WriteID != r.WriteID || !reflect.DeepEqual(outcome.Write.Intent, i) || !reflect.DeepEqual(outcome.Write.Key, key) {
		return tc.CredentialState{}, awsstore.Outcome{}, Blocked
	}
	if e = a.registry.permits(tc.CandidateWrite{Intent: i, Key: k, WriteID: r.WriteID}); e != nil {
		return tc.CredentialState{}, awsstore.Outcome{}, e
	}
	return state, outcome, nil
}

type readInvocationKey struct{}
type readInvocation struct {
	actor      *Actor
	request    ObserveRequest
	state      tc.CredentialState
	outcome    awsstore.Outcome
	writer     ownership.Writer
	session    journal.Admission
	cell       journal.Version
	live, used bool
	guardError error
}

func (a *Actor) checkReadInvocation(ctx context.Context, call *readInvocation) error {
	if a == nil || a.gate == nil || a.metadata == nil || a.observer == nil || call == nil || call.actor != a || call != a.reading || !call.live || a.closed {
		return NotAuthorized
	}
	writer, e := a.current(ctx)
	if e != nil {
		return e
	}
	if writer != call.writer || a.snapshot.Version != call.cell || a.snapshot.Envelope.Admission == nil || *a.snapshot.Envelope.Admission != call.session {
		return NotAuthorized
	}
	if e = a.registry.check(); e != nil {
		return e
	}
	state, receipt, e := a.stagedReadSnapshot(ctx, call.request)
	if e != nil {
		return e
	}
	if !reflect.DeepEqual(state, call.state) || !reflect.DeepEqual(receipt, call.outcome) {
		return Changed
	}
	return nil
}

type actorReadGuard struct{ actor *Actor }

func (g actorReadGuard) Run(ctx context.Context, ref tc.SecretVersionRef, fn func(context.Context) error) error {
	a := g.actor
	if a == nil || a.gate == nil || a.metadata == nil || a.observer == nil || a.readSettings == nil {
		return NotAuthorized
	}
	call, ok := ctx.Value(readInvocationKey{}).(*readInvocation)
	if !ok || call == nil || call.used || call != a.reading || !reflect.DeepEqual(ref, call.state.Candidate) {
		return NotAuthorized
	}
	if e := a.checkReadInvocation(ctx, call); e != nil {
		call.guardError = e
		return e
	}
	call.used = true
	err := fn(ctx)
	if ctx.Err() != nil {
		return readContextError(ctx)
	}
	if e := a.checkReadInvocation(ctx, call); e != nil {
		call.guardError = e
		return e
	}
	return err
}
