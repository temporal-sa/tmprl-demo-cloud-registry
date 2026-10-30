package credentialstaging

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/admission"
	observation "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/credentialobservation"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	awsstore "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/awssecrets"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubejournal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/localmetadata"
	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
)

type Mode string

const (
	LocalDisposableStaging             Mode = "local_disposable_staging_only"
	LocalDisposableStagingAuthReceipts Mode = "local_disposable_staging_auth_receipts"
)

type Config struct {
	Mode                       Mode
	Project                    ownership.Context
	Cell                       *kubejournal.Cell
	Writer                     func() ownership.Writer
	Namespace                  tc.Binding
	NamespaceTransport         tc.Transport
	CredentialTransport        tc.CredentialTransport
	AWSClient                  awsstore.Client
	KMS                        awsstore.KMSBinding
	MetadataRoot               *os.Root
	MetadataConfig             localmetadata.Config
	MetadataBinding            localmetadata.Binding
	RegistryRoot               *os.Root
	RegistryConfig             RegistryConfig
	RegistryBinding            RegistryBinding
	MaximumKeyTTL, CallTimeout time.Duration
	Now                        func() time.Time
	Observation                *ObservationRuntime
}

type Actor struct {
	mu                sync.Mutex
	config            Config
	registry          registry
	metadata          *localmetadata.Store
	gate              *admission.Gate
	snapshot          journal.Snapshot
	manager           *tc.CredentialManager
	namespaces        *tc.Adapter
	closed            bool
	invocation        *invocation
	observer          *observation.Observer
	readSettings      *ObservationSettings
	reading           *readInvocation
	authReceiptPolicy *localmetadata.AuthObservationPolicy
}

type Request struct {
	OperationKey, AttemptID string
	ExpiresAt               time.Time
}

// Checkpoint is supervisor-owned clean-handoff data, not a transferable writer
// capability. The old actor must finish Quiesce and exit before a successor is
// started. Registry and metadata high-water facts are pinned alongside admission.
type Checkpoint struct{ value checkpoint }
type checkpoint struct {
	Schema                      int
	Registry                    RegistryBinding
	Metadata                    localmetadata.Binding
	HistoryCount                int
	HistoryDigest               string
	Handoff                     []byte
	AuthObservationPolicyDigest string `json:",omitempty"`
}

func (c Checkpoint) JSON() ([]byte, error) {
	validSchema := c.value.Schema == 1 && c.value.AuthObservationPolicyDigest == "" || c.value.Schema == 2 && hexDigest.MatchString(c.value.AuthObservationPolicyDigest)
	if !validSchema || c.value.HistoryCount < 0 || c.value.HistoryCount > 512 || c.value.HistoryCount == 0 && c.value.HistoryDigest != "" || c.value.HistoryCount > 0 && !hexDigest.MatchString(c.value.HistoryDigest) || c.value.Registry.ManifestDigest == "" || c.value.Metadata.IdentitySHA256 == "" {
		return nil, Invalid
	}
	if _, e := admission.Parse(c.value.Handoff); e != nil {
		return nil, Invalid
	}
	b, e := json.Marshal(c.value)
	if e != nil || len(b) > 16384 {
		return nil, Invalid
	}
	return b, nil
}
func ParseCheckpoint(raw []byte) (Checkpoint, error) {
	if len(raw) > 16384 {
		return Checkpoint{}, Invalid
	}
	var c Checkpoint
	if strictDecode(raw, &c.value) != nil {
		return Checkpoint{}, Invalid
	}
	if _, e := c.JSON(); e != nil {
		return Checkpoint{}, e
	}
	return c, nil
}

func OpenInitial(ctx context.Context, c Config) (*Actor, error)             { return open(ctx, c, nil) }
func OpenClean(ctx context.Context, c Config, h Checkpoint) (*Actor, error) { return open(ctx, c, &h) }

func open(ctx context.Context, c Config, h *Checkpoint) (*Actor, error) {
	if c.Mode != LocalDisposableStaging && c.Mode != LocalDisposableStagingAuthReceipts || c.Cell == nil || c.Writer == nil || c.Project.Owner().Lifetime != ownership.Disposable || c.Project.Owner().RunID == "" || c.MaximumKeyTTL <= 0 || c.MaximumKeyTTL > time.Hour || c.CallTimeout <= 0 || c.CallTimeout > time.Minute {
		return nil, Invalid
	}
	if c.Mode == LocalDisposableStagingAuthReceipts {
		if c.MetadataConfig.AuthObservationPolicy == nil || c.RegistryConfig.AuthObservationPolicyDigest == "" {
			return nil, Invalid
		}
		policy := *c.MetadataConfig.AuthObservationPolicy
		c.MetadataConfig.AuthObservationPolicy = &policy
	} else if c.MetadataConfig.AuthObservationPolicy != nil || c.RegistryConfig.AuthObservationPolicyDigest != "" {
		return nil, Invalid
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	identity := c.Cell.Identity()
	config, e := normalized(c.RegistryConfig)
	if e != nil {
		return nil, e
	}
	c.RegistryConfig = config
	if !reflect.DeepEqual(config.Control, identity) || config.Owner != c.Project.Owner() || !reflect.DeepEqual(config.Namespace, c.Namespace) || config.Metadata != c.MetadataBinding || config.MetadataConfigDigest != c.MetadataBinding.ConfigSHA256 || c.MetadataConfig.Ownership.Owner() != c.Project.Owner() || c.MetadataConfig.NamespaceID != c.Namespace.NamespaceID || !c.MetadataConfig.NamespaceCreatedAt.Equal(c.Namespace.CreatedAt) || c.MetadataConfig.NamespaceIncarnationID != c.Namespace.Record.Ref.ProviderID || c.MetadataConfig.StoreID != config.StoreID || c.MetadataConfig.PreprovisionedServiceAccount.ID == "" {
		return nil, Invalid
	}
	for _, p := range []ownership.Provider{ownership.Temporal, ownership.AWS} {
		a, ae := c.Project.Scope(p)
		b, be := c.MetadataConfig.Ownership.Scope(p)
		if ae != nil || be != nil || a != b {
			return nil, Invalid
		}
	}
	metadata, e := localmetadata.Open(c.MetadataRoot, c.MetadataBinding, c.MetadataConfig)
	if e != nil {
		return nil, Changed
	}
	a := &Actor{config: c, registry: registry{root: c.RegistryRoot, binding: c.RegistryBinding, config: config}, metadata: metadata}
	if e = a.configureProviders(); e != nil {
		return nil, e
	}
	e = a.registry.locked(ctx, func() error {
		history, e := metadata.History(ctx)
		if e != nil {
			return Blocked
		}
		if h == nil && len(history) != 0 {
			return Blocked
		}
		if e = a.historySafe(history); e != nil {
			return e
		}
		if h != nil {
			if _, e = h.JSON(); e != nil {
				return e
			}
			if h.value.Registry != c.RegistryBinding || h.value.Metadata != c.MetadataBinding || h.value.HistoryCount != len(history) || h.value.HistoryDigest != historyDigest(history) {
				return Changed
			}
			if h.value.Schema != a.checkpointSchema() || h.value.AuthObservationPolicyDigest != config.AuthObservationPolicyDigest {
				return Changed
			}
		}
		repository, e := journal.New(c.Cell, identity.Subject, identity.CellUID)
		if e != nil {
			return Invalid
		}
		a.snapshot, e = repository.Load(ctx)
		if e != nil {
			return Changed
		}
		if e = a.envelopeSafe(); e != nil {
			return e
		}
		cfg := admission.Config{Mode: admission.LocalCleanHandoff, Repository: repository}
		var claim *admission.Claim
		if h == nil {
			claim, e = admission.BeginInitial(ctx, cfg)
		} else {
			var ready admission.Handoff
			ready, e = admission.Parse(h.value.Handoff)
			if e == nil {
				claim, e = admission.BeginResume(ctx, cfg, ready)
			}
		}
		if e != nil {
			return Changed
		}
		a.gate, e = claim.Confirm(ctx)
		if e != nil {
			return Uncertain
		}
		a.snapshot, e = a.gate.Snapshot(ctx)
		if e != nil {
			return Uncertain
		}
		return a.envelopeSafe()
	})
	if e != nil {
		return nil, e
	}
	return a, nil
}

func (a *Actor) configureProviders() error {
	c := a.config
	config := a.registry.config
	metadata := a.metadata
	var e error
	ng := namespaceGuard{actor: a}
	a.namespaces, e = tc.New(tc.Config{Ownership: c.Project, Bindings: []tc.Binding{c.Namespace}, CallTimeout: c.CallTimeout, Guard: ng, Now: c.Now}, c.NamespaceTransport)
	if e != nil {
		return Invalid
	}
	store, e := awsstore.New(awsstore.Config{Ownership: c.Project, StoreID: config.StoreID, KMS: c.KMS, Namespaces: []awsstore.NamespaceBinding{{ID: c.Namespace.NamespaceID, IncarnationID: c.Namespace.Record.Ref.ProviderID, Owner: c.Project.Owner()}}, AllowCandidateCreate: true, CallTimeout: c.CallTimeout}, c.AWSClient, metadata.CredentialJournal(), metadata.OutcomeLedger(), candidateGuard{actor: a})
	if e != nil {
		return Invalid
	}
	sa := c.MetadataConfig.PreprovisionedServiceAccount
	a.manager, e = tc.NewCredentialManager(tc.CredentialConfig{FailurePolicy: tc.DisposableRetainOnFailure, Capabilities: tc.CredentialCapabilities{StageAPIKeys: true}, MaximumKeyTTL: c.MaximumKeyTTL, MaximumRecoveryPages: 1, StoreID: config.StoreID, PreprovisionedServiceAccounts: []tc.ScopedServiceAccountBinding{{NamespaceID: c.Namespace.NamespaceID, NamespaceIncarnationID: c.Namespace.Record.Ref.ProviderID, Owner: c.Project.Owner(), Reference: sa}}}, a.namespaces, c.CredentialTransport, metadata.CredentialJournal(), store, nil)
	if e != nil {
		return Invalid
	}
	if e = a.configureObservation(); e != nil {
		return e
	}
	return a.configureAuthReceipts()
}

func (a *Actor) envelopeSafe() error {
	e := a.snapshot.Envelope
	if len(e.Attempts) != 0 || len(e.Writes) != 0 || e.Projection != nil || len(e.Archive) != 0 || e.WriterEpoch != a.config.Namespace.Record.WriterEpoch {
		return Invalid
	}
	return nil
}
func historyDigest(h []localmetadata.Revision) string {
	if len(h) == 0 {
		return ""
	}
	return h[len(h)-1].Digest
}
func (a *Actor) historySafe(h []localmetadata.Revision) error {
	var latest *tc.CredentialState
	for _, r := range h {
		if r.Journal != nil {
			latest = r.Journal
		}
	}
	if latest == nil {
		return nil
	}
	if latest.Intent.FailurePolicy != tc.DisposableRetainOnFailure {
		return Blocked
	}
	switch latest.Phase {
	case tc.CredentialRecorded, tc.CredentialSAReady, tc.CredentialStaged:
		return nil
	}
	return Blocked
}
func (a *Actor) current(ctx context.Context) (ownership.Writer, error) {
	if a.closed {
		return ownership.Writer{}, Closed
	}
	return a.authority(ctx)
}

// authority verifies the pinned admission without granting dispatch. Quiesce
// uses it only after closing dispatch under mu; ordinary calls use current.
func (a *Actor) authority(ctx context.Context) (ownership.Writer, error) {
	s, e := a.gate.Snapshot(ctx)
	if e != nil || s.Version != a.snapshot.Version || s.Envelope.Digest != a.snapshot.Envelope.Digest {
		return ownership.Writer{}, Changed
	}
	if e = a.envelopeSafe(); e != nil {
		return ownership.Writer{}, e
	}
	w := a.config.Writer()
	if w.Owner != a.config.Project.Owner() || !safeID.MatchString(w.ID) || w.Epoch != s.Envelope.WriterEpoch || !a.config.Now().Before(w.ExpiresAt) {
		return ownership.Writer{}, NotAuthorized
	}
	return w, nil
}

// Advance performs one bounded create/stage transition. Staged is terminal in
// this actor. Ambiguous create/store/metadata phases remain retained and block
// dispatch and clean handoff; there is no automatic recovery, revoke or replay.
func (a *Actor) Advance(ctx context.Context, r Request) (tc.CredentialState, error) {
	if a == nil {
		return tc.CredentialState{}, Invalid
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return tc.CredentialState{}, Closed
	}
	var result tc.CredentialState
	e := a.registry.locked(ctx, func() error {
		writer, e := a.current(ctx)
		if e != nil {
			return e
		}
		history, e := a.metadata.History(ctx)
		if e != nil {
			return Blocked
		}
		if e = a.historySafe(history); e != nil {
			return e
		}
		snapshot, e := a.namespaces.Observe(ctx, a.config.Namespace.NamespaceID)
		if e != nil || !snapshot.Exists || snapshot.IncarnationID != a.config.Namespace.Record.Ref.ProviderID || snapshot.State != "RESOURCE_STATE_ACTIVE" || !snapshot.APIKeyEnabled {
			return Changed
		}
		request := tc.StageRequest{Namespace: tc.WriteRequest{NamespaceID: snapshot.NamespaceID, ExpectedIncarnationID: snapshot.IncarnationID, ExpectedVersion: snapshot.ResourceVersion, ExpectedRevision: a.config.Namespace.Record.Revision, OperationKey: r.OperationKey, Writer: writer}, AttemptID: r.AttemptID, ExpiresAt: r.ExpiresAt}
		intent, e := a.manager.Prepare(request)
		if e != nil {
			return Invalid
		}
		if !intent.ExpiresAt.After(a.config.Now()) {
			return NotAuthorized
		}
		if e = a.registry.permits(tc.CandidateWrite{Intent: intent}); e != nil {
			return e
		}
		call := &invocation{actor: a, intent: intent, writer: writer, live: true}
		a.invocation = call
		defer func() { call.live = false; a.invocation = nil }()
		result, e = a.manager.Advance(context.WithValue(ctx, invocationKey{}, call), request)
		if e != nil {
			return e
		}
		if _, e = a.current(ctx); e != nil {
			return e
		}
		return a.registry.check()
	})
	return result, e
}

// Quiesce drains callbacks by taking the actor and registry lifecycle locks.
// Unknown metadata/provider phases block handoff. The returned exact history
// high-water prevents a clean successor from accepting an older valid prefix;
// crash recovery without this independent witness remains unsupported.
func (a *Actor) Quiesce(ctx context.Context) (Checkpoint, error) {
	if a == nil {
		return Checkpoint{}, Invalid
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return Checkpoint{}, Closed
	}
	// Close intent is monotone before any filesystem/admission I/O, including
	// canceled or failed acquisition. Queued calls must never resume dispatch.
	a.closed = true
	var result Checkpoint
	e := a.registry.locked(ctx, func() error {
		if _, e := a.authority(ctx); e != nil {
			return e
		}
		h, e := a.metadata.History(ctx)
		if e != nil {
			return Blocked
		}
		if e = a.historySafe(h); e != nil {
			return e
		}
		ready, e := a.gate.Ready(ctx)
		if e != nil {
			return Uncertain
		}
		raw, e := ready.JSON()
		if e != nil {
			return Invalid
		}
		result = Checkpoint{checkpoint{Schema: a.checkpointSchema(), Registry: a.config.RegistryBinding, Metadata: a.config.MetadataBinding, HistoryCount: len(h), HistoryDigest: historyDigest(h), Handoff: raw, AuthObservationPolicyDigest: a.registry.config.AuthObservationPolicyDigest}}
		_, e = result.JSON()
		return e
	})
	return result, e
}

func (a *Actor) checkpointSchema() int {
	if a.config.Mode == LocalDisposableStagingAuthReceipts {
		return 2
	}
	return 1
}

type invocationKey struct{}
type invocation struct {
	actor                              *Actor
	intent                             tc.CredentialIntent
	writer                             ownership.Writer
	live, namespaceUsed, candidateUsed bool
}
type namespaceGuard struct{ actor *Actor }

func (g namespaceGuard) Run(ctx context.Context, id string, w ownership.Writer, fn func(context.Context) error) error {
	a := g.actor
	call, ok := ctx.Value(invocationKey{}).(*invocation)
	if !ok || call == nil || call != a.invocation || call.actor != a || !call.live || call.namespaceUsed || id != call.intent.NamespaceID || w != call.writer {
		return NotAuthorized
	}
	if writer, e := a.current(ctx); e != nil {
		return e
	} else if writer != call.writer {
		return NotAuthorized
	}
	if e := a.registry.check(); e != nil {
		return e
	}
	call.namespaceUsed = true
	return fn(ctx)
}

type candidateGuard struct{ actor *Actor }

func (g candidateGuard) Run(ctx context.Context, w tc.CandidateWrite, fn func(context.Context) error) error {
	a := g.actor
	call, ok := ctx.Value(invocationKey{}).(*invocation)
	if !ok || call == nil || call != a.invocation || call.actor != a || !call.live || !call.namespaceUsed || call.candidateUsed || !reflect.DeepEqual(w.Intent, call.intent) {
		return NotAuthorized
	}
	if writer, e := a.current(ctx); e != nil {
		return e
	} else if writer != call.writer {
		return NotAuthorized
	}
	if e := a.registry.check(); e != nil {
		return e
	}
	state, found, e := a.metadata.CredentialJournal().Load(ctx, w.Intent.SlotID)
	if e != nil {
		return Blocked
	}
	if !found || state.Phase != tc.CredentialStoreRequested || !reflect.DeepEqual(state.Intent, w.Intent) || !reflect.DeepEqual(state.Key, w.Key) || state.CandidateWriteID != w.WriteID || state.ServiceAccount.ID != w.Key.ServiceAccountID || !w.Key.Verified || w.Key.VersionDigest == "" || !call.intent.ExpiresAt.After(a.config.Now()) {
		return NotAuthorized
	}
	if e = a.registry.permits(w); e != nil {
		return e
	}
	call.candidateUsed = true
	return fn(ctx)
}

func strictDecode(raw []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(v) != nil || d.Decode(new(any)) != io.EOF {
		return Invalid
	}
	return nil
}
