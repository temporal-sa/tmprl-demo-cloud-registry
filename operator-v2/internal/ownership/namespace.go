package ownership

import (
	"regexp"
	"strings"
	"time"
)

const ParentNamespaceUIDAnnotation = "registry.tmprl-demo.cloud/parent-namespace-uid"

// NamespaceParent pins stable namespace ownership and incarnation. It omits
// mutable revision, resourceVersion and writer epoch: ordinary namespace edits
// must not rename or replace application Service/Deployment identities.
type NamespaceParent struct {
	ClusterID              string
	Name                   string
	UID                    string
	Owner                  Owner
	CreationOperationID    string
	CreationIdempotencyKey string
}

type ParentProtection string

const (
	// Kubernetes child creates cannot atomically compare Namespace UID using
	// a normal pre-read. The executor must serialize/drain the parent lifecycle
	// and verify the expected UID marker (optionally through admission). A
	// webhook which only performs another pre-read is not an atomic guarantee.
	RequireSerializedParentLifecycle      ParentProtection = "requires_serialized_parent_namespace_lifecycle"
	RequireParentAndChildUIDPreconditions ParentProtection = "requires_parent_and_child_uid_preconditions"
)

type ParentPreconditions struct {
	Parent             NamespaceParent
	ResourceVersion    string
	RequiredProtection ParentProtection
	UIDAnnotation      string
}

// NamespacedContext is constructed only after observing an owned cluster-scoped
// namespace. It allows logical child names such as backend without weakening
// Context's generic name checks or authorizing another provider/namespace.
type NamespacedContext struct {
	base     Context
	parent   Record
	identity NamespaceParent
	scope    Scope
}

func (c Context) BindNamespace(parent Record, current Observation) (NamespacedContext, error) {
	scope, err := c.Scope(Kubernetes)
	if err != nil {
		return NamespacedContext{}, err
	}
	if scope.Namespace != "" || parent.Ref.Scope != scope || parent.Ref.Purpose != "namespace" && parent.Ref.Purpose != "runtime-namespace" || !slugPattern.MatchString(parent.Ref.Name) || len(parent.Ref.Name) > 63 {
		return NamespacedContext{}, reject(ForeignScope, "parent must be an explicitly owned cluster-scoped Kubernetes Namespace")
	}
	if err := c.checkRef(parent.Ref); err != nil {
		return NamespacedContext{}, err
	}
	identity := NamespaceParent{ClusterID: scope.ClusterID, Name: parent.Ref.Name, UID: parent.Ref.ProviderID, Owner: parent.Owner, CreationOperationID: parent.CreationOperationID, CreationIdempotencyKey: parent.CreationIdempotencyKey}
	namespaced := NamespacedContext{base: c, parent: parent, identity: identity, scope: Scope{Provider: Kubernetes, ClusterID: scope.ClusterID, Namespace: parent.Ref.Name}}
	if err := namespaced.validateParent(current); err != nil {
		return NamespacedContext{}, err
	}
	return namespaced, nil
}

func (c NamespacedContext) Owner() Owner            { return c.base.Owner() }
func (c NamespacedContext) Scope() Scope            { return c.scope }
func (c NamespacedContext) Parent() NamespaceParent { return c.identity }
func (c NamespacedContext) NamespaceName() string   { return c.scope.Namespace }

// ParentRevision is the revision attested by the verified parent binding.
// Rendering another candidate must not silently reuse this namespace.
func (c NamespacedContext) ParentRevision() string { return c.parent.Revision }

// ParentPurpose describes the sealed namespace binding; it grants no privilege.
func (c NamespacedContext) ParentPurpose() string { return c.parent.Ref.Purpose }

type NamespacedIntent struct {
	Intent Intent
	Parent NamespaceParent
}

type NamespacedRecord struct {
	Record Record
	Parent NamespaceParent
}

// ParentUID is independently read from the child's owner reference or expected
// parent UID marker. It is never filled by copying the desired intent. The
// current parent observation is also mandatory: child metadata alone cannot
// authorize a recreated namespace with the same name.
type NamespacedObservation struct {
	Observation Observation
	ParentUID   string
}

type NamespacedCreationPreconditions struct {
	Creation CreationPreconditions
	Parent   ParentPreconditions
}

type NamespacedMutationRequest struct {
	Action           Action
	Record           NamespacedRecord
	Observed         NamespacedObservation
	ParentObserved   Observation
	ExpectedRevision string
	Writer           Writer
	Now              time.Time
}

type NamespacedPreconditions struct {
	Mutation Preconditions
	Parent   ParentPreconditions
}

var localKubernetesName = regexp.MustCompile(`^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?(?:\.[a-z0-9](?:[-a-z0-9]*[a-z0-9])?)*$`)

// Intent preserves the logical name verbatim. Capability adapters must select
// an actually namespaced Kubernetes GVK and enforce its own name restrictions;
// a forged Scope.Namespace is not a substitute for API scope discovery/schema.
func (c NamespacedContext) Intent(purpose, logicalName, revision, operationID, specFingerprint string) (NamespacedIntent, error) {
	if c.identity.UID == "" || c.scope.Provider != Kubernetes || c.scope.Namespace != c.identity.Name {
		return NamespacedIntent{}, invalid("a verified parent namespace context is required")
	}
	if !validLocalName(logicalName) || !slugPattern.MatchString(purpose) || len(purpose) > 64 || !validID(revision) || !validID(operationID) || !validID(specFingerprint) {
		return NamespacedIntent{}, invalid("explicit namespaced purpose, valid logical name and operation provenance are required")
	}
	intent := Intent{Owner: c.Owner(), Scope: c.scope, Purpose: purpose, Name: logicalName, Revision: revision, OperationID: operationID, SpecFingerprint: specFingerprint}
	intent.IdempotencyKey = digestOf(intent.Owner, c.identity, intent.Scope, intent.Purpose, intent.Name, intent.Revision, intent.OperationID, intent.SpecFingerprint)
	return NamespacedIntent{Intent: intent, Parent: c.identity}, nil
}

func (c NamespacedContext) AuthorizeCreate(intent NamespacedIntent, currentParent Observation, writer Writer, now time.Time) (NamespacedCreationPreconditions, error) {
	if err := c.validateParent(currentParent); err != nil {
		return NamespacedCreationPreconditions{}, err
	}
	if err := c.checkIntent(intent); err != nil {
		return NamespacedCreationPreconditions{}, err
	}
	if err := c.base.checkWriter(writer, now); err != nil {
		return NamespacedCreationPreconditions{}, err
	}
	if writer.Epoch != c.parent.WriterEpoch {
		return NamespacedCreationPreconditions{}, reject(StaleWriter, "child creation writer differs from the active parent owner epoch")
	}
	return NamespacedCreationPreconditions{Creation: CreationPreconditions{Intent: intent.Intent, WriterID: writer.ID, WriterEpoch: writer.Epoch}, Parent: c.parentPreconditions(currentParent, RequireSerializedParentLifecycle)}, nil
}

func (c NamespacedContext) BindCreated(intent NamespacedIntent, currentParent Observation, observed NamespacedObservation, writer Writer, now time.Time) (NamespacedRecord, error) {
	if _, err := c.AuthorizeCreate(intent, currentParent, writer, now); err != nil {
		return NamespacedRecord{}, err
	}
	if err := c.checkChildRef(observed.Observation.Ref); err != nil {
		return NamespacedRecord{}, err
	}
	o, i := observed.Observation, intent.Intent
	if observed.ParentUID != c.identity.UID {
		return NamespacedRecord{}, reject(IdentityMismatch, "created child belongs to another parent namespace incarnation")
	}
	if o.Owner != c.Owner() {
		return NamespacedRecord{}, reject(ForeignOwner, "created child has another owner")
	}
	if o.Ref.Scope != i.Scope || o.Ref.Purpose != i.Purpose || o.Ref.Name != i.Name {
		return NamespacedRecord{}, reject(IdentityMismatch, "created child does not match the persisted intent")
	}
	if o.CreationIdempotencyKey != i.IdempotencyKey {
		return NamespacedRecord{}, reject(UnprovenCreation, "child creation token does not match persisted intent")
	}
	if o.Revision != i.Revision || o.SpecFingerprint != i.SpecFingerprint {
		return NamespacedRecord{}, reject(StaleRevision, "created child revision or fingerprint differs")
	}
	if o.WriterEpoch != writer.Epoch {
		return NamespacedRecord{}, reject(StaleWriter, "created child writer epoch differs")
	}
	return NamespacedRecord{Record: Record{Ref: o.Ref, Owner: o.Owner, CreationOperationID: i.OperationID, CreationIdempotencyKey: i.IdempotencyKey, Revision: i.Revision, SpecFingerprint: i.SpecFingerprint, WriterEpoch: writer.Epoch}, Parent: c.identity}, nil
}

func (c NamespacedContext) Authorize(request NamespacedMutationRequest) (NamespacedPreconditions, error) {
	if err := c.validateParent(request.ParentObserved); err != nil {
		return NamespacedPreconditions{}, err
	}
	if request.Record.Parent != c.identity || request.Observed.ParentUID != c.identity.UID {
		return NamespacedPreconditions{}, reject(IdentityMismatch, "child record and observation must bind the current namespace incarnation")
	}
	if request.Writer.Epoch != c.parent.WriterEpoch {
		return NamespacedPreconditions{}, reject(StaleWriter, "child writer differs from the active parent owner epoch")
	}
	preconditions, err := c.base.authorizeWithRefCheck(MutationRequest{Action: request.Action, Record: request.Record.Record, Observed: request.Observed.Observation, ExpectedRevision: request.ExpectedRevision, Writer: request.Writer, Now: request.Now, Fencing: ProviderConditional}, c.checkChildRef)
	if err != nil {
		return NamespacedPreconditions{}, err
	}
	return NamespacedPreconditions{Mutation: preconditions, Parent: c.parentPreconditions(request.ParentObserved, RequireParentAndChildUIDPreconditions)}, nil
}

func (c NamespacedContext) parentPreconditions(observed Observation, protection ParentProtection) ParentPreconditions {
	return ParentPreconditions{Parent: c.identity, ResourceVersion: observed.Version, RequiredProtection: protection, UIDAnnotation: ParentNamespaceUIDAnnotation}
}

func (c NamespacedContext) validateParent(observed Observation) error {
	p := c.parent
	if p.Owner != c.base.Owner() || observed.Owner != c.base.Owner() {
		return reject(ForeignOwner, "parent namespace has another owner")
	}
	if err := c.base.checkRef(observed.Ref); err != nil {
		return err
	}
	if p.Ref != observed.Ref || p.Ref.ProviderID == "" {
		return reject(IdentityMismatch, "parent namespace UID, scope or name changed")
	}
	if !validID(p.CreationOperationID) || !validID(p.CreationIdempotencyKey) || p.CreationIdempotencyKey != observed.CreationIdempotencyKey {
		return reject(UnprovenCreation, "parent namespace lacks independent creation provenance")
	}
	if !validID(p.Revision) || p.Revision != observed.Revision || !validID(p.SpecFingerprint) || p.SpecFingerprint != observed.SpecFingerprint {
		return reject(StaleRevision, "parent namespace intent differs from its trusted ledger")
	}
	if p.WriterEpoch == 0 || p.WriterEpoch != observed.WriterEpoch {
		return reject(StaleWriter, "parent namespace writer epoch differs")
	}
	if !validID(observed.Version) {
		return reject(FencingRequired, "independently observed parent namespace version is required")
	}
	return nil
}

func (c NamespacedContext) checkIntent(intent NamespacedIntent) error {
	i := intent.Intent
	if intent.Parent != c.identity {
		return reject(IdentityMismatch, "child intent binds another parent namespace incarnation")
	}
	if i.Owner != c.Owner() {
		return reject(ForeignOwner, "child intent has another owner")
	}
	if i.Scope != c.scope {
		return reject(ForeignScope, "child intent is outside the owned Kubernetes namespace")
	}
	if !validLocalName(i.Name) || !slugPattern.MatchString(i.Purpose) || !validID(i.Revision) || !validID(i.OperationID) || !validID(i.SpecFingerprint) || i.IdempotencyKey != digestOf(i.Owner, c.identity, i.Scope, i.Purpose, i.Name, i.Revision, i.OperationID, i.SpecFingerprint) {
		return reject(UnprovenCreation, "child intent is invalid or its creation provenance changed")
	}
	return nil
}

func (c NamespacedContext) checkChildRef(ref ResourceRef) error {
	if ref.Scope != c.scope {
		return reject(ForeignScope, "child must be a Kubernetes object in this exact cluster and namespace")
	}
	if !validLocalName(ref.Name) || !slugPattern.MatchString(ref.Purpose) || !validID(ref.ProviderID) {
		return reject(IdentityMismatch, "child name, purpose and canonical provider UID are required")
	}
	return nil
}

func validLocalName(value string) bool {
	if len(value) == 0 || len(value) > 253 || !localKubernetesName.MatchString(value) {
		return false
	}
	for _, label := range strings.Split(value, ".") {
		if len(label) > 63 {
			return false
		}
	}
	return true
}
