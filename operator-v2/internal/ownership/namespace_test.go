package ownership

import (
	"strings"
	"testing"
	"time"
)

type namespaceFixture struct {
	base     Context
	context  NamespacedContext
	parent   Record
	observed Observation
	writer   Writer
	now      time.Time
}

func ownedNamespace(t *testing.T) *namespaceFixture {
	t.Helper()
	cfg := config()
	cfg.Scopes = []Scope{{Provider: Kubernetes, ClusterID: "kind-lab"}}
	base := contextFor(t, cfg)
	intent, err := base.Intent(Kubernetes, "runtime-namespace", "revision-one", "revision-one", "namespace-create", "namespace-spec", 63)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 30, 1, 0, 0, 0, time.UTC)
	writer := Writer{ID: "writer-one", Owner: base.Owner(), Epoch: 4, ExpiresAt: now.Add(time.Hour)}
	observed := Observation{Ref: ResourceRef{Scope: intent.Scope, Purpose: intent.Purpose, Name: intent.Name, ProviderID: "canonical-namespace-uid"}, Owner: intent.Owner, CreationIdempotencyKey: intent.IdempotencyKey, Revision: intent.Revision, SpecFingerprint: intent.SpecFingerprint, WriterEpoch: writer.Epoch, Version: "namespace-version-one"}
	parent, err := base.BindCreated(intent, observed, writer, now)
	if err != nil {
		t.Fatal(err)
	}
	context, err := base.BindNamespace(parent, observed)
	if err != nil {
		t.Fatal(err)
	}
	return &namespaceFixture{base, context, parent, observed, writer, now}
}

func (f *namespaceFixture) child(t *testing.T) (NamespacedIntent, NamespacedRecord, NamespacedObservation) {
	t.Helper()
	intent, err := f.context.Intent("service", "backend", "child-revision", "child-create", "child-spec")
	if err != nil {
		t.Fatal(err)
	}
	observed := NamespacedObservation{ParentUID: f.parent.Ref.ProviderID, Observation: Observation{Ref: ResourceRef{Scope: intent.Intent.Scope, Purpose: intent.Intent.Purpose, Name: intent.Intent.Name, ProviderID: "canonical-service-uid"}, Owner: f.base.Owner(), CreationIdempotencyKey: intent.Intent.IdempotencyKey, Revision: intent.Intent.Revision, SpecFingerprint: intent.Intent.SpecFingerprint, WriterEpoch: f.writer.Epoch, Version: "service-version-one"}}
	record, err := f.context.BindCreated(intent, f.observed, observed, f.writer, f.now)
	if err != nil {
		t.Fatal(err)
	}
	return intent, record, observed
}

func TestLogicalServiceNameRequiresVerifiedOwnedNamespace(t *testing.T) {
	f := ownedNamespace(t)
	intent, record, observed := f.child(t)
	if intent.Intent.Name != "backend" || intent.Intent.Scope.Namespace != f.parent.Ref.Name || f.context.NamespaceName() != f.parent.Ref.Name {
		t.Fatal("stable application DNS name was changed")
	}
	if f.context.ParentRevision() != f.parent.Revision {
		t.Fatal("parent-bound revision was lost")
	}
	if f.context.ParentPurpose() != f.parent.Ref.Purpose {
		t.Fatal("parent-bound purpose was lost")
	}
	creation, err := f.context.AuthorizeCreate(intent, f.observed, f.writer, f.now)
	if err != nil {
		t.Fatal(err)
	}
	if creation.Parent.Parent.UID != f.parent.Ref.ProviderID || creation.Parent.RequiredProtection != RequireSerializedParentLifecycle || creation.Parent.UIDAnnotation != ParentNamespaceUIDAnnotation {
		t.Fatalf("creation lost required parent lifecycle/UID enforcement: %+v", creation)
	}
	for _, action := range []Action{Mutate, Adopt, Promote, Cleanup} {
		preconditions, err := f.context.Authorize(NamespacedMutationRequest{Action: action, Record: record, Observed: observed, ParentObserved: f.observed, ExpectedRevision: record.Record.Revision, Writer: f.writer, Now: f.now})
		if err != nil {
			t.Fatal(err)
		}
		if preconditions.Mutation.Ref.ProviderID != "canonical-service-uid" || preconditions.Mutation.ProviderVersion != "service-version-one" || preconditions.Parent.Parent.UID != "canonical-namespace-uid" || preconditions.Parent.RequiredProtection != RequireParentAndChildUIDPreconditions {
			t.Fatalf("missing canonical child/parent preconditions: %+v", preconditions)
		}
	}
}

func TestUnownedProductionOrRecreatedParentCannotBindAuthority(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*Record, *Observation)
		code   ErrorCode
	}{
		{"production name", func(r *Record, o *Observation) { r.Ref.Name = "tmprl-dem-cld-production"; o.Ref.Name = r.Ref.Name }, IdentityMismatch},
		{"foreign cluster", func(r *Record, o *Observation) {
			r.Ref.Scope.ClusterID = "production-cluster"
			o.Ref.Scope = r.Ref.Scope
		}, ForeignScope},
		{"foreign owner", func(r *Record, o *Observation) { o.Owner.ProjectUID = "foreign" }, ForeignOwner},
		{"name reused with new UID", func(r *Record, o *Observation) { o.Ref.ProviderID = "replacement-namespace-uid" }, IdentityMismatch},
		{"missing UID", func(r *Record, o *Observation) { o.Ref.ProviderID = "" }, IdentityMismatch},
		{"missing creation token", func(r *Record, o *Observation) { o.CreationIdempotencyKey = "" }, UnprovenCreation},
		{"foreign creation token", func(r *Record, o *Observation) { o.CreationIdempotencyKey = "foreign-operation" }, UnprovenCreation},
		{"stale parent revision", func(r *Record, o *Observation) { o.Revision = "another-intent" }, StaleRevision},
		{"stale parent epoch", func(r *Record, o *Observation) { o.WriterEpoch-- }, StaleWriter},
		{"missing parent version", func(r *Record, o *Observation) { o.Version = "" }, FencingRequired},
		{"non-namespace parent", func(r *Record, o *Observation) { r.Ref.Purpose = "service"; o.Ref.Purpose = "service" }, ForeignScope},
		{"parent path alias", func(r *Record, o *Observation) { r.Ref.Name = "lab/production"; o.Ref.Name = r.Ref.Name }, ForeignScope},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := ownedNamespace(t)
			parent, observed := f.parent, f.observed
			test.mutate(&parent, &observed)
			_, err := f.base.BindNamespace(parent, observed)
			requireCode(t, err, test.code)
		})
	}
}

func TestParentChangesInvalidateExistingChildCleanup(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*NamespacedMutationRequest)
		code   ErrorCode
	}{
		{"recreated parent", func(r *NamespacedMutationRequest) { r.ParentObserved.Ref.ProviderID = "replacement-namespace-uid" }, IdentityMismatch},
		{"foreign observed parent owner", func(r *NamespacedMutationRequest) { r.ParentObserved.Owner.EnvironmentID = "production" }, ForeignOwner},
		{"missing parent observation", func(r *NamespacedMutationRequest) { r.ParentObserved = Observation{} }, ForeignOwner},
		{"child has foreign parent marker", func(r *NamespacedMutationRequest) { r.Observed.ParentUID = "replacement-namespace-uid" }, IdentityMismatch},
		{"child ledger has old parent", func(r *NamespacedMutationRequest) { r.Record.Parent.UID = "old-parent-uid" }, IdentityMismatch},
		{"child is cluster-scoped", func(r *NamespacedMutationRequest) { r.Observed.Observation.Ref.Scope.Namespace = "" }, ForeignScope},
		{"child in foreign namespace", func(r *NamespacedMutationRequest) { r.Observed.Observation.Ref.Scope.Namespace = "production" }, ForeignScope},
		{"child in foreign cluster", func(r *NamespacedMutationRequest) { r.Observed.Observation.Ref.Scope.ClusterID = "production-cluster" }, ForeignScope},
		{"foreign child owner", func(r *NamespacedMutationRequest) { r.Observed.Observation.Owner.ProjectUID = "foreign" }, ForeignOwner},
		{"recreated child", func(r *NamespacedMutationRequest) { r.Observed.Observation.Ref.ProviderID = "replacement-child-uid" }, IdentityMismatch},
		{"missing child UID", func(r *NamespacedMutationRequest) { r.Observed.Observation.Ref.ProviderID = "" }, IdentityMismatch},
		{"missing child version", func(r *NamespacedMutationRequest) { r.Observed.Observation.Version = "" }, FencingRequired},
		{"stale child revision", func(r *NamespacedMutationRequest) { r.ExpectedRevision = "old-revision" }, StaleRevision},
		{"stale writer", func(r *NamespacedMutationRequest) { r.Writer.Epoch-- }, StaleWriter},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := ownedNamespace(t)
			_, record, observed := f.child(t)
			request := NamespacedMutationRequest{Action: Cleanup, Record: record, Observed: observed, ParentObserved: f.observed, ExpectedRevision: record.Record.Revision, Writer: f.writer, Now: f.now}
			test.mutate(&request)
			_, err := f.context.Authorize(request)
			requireCode(t, err, test.code)
		})
	}
}

func TestNamespacedExceptionNeverRelaxesGenericProviderChecks(t *testing.T) {
	f := ownedNamespace(t)
	intent, record, observed := f.child(t)
	_, err := f.base.AuthorizeCreate(intent.Intent, f.writer, f.now)
	requireCode(t, err, ForeignScope)
	_, err = f.base.Authorize(MutationRequest{Action: Cleanup, Record: record.Record, Observed: observed.Observation, ExpectedRevision: record.Record.Revision, Writer: f.writer, Now: f.now, Fencing: ProviderConditional})
	requireCode(t, err, ForeignScope)
	observed.Observation.Ref.Scope = Scope{Provider: AWS, Partition: "aws", AccountID: "123456789012", Region: "us-west-1"}
	_, err = f.context.BindCreated(intent, f.observed, observed, f.writer, f.now)
	requireCode(t, err, ForeignScope)
	observed.Observation.Ref.Scope = Scope{Provider: Temporal, AccountID: "test-account", Namespace: "control"}
	_, err = f.context.BindCreated(intent, f.observed, observed, f.writer, f.now)
	requireCode(t, err, ForeignScope)
	cfg := config()
	cfg.Scopes = []Scope{{Provider: Kubernetes, ClusterID: "kind-lab", Namespace: "lab-other"}}
	_, err = contextFor(t, cfg).BindNamespace(f.parent, f.observed)
	requireCode(t, err, ForeignScope)
}

func TestNamespacedIntentsAreStableAndIncludeParentIncarnation(t *testing.T) {
	f := ownedNamespace(t)
	first, _, _ := f.child(t)
	retry, err := f.context.Intent("service", "backend", "child-revision", "child-create", "child-spec")
	if err != nil || first != retry {
		t.Fatalf("retry changed identity: %v", err)
	}
	parent, observed := f.parent, f.observed
	parent.Ref.ProviderID = "new-parent-uid"
	observed.Ref.ProviderID = parent.Ref.ProviderID
	parent.CreationOperationID = "replacement-create"
	parent.CreationIdempotencyKey = "replacement-token"
	observed.CreationIdempotencyKey = parent.CreationIdempotencyKey
	replacement, err := f.base.BindNamespace(parent, observed)
	if err != nil {
		t.Fatal(err)
	}
	second, err := replacement.Intent("service", "backend", "child-revision", "child-create", "child-spec")
	if err != nil {
		t.Fatal(err)
	}
	if second.Intent.Name != "backend" || second.Intent.IdempotencyKey == first.Intent.IdempotencyKey {
		t.Fatal("name reuse shared old parent creation authority")
	}
	mutated := f.context.Parent()
	mutated.UID = "caller-mutation"
	if f.context.Parent().UID != f.parent.Ref.ProviderID {
		t.Fatal("parent accessor aliases immutable context")
	}
}

func TestNamespacedCreationRequiresParentAndIndependentChildProvenance(t *testing.T) {
	f := ownedNamespace(t)
	intent, _, observed := f.child(t)
	observed.ParentUID = ""
	_, err := f.context.BindCreated(intent, f.observed, observed, f.writer, f.now)
	requireCode(t, err, IdentityMismatch)
	_, _, observed = f.child(t)
	observed.Observation.CreationIdempotencyKey = ""
	_, err = f.context.BindCreated(intent, f.observed, observed, f.writer, f.now)
	requireCode(t, err, UnprovenCreation)
	_, _, observed = f.child(t)
	observed.Observation.Ref.Scope.Namespace = "production"
	_, err = f.context.BindCreated(intent, f.observed, observed, f.writer, f.now)
	requireCode(t, err, ForeignScope)
	for _, name := range []string{"../production", "backend/production", "Backend", "backend..production", strings.Repeat("a", 64)} {
		_, err := f.context.Intent("service", name, "r1", "op1", "fp1")
		requireCode(t, err, InvalidContext)
	}
	_, err = (NamespacedContext{}).Intent("service", "backend", "r1", "op1", "fp1")
	requireCode(t, err, InvalidContext)
}
