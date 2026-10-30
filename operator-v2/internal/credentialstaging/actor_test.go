package credentialstaging

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	awstypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	awsstore "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/awssecrets"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubejournal"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/localmetadata"
	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
	cloud "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	identity "go.temporal.io/cloud-sdk/api/identity/v1"
	namespace "go.temporal.io/cloud-sdk/api/namespace/v1"
	operation "go.temporal.io/cloud-sdk/api/operation/v1"
	resource "go.temporal.io/cloud-sdk/api/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

var background = context.Background()
var now = time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)

const material = "STAGING-ACTOR-SYNTHETIC-ONE-TIME-MATERIAL-52dd"

// This is an owned synchronous fake provider, not a Kubernetes API or real
// permission/finality test. The actual pinned Cell/Repository/admission run.
type controlBinding struct {
	mu                             sync.Mutex
	bound                          ownership.NamespacedContext
	record                         ownership.NamespacedRecord
	object                         *corev1.ConfigMap
	version                        int
	observeEntered, observeRelease chan struct{}
	observeError                   error
}

func (b *controlBinding) Context() ownership.NamespacedContext { return b.bound }
func (b *controlBinding) ObserveConfigMap(context.Context, ownership.NamespacedRecord) (kube.ConfigMapSnapshot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.observeEntered != nil {
		entered, release, err := b.observeEntered, b.observeRelease, b.observeError
		b.observeEntered, b.observeRelease, b.observeError = nil, nil, nil
		close(entered)
		<-release
		if err != nil {
			return kube.ConfigMapSnapshot{}, err
		}
	}
	r := b.record.Record
	return kube.ConfigMapSnapshot{Object: b.object.DeepCopy(), Observation: ownership.NamespacedObservation{ParentUID: b.bound.Parent().UID, Observation: ownership.Observation{Ref: r.Ref, Owner: r.Owner, CreationIdempotencyKey: r.CreationIdempotencyKey, Revision: r.Revision, SpecFingerprint: r.SpecFingerprint, WriterEpoch: r.WriterEpoch, Version: b.object.ResourceVersion}}}, nil
}
func (b *controlBinding) UpdateConfigMap(_ context.Context, r kube.ConfigMapUpdate) (ownership.NamespacedRecord, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if r.ExpectedVersion != b.object.ResourceVersion {
		return ownership.NamespacedRecord{}, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, b.object.Name, errors.New("conflict"))
	}
	b.version++
	b.object = r.Desired.DeepCopy()
	b.object.UID = types.UID(b.record.Record.Ref.ProviderID)
	b.object.ResourceVersion = strconv.Itoa(b.version)
	b.object.Annotations = map[string]string{kube.CreationOperationAnnotation: b.record.Record.CreationOperationID}
	b.record.Record.SpecFingerprint = r.Intent.Intent.SpecFingerprint
	return b.record, nil
}
func (b *controlBinding) ConfirmConfigMapUpdate(context.Context, kube.ConfigMapUpdate) (ownership.NamespacedRecord, error) {
	return ownership.NamespacedRecord{}, errors.New("no unknown fake writes")
}
func (b *controlBinding) HasUnknownConfigMapUpdate(kube.ConfigMapUpdate) (bool, error) {
	return false, nil
}

type fakeCloud struct {
	tc.Transport
	tc.CredentialTransport
	binding                 tc.Binding
	sa                      tc.ServiceAccountRef
	key                     *identity.ApiKey
	creates, deletes, reads int
	lost                    bool
	entered, release        chan struct{}
}

func (f *fakeCloud) GetNamespace(_ context.Context, r *cloud.GetNamespaceRequest, _ ...grpc.CallOption) (*cloud.GetNamespaceResponse, error) {
	f.reads++
	if r.Namespace != f.binding.NamespaceID {
		return nil, errors.New(material)
	}
	return &cloud.GetNamespaceResponse{Namespace: &namespace.Namespace{Namespace: r.Namespace, Spec: &namespace.NamespaceSpec{Name: f.binding.Record.Ref.Name, ApiKeyAuth: &namespace.ApiKeyAuthSpec{Enabled: true}}, CreatedTime: timestamppb.New(f.binding.CreatedAt), ResourceVersion: "namespace-v1", State: resource.ResourceState_RESOURCE_STATE_ACTIVE}}, nil
}
func (f *fakeCloud) GetServiceAccount(_ context.Context, r *cloud.GetServiceAccountRequest, _ ...grpc.CallOption) (*cloud.GetServiceAccountResponse, error) {
	if r.ServiceAccountId != f.sa.ID {
		return nil, errors.New(material)
	}
	slot := sum(struct {
		Owner       any
		Incarnation string
	}{f.binding.Record.Owner, f.binding.Record.Ref.ProviderID})
	return &cloud.GetServiceAccountResponse{ServiceAccount: &identity.ServiceAccount{Id: f.sa.ID, Spec: &identity.ServiceAccountSpec{Name: f.sa.Name, Description: "registry-sa:" + slot, NamespaceScopedAccess: &identity.NamespaceScopedAccess{Namespace: f.binding.NamespaceID, Access: &identity.NamespaceAccess{Permission: identity.NamespaceAccess_PERMISSION_WRITE}}}, CreatedTime: timestamppb.New(f.sa.CreatedAt), ResourceVersion: "sa-v1", State: resource.ResourceState_RESOURCE_STATE_ACTIVE}}, nil
}
func (f *fakeCloud) CreateApiKey(_ context.Context, r *cloud.CreateApiKeyRequest, _ ...grpc.CallOption) (*cloud.CreateApiKeyResponse, error) {
	f.creates++
	if f.entered != nil {
		close(f.entered)
		<-f.release
	}
	f.key = &identity.ApiKey{Id: "owned-stage-key", Spec: proto.Clone(r.Spec).(*identity.ApiKeySpec), CreatedTime: timestamppb.New(now), ResourceVersion: "key-v1", State: resource.ResourceState_RESOURCE_STATE_ACTIVE}
	if f.lost {
		return nil, errors.New(material)
	}
	return &cloud.CreateApiKeyResponse{KeyId: f.key.Id, Token: material, AsyncOperation: &operation.AsyncOperation{Id: r.AsyncOperationId}}, nil
}
func (f *fakeCloud) GetApiKey(context.Context, *cloud.GetApiKeyRequest, ...grpc.CallOption) (*cloud.GetApiKeyResponse, error) {
	return &cloud.GetApiKeyResponse{ApiKey: proto.Clone(f.key).(*identity.ApiKey)}, nil
}
func (f *fakeCloud) DeleteApiKey(context.Context, *cloud.DeleteApiKeyRequest, ...grpc.CallOption) (*cloud.DeleteApiKeyResponse, error) {
	f.deletes++
	return nil, errors.New(material)
}

type fakeAWS struct {
	metadata *secretsmanager.DescribeSecretOutput
	creates  int
	lost     bool
}

func (f *fakeAWS) DescribeSecret(_ context.Context, r *secretsmanager.DescribeSecretInput, _ ...func(*secretsmanager.Options)) (*secretsmanager.DescribeSecretOutput, error) {
	if f.metadata == nil {
		return nil, &awstypes.ResourceNotFoundException{}
	}
	if aws.ToString(r.SecretId) != aws.ToString(f.metadata.ARN) {
		return nil, errors.New(material)
	}
	return f.metadata, nil
}
func (f *fakeAWS) CreateSecret(_ context.Context, r *secretsmanager.CreateSecretInput, _ ...func(*secretsmanager.Options)) (*secretsmanager.CreateSecretOutput, error) {
	f.creates++
	if !bytes.Equal(r.SecretBinary, []byte(material)) {
		return nil, errors.New(material)
	}
	arn := "arn:aws:secretsmanager:us-west-2:123456789012:secret:" + aws.ToString(r.Name) + "-AbC123"
	f.metadata = &secretsmanager.DescribeSecretOutput{ARN: aws.String(arn), Name: r.Name, KmsKeyId: r.KmsKeyId, Tags: r.Tags, CreatedDate: aws.Time(now), VersionIdsToStages: map[string][]string{aws.ToString(r.ClientRequestToken): {"AWSCURRENT"}}}
	if f.lost {
		return nil, errors.New(material)
	}
	return &secretsmanager.CreateSecretOutput{ARN: aws.String(arn), Name: r.Name, VersionId: r.ClientRequestToken}, nil
}

type fixture struct {
	config       Config
	cloud        *fakeCloud
	aws          *fakeAWS
	control      *controlBinding
	request      Request
	registryPath string
}

func root(t *testing.T) (*os.Root, string) {
	t.Helper()
	p := t.TempDir()
	if e := os.Chmod(p, 0700); e != nil {
		t.Fatal(e)
	}
	r, e := os.OpenRoot(p)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r, p
}
func must[T any](v T, e error) T {
	if e != nil {
		panic(e)
	}
	return v
}

func setup(t *testing.T, reservations []Reservation) *fixture {
	t.Helper()
	owner := ownership.Owner{EnvironmentID: "lab", InstallationID: "staging-tests", ProjectUID: "user-project", Lifetime: ownership.Disposable, RunID: "run-one"}
	p := must(ownership.NewContext(ownership.Config{Owner: owner, ResourceRoot: "rv2", Scopes: []ownership.Scope{{Provider: ownership.Temporal, AccountID: "testacct", Namespace: "fixture"}, {Provider: ownership.AWS, Partition: "aws", AccountID: "123456789012", Region: "us-west-2"}}}))
	w := ownership.Writer{ID: "staging-writer", Owner: owner, Epoch: 1, ExpiresAt: now.Add(time.Hour)}
	i := must(p.Intent(ownership.Temporal, "namespace", "primary", "fixture-v1", "namespace-create", "fixture-fingerprint", 39))
	id := i.Name + ".testacct"
	born := now.Add(-time.Hour)
	r := must(p.BindCreated(i, ownership.Observation{Ref: ownership.ResourceRef{Scope: i.Scope, Purpose: i.Purpose, Name: i.Name, ProviderID: tc.IncarnationID(id, born)}, Owner: owner, CreationIdempotencyKey: i.IdempotencyKey, Revision: i.Revision, SpecFingerprint: i.SpecFingerprint, WriterEpoch: 1, Version: "namespace-v1"}, w, now))
	ns := tc.Binding{NamespaceID: id, CreatedAt: born, ProvenanceID: "independent-namespace-create", Record: r}
	slot := sum(struct {
		Owner       any
		Incarnation string
	}{owner, r.Ref.ProviderID})
	saName := must(p.Name(ownership.Temporal, "service-account", slot, 63))
	sa := tc.ServiceAccountRef{ID: "preprovisioned-owned-sa", Name: saName, CreatedAt: now.Add(-time.Minute), VersionDigest: tc.ProviderVersionDigest("sa-v1"), ProvenanceID: "fixture-sa-create"}
	controlOwner := owner
	controlOwner.ProjectUID = "control-plane"
	controlOwner.Lifetime = ownership.Stable
	controlOwner.RunID = ""
	cc := must(ownership.NewContext(ownership.Config{Owner: controlOwner, ResourceRoot: "rv2", Scopes: []ownership.Scope{{Provider: ownership.Kubernetes, ClusterID: "owned-cluster"}}}))
	cw := w
	cw.Owner = controlOwner
	ni := must(cc.Intent(ownership.Kubernetes, "namespace", "control", "control-v1", "control-bootstrap", "control-fingerprint", 63))
	po := ownership.Observation{Ref: ownership.ResourceRef{Scope: ni.Scope, Purpose: ni.Purpose, Name: ni.Name, ProviderID: "control-parent-uid"}, Owner: controlOwner, CreationIdempotencyKey: ni.IdempotencyKey, Revision: ni.Revision, SpecFingerprint: ni.SpecFingerprint, WriterEpoch: 1, Version: "1"}
	pr := must(cc.BindCreated(ni, po, cw, now))
	bound := must(cc.BindNamespace(pr, po))
	subject := rollout.Subject{Environment: owner.EnvironmentID, Installation: owner.InstallationID, ProjectUID: owner.ProjectUID}
	initial := must(journal.Initial(subject, 1))
	object := must(kubejournal.BootstrapObject(bound.NamespaceName(), initial))
	fingerprint := must(kube.ConfigMapFingerprint(object))
	ci := must(bound.Intent("configmap", object.Name, kubejournal.Revision, "cell-bootstrap", fingerprint))
	co := ownership.NamespacedObservation{ParentUID: bound.Parent().UID, Observation: ownership.Observation{Ref: ownership.ResourceRef{Scope: bound.Scope(), Purpose: "configmap", Name: object.Name, ProviderID: "control-cell-uid"}, Owner: controlOwner, CreationIdempotencyKey: ci.Intent.IdempotencyKey, Revision: kubejournal.Revision, SpecFingerprint: fingerprint, WriterEpoch: 1, Version: "1"}}
	cr := must(bound.BindCreated(ci, po, co, cw, now))
	object.UID = types.UID(cr.Record.Ref.ProviderID)
	object.ResourceVersion = "1"
	object.Annotations = map[string]string{kube.CreationOperationAnnotation: cr.Record.CreationOperationID}
	cb := &controlBinding{bound: bound, record: cr, object: object, version: 1}
	cell := must(kubejournal.New(kubejournal.Config{Namespace: cb, Pinned: cr, Subject: subject, Writer: func() ownership.Writer { return cw }}))
	metadataRoot, _ := root(t)
	mc := localmetadata.Config{BootstrapUID: "staging-metadata-bootstrap", Ownership: p, NamespaceID: id, NamespaceCreatedAt: born, NamespaceIncarnationID: r.Ref.ProviderID, StoreID: "fixture-store", PreprovisionedServiceAccount: sa, MaximumRevisions: 64, MaximumRecordBytes: 32768}
	_, mb, e := localmetadata.Bootstrap(background, metadataRoot, mc)
	if e != nil {
		t.Fatal(e)
	}
	registryRoot, path := root(t)
	rc := RegistryConfig{BootstrapUID: "staging-registry-bootstrap", Owner: owner, Control: cell.Identity(), Namespace: ns, Metadata: mb, MetadataConfigDigest: mb.ConfigSHA256, StoreID: mc.StoreID, InventoryReceipt: "independent-complete-runtime-inventory", Reservations: reservations}
	rb := must(BootstrapRegistry(background, registryRoot, rc))
	fc := &fakeCloud{binding: ns, sa: sa}
	fa := &fakeAWS{}
	return &fixture{config: Config{Mode: LocalDisposableStaging, Project: p, Cell: cell, Writer: func() ownership.Writer { return w }, Namespace: ns, NamespaceTransport: fc, CredentialTransport: fc, AWSClient: fa, KMS: awsstore.KMSBinding{ARN: "arn:aws:kms:us-west-2:123456789012:key/11111111-1111-4111-8111-111111111111", Owner: owner, ProvenanceID: "fixture-kms-create"}, MetadataRoot: metadataRoot, MetadataConfig: mc, MetadataBinding: mb, RegistryRoot: registryRoot, RegistryConfig: rc, RegistryBinding: rb, MaximumKeyTTL: time.Hour, CallTimeout: time.Second, Now: func() time.Time { return now }}, cloud: fc, aws: fa, control: cb, request: Request{OperationKey: "stage-operation", AttemptID: "stage-attempt", ExpiresAt: now.Add(time.Hour)}, registryPath: path}
}

func TestActualStagingActorAndCleanPinnedHandoff(t *testing.T) {
	f := setup(t, nil)
	a := must(OpenInitial(background, f.config))
	for i := 0; i < 3; i++ {
		if _, e := a.Advance(background, f.request); e != nil {
			t.Fatal(e)
		}
	}
	state, e := a.Advance(background, f.request)
	if e != nil || state.Phase != tc.CredentialStaged || f.cloud.creates != 1 || f.aws.creates != 1 || f.cloud.deletes != 0 {
		t.Fatal("actor did not stage exactly once")
	}
	h := must(a.Quiesce(background))
	encoded := must(h.JSON())
	parsed := must(ParseCheckpoint(encoded))
	b := must(OpenClean(background, f.config, parsed))
	state, e = b.Advance(background, f.request)
	if e != nil || state.Phase != tc.CredentialStaged || f.cloud.creates != 1 || f.aws.creates != 1 {
		t.Fatal("clean handoff replayed stage")
	}
	if _, e = OpenClean(background, f.config, parsed); e == nil {
		t.Fatal("handoff admitted twice")
	}
	for _, r := range []*os.Root{f.config.RegistryRoot, f.config.MetadataRoot} {
		d, e := r.Open(".")
		if e != nil {
			t.Fatal(e)
		}
		entries, e := d.ReadDir(200)
		_ = d.Close()
		if e != nil {
			t.Fatal(e)
		}
		for _, entry := range entries {
			raw, e := r.ReadFile(entry.Name())
			if e != nil || bytes.Contains(raw, []byte(material)) {
				t.Fatal("material entered durable metadata")
			}
		}
	}
}

func TestStagingActorRetainsUnknownWithoutReplayHandoffOrDelete(t *testing.T) {
	for _, mode := range []string{"cloud", "aws"} {
		t.Run(mode, func(t *testing.T) {
			f := setup(t, nil)
			a := must(OpenInitial(background, f.config))
			if mode == "cloud" {
				f.cloud.lost = true
			} else {
				f.aws.lost = true
			}
			for i := 0; i < 2; i++ {
				if _, e := a.Advance(background, f.request); e != nil {
					t.Fatal(e)
				}
			}
			if _, e := a.Advance(background, f.request); e == nil {
				t.Fatal("lost reply reported success")
			}
			before := f.cloud.creates
			stores := f.aws.creates
			if _, e := a.Advance(background, f.request); !errors.Is(e, Blocked) {
				t.Fatal("unknown phase replayed")
			}
			if _, e := a.Quiesce(background); !errors.Is(e, Blocked) {
				t.Fatal("unknown phase issued handoff")
			}
			if f.cloud.creates != before || f.aws.creates != stores || f.cloud.deletes != 0 {
				t.Fatal("unknown replayed or deleted")
			}
		})
	}
}

func TestDurableRuntimeReservationRefusesCandidateBeforeAWSCreate(t *testing.T) {
	f := setup(t, []Reservation{{StoreID: "fixture-store", KeyID: "owned-stage-key"}})
	a := must(OpenInitial(background, f.config))
	for i := 0; i < 2; i++ {
		if _, e := a.Advance(background, f.request); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := a.Advance(background, f.request); e == nil {
		t.Fatal("reserved key staged")
	}
	if f.aws.creates != 0 || f.cloud.deletes != 0 {
		t.Fatal("reserved runtime identity mutated")
	}
}

func TestRegistryMissingReplacedPartialOrUnpinnedInventoryDenies(t *testing.T) {
	for _, mode := range []string{"missing", "manifest-replaced", "lock-replaced", "partial", "wrong-binding", "no-inventory"} {
		t.Run(mode, func(t *testing.T) {
			f := setup(t, nil)
			switch mode {
			case "missing":
				_ = f.config.RegistryRoot.Remove("reservations.json")
			case "manifest-replaced", "lock-replaced":
				name := "reservations.json"
				if mode == "lock-replaced" {
					name = "lock"
				}
				raw := must(f.config.RegistryRoot.ReadFile(name))
				if e := f.config.RegistryRoot.Rename(name, "previous-"+name); e != nil {
					t.Fatal(e)
				}
				if e := f.config.RegistryRoot.WriteFile(name, raw, 0600); e != nil {
					t.Fatal(e)
				}
			case "partial":
				if e := f.config.RegistryRoot.WriteFile("reservations.json", []byte("{"), 0600); e != nil {
					t.Fatal(e)
				}
			case "wrong-binding":
				f.config.RegistryBinding.ManifestDigest = strings.Repeat("a", 64)
			case "no-inventory":
				f.config.RegistryConfig.InventoryReceipt = ""
			}
			if _, e := OpenInitial(background, f.config); e == nil {
				t.Fatal("registry failure admitted")
			}
			if f.cloud.creates != 0 || f.cloud.reads != 0 || f.aws.creates != 0 {
				t.Fatal("invalid registry reached provider")
			}
		})
	}
}

func TestPrivateInvocationCannotBeForgedOrReused(t *testing.T) {
	f := setup(t, nil)
	a := must(OpenInitial(background, f.config))
	called := false
	if e := (candidateGuard{a}).Run(background, tc.CandidateWrite{}, func(context.Context) error { called = true; return nil }); !errors.Is(e, NotAuthorized) || called {
		t.Fatal("public context forged nested permit")
	}
	call := &invocation{actor: a, live: false}
	ctx := context.WithValue(background, invocationKey{}, call)
	a.invocation = call
	if e := (namespaceGuard{a}).Run(ctx, f.config.Namespace.NamespaceID, f.config.Writer(), func(context.Context) error { called = true; return nil }); !errors.Is(e, NotAuthorized) || called {
		t.Fatal("expired invocation reused")
	}
}

func TestQuiesceWaitsForActualCallbackReturn(t *testing.T) {
	f := setup(t, nil)
	a := must(OpenInitial(background, f.config))
	for i := 0; i < 2; i++ {
		if _, e := a.Advance(background, f.request); e != nil {
			t.Fatal(e)
		}
	}
	f.cloud.entered = make(chan struct{})
	f.cloud.release = make(chan struct{})
	advance := make(chan error, 1)
	go func() { _, e := a.Advance(background, f.request); advance <- e }()
	<-f.cloud.entered
	handoff := make(chan error, 1)
	started := make(chan struct{})
	go func() { close(started); _, e := a.Quiesce(background); handoff <- e }()
	<-started
	select {
	case <-handoff:
		t.Fatal("handoff escaped an in-flight provider callback")
	case <-time.After(10 * time.Millisecond):
	}
	close(f.cloud.release)
	if e := <-advance; e != nil {
		t.Fatal(e)
	}
	if e := <-handoff; e != nil {
		t.Fatal(e)
	}
}

func TestCanceledOrFailedQuiescePermanentlyClosesDispatch(t *testing.T) {
	for _, mode := range []string{"canceled", "registry-check"} {
		t.Run(mode, func(t *testing.T) {
			f := setup(t, nil)
			a := must(OpenInitial(background, f.config))
			ctx := background
			if mode == "canceled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			} else {
				if e := f.config.RegistryRoot.Rename("reservations.json", "held-reservations.json"); e != nil {
					t.Fatal(e)
				}
			}
			if _, e := a.Quiesce(ctx); e == nil {
				t.Fatal("quiesce failure reported handoff")
			}
			if mode == "registry-check" {
				if e := f.config.RegistryRoot.Rename("held-reservations.json", "reservations.json"); e != nil {
					t.Fatal(e)
				}
			}
			// Original registry inode and healthy admission are available again;
			// shutdown intent itself must stop dispatch before any provider read.
			if _, e := a.gate.Snapshot(background); e != nil {
				t.Fatal("fixture admission no longer healthy")
			}
			if _, e := a.Advance(background, f.request); !errors.Is(e, Closed) {
				t.Fatal("failed shutdown allowed future dispatch")
			}
			if _, e := a.Quiesce(background); !errors.Is(e, Closed) {
				t.Fatal("failed shutdown was reopened")
			}
			if f.cloud.reads != 0 || f.cloud.creates != 0 || f.aws.creates != 0 || f.cloud.deletes != 0 {
				t.Fatal("closed actor reached provider")
			}
		})
	}
}

func TestQueuedAdvanceDeniedAfterQuiesceAdmissionReadFailure(t *testing.T) {
	f := setup(t, nil)
	a := must(OpenInitial(background, f.config))
	entered, release := make(chan struct{}), make(chan struct{})
	f.control.observeEntered, f.control.observeRelease, f.control.observeError = entered, release, journal.ErrUnavailable
	quiesced := make(chan error, 1)
	go func() { _, e := a.Quiesce(background); quiesced <- e }()
	<-entered // Quiesce holds actor/registry locks while its Gate read waits.
	queued, started := make(chan error, 1), make(chan struct{})
	go func() { close(started); _, e := a.Advance(background, f.request); queued <- e }()
	<-started
	select {
	case <-queued:
		t.Fatal("queued Advance escaped lifecycle serialization")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	if e := <-quiesced; e == nil {
		t.Fatal("failed admission read issued handoff")
	}
	if e := <-queued; !errors.Is(e, Closed) {
		t.Fatal("queued Advance dispatched after shutdown failure")
	}
	if _, e := a.gate.Snapshot(background); e != nil {
		t.Fatal("transient failure unexpectedly disabled admission")
	}
	if _, e := a.Advance(background, f.request); !errors.Is(e, Closed) {
		t.Fatal("later Advance reopened actor")
	}
	if f.cloud.reads != 0 || f.cloud.creates != 0 || f.aws.creates != 0 || f.cloud.deletes != 0 {
		t.Fatal("shutdown failure reached provider")
	}
}

func TestActorRejectsChangedBirthWriterAndAdmission(t *testing.T) {
	for _, mode := range []string{"birth", "writer-expired", "writer-epoch", "admission"} {
		t.Run(mode, func(t *testing.T) {
			f := setup(t, nil)
			a := must(OpenInitial(background, f.config))
			switch mode {
			case "birth":
				f.cloud.binding.CreatedAt = f.cloud.binding.CreatedAt.Add(time.Second)
			case "writer-expired":
				w := f.config.Writer()
				w.ExpiresAt = now
				a.config.Writer = func() ownership.Writer { return w }
			case "writer-epoch":
				w := f.config.Writer()
				w.Epoch++
				a.config.Writer = func() ownership.Writer { return w }
			case "admission":
				f.control.mu.Lock()
				f.control.object.ResourceVersion = "externally-changed"
				f.control.mu.Unlock()
			}
			if _, e := a.Advance(background, f.request); e == nil {
				t.Fatal("changed authority admitted")
			}
			if f.cloud.creates != 0 || f.aws.creates != 0 || f.cloud.deletes != 0 {
				t.Fatal("changed authority mutated provider")
			}
		})
	}
}

func TestCleanCheckpointRejectsRestoredValidMetadataPrefix(t *testing.T) {
	f := setup(t, nil)
	a := must(OpenInitial(background, f.config))
	for i := 0; i < 3; i++ {
		if _, e := a.Advance(background, f.request); e != nil {
			t.Fatal(e)
		}
	}
	h := must(a.Quiesce(background))
	// Delete complete committed transactions, preserving a valid, non-ambiguous
	// prefix ending in ServiceAccountReady. A phase blocker cannot satisfy this
	// regression; the independent history checkpoint must detect the rollback.
	// The independent clean checkpoint must detect what hash chaining cannot.
	d := must(f.config.MetadataRoot.Open("."))
	entries := must(d.ReadDir(200))
	_ = d.Close()
	removed := 0
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, "tx-") && name > "tx-000002.intent.json" {
			if e := f.config.MetadataRoot.Remove(name); e != nil {
				t.Fatal(e)
			}
			removed++
		}
	}
	if removed == 0 {
		t.Fatal("no metadata history")
	}
	if _, e := OpenClean(background, f.config, h); !errors.Is(e, Changed) {
		t.Fatal("clean handoff accepted restored metadata prefix")
	}
}

func TestNestedCandidateGuardPinsExactMetadataAndInvocation(t *testing.T) {
	f := setup(t, nil)
	a := must(OpenInitial(background, f.config))
	f.aws.lost = true
	for i := 0; i < 2; i++ {
		if _, e := a.Advance(background, f.request); e != nil {
			t.Fatal(e)
		}
	}
	state, _ := a.Advance(background, f.request)
	if state.Phase != tc.CredentialStoreRequested {
		t.Fatal("fixture did not retain store intent")
	}
	write := tc.CandidateWrite{Intent: state.Intent, Key: state.Key, WriteID: state.CandidateWriteID}
	for _, mode := range []string{"stage", "write", "key", "sa", "digest", "session", "expired"} {
		t.Run(mode, func(t *testing.T) {
			call := &invocation{actor: a, intent: state.Intent, writer: f.config.Writer(), live: true, namespaceUsed: true}
			a.invocation = call
			w := write
			switch mode {
			case "stage":
				w.Intent.StageID = strings.Repeat("a", 64)
			case "write":
				w.WriteID = strings.Repeat("b", 64)
			case "key":
				w.Key.ID = "foreign-key"
			case "sa":
				w.Key.ServiceAccountID = "foreign-sa"
			case "digest":
				w.Key.VersionDigest = strings.Repeat("c", 64)
			case "session":
				call.actor = nil
			case "expired":
				call.live = false
			}
			called := false
			e := (candidateGuard{a}).Run(context.WithValue(background, invocationKey{}, call), w, func(context.Context) error { called = true; return nil })
			if e == nil || called {
				t.Fatal("mismatched nested identity authorized")
			}
			call.live = false
			a.invocation = nil
		})
	}
	if f.cloud.creates != 1 || f.aws.creates != 1 || f.cloud.deletes != 0 {
		t.Fatal("negative guard checks dispatched providers")
	}
}
