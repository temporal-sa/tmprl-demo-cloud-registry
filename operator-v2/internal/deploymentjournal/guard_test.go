package deploymentjournal

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/admission"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/childjournal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
)

type cell struct {
	data      []byte
	version   journal.Version
	uncertain bool
}

func (c *cell) Read(context.Context) ([]byte, journal.Version, error) {
	return append([]byte(nil), c.data...), c.version, nil
}
func (c *cell) CompareAndSwap(_ context.Context, v journal.Version, b []byte, _ string) error {
	if v != c.version {
		return journal.ErrConflict
	}
	if c.uncertain {
		return journal.ErrUnknown
	}
	c.data = append([]byte(nil), b...)
	n, _ := strconv.Atoi(v.ResourceVersion)
	c.version.ResourceVersion = strconv.Itoa(n + 1)
	return nil
}
func snapshot(t *testing.T, g *admission.Gate) journal.Envelope {
	t.Helper()
	s, err := g.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return s.Envelope
}
func save(t *testing.T, g *admission.Gate, e journal.Envelope) {
	t.Helper()
	if _, err := g.Commit(context.Background(), e); err != nil {
		t.Fatal(err)
	}
}
func fixture(t *testing.T) (Config, *cell, kube.DeploymentWriteIntent) {
	t.Helper()
	owner := ownership.Owner{EnvironmentID: "lab", InstallationID: "test", ProjectUID: "project", Lifetime: ownership.Disposable, RunID: "owned-run"}
	e, err := journal.Initial(rollout.Subject{Environment: owner.EnvironmentID, Installation: owner.InstallationID, ProjectUID: owner.ProjectUID}, 1)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := e.JSON()
	store := &cell{data: data, version: journal.Version{UID: "cell-uid", ResourceVersion: "1"}}
	repo, err := journal.New(store, e.Subject, store.version.UID)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := admission.BeginInitial(context.Background(), admission.Config{Mode: admission.LocalCleanHandoff, Repository: repo})
	if err != nil {
		t.Fatal(err)
	}
	gate, err := claim.Confirm(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	e = snapshot(t, gate)
	e.Attempts = []journal.Attempt{{Number: 1, OperationID: "attempt-one", NamespaceName: "candidate-one", State: journal.Reserved, Revision: "revision-one", DesiredDigest: strings.Repeat("a", 64)}}
	save(t, gate, e)
	e = snapshot(t, gate)
	e.Writes = []journal.Write{{ID: "namespace-create", OperationID: "attempt-one", WriterEpoch: 1, State: journal.Pending, Preconditions: journal.Preconditions{Kind: "Namespace", Action: "Create", Name: "candidate-one", Fingerprint: strings.Repeat("b", 64), TargetRevision: "revision-one", ProviderOperationID: "attempt-one"}}}
	save(t, gate, e)
	e = snapshot(t, gate)
	e.Attempts[0].State = journal.Observed
	e.Attempts[0].NamespaceUID = "namespace-uid"
	e.Writes[0].State = journal.Committed
	e.Writes[0].ResultUID = "namespace-uid"
	e.Writes[0].ResultVersion = "2"
	save(t, gate, e)
	parent := ownership.NamespaceParent{ClusterID: "owned-cluster", Name: "candidate-one", UID: "namespace-uid", Owner: owner, CreationOperationID: "attempt-one", CreationIdempotencyKey: "namespace-creation-key"}
	config := Config{Gate: gate, Owner: owner, Parent: parent, WriterID: "actor-writer"}
	request := kube.DeploymentWriteIntent{Owner: owner, Parent: parent, OperationID: "attempt-one", ProviderOperationID: "apply-app", Name: "app", Revision: "revision-one", Fingerprint: strings.Repeat("c", 64), WriteID: "deployment-create", WriterID: config.WriterID, WriterEpoch: 1}
	return config, store, request
}
func pending(e journal.Envelope, r kube.DeploymentWriteIntent) journal.Envelope {
	e.Writes = append(e.Writes, journal.Write{ID: r.WriteID, OperationID: r.OperationID, WriterEpoch: r.WriterEpoch, State: journal.Pending, Preconditions: journal.Preconditions{Kind: "Deployment", Action: "Create", Name: r.Name, ParentUID: r.Parent.UID, Fingerprint: r.Fingerprint, TargetRevision: r.Revision, ProviderOperationID: r.ProviderOperationID}})
	return e
}

func TestGuardRequiresExactAcknowledgedPendingIntentAndRejectsReplay(t *testing.T) {
	config, _, request := fixture(t)
	guard, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err = guard.VerifyDeploymentIntent(context.Background(), request); !errors.Is(err, ErrUnverified) {
		t.Fatal("unpersisted write verified")
	}
	save(t, config.Gate, pending(snapshot(t, config.Gate), request))
	if err = guard.VerifyDeploymentIntent(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*kube.DeploymentWriteIntent){func(r *kube.DeploymentWriteIntent) { r.Parent.UID = "foreign" }, func(r *kube.DeploymentWriteIntent) { r.Owner.ProjectUID = "foreign" }, func(r *kube.DeploymentWriteIntent) { r.Name = "other" }, func(r *kube.DeploymentWriteIntent) { r.WriteID = "other" }, func(r *kube.DeploymentWriteIntent) { r.ProviderOperationID = "other" }, func(r *kube.DeploymentWriteIntent) { r.OperationID = "other" }, func(r *kube.DeploymentWriteIntent) { r.Revision = "other" }, func(r *kube.DeploymentWriteIntent) { r.Fingerprint = strings.Repeat("d", 64) }, func(r *kube.DeploymentWriteIntent) { r.WriterID = "other" }, func(r *kube.DeploymentWriteIntent) { r.WriterEpoch++ }} {
		bad := request
		change(&bad)
		if err = guard.VerifyDeploymentIntent(context.Background(), bad); !errors.Is(err, ErrUnverified) {
			t.Fatal("mismatched write verified")
		}
	}
	e := snapshot(t, config.Gate)
	e.Writes[1].State = journal.Committed
	e.Writes[1].ResultUID = "deployment-uid"
	e.Writes[1].ResultVersion = "3"
	save(t, config.Gate, e)
	if err = guard.VerifyDeploymentIntent(context.Background(), request); !errors.Is(err, ErrUnverified) {
		t.Fatal("committed write authorized another POST")
	}
	if _, err = config.Gate.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = guard.VerifyDeploymentIntent(context.Background(), request); !errors.Is(err, ErrUnverified) {
		t.Fatal("closed session authorized dispatch")
	}
}

func TestAmbiguousPendingCommitCannotAuthorizeDispatch(t *testing.T) {
	config, store, request := fixture(t)
	guard, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	store.uncertain = true
	if _, err = config.Gate.Commit(context.Background(), pending(snapshot(t, config.Gate), request)); !errors.Is(err, admission.ErrUncertain) {
		t.Fatal("expected uncertain CAS")
	}
	if err = guard.VerifyDeploymentIntent(context.Background(), request); !errors.Is(err, ErrUnverified) {
		t.Fatal("unknown journal write authorized provider")
	}
}

func TestGuardRejectsUnconstructedForeignAndUnobservedAuthority(t *testing.T) {
	config, _, _ := fixture(t)
	for _, change := range []func(*Config){func(c *Config) { c.Gate = nil }, func(c *Config) { c.Gate = new(admission.Gate) }, func(c *Config) { c.Parent.UID = "foreign" }, func(c *Config) { c.Owner.ProjectUID = "foreign"; c.Parent.Owner = c.Owner }, func(c *Config) { c.Parent.CreationOperationID = "foreign" }} {
		bad := config
		change(&bad)
		if _, err := New(context.Background(), bad); !errors.Is(err, ErrUnverified) {
			t.Fatal("unconstructed/foreign guard accepted")
		}
	}
}

func TestAnotherUnresolvedWriteBlocksExactPendingDeployment(t *testing.T) {
	for _, state := range []journal.WriteState{journal.Pending, journal.Unknown} {
		t.Run(string(state), func(t *testing.T) {
			config, _, request := fixture(t)
			guard, err := New(context.Background(), config)
			if err != nil {
				t.Fatal(err)
			}
			e := pending(snapshot(t, config.Gate), request)
			e.Writes = append(e.Writes, journal.Write{ID: "other-create", OperationID: request.OperationID, WriterEpoch: 1, State: journal.Pending, Preconditions: journal.Preconditions{Kind: "ConfigMap", Action: "Create", Name: "other", ParentUID: request.Parent.UID, Fingerprint: strings.Repeat("d", 64), TargetRevision: request.Revision, ProviderOperationID: "create-other"}})
			save(t, config.Gate, e)
			if state == journal.Unknown {
				e = snapshot(t, config.Gate)
				e.Writes[2].State = journal.Unknown
				save(t, config.Gate, e)
			}
			if err = guard.VerifyDeploymentIntent(context.Background(), request); !errors.Is(err, ErrUnverified) {
				t.Fatal("another unresolved write allowed dispatch")
			}
		})
	}
}

func TestCleanDrainingHandoffDoesNotReopenNormalDispatch(t *testing.T) {
	config, store, request := fixture(t)
	e := snapshot(t, config.Gate)
	e.Attempts[0].State = journal.Draining
	e.Attempts[0].DrainReason = journal.DrainProjectFinalization
	save(t, config.Gate, e)
	if _, err := New(context.Background(), config); !errors.Is(err, ErrUnverified) {
		t.Fatal("draining namespace authorized new deployment guard")
	}
	h, err := config.Gate.Ready(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	repo, err := journal.New(store, e.Subject, store.version.UID)
	if err != nil {
		t.Fatal(err)
	}
	claim, err := admission.BeginResume(context.Background(), admission.Config{Mode: admission.LocalCleanHandoff, Repository: repo}, h)
	if err != nil {
		t.Fatal(err)
	}
	gate, err := claim.Confirm(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	e = snapshot(t, gate)
	if e.Attempts[0].State != journal.Draining || e.Attempts[0].DrainReason != journal.DrainProjectFinalization {
		t.Fatal("handoff lost durable drain")
	}
	if _, err = gate.Commit(context.Background(), pending(e, request)); !errors.Is(err, admission.ErrInvalid) {
		t.Fatal("clean successor reopened normal deployment writes")
	}
}

func TestGuardPinsExactPrivateInputReferenceSeparatelyFromBodyFingerprint(t *testing.T) {
	for _, match := range []bool{false, true} {
		config, _, request := fixture(t)
		reference := journal.InputReference{Kind: "DeploymentRender", UID: "render-" + strings.Repeat("a", 64), Version: "1", Digest: strings.Repeat("b", 64), Contract: journal.DeploymentInputContract}
		config.Input = reference
		guard, err := New(context.Background(), config)
		if err != nil {
			t.Fatal(err)
		}
		e := pending(snapshot(t, config.Gate), request)
		e.Writes[1].Input = reference
		if !match {
			e.Writes[1].Input.UID = "render-" + strings.Repeat("d", 64)
		}
		save(t, config.Gate, e)
		// Mutating the constructor DTO cannot change the pinned capability.
		config.Input = e.Writes[1].Input
		err = guard.VerifyDeploymentIntent(context.Background(), request)
		if match && err != nil || !match && !errors.Is(err, ErrUnverified) {
			t.Fatal("private input reference was not independently pinned", err)
		}
	}
}

func TestFiniteSharedCoreDoesNotAuthorizeAnotherChildKind(t *testing.T) {
	for _, kind := range []string{"Deployment", "Service"} {
		config, _, request := fixture(t)
		deployment, e := New(context.Background(), config)
		if e != nil {
			t.Fatal(e)
		}
		service, e := childjournal.New(context.Background(), childjournal.Config{Kind: childjournal.Service, Gate: config.Gate, Owner: config.Owner, Parent: config.Parent, WriterID: config.WriterID})
		if e != nil {
			t.Fatal(e)
		}
		n := pending(snapshot(t, config.Gate), request)
		n.Writes[1].Preconditions.Kind = kind
		save(t, config.Gate, n)
		e = deployment.VerifyDeploymentIntent(context.Background(), request)
		if kind == "Deployment" && e != nil || kind == "Service" && !errors.Is(e, ErrUnverified) {
			t.Fatal("Deployment facade crossed kind boundary", e)
		}
		e = service.Verify(context.Background(), childjournal.Intent{Owner: request.Owner, Parent: request.Parent, OperationID: request.OperationID, ProviderOperationID: request.ProviderOperationID, Name: request.Name, Revision: request.Revision, Fingerprint: request.Fingerprint, WriteID: request.WriteID, WriterID: request.WriterID, WriterEpoch: request.WriterEpoch})
		if kind == "Service" && e != nil || kind == "Deployment" && !errors.Is(e, ErrUnverified) {
			t.Fatal("Service core crossed kind boundary", e)
		}
	}
	config, _, _ := fixture(t)
	for _, kind := range []childjournal.Kind{"", "ClusterRole", "ConfigMap"} {
		if _, e := childjournal.New(context.Background(), childjournal.Config{Kind: kind, Gate: config.Gate, Owner: config.Owner, Parent: config.Parent, WriterID: config.WriterID}); !errors.Is(e, ErrUnverified) {
			t.Fatal("unreviewed generic kind created a capability", e)
		}
	}
}
