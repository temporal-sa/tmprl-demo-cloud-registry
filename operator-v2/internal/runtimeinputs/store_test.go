package runtimeinputs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func fixture(t *testing.T, capacity int) (*Store, Pin, *os.Root, Capture) {
	t.Helper()
	return fixtureWithServiceAccounts(t, capacity, false)
}

func fixtureWithServiceAccounts(t *testing.T, capacity int, serviceAccounts bool) (*Store, Pin, *os.Root, Capture) {
	t.Helper()
	dir := t.TempDir()
	if e := os.Chmod(dir, 0700); e != nil {
		t.Fatal(e)
	}
	root, e := os.OpenRoot(dir)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = root.Close() })
	owner := ownership.Owner{EnvironmentID: "lab", InstallationID: "test", ProjectUID: "project-one", Lifetime: ownership.Disposable, RunID: "owned-run"}
	cfg := Config{BootstrapUID: strings.Repeat("a", 64), Owner: owner, CellUID: "control-cell-uid", MaximumEntries: capacity}
	cfg.ServiceAccounts = serviceAccounts
	s, p, e := Bootstrap(context.Background(), root, cfg)
	if e != nil {
		t.Fatal(e)
	}
	object := &appsv1.Deployment{TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"}, ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "candidate-one"}, Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "repo@sha256:" + strings.Repeat("b", 64), Env: []corev1.EnvVar{{Name: "PRIVATE", Value: "authored-private-sentinel"}}}}}}}}
	fp, e := kube.DeploymentFingerprint(object)
	if e != nil {
		t.Fatal(e)
	}
	c := Capture{Target: Target{CellUID: cfg.CellUID, Parent: ownership.NamespaceParent{ClusterID: "cluster-one", Name: object.Namespace, UID: "canonical-namespace-uid", Owner: owner, CreationOperationID: "accepted-one", CreationIdempotencyKey: "namespace-key"}, AttemptID: "accepted-one", Revision: "revision-one", DesiredDigest: strings.Repeat("c", 64), Name: object.Name, Fingerprint: fp}, Desired: object}
	return s, p, root, c
}

func TestOriginalBodySurvivesReopenCurrentAuthoringChangeAndPrivateCopies(t *testing.T) {
	s, p, root, c := fixture(t, 2)
	ref, e := s.Retain(context.Background(), c)
	if e != nil {
		t.Fatal(e)
	}
	if !ref.Valid() || ref.Digest == c.Target.Fingerprint {
		t.Fatal("private envelope/body digest boundary lost")
	}
	// The current mutable authoring can change without becoming desired input
	// for this older accepted journal reference.
	c.Desired.Spec.Template.Spec.Containers[0].Env[0].Value = "new-current-authoring"
	fresh, e := Open(context.Background(), root, p, s.config)
	if e != nil {
		t.Fatal(e)
	}
	payload, e := fresh.Resolve(context.Background(), ref, c.Target)
	if e != nil {
		t.Fatal(e)
	}
	object := payload.ObjectForProvider()
	if object.Spec.Template.Spec.Containers[0].Env[0].Value != "authored-private-sentinel" {
		t.Fatal("current authoring replaced original accepted body")
	}
	object.Spec.Template.Spec.Containers[0].Env[0].Value = "mutated-caller-copy"
	if payload.ObjectForProvider().Spec.Template.Spec.Containers[0].Env[0].Value != "authored-private-sentinel" {
		t.Fatal("payload aliases caller's execution copy")
	}
	for _, format := range []string{"%v", "%+v", "%#v", "%p", "%d", "%x", "%q"} {
		for _, value := range []any{payload, &payload, c, &c} {
			if strings.Contains(fmt.Sprintf(format, value), "authored-private-sentinel") || strings.Contains(fmt.Sprintf(format, value), "new-current-authoring") {
				t.Fatal("private body escaped fmt", format)
			}
		}
	}
	encoded, _ := json.Marshal(payload)
	if bytes.Contains(encoded, []byte("sentinel")) {
		t.Fatal("private body escaped JSON")
	}
}

func TestReferencePinsScopeIncarnationAttemptBodyAndContract(t *testing.T) {
	s, _, _, c := fixture(t, 2)
	ref, e := s.Retain(context.Background(), c)
	if e != nil {
		t.Fatal(e)
	}
	for _, change := range []func(*Target){func(v *Target) { v.Parent.UID = "replacement" }, func(v *Target) { v.Parent.Owner.ProjectUID = "foreign" }, func(v *Target) { v.AttemptID = "another"; v.Parent.CreationOperationID = "another" }, func(v *Target) { v.Revision = "other" }, func(v *Target) { v.DesiredDigest = strings.Repeat("d", 64) }, func(v *Target) { v.Name = "other" }, func(v *Target) { v.Fingerprint = strings.Repeat("e", 64) }} {
		bad := c.Target
		change(&bad)
		if _, e = s.Resolve(context.Background(), ref, bad); e == nil {
			t.Fatal("foreign accepted input target resolved")
		}
	}
	bad := ref
	bad.Contract = "future-unsupported"
	if _, e = s.Resolve(context.Background(), bad, c.Target); e != ErrInvalid {
		t.Fatal("historical contract silently reinterpreted", e)
	}
	bad = ref
	bad.Digest = strings.Repeat("0", 64)
	if _, e = s.Resolve(context.Background(), bad, c.Target); e != ErrUnproven {
		t.Fatal("foreign payload digest accepted", e)
	}
	c.Desired.Spec.Template.Spec.Containers[0].Image = "changed-current-image"
	if _, e = s.Retain(context.Background(), c); e != ErrInvalid {
		t.Fatal("changed body retained under old fingerprint", e)
	}
}

func TestMissingTamperedPartialAndSymlinkInputsNeverRepairOrAdopt(t *testing.T) {
	for _, mode := range []string{"missing", "tampered", "symlink", "commit-missing", "commit-tampered"} {
		t.Run(mode, func(t *testing.T) {
			s, _, root, c := fixture(t, 2)
			ref, e := s.Retain(context.Background(), c)
			if e != nil {
				t.Fatal(e)
			}
			name := ref.UID + ".input.json"
			switch mode {
			case "missing":
				_ = root.Remove(name)
			case "tampered":
				_ = root.WriteFile(name, []byte(`{"private":"different"}`), 0600)
			case "symlink":
				_ = root.Remove(name)
				_ = root.Symlink("manifest.json", name)
			case "commit-missing":
				_ = root.Remove(ref.UID + ".commit.json")
			case "commit-tampered":
				_ = root.WriteFile(ref.UID+".commit.json", []byte(`{}`), 0600)
			}
			if _, e = s.Resolve(context.Background(), ref, c.Target); e == nil {
				t.Fatal("unproven artifact accepted")
			}
			for attempt := 0; attempt < 2; attempt++ {
				if _, e = s.Retain(context.Background(), c); e == nil {
					t.Fatal("partial/changed/missing existing artifact repaired or adopted")
				}
				if mode == "missing" {
					if _, e = root.Lstat(name); !os.IsNotExist(e) {
						t.Fatal("Retain rewrote missing committed body", e)
					}
				}
			}
		})
	}
}

func TestPartialWriteRetainedWhileExactLostAcknowledgementCanBeConfirmed(t *testing.T) {
	for _, point := range []string{"after-input", "after-commit"} {
		t.Run(point, func(t *testing.T) {
			s, p, root, c := fixture(t, 2)
			s.fault = func(at string) error {
				if at == point {
					return ErrUncertain
				}
				return nil
			}
			ref, e := s.Retain(context.Background(), c)
			if e != ErrUncertain || ref != (journal.InputReference{}) {
				t.Fatal("uncertain artifact acknowledged", e)
			}
			fresh, e := Open(context.Background(), root, p, s.config)
			if e != nil {
				t.Fatal(e)
			}
			ref, e = fresh.Retain(context.Background(), c)
			if point == "after-input" {
				if e != ErrUncertain {
					t.Fatal("uncommitted partial entry repaired", e)
				}
				if _, e = root.Lstat(s.uid(c.Target) + ".input.json"); e != nil {
					t.Fatal("partial evidence removed")
				}
			} else {
				if e != nil {
					t.Fatal("exact immutable committed retry failed", e)
				}
				if _, e = fresh.Resolve(context.Background(), ref, c.Target); e != nil {
					t.Fatal(e)
				}
			}
		})
	}
}

func TestPinnedRootLockConfigurationAndNoImplicitBootstrap(t *testing.T) {
	s, p, root, _ := fixture(t, 2)
	copyRoot, e := os.OpenRoot(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer copyRoot.Close()
	if e = os.Chmod(copyRoot.Name(), 0700); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"manifest.json", "lock"} {
		b, _ := root.ReadFile(name)
		if e = copyRoot.WriteFile(name, b, 0600); e != nil {
			t.Fatal(e)
		}
	}
	if _, e = Open(context.Background(), copyRoot, p, s.config); e != ErrUnproven {
		t.Fatal("copied root provenance accepted", e)
	}
	bad := s.config
	bad.Owner.ProjectUID = "foreign"
	if _, e = Open(context.Background(), root, p, bad); e != ErrUnproven {
		t.Fatal("foreign configured owner accepted", e)
	}
	if e = root.Remove("lock"); e != nil {
		t.Fatal(e)
	}
	if e = root.WriteFile("lock", nil, 0600); e != nil {
		t.Fatal(e)
	}
	if _, e = Open(context.Background(), root, p, s.config); e != ErrUnproven {
		t.Fatal("replacement lock accepted", e)
	}
	empty, e := os.OpenRoot(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer empty.Close()
	if e = os.Chmod(empty.Name(), 0700); e != nil {
		t.Fatal(e)
	}
	if _, e = Open(context.Background(), empty, p, s.config); e != ErrUnproven {
		t.Fatal("Open implicitly bootstrapped")
	}
	entries, _ := os.ReadDir(empty.Name())
	if len(entries) != 0 {
		t.Fatal("Open wrote files")
	}
}

func TestConcurrentExactRetentionAndBoundedCapacity(t *testing.T) {
	s, p, root, c := fixture(t, 1)
	other, e := Open(context.Background(), root, p, s.config)
	if e != nil {
		t.Fatal(e)
	}
	var group sync.WaitGroup
	group.Add(2)
	refs := make(chan journal.InputReference, 2)
	errs := make(chan error, 2)
	for _, store := range []*Store{s, other} {
		go func(store *Store) {
			defer group.Done()
			ref, e := store.Retain(context.Background(), c)
			refs <- ref
			errs <- e
		}(store)
	}
	group.Wait()
	close(refs)
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	var first journal.InputReference
	for ref := range refs {
		if first == (journal.InputReference{}) {
			first = ref
		} else if first != ref {
			t.Fatal("same accepted input retained with two identities")
		}
	}
	c.Desired = c.Desired.DeepCopy()
	c.Desired.Name = "another"
	c.Target.Name = "another"
	c.Target.Fingerprint, _ = kube.DeploymentFingerprint(c.Desired)
	if _, e = s.Retain(context.Background(), c); e != ErrCapacity {
		t.Fatal("bounded capacity bypassed", e)
	}
	files, _ := filepath.Glob(filepath.Join(root.Name(), "*.input.json"))
	if len(files) != 1 {
		t.Fatal("capacity wrote hidden extra artifact")
	}
}

func TestOrphanCommitStillReservesCapacityBeforeAnyNewWrite(t *testing.T) {
	s, _, root, c := fixture(t, 1)
	ref, e := s.Retain(context.Background(), c)
	if e != nil {
		t.Fatal(e)
	}
	if e = root.Remove(ref.UID + ".input.json"); e != nil {
		t.Fatal(e)
	}
	c.Desired = c.Desired.DeepCopy()
	c.Desired.Name = "other"
	c.Target.Name = "other"
	c.Target.Fingerprint, _ = kube.DeploymentFingerprint(c.Desired)
	if _, e = s.Retain(context.Background(), c); e != ErrCapacity {
		t.Fatal("commit-only evidence released capacity", e)
	}
	if _, e = root.Lstat(s.uid(c.Target) + ".input.json"); !os.IsNotExist(e) {
		t.Fatal("capacity refusal wrote a new private body", e)
	}
}
