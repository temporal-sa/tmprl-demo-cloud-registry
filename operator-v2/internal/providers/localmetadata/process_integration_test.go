//go:build localmetadata_integration

package localmetadata

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
)

// This tier deliberately executes only this already-built test executable,
// against a uniquely owned temporary private root and synthetic safe metadata.
// It has no provider/credential/network access and is excluded from unit tests.
func TestOwnedTwoProcessCAS(t *testing.T) {
	if os.Getenv("REGISTRY_LOCAL_METADATA_CHILD") == "owned-test-child" {
		cfg, i := fixtureConfig(t, 16)
		root, err := os.OpenRoot(os.Getenv("REGISTRY_LOCAL_METADATA_ROOT"))
		if err != nil {
			t.Fatal("child root unavailable")
		}
		defer root.Close()
		var binding Binding
		outside := filepath.Join(filepath.Dir(root.Name()), "outside-binding")
		ordinary, readErr := os.ReadFile(outside)
		if readErr != nil || string(ordinary) != "owned harmless confinement marker" {
			t.Fatal("owned sibling marker is not ordinarily readable")
		}
		raw, err := root.ReadFile("../outside-binding")
		if err == nil || raw != nil {
			t.Fatal("os.Root escaped owned directory")
		}
		if json.Unmarshal([]byte(os.Getenv("REGISTRY_LOCAL_METADATA_BINDING")), &binding) != nil {
			t.Fatal("child safe binding invalid")
		}
		s, err := Open(root, binding, cfg)
		if err != nil {
			t.Fatal(err)
		}
		v, found, err := s.CredentialJournal().Load(ctx, i.SlotID)
		if err != nil || !found {
			t.Fatal("child durable base unavailable")
		}
		// Use the parent's fixed original version, even when this child starts
		// after the competitor has already committed its different next phase.
		expected := os.Getenv("REGISTRY_LOCAL_METADATA_EXPECTED")
		next := v
		next.Version = ""
		if os.Getenv("REGISTRY_LOCAL_METADATA_CHOICE") == "ready" {
			next.Phase = tc.CredentialSAReady
			next.ServiceAccount = cfg.PreprovisionedServiceAccount
			next.SAReceipt = tc.Receipt{}
		} else {
			next.Phase = tc.CredentialSARequested
			next.ServiceAccount = tc.ServiceAccountRef{}
			next.SAReceipt = s.expectedReceipt("service-account-create", i.StageID, saFingerprint(i))
		}
		_, err = s.CredentialJournal().CompareAndSwap(ctx, i.SlotID, expected, next)
		if err != nil && err != CASConflict {
			t.Fatal(err)
		}
		return
	}
	s, b, root, i := testStore(t, 16)
	outside := filepath.Join(filepath.Dir(root.Name()), "outside-binding")
	if os.WriteFile(outside, []byte("owned harmless confinement marker"), 0600) != nil {
		t.Fatal("owned sibling marker unavailable")
	}
	initial := mustSave(t, s, tc.CredentialState{}, tc.CredentialState{Intent: i, Phase: tc.CredentialRecorded})
	// Test metadata never enters ordinary process flags; no provider secrets
	// exist here. Each subprocess receives only a scrubbed environment plus its
	// exact owned-root binding/capability and expected semantic CAS version.
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binding, _ := json.Marshal(b)
	childHome := t.TempDir()
	childTemporary := t.TempDir()
	if os.Chmod(childHome, 0700) != nil || os.Chmod(childTemporary, 0700) != nil {
		t.Fatal("owned child environment unavailable")
	}
	deadline, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(2)
	results := make(chan error, 2)
	for _, choice := range []string{"ready", "requested"} {
		go func(choice string) {
			defer wg.Done()
			command := exec.CommandContext(deadline, executable, "-test.run=^TestOwnedTwoProcessCAS$", "-test.count=1")
			command.Env = []string{"PATH=/registry-test-no-executables", "HOME=" + childHome, "TMPDIR=" + childTemporary, "REGISTRY_LOCAL_METADATA_CHILD=owned-test-child", "REGISTRY_LOCAL_METADATA_ROOT=" + root.Name(), "REGISTRY_LOCAL_METADATA_BINDING=" + string(binding), "REGISTRY_LOCAL_METADATA_EXPECTED=" + initial.Version, "REGISTRY_LOCAL_METADATA_CHOICE=" + choice}
			results <- command.Run()
		}(choice)
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal("owned child persistence test failed")
		}
	}
	h, err := s.History(ctx)
	if err != nil || len(h) != 2 {
		t.Fatal("two processes bypassed one accepted CAS")
	}
	if h[1].Journal.Phase != tc.CredentialSAReady && h[1].Journal.Phase != tc.CredentialSARequested {
		t.Fatal("unexpected durable child state")
	}
	if _, err = os.Stat(filepath.Join(root.Name(), "tx-000003.intent.json")); !os.IsNotExist(err) {
		t.Fatal("conflicting child wrote a hidden new revision")
	}
}
