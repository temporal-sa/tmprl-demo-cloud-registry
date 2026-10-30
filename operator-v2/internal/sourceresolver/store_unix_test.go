//go:build darwin || linux

package sourceresolver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/buildplan"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
)

func storeFixture(t *testing.T) (*os.Root, *LocalStore, string, buildplan.SourceSelection) {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(directory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = root.Close() })
	selection := sourceStoreSelection()
	store, pin, err := BootstrapLocalStore(context.Background(), root, selection.Owner, selection.Binding.CheckoutURL)
	if err != nil {
		t.Fatal(err)
	}
	return root, store, pin, selection
}
func sourceStoreSelection() buildplan.SourceSelection {
	owner := ownership.Owner{EnvironmentID: "local", InstallationID: "test", ProjectUID: "project-uid", Lifetime: ownership.Disposable, RunID: "local-run"}
	repo := "https://github.com/temporal-sa/example"
	binding := buildplan.SourceBinding{AuthoredRepository: repo, CheckoutURL: repo, Ref: "main", AuthPolicy: "None", PolicyRevision: "policy-1", SelectionHash: strings.Repeat("a", 64)}
	return buildplan.SourceSelection{Owner: owner, Binding: binding, RequestedRef: "main", Source: project.Source{Repo: repo}}
}
func factsFor(selection buildplan.SourceSelection, payload []byte) Facts {
	sum := sha256.Sum256(payload)
	return Facts{Owner: selection.Owner, CheckoutURL: selection.Binding.CheckoutURL, RequestedRef: selection.RequestedRef, PolicyRevision: selection.Binding.PolicyRevision, BindingHash: buildplan.SourceBindingHash(selection.Binding), Commit: strings.Repeat("b", 40), ArtifactDigest: "sha256:" + hex.EncodeToString(sum[:]), WatchSelectionDigest: "sha256:" + strings.Repeat("c", 64), ArchiveFormat: ArchiveFormat, WatchProfile: WatchProfile, Files: 1, ArchiveBytes: int64(len(payload))}
}
func TestOwnedLocalSourceRecordPersistsWithExternalPinAndArchiveReadback(t *testing.T) {
	root, store, pin, selection := storeFixture(t)
	reservation, err := store.Reserve(context.Background(), selection)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("private-source-file-content")
	facts := factsFor(selection, payload)
	receipt, err := store.Commit(context.Background(), reservation, facts, payload)
	if err != nil || receipt.SourceUID != pin || receipt.Version != 1 {
		t.Fatalf("commit: %v %+v", err, receipt)
	}
	reopened, err := OpenLocalStore(context.Background(), root, selection.Owner, selection.Binding.CheckoutURL, pin)
	if err != nil {
		t.Fatal(err)
	}
	next, err := reopened.Reserve(context.Background(), selection)
	if err != nil || next.Version != 1 || next.SourceUID != pin {
		t.Fatal("restart lost owned record")
	}
	same, err := reopened.Commit(context.Background(), reservation, facts, payload)
	if err != nil || same.Version != 1 {
		t.Fatal("same known commit did not recover idempotently")
	}
	for _, name := range []string{recordName, lockName, archiveName(facts)} {
		info, err := root.Lstat(name)
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatal("source storage is not private")
		}
	}
	foreign := selection.Owner
	foreign.ProjectUID = "foreign"
	if _, err = OpenLocalStore(context.Background(), root, foreign, selection.Binding.CheckoutURL, pin); !errors.Is(err, ErrStoreMissing) {
		t.Fatal("foreign owner adopted source record")
	}
	if _, err = OpenLocalStore(context.Background(), root, selection.Owner, "https://github.com/temporal-sa/other", pin); !errors.Is(err, ErrStoreMissing) {
		t.Fatal("foreign repository adopted source record")
	}
	if _, err = OpenLocalStore(context.Background(), root, selection.Owner, selection.Binding.CheckoutURL, "source-"+strings.Repeat("f", 32)); !errors.Is(err, ErrStoreMissing) {
		t.Fatal("replacement source UID adopted")
	}
}
func TestOwnedLocalSourceCASRejectsConcurrentStaleBranchResult(t *testing.T) {
	root, store, pin, selection := storeFixture(t)
	second, err := OpenLocalStore(context.Background(), root, selection.Owner, selection.Binding.CheckoutURL, pin)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := store.Reserve(context.Background(), selection)
	b, _ := second.Reserve(context.Background(), selection)
	start := make(chan struct{})
	outcomes := make(chan error, 2)
	var wg sync.WaitGroup
	for i, instance := range []*LocalStore{store, second} {
		wg.Add(1)
		go func(i int, instance *LocalStore) {
			defer wg.Done()
			<-start
			payload := []byte{byte('a' + i)}
			facts := factsFor(selection, payload)
			facts.Commit = strings.Repeat(string(rune('a'+i)), 40)
			reservation := a
			if i == 1 {
				reservation = b
			}
			_, err := instance.Commit(context.Background(), reservation, facts, payload)
			outcomes <- err
		}(i, instance)
	}
	close(start)
	wg.Wait()
	close(outcomes)
	success, conflict := 0, 0
	for err := range outcomes {
		if err == nil {
			success++
		} else if errors.Is(err, ErrStoreConflict) {
			conflict++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatal("stale branch fetch overwrote newer source result")
	}
	current, err := store.Reserve(context.Background(), selection)
	if err != nil || current.Version != 1 {
		t.Fatal("CAS version changed twice")
	}
}
func TestLocalSourceStoreTamperingAndUnknownPartialWritesFailClosed(t *testing.T) {
	for _, kind := range []string{"missing-record", "replacement-uid", "archive-bytes", "record-checksum", "pending-record", "pending-archive", "symlink", "hardlink", "public-mode"} {
		t.Run(kind, func(t *testing.T) {
			root, store, pin, selection := storeFixture(t)
			reservation, _ := store.Reserve(context.Background(), selection)
			payload := []byte("private-source-sentinel")
			facts := factsFor(selection, payload)
			if _, err := store.Commit(context.Background(), reservation, facts, payload); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "missing-record":
				if err := root.Remove(recordName); err != nil {
					t.Fatal(err)
				}
			case "replacement-uid", "record-checksum":
				data, _ := root.ReadFile(recordName)
				var record localRecord
				_ = jsonUnmarshal(data, &record)
				if kind == "replacement-uid" {
					record.UID = "source-" + strings.Repeat("f", 32)
					record.Digest = recordDigest(record)
				} else {
					record.Version++
				}
				if err := root.WriteFile(recordName, mustJSON(record), 0600); err != nil {
					t.Fatal(err)
				}
			case "archive-bytes":
				if err := root.WriteFile(archiveName(facts), []byte("tampered"), 0600); err != nil {
					t.Fatal(err)
				}
			case "pending-record":
				if err := root.WriteFile(recordName+".pending", payload, 0600); err != nil {
					t.Fatal(err)
				}
			case "pending-archive":
				if err := root.WriteFile("artifact-"+strings.Repeat("f", 64)+".tar.pending", payload, 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := root.Remove(recordName); err != nil {
					t.Fatal(err)
				}
				if err := root.Symlink(lockName, recordName); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := root.Link(lockName, "hardlink"); err != nil {
					t.Fatal(err)
				}
			case "public-mode":
				if err := root.Chmod(recordName, 0644); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := OpenLocalStore(context.Background(), root, selection.Owner, selection.Binding.CheckoutURL, pin); err == nil || strings.Contains(err.Error(), "private-source-sentinel") {
				t.Fatal("unsafe state accepted or source contents leaked")
			}
			if _, err := store.Reserve(context.Background(), selection); err == nil {
				t.Fatal("tampered store permitted source fetch reservation")
			}
			if kind == "pending-record" {
				data, _ := root.ReadFile(recordName + ".pending")
				if string(data) != string(payload) {
					t.Fatal("unknown evidence overwritten")
				}
			}
		})
	}
}
func jsonUnmarshal(data []byte, target any) error { return json.Unmarshal(data, target) }
