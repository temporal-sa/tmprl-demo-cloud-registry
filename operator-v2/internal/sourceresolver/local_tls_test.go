//go:build source_integration && (darwin || linux)

package sourceresolver

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// This tier owns its ephemeral loopback TLS server and executes only a fixed
// copy of this test binary for filesystem CAS. It never executes Git/source
// commands, loads user credentials, contacts GitHub or proves cloud isolation.
func TestOwnedLocalTLSGitAndConsumerArtifact(t *testing.T) {
	fixture := newFixture()
	fixture.private = true
	server := httptest.NewUnstartedServer(fixture)
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	defer server.Close()
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	resolver, d, b, _ := resolverFixture(t, server.URL+"/repo.git", roots, nil)
	makePrivate(t, resolver, &d, &b)
	artifact, err := resolver.Resolve(context.Background(), d, b)
	if err != nil {
		t.Fatal(err)
	}
	if fixture.count() != 2 || artifact.Source().Commit != fixture.branch.String() || artifact.Source().SourceVersion != "1" {
		t.Fatal("owned TLS Git exchange did not persist exact content")
	}
	before := fixture.count()
	resolver.policy.RootCAs = fixtureRoots(t)
	if _, err = resolver.Resolve(context.Background(), d, b); err == nil || fixture.count() != before {
		t.Fatal("wrong TLS root reached authenticated HTTP source handler")
	}
	assertPrivateAbsent(t, err.Error())
	_, port, splitErr := net.SplitHostPort(strings.TrimPrefix(server.URL, "https://"))
	if splitErr != nil {
		t.Fatal(splitErr)
	}
	wrongHost, wrongDocument, wrongBindings, _ := resolverFixture(t, "https://wrong-source.invalid:"+port+"/repo.git", roots, nil)
	makePrivate(t, wrongHost, &wrongDocument, &wrongBindings)
	if _, err = wrongHost.Resolve(context.Background(), wrongDocument, wrongBindings); err == nil || fixture.count() != before {
		t.Fatal("wrong TLS hostname reached authenticated HTTP handler")
	}
	assertPrivateAbsent(t, err.Error())
	resolver.policy.RootCAs = roots
	fixture.redirect = true
	if _, err = resolver.Resolve(context.Background(), d, b); err == nil || fixture.count() != before+1 {
		t.Fatal("redirect was not rejected at owned source origin")
	}
	assertPrivateAbsent(t, err.Error())
	// An explicitly supplied owned output directory lets the validation harness
	// feed these actual bytes to the buildspec Python extractor without ever
	// executing authored build commands. Never overwrite caller files.
	if directory := os.Getenv("OPERATOR_SOURCE_COMPAT_OUTPUT"); directory != "" {
		root, err := os.OpenRoot(directory)
		if err != nil {
			t.Fatal(err)
		}
		defer root.Close()
		if !(&LocalStore{root: root}).validRoot() {
			t.Fatal("compatibility fixture output must be a private owned root")
		}
		for name, data := range map[string][]byte{"source.tar": artifact.PrivateArchive(), "snapshot.json": mustJSON(artifact.Summary())} {
			f, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
			if err != nil {
				t.Fatal("compatibility fixture output already exists")
			}
			_, writeErr := f.Write(data)
			closeErr := f.Close()
			if writeErr != nil || closeErr != nil {
				t.Fatal("compatibility fixture output failed")
			}
		}
	}
}

func TestOwnedLocalSourceStoreCrossProcessCAS(t *testing.T) {
	root, store, pin, selection := storeFixture(t)
	barrier := t.TempDir()
	if err := os.Chmod(barrier, 0700); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	children := []*exec.Cmd{}
	for _, kind := range []string{"a", "b"} {
		command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestOwnedLocalSourceStoreCASChild$")
		command.Env = []string{"PATH=/usr/bin:/bin", "OPERATOR_SOURCE_CAS_CHILD=1", "OPERATOR_SOURCE_ROOT=" + root.Name(), "OPERATOR_SOURCE_UID=" + pin, "OPERATOR_SOURCE_BARRIER=" + barrier, "OPERATOR_SOURCE_KIND=" + kind}
		command.Stdout = io.Discard
		command.Stderr = io.Discard
		if err := command.Start(); err != nil {
			t.Fatal("fixed owned CAS test child failed to start")
		}
		children = append(children, command)
	}
	defer func() {
		for _, child := range children {
			if child.ProcessState == nil {
				_ = child.Process.Kill()
				_ = child.Wait()
			}
		}
	}()
	for {
		_, a := os.Stat(filepath.Join(barrier, "a.ready"))
		_, b := os.Stat(filepath.Join(barrier, "b.ready"))
		if a == nil && b == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("owned CAS children did not reserve")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if err := os.WriteFile(filepath.Join(barrier, "start"), []byte("start"), 0600); err != nil {
		t.Fatal(err)
	}
	success, conflict := 0, 0
	for _, child := range children {
		err := child.Wait()
		if err == nil {
			success++
		} else {
			var exit *exec.ExitError
			if errors.As(err, &exit) && exit.ExitCode() == 12 {
				conflict++
			} else {
				t.Fatal("owned CAS child failed outside expected version conflict")
			}
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatal("cross-process stale fetch committed twice")
	}
	reservation, err := store.Reserve(ctx, selection)
	if err != nil || reservation.Version != 1 {
		t.Fatal("cross-process source version incorrect")
	}
}
func TestOwnedLocalSourceStoreCASChild(t *testing.T) {
	if os.Getenv("OPERATOR_SOURCE_CAS_CHILD") != "1" {
		t.Skip("fixed helper invoked only by owned CAS parent")
	}
	root, err := os.OpenRoot(os.Getenv("OPERATOR_SOURCE_ROOT"))
	if err != nil {
		t.Fatal("owned CAS root unavailable")
	}
	defer root.Close()
	// Use the same reviewed fixture owner/repository, never caller credential or
	// arbitrary command input. Only bounded root/pin/rendezvous data crosses env.
	selection := sourceStoreSelection()
	store, err := OpenLocalStore(context.Background(), root, selection.Owner, selection.Binding.CheckoutURL, os.Getenv("OPERATOR_SOURCE_UID"))
	if err != nil {
		t.Fatal(err)
	}
	reservation, err := store.Reserve(context.Background(), selection)
	if err != nil || reservation.Version != 0 {
		t.Fatal("owned child did not reserve initial version")
	}
	kind := os.Getenv("OPERATOR_SOURCE_KIND")
	if kind != "a" && kind != "b" {
		t.Fatal("invalid owned CAS child kind")
	}
	barrier := os.Getenv("OPERATOR_SOURCE_BARRIER")
	if err = os.WriteFile(filepath.Join(barrier, kind+".ready"), []byte(strconv.FormatUint(reservation.Version, 10)), 0600); err != nil {
		t.Fatal("owned child rendezvous failed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		if _, err = os.Stat(filepath.Join(barrier, "start")); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("owned CAS rendezvous timed out")
		case <-time.After(5 * time.Millisecond):
		}
	}
	payload := []byte("owned-child-" + kind)
	facts := factsFor(selection, payload)
	facts.Commit = strings.Repeat(kind, 40)
	_, err = store.Commit(ctx, reservation, facts, payload)
	if errors.Is(err, ErrStoreConflict) {
		os.Exit(12)
	}
	if err != nil {
		data, _ := json.Marshal(map[string]string{"error": "SourceStoreCommitFailed"})
		t.Fatal(string(data))
	}
}
