//go:build wrapper_integration

package lambdawrapper

import (
	"context"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// This test executes only this owned compiled test helper, with a scrubbed
// environment. It proves native exec/argv/env/file handoff, not Lambda delivery,
// image compatibility, IAM, or an AWS runtime-interface client.
func TestNativeProcessHandoff(t *testing.T) {
	switch os.Getenv("WRAPPER_TEST_STAGE") {
	case "prepare":
		policy := testPolicy(t, Grant{Ref: testRef, Property: "key", Env: "APP_TOKEN"})
		calls := 0
		in := input(t, []string{"WRAPPER_TEST_STAGE=target", OriginalEntrypoint + "=" + b64([]string{os.Args[0], "-test.run=^TestNativeProcessHandoff$"}), EnvManifest + "=" + encode([]manifestRef{{Name: "APP_TOKEN", ARN: testRef.ARN, Property: "key"}})}, "--", "handler-argument")
		p, err := Prepare(context.Background(), in, policy, testSource(t, `{"key":"SYNTHETIC_ONLY"}`, &calls), os.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		err = p.Execute(syscall.Exec)
		t.Fatal(err)
	case "target":
		if os.Getenv("APP_TOKEN") != "SYNTHETIC_ONLY" || os.Getenv(EnvManifest) != "" || !reflect.DeepEqual(os.Args[2:], []string{"--", "handler-argument"}) {
			os.Exit(33)
		}
		os.Exit(0)
	default:
		cmd := exec.Command(os.Args[0], "-test.run=^TestNativeProcessHandoff$")
		cmd.Env = []string{"WRAPPER_TEST_STAGE=prepare"}
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("owned helper failed: %v (%d bytes)", err, len(out))
		}
	}
}

// The harness supplies the actual maintained binary. Bootstrap grants are not
// requested by manifests, so constructing its strict SDK client remains lazy.
// All credential text is synthetic. No network or Lambda service is involved.
func TestNativeBinaryBootstrapFD(t *testing.T) {
	if os.Getenv("WRAPPER_FD_STAGE") == "target" {
		want, err := strconv.ParseUint(os.Getenv("BOOTSTRAP_TEST_INODE"), 10, 64)
		if err != nil {
			os.Exit(41)
		}
		var stat syscall.Stat_t
		if syscall.Fstat(3, &stat) == nil && stat.Ino == want {
			os.Exit(42)
		}
		if os.Getenv(BootstrapMode) != "" || os.Getenv(OriginalEntrypoint) != "" || os.Getenv("APP_KEEP") != "preserved" || !reflect.DeepEqual(os.Args[2:], []string{"--", "handler"}) {
			os.Exit(43)
		}
		os.Exit(0)
	}
	binary := os.Getenv("WRAPPER_NATIVE_BINARY")
	if binary == "" {
		t.Fatal("run through scripts/run_lambda_wrapper_native_tests.py")
	}
	f, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err = w.WriteString(bootstrapJSON()); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	inode := info.Sys().(*syscall.Stat_t).Ino
	cmd := exec.Command(binary, "--", "handler")
	cmd.Env = []string{"WRAPPER_FD_STAGE=target", "BOOTSTRAP_TEST_INODE=" + strconv.FormatUint(inode, 10), "APP_KEEP=preserved", BootstrapMode + "=" + BootstrapFDMode, OriginalEntrypoint + "=" + b64([]string{os.Args[0], "-test.run=^TestNativeBinaryBootstrapFD$"})}
	cmd.ExtraFiles = []*os.File{f}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("owned wrapper binary failed: %v (%d bytes)", err, len(out))
	}
}

func TestNativeBootstrapStalledWriterAndWrongDescriptor(t *testing.T) {
	binary := os.Getenv("WRAPPER_NATIVE_BINARY")
	if binary == "" {
		t.Fatal("run through scripts/run_lambda_wrapper_native_tests.py")
	}
	t.Run("stalled-writer", func(t *testing.T) {
		reader, writer, err := os.Pipe()
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
		defer writer.Close()
		// A partial bootstrap plus a live, non-closing writer must not leave a
		// blocking read or goroutine behind after the fixed five-second deadline.
		if _, err = writer.WriteString("{"); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, binary, "/never-executed")
		cmd.Env = []string{BootstrapMode + "=" + BootstrapFDMode}
		cmd.ExtraFiles = []*os.File{reader}
		started := time.Now()
		out, err := cmd.CombinedOutput()
		if err == nil || ctx.Err() != nil || time.Since(started) > 7*time.Second || !strings.Contains(string(out), "WrapperBootstrapReadFailed") {
			t.Fatalf("stalled pipe not bounded: %v (%d bytes)", err, len(out))
		}
	})
	t.Run("regular-file", func(t *testing.T) {
		f, err := os.CreateTemp(t.TempDir(), "bootstrap-regular")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		if _, err = f.WriteString(bootstrapJSON()); err != nil {
			t.Fatal(err)
		}
		if _, err = f.Seek(0, 0); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, binary, "/never-executed")
		cmd.Env = []string{BootstrapMode + "=" + BootstrapFDMode}
		cmd.ExtraFiles = []*os.File{f}
		out, err := cmd.CombinedOutput()
		if err == nil || ctx.Err() != nil || !strings.Contains(string(out), "WrapperBootstrapNotPollablePipe") {
			t.Fatalf("wrong descriptor not rejected: %v (%d bytes)", err, len(out))
		}
	})
}
