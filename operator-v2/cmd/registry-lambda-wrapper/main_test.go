package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestExplicitChildPathResolution(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "owned-program")
	if err := os.WriteFile(filename, []byte("owned fixture, never executed"), 0700); err != nil {
		t.Fatal(err)
	}
	name, err := executable("owned-program", []string{"PATH=" + dir})
	if err != nil || name != filename {
		t.Fatal("explicit PATH ignored")
	}
	if _, err := executable("owned-program", nil); err == nil {
		t.Fatal("ambient PATH fallback")
	}
	if name, err := executable("./relative", nil); err != nil || name != "./relative" {
		t.Fatal("relative command metadata changed")
	}
	if err := os.Chmod(filename, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := executable("owned-program", []string{"PATH=" + dir}); err == nil {
		t.Fatal("nonexecutable accepted")
	}
}

func TestNoImplicitSecretBootstrap(t *testing.T) {
	// run fails before reaching native Exec. Synthetic input only; no descriptor,
	// credentials, network, or application command is opened/executed.
	t.Setenv("TMPRL_DEMO_CLOUD_SECRET_ENV", `[{"name":"APP_TOKEN","secretArn":"untrusted","property":"key"}]`)
	t.Setenv("TMPRL_WRAPPER_BOOTSTRAP", "")
	old := os.Args
	os.Args = []string{"wrapper", "/never-executed"}
	defer func() { os.Args = old }()
	err := run()
	if err == nil || !strings.Contains(err.Error(), "WrapperSecretsDisabled") {
		t.Fatal(err)
	}
}
