package lambdawrapper

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

var testRef = Ref{"arn:aws:secretsmanager:us-west-2:123456789012:secret:runtime-a1B2c3", "12345678-1234-1234-1234-123456789012"}

func testPolicy(t *testing.T, grants ...Grant) Policy {
	t.Helper()
	p, e := NewPolicy(Config{Scope: Scope{"installation", "project", "component", "arn:aws:lambda:us-west-2:123456789012:function:test", "us-west-2", "123456789012"}, Grants: grants})
	if e != nil {
		t.Fatal(e)
	}
	return p
}
func encode(v any) string { b, _ := json.Marshal(v); return string(b) }
func b64(v any) string    { return base64.StdEncoding.EncodeToString([]byte(encode(v))) }
func input(t *testing.T, env []string, args ...string) Input {
	t.Helper()
	in, e := NewInput(env, args)
	if e != nil {
		t.Fatal(e)
	}
	return in
}

type sourceFunc func(context.Context, Ref) (Secret, error)

func (f sourceFunc) Get(c context.Context, r Ref) (Secret, error) { return f(c, r) }
func testSource(t *testing.T, value string, calls *int) Source {
	return sourceFunc(func(_ context.Context, ref Ref) (Secret, error) { *calls++; return NewSecret(ref, []byte(value)) })
}
func envMap(env []string) map[string]string {
	m := map[string]string{}
	for _, s := range env {
		k, v, _ := strings.Cut(s, "=")
		m[k] = v
	}
	return m
}

func TestOriginalProcessArguments(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		entry, cmd, args, want []string
	}{
		{"default", []string{"/entry", "fixed"}, []string{"handler"}, nil, []string{"/entry", "fixed", "handler"}},
		{"override", []string{"/entry", "fixed"}, []string{"old"}, []string{"new", ""}, []string{"/entry", "fixed", "new", ""}},
		{"no-entry", nil, []string{"/program", "arg"}, nil, []string{"/program", "arg"}},
		{"cli-no-entry", nil, []string{"/old"}, []string{"/new", "arg"}, []string{"/new", "arg"}},
		{"entry-only", []string{"/entry"}, nil, nil, []string{"/entry"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, e := Prepare(context.Background(), input(t, []string{OriginalEntrypoint + "=" + b64(tc.entry), OriginalCommand + "=" + b64(tc.cmd), "APP=plain"}, tc.args...), Policy{}, nil, t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			called := false
			e = p.Execute(func(path string, args, env []string) error {
				called = true
				if path != tc.want[0] || !reflect.DeepEqual(args, tc.want) {
					t.Fatalf("argument contract differs: %v", args)
				}
				if envMap(env)["APP"] != "plain" || len(envMap(env)) != 1 {
					t.Fatal("bootstrap configuration escaped")
				}
				return errors.New("untrusted exec failure")
			})
			if !called || e.Error() != "WrapperExecFailed" {
				t.Fatal(e)
			}
		})
	}
	for _, raw := range []string{b64([]string{}), "%%", b64([]string{"\x00"}), b64([]int{1}), b64([]any{"/entry", nil})} {
		_, err := Prepare(context.Background(), input(t, []string{OriginalCommand + "=" + raw}), Policy{}, nil, t.TempDir())
		if err == nil {
			t.Fatal("invalid command accepted")
		}
	}
}

func TestHandoffIsSingleUseAndInputIsCopied(t *testing.T) {
	env := []string{"APP=original"}
	args := []string{"/entry", "original"}
	in, err := NewInput(env, args)
	if err != nil {
		t.Fatal(err)
	}
	env[0] = "APP=changed"
	args[1] = "changed"
	p, err := Prepare(context.Background(), in, Policy{}, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	alias := p
	calls := 0
	if err = p.Execute(func(_ string, args, env []string) error {
		calls++
		if args[1] != "original" || envMap(env)["APP"] != "original" {
			t.Fatal("input alias escaped")
		}
		return errors.New("raw")
	}); err == nil {
		t.Fatal("returned exec treated as success")
	}
	if err = alias.Execute(func(string, []string, []string) error { calls++; return nil }); err == nil || err.Error() != "WrapperHandoffConsumed" || calls != 1 {
		t.Fatal("handoff replayed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = Prepare(ctx, in, Policy{}, nil, t.TempDir()); err == nil || err.Error() != "WrapperCancelled" {
		t.Fatal("cancelled process prepared")
	}
}

func TestCloseWaitsForHandoffBeforeRemovingFiles(t *testing.T) {
	policy := testPolicy(t, Grant{Ref: testRef, Property: "key", Path: "/tmp/key", PathEnv: "KEY_FILE"})
	calls := 0
	p, err := Prepare(context.Background(), input(t, []string{FileManifest + "=" + encode([]manifestRef{{ARN: testRef.ARN, Property: "key", Path: "/tmp/key"}})}, "/entry"), policy, testSource(t, `{"key":"synthetic"}`, &calls), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan string, 1), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	executed := make(chan error, 1)
	go func() {
		executed <- p.Execute(func(_ string, _ []string, env []string) error {
			entered <- envMap(env)["KEY_FILE"]
			<-release
			return errors.New("raw")
		})
	}()
	filename := <-entered
	attempted, closed := make(chan struct{}), make(chan error, 1)
	go func() { close(attempted); closed <- p.Close() }()
	<-attempted
	select {
	case <-closed:
		t.Fatal("Close returned while exec dispatch was in flight")
	case <-time.After(25 * time.Millisecond):
	}
	if _, err := os.Stat(filename); err != nil {
		t.Fatal("in-flight dispatch lost its secret file")
	}
	unblock()
	if err := <-executed; err == nil || err.Error() != "WrapperExecFailed" {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filename); !os.IsNotExist(err) {
		t.Fatal("handoff cleanup did not finish")
	}
}

func TestFunctionARNGrammarIsBounded(t *testing.T) {
	scope := testPolicy(t).private.scope
	for _, suffix := range []string{"name", "name_1-A:$LATEST", "name:12", "name:release-2026"} {
		scope.FunctionARN = "arn:aws:lambda:us-west-2:123456789012:function:" + suffix
		if _, err := NewPolicy(Config{Scope: scope}); err != nil {
			t.Fatalf("valid bounded ARN denied: %s", suffix)
		}
	}
	for _, suffix := range []string{"name/path", "name.with.dot", "name:bad/path", "name:", "name:alias:extra", "name:$OTHER", "name:" + strings.Repeat("x", 129), strings.Repeat("n", 65)} {
		scope.FunctionARN = "arn:aws:lambda:us-west-2:123456789012:function:" + suffix
		if _, err := NewPolicy(Config{Scope: scope}); err == nil {
			t.Fatal("invalid function ARN accepted")
		}
	}
}

func TestExactAuthorizationPrecedesFetchAndMutation(t *testing.T) {
	grant := Grant{Ref: testRef, Property: "key", Env: "APP_TOKEN"}
	policy := testPolicy(t, grant)
	calls := 0
	src := testSource(t, `{"key":"SYNTHETIC_PRIVATE_VALUE"}`, &calls)
	for _, tc := range []struct {
		name     string
		policy   Policy
		manifest string
	}{
		{"default-off", Policy{}, encode([]manifestRef{{Name: "APP_TOKEN", ARN: testRef.ARN, Property: "key"}})},
		{"wrong-property", policy, encode([]manifestRef{{Name: "APP_TOKEN", ARN: testRef.ARN, Property: "other"}})},
		{"wrong-target", policy, encode([]manifestRef{{Name: "OTHER", ARN: testRef.ARN, Property: "key"}})},
		{"name-fallback", policy, encode([]manifestRef{{Name: "APP_TOKEN", ID: "runtime", Property: "key"}})},
		{"inconsistent-legacy-id", policy, encode([]manifestRef{{Name: "APP_TOKEN", ARN: testRef.ARN, ID: "foreign", Property: "key"}})},
		{"later-denied", policy, encode([]manifestRef{{Name: "APP_TOKEN", ARN: testRef.ARN, Property: "key"}, {Name: "FOREIGN", ARN: testRef.ARN, Property: "key"}})},
		{"duplicate", policy, encode([]manifestRef{{Name: "APP_TOKEN", ARN: testRef.ARN, Property: "key"}, {Name: "APP_TOKEN", ARN: testRef.ARN, Property: "key"}})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Prepare(context.Background(), input(t, []string{EnvManifest + "=" + tc.manifest}, "/entry"), tc.policy, src, t.TempDir())
			if err == nil || calls != 0 {
				t.Fatal("denial happened after fetch")
			}
		})
	}
	for _, key := range []string{"AWS_SECRET_ACCESS_KEY", "TMPRL_ORIGINAL_CMD_B64", "PATH", "LD_PRELOAD", "NODE_OPTIONS", "https_proxy", "BASH_ENV"} {
		_, err := NewPolicy(Config{Scope: policy.private.scope, Grants: []Grant{{Ref: testRef, Property: "key", Env: key}}})
		if err == nil {
			t.Fatalf("control target accepted: %s", key)
		}
	}
	wrong := grant
	wrong.Ref.ARN = strings.ReplaceAll(testRef.ARN, "123456789012", "999999999999")
	if _, err := NewPolicy(Config{Scope: policy.private.scope, Grants: []Grant{wrong}}); err == nil {
		t.Fatal("foreign account accepted")
	}
	wrong = grant
	wrong.Ref.VersionID = "AWSCURRENT"
	if _, err := NewPolicy(Config{Scope: policy.private.scope, Grants: []Grant{wrong}}); err == nil {
		t.Fatal("unpinned version accepted")
	}
}

func TestPrivateFilesAndPropertyParity(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(root, "outside")
	if err := os.WriteFile(outside, []byte("unchanged"), 0600); err != nil {
		t.Fatal(err)
	}
	logical := "/tmp/legacy/tls.key"
	grants := []Grant{{Ref: testRef, Property: "key", Env: "APP_TOKEN"}, {Ref: testRef, Property: "cert", Path: logical, PathEnv: "TEMPORAL_TLS_CLIENT_KEY"}, {Ref: testRef, Property: "cert", Path: "/tmp/legacy/second", PathEnv: "CERT_TWO"}, {Ref: testRef, Property: "bool", Env: "TLS"}, {Ref: testRef, Property: "null", Env: "EMPTY"}, {Ref: testRef, Property: "nested", Env: "CONFIG"}, {Ref: testRef, Property: "number", Env: "COUNT"}}
	policy := testPolicy(t, grants...)
	calls := 0
	src := testSource(t, `{"key":"SYNTHETIC_PRIVATE_VALUE","cert":"certificate\u0000bytes","bool":true,"null":null,"nested":{"x":[1,false]},"number":1.20e3}`, &calls)
	var envRefs, fileRefs []manifestRef
	for _, g := range grants {
		m := manifestRef{Name: g.Env, ARN: g.Ref.ARN, Property: g.Property, Path: g.Path}
		if g.Env != "" {
			envRefs = append(envRefs, m)
		} else {
			fileRefs = append(fileRefs, m)
		}
	}
	in := input(t, []string{EnvManifest + "=" + encode(envRefs), FileManifest + "=" + encode(fileRefs), "APP_TOKEN=old", "CERT_TWO=old", "TEMPORAL_TLS_CLIENT_KEY=old", BootstrapMode + "=" + BootstrapFDMode}, "/entry", "handler")
	p, err := Prepare(context.Background(), in, policy, src, root)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatal("secret version was not cached")
	}
	files := []string{}
	err = p.Execute(func(_ string, args, env []string) error {
		if !reflect.DeepEqual(args, []string{"/entry", "handler"}) {
			t.Fatal("materialization changed argv")
		}
		m := envMap(env)
		if m["APP_TOKEN"] != "SYNTHETIC_PRIVATE_VALUE" || m["TLS"] != "true" || m["EMPTY"] != "" || m["CONFIG"] != `{"x":[1,false]}` || m["COUNT"] != "1.20e3" {
			t.Fatal("property conversion differs")
		}
		for _, key := range []string{"TEMPORAL_TLS_CLIENT_KEY", "CERT_TWO"} {
			name := m[key]
			if name == logical || !strings.HasPrefix(name, root+string(os.PathSeparator)) {
				t.Fatal("legacy path was followed")
			}
			data, e := os.ReadFile(name)
			if e != nil || string(data) != "certificate\x00bytes" {
				t.Fatal("file content differs")
			}
			info, _ := os.Stat(name)
			parent, _ := os.Stat(filepath.Dir(name))
			if info.Mode().Perm() != 0600 || parent.Mode().Perm() != 0700 {
				t.Fatal("secret permissions too broad")
			}
			files = append(files, name)
		}
		return errors.New("raw provider token SYNTHETIC_PRIVATE_VALUE")
	})
	if err.Error() != "WrapperExecFailed" {
		t.Fatal(err)
	}
	for _, name := range files {
		if _, err := os.Stat(name); !os.IsNotExist(err) {
			t.Fatal("failed handoff left private files")
		}
	}
	data, _ := os.ReadFile(outside)
	if string(data) != "unchanged" {
		t.Fatal("unrelated file changed")
	}
}

func TestMaterializationFailureIsSafeAndLeavesNoFiles(t *testing.T) {
	grant := Grant{Ref: testRef, Property: "key", Path: "/tmp/key", PathEnv: "KEY_FILE"}
	policy := testPolicy(t, grant)
	for _, value := range []string{`{"key":"one","key":"two"}`, `{"key":{"x":1,"x":2}}`, `{}`, `null`, `{"key":"x"} trailing`, strings.Repeat(" ", 65537)} {
		root := t.TempDir()
		calls := 0
		src := testSource(t, value, &calls)
		_, err := Prepare(context.Background(), input(t, []string{FileManifest + "=" + encode([]manifestRef{{ARN: testRef.ARN, Property: "key", Path: "/tmp/key"}})}, "/entry"), policy, src, root)
		if err == nil {
			t.Fatal("invalid secret accepted")
		}
		files, _ := os.ReadDir(root)
		if len(files) != 0 {
			t.Fatal("failed secret decoding wrote files")
		}
	}
	for _, manifest := range []string{`[{"name":"APP","name":"OTHER"}]`, `[{"Name":"APP"}]`, `[{"unknown":"APP"}]`, `null`, `[null]`} {
		calls := 0
		_, err := Prepare(context.Background(), input(t, []string{EnvManifest + "=" + manifest}, "/entry"), policy, testSource(t, `{}`, &calls), t.TempDir())
		if err == nil || calls != 0 {
			t.Fatal("invalid manifest reached source")
		}
	}
	wrong := sourceFunc(func(context.Context, Ref) (Secret, error) {
		r := testRef
		r.VersionID = strings.Repeat("0", 32)
		return NewSecret(r, []byte(`{"key":"x"}`))
	})
	_, err := Prepare(context.Background(), input(t, []string{FileManifest + "=" + encode([]manifestRef{{ARN: testRef.ARN, Property: "key", Path: "/tmp/key"}})}, "/entry"), policy, wrong, t.TempDir())
	if err == nil || err.Error() != "WrapperSecretIdentityMismatch" {
		t.Fatal(err)
	}
	raw := sourceFunc(func(context.Context, Ref) (Secret, error) {
		return Secret{}, errors.New("RAW_CREDENTIAL_DO_NOT_PUBLISH")
	})
	_, err = Prepare(context.Background(), input(t, []string{FileManifest + "=" + encode([]manifestRef{{ARN: testRef.ARN, Property: "key", Path: "/tmp/key"}})}, "/entry"), policy, raw, t.TempDir())
	if err == nil || err.Error() != "WrapperSecretReadFailed" {
		t.Fatal(err)
	}
}

func TestOpaqueExecutionValuesNeverFormatSecrets(t *testing.T) {
	marker := "PRIVATE_EXECUTION_MARKER"
	secret, _ := NewSecret(testRef, []byte(marker))
	in := input(t, []string{"TOKEN=" + marker}, "/"+marker)
	p, err := Prepare(context.Background(), in, Policy{}, nil, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for _, value := range []any{secret, &secret, in, &in, p, &p, testPolicy(t), credentials{&credentialData{marker, marker, marker}}} {
		for _, verb := range []string{"%v", "%+v", "%#v", "%d", "%x", "%q", "%p", "%032.4v"} {
			out := fmt.Sprintf(verb, value)
			if strings.Contains(out, marker) || strings.Contains(out, "50524956415445") || len(out) > 256 {
				t.Fatalf("unsafe format for %s", verb)
			}
		}
		data, err := json.Marshal(value)
		if err != nil || strings.Contains(string(data), marker) {
			t.Fatal("unsafe JSON")
		}
	}
}
