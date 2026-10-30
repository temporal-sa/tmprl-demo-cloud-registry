package lambdawrapper

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

type manifestRef struct {
	Name     string `json:"name,omitempty"`
	ARN      string `json:"secretArn"`
	ID       string `json:"secretId,omitempty"`
	Property string `json:"property"`
	Path     string `json:"path,omitempty"`
}
type projection struct {
	grant Grant
	value []byte
}
type Prepared struct{ private *preparedData }
type preparedData struct {
	mu           sync.Mutex
	command, env []string
	dir          string
	consumed     bool
}

func (Prepared) String() string               { return "LambdaWrapperPrepared{private}" }
func (p Prepared) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, p.String()) }
func (Prepared) MarshalJSON() ([]byte, error) { return json.Marshal("private") }

// Prepare validates ALL target grants before any source call and fetches and
// validates ALL values before creating files. It never changes os.Environ.
// tempParent is an explicit trusted filesystem capability (normally /tmp).
// The parent must not be replaceable by an attacker. Fresh 0700 staging avoids
// following legacy manifest paths or overwriting files in a reused sandbox.
func Prepare(ctx context.Context, in Input, policy Policy, source Source, tempParent string) (Prepared, error) {
	if in.private == nil {
		return Prepared{}, fail("WrapperInvalidInput")
	}
	if err := CheckProcessSafety(); err != nil {
		return Prepared{}, err
	}
	if ctx.Err() != nil {
		return Prepared{}, fail("WrapperCancelled")
	}
	command, err := originalCommand(in.private)
	if err != nil {
		return Prepared{}, err
	}
	var refs []manifestRef
	for _, kind := range []string{EnvManifest, FileManifest} {
		s := in.private.env[kind]
		if s == "" {
			continue
		}
		var items []manifestRef
		if err = decodeJSON([]byte(s), 32768, &items); err != nil {
			return Prepared{}, err
		}
		if items == nil {
			return Prepared{}, fail("WrapperInvalidManifest")
		}
		if len(items) > 64 || len(refs)+len(items) > 64 {
			return Prepared{}, fail("WrapperInputLimit")
		}
		for _, m := range items {
			if kind == EnvManifest {
				if m.Name == "" || m.Path != "" {
					return Prepared{}, fail("WrapperInvalidManifest")
				}
			} else {
				if m.Name != "" || m.Path == "" {
					return Prepared{}, fail("WrapperInvalidManifest")
				}
			}
			refs = append(refs, m)
		}
	}
	var work []projection
	targets := map[string]bool{}
	for _, m := range refs {
		if policy.private == nil || source == nil {
			return Prepared{}, fail("WrapperSecretsDisabled")
		}
		var matched *Grant
		for _, g := range policy.private.grants {
			if g.Ref.ARN == m.ARN && g.Property == m.Property && g.Env == m.Name && g.Path == m.Path {
				g := g
				matched = &g
				break
			}
		}
		if matched == nil {
			return Prepared{}, fail("WrapperProjectionDenied")
		}
		// Legacy secretId is only a consistency hint, never a name fallback.
		name := strings.SplitN(m.ARN, ":secret:", 2)[1]
		name = name[:len(name)-7]
		if m.ID != "" && m.ID != name {
			return Prepared{}, fail("WrapperProjectionDenied")
		}
		target := matched.Env
		if target == "" {
			target = matched.PathEnv
		}
		if targets[target] {
			return Prepared{}, fail("WrapperProjectionDenied")
		}
		targets[target] = true
		work = append(work, projection{grant: *matched})
	}
	cache := map[Ref]map[string]json.RawMessage{}
	total := 0
	for i := range work {
		if ctx.Err() != nil {
			return Prepared{}, fail("WrapperCancelled")
		}
		g := work[i].grant
		object, ok := cache[g.Ref]
		if !ok {
			secret, err := source.Get(ctx, g.Ref)
			if err != nil {
				return Prepared{}, fail("WrapperSecretReadFailed")
			}
			if ctx.Err() != nil {
				return Prepared{}, fail("WrapperCancelled")
			}
			if secret.private == nil || secret.private.ref != g.Ref {
				return Prepared{}, fail("WrapperSecretIdentityMismatch")
			}
			if err = decodeJSON(secret.private.data, 65536, &object); err != nil || object == nil {
				return Prepared{}, fail("WrapperInvalidSecret")
			}
			cache[g.Ref] = object
		}
		raw, exists := object[g.Property]
		if !exists {
			return Prepared{}, fail("WrapperMissingProperty")
		}
		var s string
		if len(raw) > 0 && raw[0] == '"' {
			if json.Unmarshal(raw, &s) != nil {
				return Prepared{}, fail("WrapperInvalidSecret")
			}
		} else if string(raw) == "null" {
			s = ""
		} else {
			var compact bytes.Buffer
			if json.Compact(&compact, raw) != nil {
				return Prepared{}, fail("WrapperInvalidSecret")
			}
			s = compact.String()
		}
		if g.Env != "" && strings.ContainsRune(s, 0) {
			return Prepared{}, fail("WrapperInvalidSecret")
		}
		total += len(s)
		if total > 256*1024 {
			return Prepared{}, fail("WrapperSecretLimit")
		}
		work[i].value = []byte(s)
	}
	p := Prepared{&preparedData{command: command}}
	env := map[string]string{}
	for k, v := range in.private.env {
		if !strings.HasPrefix(k, "TMPRL_") {
			env[k] = v
		}
	}
	for i, w := range work {
		if w.grant.Env != "" {
			env[w.grant.Env] = string(w.value)
			continue
		}
		if p.private.dir == "" {
			if !filepath.IsAbs(tempParent) || filepath.Clean(tempParent) != tempParent {
				return Prepared{}, fail("WrapperInvalidTempRoot")
			}
			dir, e := os.MkdirTemp(tempParent, "registry-lambda-secrets-")
			if e != nil {
				return Prepared{}, fail("WrapperFileMaterializationFailed")
			}
			p.private.dir = dir
		}
		filename := filepath.Join(p.private.dir, fmt.Sprintf("secret-%03d", i))
		f, e := os.OpenFile(filename, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e == nil {
			_, e = f.Write(w.value)
			if closeErr := f.Close(); e == nil {
				e = closeErr
			}
		}
		if e != nil {
			return Prepared{}, p.failure("WrapperFileMaterializationFailed")
		}
		env[w.grant.PathEnv] = filename
	}
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	total = 0
	for _, k := range keys {
		value := k + "=" + env[k]
		total += len(value)
		p.private.env = append(p.private.env, value)
	}
	if total > 256*1024 {
		return Prepared{}, p.failure("WrapperEnvironmentLimit")
	}
	if ctx.Err() != nil {
		return Prepared{}, p.failure("WrapperCancelled")
	}
	return p, nil
}

func originalCommand(d *inputData) ([]string, error) {
	decode := func(k string) ([]string, error) {
		s := d.env[k]
		if s == "" {
			return nil, nil
		}
		if len(s) > 32768 {
			return nil, fail("WrapperCommandLimit")
		}
		b, e := base64.StdEncoding.Strict().DecodeString(s)
		if e != nil {
			return nil, fail("WrapperInvalidCommand")
		}
		var raw []json.RawMessage
		if e = decodeJSON(b, 24576, &raw); e != nil {
			return nil, fail("WrapperInvalidCommand")
		}
		if len(raw) > 256 {
			return nil, fail("WrapperCommandLimit")
		}
		a := make([]string, len(raw))
		for i, v := range raw {
			if len(v) == 0 || v[0] != '"' || json.Unmarshal(v, &a[i]) != nil || strings.ContainsRune(a[i], 0) {
				return nil, fail("WrapperInvalidCommand")
			}
		}
		return a, nil
	}
	entry, err := decode(OriginalEntrypoint)
	if err != nil {
		return nil, err
	}
	cmd, err := decode(OriginalCommand)
	if err != nil {
		return nil, err
	}
	args := d.args
	if len(args) == 0 {
		args = cmd
	}
	result := append(append([]string(nil), entry...), args...)
	if len(result) == 0 || result[0] == "" || len(result) > 256 {
		return nil, fail("WrapperInvalidCommand")
	}
	return result, nil
}

// Execute is the sole raw process boundary. It passes secrets only in the child
// environment/private files, never as newly synthesized argv. replace must exec
// the process (not a shell); returned raw errors are discarded. On successful
// exec no Go cleanup runs; the private directory lives for this sandbox only.
func (p Prepared) Execute(replace func(string, []string, []string) error) error {
	if p.private == nil || replace == nil {
		return fail("WrapperInvalidHandoff")
	}
	p.private.mu.Lock()
	if p.private.consumed {
		p.private.mu.Unlock()
		return fail("WrapperHandoffConsumed")
	}
	p.private.consumed = true
	command := append([]string(nil), p.private.command...)
	env := append([]string(nil), p.private.env...)
	defer p.private.mu.Unlock()
	// Keep cleanup serialized through dispatch. A concurrent Close cannot remove
	// private files while the replacement callback is using them. Callbacks must
	// not reenter Close/Execute; the production callback is native exec.
	code := "WrapperExecReturned"
	if err := replace(command[0], command, env); err != nil {
		code = "WrapperExecFailed"
	}
	e := &Error{Code: code}
	if p.closeLocked() != nil {
		e.CleanupIncomplete = true
	}
	return e
}
func (p Prepared) failure(code string) error {
	e := &Error{Code: code}
	if p.Close() != nil {
		e.CleanupIncomplete = true
	}
	return e
}
func (p Prepared) Close() error {
	if p.private != nil {
		p.private.mu.Lock()
		defer p.private.mu.Unlock()
		return p.closeLocked()
	}
	return nil
}
func (p Prepared) closeLocked() error {
	p.private.consumed = true
	p.private.command = nil
	p.private.env = nil
	if p.private.dir != "" && os.RemoveAll(p.private.dir) != nil {
		return fail("WrapperFileCleanupFailed")
	}
	p.private.dir = ""
	return nil
}
