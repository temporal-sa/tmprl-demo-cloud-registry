// Package lambdawrapper prepares a Lambda image's original process without
// becoming a runtime-interface client. Secret access is disabled by default.
// Policy is trusted installation configuration, not evidence that a function,
// owner or secret exists. Its producer must independently verify those bindings
// and deliver the policy and explicit credentials privately to the bootstrap.
// None of the execution inputs or returned handoff bytes belong in history/logs.
package lambdawrapper

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"strings"
)

const Version = "registry-lambda-wrapper-v1"
const (
	EnvManifest        = "TMPRL_DEMO_CLOUD_SECRET_ENV"
	FileManifest       = "TMPRL_DEMO_CLOUD_SECRET_FILES"
	OriginalEntrypoint = "TMPRL_ORIGINAL_ENTRYPOINT_B64"
	OriginalCommand    = "TMPRL_ORIGINAL_CMD_B64"
	BootstrapMode      = "TMPRL_WRAPPER_BOOTSTRAP"
	BootstrapFDMode    = "trusted-fd3-v1"
)

// Error contains only fixed codes, never provider errors, paths or secret data.
// CleanupIncomplete preserves the primary cause while reporting cleanup failure.
type Error struct {
	Code              string
	CleanupIncomplete bool
}

func (e *Error) Error() string {
	if e.CleanupIncomplete {
		return e.Code + ": private-file cleanup incomplete"
	}
	return e.Code
}
func fail(code string) error { return &Error{Code: code} }

type Scope struct{ Installation, Project, Component, FunctionARN, Region, Account string }
type Ref struct{ ARN, VersionID string }

// Grant authorizes one projection, not arbitrary access to all properties of an
// ARN. For files, Path is a logical legacy manifest target; PathEnv receives the
// actual private temporary filename. Path is NEVER followed by the filesystem.
type Grant struct {
	Ref                          Ref
	Property, Env, Path, PathEnv string
}
type Config struct {
	Scope  Scope
	Grants []Grant
}
type Policy struct{ private *policyData }
type policyData struct {
	scope  Scope
	grants []Grant
}

func (Policy) String() string               { return "LambdaWrapperPolicy{private}" }
func (p Policy) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, p.String()) }
func (Policy) MarshalJSON() ([]byte, error) { return json.Marshal("private") }

var regionPattern = regexp.MustCompile(`^[a-z]{2}(?:-[a-z]+)+-[0-9]+$`)
var accountPattern = regexp.MustCompile(`^[0-9]{12}$`)
var idPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:/-]{0,255}$`)
var versionPattern = regexp.MustCompile(`^[A-Za-z0-9-]{32,64}$`)
var envPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,127}$`)
var secretNamePattern = regexp.MustCompile(`^[A-Za-z0-9/_+=.@-]+-[A-Za-z0-9]{6}$`)
var functionNamePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
var qualifierPattern = regexp.MustCompile(`^(?:\$LATEST|[A-Za-z0-9_-]{1,128})$`)

func validFunctionARN(s Scope) bool {
	prefix := "arn:aws:lambda:" + s.Region + ":" + s.Account + ":function:"
	if !strings.HasPrefix(s.FunctionARN, prefix) || len(s.FunctionARN) > 256 {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(s.FunctionARN, prefix), ":")
	return (len(parts) == 1 || len(parts) == 2 && qualifierPattern.MatchString(parts[1])) && functionNamePattern.MatchString(parts[0])
}

func NewPolicy(c Config) (Policy, error) {
	s := c.Scope
	if !regionPattern.MatchString(s.Region) || !accountPattern.MatchString(s.Account) || !idPattern.MatchString(s.Installation) || !idPattern.MatchString(s.Project) || !idPattern.MatchString(s.Component) || !validFunctionARN(s) || len(c.Grants) > 64 {
		return Policy{}, fail("WrapperInvalidPolicy")
	}
	p := Policy{&policyData{scope: s, grants: append([]Grant(nil), c.Grants...)}}
	targets := map[string]bool{}
	paths := map[string]bool{}
	for _, g := range p.private.grants {
		prefix := "arn:aws:secretsmanager:" + s.Region + ":" + s.Account + ":secret:"
		if !strings.HasPrefix(g.Ref.ARN, prefix) || !secretNamePattern.MatchString(strings.TrimPrefix(g.Ref.ARN, prefix)) || len(g.Ref.ARN) > 2048 || !versionPattern.MatchString(g.Ref.VersionID) || len(g.Property) == 0 || len(g.Property) > 256 || strings.ContainsRune(g.Property, 0) {
			return Policy{}, fail("WrapperInvalidPolicy")
		}
		var target string
		if g.Env != "" {
			if g.Path != "" || g.PathEnv != "" || !allowedEnv(g.Env) {
				return Policy{}, fail("WrapperInvalidPolicy")
			}
			target = g.Env
		} else {
			if !strings.HasPrefix(g.Path, "/tmp/") || path.Clean(g.Path) != g.Path || len(g.Path) > 1024 || strings.ContainsRune(g.Path, 0) || !allowedEnv(g.PathEnv) || paths[g.Path] {
				return Policy{}, fail("WrapperInvalidPolicy")
			}
			paths[g.Path] = true
			target = g.PathEnv
		}
		if targets[target] {
			return Policy{}, fail("WrapperInvalidPolicy")
		}
		targets[target] = true
	}
	return p, nil
}

func allowedEnv(s string) bool {
	if !envPattern.MatchString(s) {
		return false
	}
	u := strings.ToUpper(s)
	if unsafeProcessSetting(u, "projected") {
		return false
	}
	for _, prefix := range []string{"AWS_", "TMPRL_", "LD_", "DYLD_", "BASH_FUNC_", "LC_"} {
		if strings.HasPrefix(u, prefix) {
			return false
		}
	}
	switch u {
	case "PATH", "HOME", "SHELL", "ENV", "BASH_ENV", "IFS", "CDPATH", "GODEBUG", "GOTRACEBACK", "GOPROXY", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "PYTHONPATH", "PYTHONHOME", "PYTHONSTARTUP", "PYTHONINSPECT", "NODE_OPTIONS", "NODE_PATH", "NODE_EXTRA_CA_CERTS", "JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "RUBYOPT", "RUBYLIB", "PERL5OPT", "PERL5LIB", "GCONV_PATH", "LOCPATH", "LANG", "SSL_CERT_FILE", "SSL_CERT_DIR", "OPENSSL_CONF", "OPENSSL_MODULES":
		return false
	}
	return true
}

// Source is an explicitly injected activity/process-local capability. Returned
// bytes are secret material, not durable DTOs. Implementations must bound their
// transport before decoding and must not log request/response/provider errors.
type Source interface {
	Get(context.Context, Ref) (Secret, error)
}
type Secret struct{ private *secretData }
type secretData struct {
	ref  Ref
	data []byte
}

func NewSecret(ref Ref, data []byte) (Secret, error) {
	if len(data) == 0 || len(data) > 65536 {
		return Secret{}, fail("WrapperSecretLimit")
	}
	return Secret{&secretData{ref, append([]byte(nil), data...)}}, nil
}
func (Secret) String() string               { return "LambdaWrapperSecret{redacted}" }
func (s Secret) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, s.String()) }
func (Secret) MarshalJSON() ([]byte, error) { return json.Marshal("redacted") }

type Input struct{ private *inputData }
type inputData struct {
	env  map[string]string
	args []string
}

func (Input) String() string               { return "LambdaWrapperInput{private}" }
func (i Input) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, i.String()) }
func (Input) MarshalJSON() ([]byte, error) { return json.Marshal("private") }

// NewInput snapshots the inherited environment and argv; it never mutates the
// process environment. Values are execution-only and deliberately opaque.
func NewInput(env, args []string) (Input, error) {
	if len(env) > 2048 || len(args) > 256 {
		return Input{}, fail("WrapperInputLimit")
	}
	d := &inputData{env: map[string]string{}, args: append([]string(nil), args...)}
	total := 0
	for _, s := range env {
		total += len(s)
		k, v, ok := strings.Cut(s, "=")
		if !ok || k == "" || strings.ContainsRune(s, 0) {
			return Input{}, fail("WrapperInvalidInput")
		}
		if _, exists := d.env[k]; exists {
			return Input{}, fail("WrapperInvalidInput")
		}
		if unsafeProcessSetting(k, v) {
			return Input{}, fail("WrapperUnsafeProcessSettings")
		}
		d.env[k] = v
	}
	for _, s := range args {
		total += len(s)
		if strings.ContainsRune(s, 0) {
			return Input{}, fail("WrapperInvalidInput")
		}
	}
	if total > 256*1024 {
		return Input{}, fail("WrapperInputLimit")
	}
	return Input{d}, nil
}

func unsafeProcessSetting(key, value string) bool {
	if value == "" {
		return false
	}
	switch key {
	case "GODEBUG", "GOTRACEBACK", "SSLKEYLOGFILE", "GRPC_BINARY_LOG_FILTER", "GRPC_GO_LOG_SEVERITY_LEVEL", "GRPC_GO_LOG_VERBOSITY_LEVEL", "AWS_SDK_GO_LOG_MODE", "AWS_SDK_GO_LOG_LEVEL":
		return true
	}
	return false
}

// CheckProcessSafety reads ONLY known process tracing switches. Go's HTTP/2
// GODEBUG tracing can log plaintext headers/payloads independently of an SDK
// NOP logger. Call before bootstrap decoding and before each secret read; do
// not attempt to fix it by clearing an environment variable after Go init.
func CheckProcessSafety() error {
	for _, key := range []string{"GODEBUG", "GOTRACEBACK", "SSLKEYLOGFILE", "GRPC_BINARY_LOG_FILTER", "GRPC_GO_LOG_SEVERITY_LEVEL", "GRPC_GO_LOG_VERBOSITY_LEVEL", "AWS_SDK_GO_LOG_MODE", "AWS_SDK_GO_LOG_LEVEL"} {
		if unsafeProcessSetting(key, os.Getenv(key)) {
			return fail("WrapperUnsafeProcessSettings")
		}
	}
	return nil
}
