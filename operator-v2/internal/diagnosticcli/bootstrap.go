// Package diagnosticcli implements privileged operator-local diagnosis. The
// caller supplies both Kubernetes credentials and an HS256 verification key;
// that shared key also permits issuing sessions. This is not a team API client.
package diagnosticcli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"sync"
	"time"

	c "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/catalogdiagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubejournal"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
)

const LocalMode = "privileged_operator_local_read_v1"
const MaxBootstrapBytes = 128 * 1024

var targetID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
var sessionValue = regexp.MustCompile(`^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$`)

type Error struct{ Code string }

func (e *Error) Error() string { return e.Code }
func fail(code string) error   { return &Error{Code: code} }

// Metadata is trusted installation configuration, never project authoring or
// proof created by discovering a matching resource name. Every UID/provenance
// record must originate from the separately authorized local bootstrap.
type Metadata struct {
	Mode             string                     `json:"mode"`
	Target           c.Target                   `json:"target"`
	Owner            ownership.Owner            `json:"owner"`
	ResourceRoot     string                     `json:"resourceRoot"`
	Cluster          kube.ClusterIdentity       `json:"cluster"`
	Endpoint         string                     `json:"endpoint"`
	ControlNamespace ownership.Record           `json:"controlNamespace"`
	ControlAttempt   string                     `json:"controlAttempt"`
	Journal          ownership.NamespacedRecord `json:"journal"`
	Desired          ownership.NamespacedRecord `json:"desired"`
	Policy           ownership.NamespacedRecord `json:"policy"`
	Issuer           string                     `json:"issuer"`
	Audience         string                     `json:"audience"`
	AllowedDomain    string                     `json:"allowedDomain"`
	SessionSeconds   int                        `json:"sessionSeconds"`
}

type SecretInputs struct{ private *secretValues }
type secretValues struct {
	mu                               sync.Mutex
	ca, certificate, key, sessionKey []byte
	cookie                           string
}

func NewSecretInputs(ca, certificate, key, sessionKey []byte, cookie string) (SecretInputs, error) {
	if len(ca) > 32768 || len(certificate) > 32768 || len(key) > 16384 || len(sessionKey) < 32 || len(sessionKey) > 4096 || len(cookie) > 8192 || !sessionValue.MatchString(cookie) {
		return SecretInputs{}, fail("LocalBootstrapInvalid")
	}
	if _, err := kube.NewCredentials(ca, certificate, key); err != nil {
		return SecretInputs{}, fail("LocalBootstrapInvalid")
	}
	return SecretInputs{&secretValues{ca: bytes.Clone(ca), certificate: bytes.Clone(certificate), key: bytes.Clone(key), sessionKey: bytes.Clone(sessionKey), cookie: cookie}}, nil
}
func (s SecretInputs) Close() {
	if s.private == nil {
		return
	}
	s.private.mu.Lock()
	defer s.private.mu.Unlock()
	clear(s.private.ca)
	clear(s.private.certificate)
	clear(s.private.key)
	clear(s.private.sessionKey)
	s.private.ca = nil
	s.private.certificate = nil
	s.private.key = nil
	s.private.sessionKey = nil
	s.private.cookie = ""
}
func (SecretInputs) Format(s fmt.State, _ rune) {
	_, _ = io.WriteString(s, "[private local diagnostic credentials]")
}
func (SecretInputs) MarshalJSON() ([]byte, error) {
	return nil, fail("LocalBootstrapSerializationDenied")
}

type Bootstrap struct{ private *bootstrapValue }
type bootstrapValue struct {
	mu       sync.Mutex
	metadata Metadata
	secrets  SecretInputs
}

func (Bootstrap) Format(s fmt.State, _ rune) {
	_, _ = io.WriteString(s, "[privileged local diagnostic bootstrap]")
}
func (Bootstrap) MarshalJSON() ([]byte, error) { return nil, fail("LocalBootstrapSerializationDenied") }

type deniedPolicy struct{}

func (deniedPolicy) CheckDiagnosticRead(context.Context, c.SessionIdentity, c.Target) (c.ReadDecision, error) {
	return c.ReadDecision{}, c.ErrForbidden
}

func NewBootstrap(metadata Metadata, secrets SecretInputs) (Bootstrap, error) {
	if secrets.private == nil {
		return Bootstrap{}, fail("LocalBootstrapInvalid")
	}
	secrets.private.mu.Lock()
	defer secrets.private.mu.Unlock()
	if metadata.Mode != LocalMode || metadata.Owner.Lifetime != ownership.Disposable || metadata.Owner.RunID == "" || metadata.Target.Environment != metadata.Owner.EnvironmentID || metadata.Target.Installation != metadata.Owner.InstallationID || metadata.Cluster.ID == "" || metadata.Cluster.KubeSystemUID == "" || metadata.ControlAttempt == "" || metadata.ControlNamespace.Owner != metadata.Owner || metadata.ControlNamespace.Ref.Purpose != "namespace" || metadata.ControlNamespace.Ref.ProviderID == "" || metadata.ControlNamespace.Ref.Scope.Provider != ownership.Kubernetes || metadata.ControlNamespace.Ref.Scope.ClusterID != metadata.Cluster.ID || metadata.ControlNamespace.Ref.Scope.Namespace != "" || metadata.SessionSeconds <= 0 || metadata.SessionSeconds > 7*24*60*60 {
		return Bootstrap{}, fail("LocalBootstrapInvalid")
	}
	if _, err := ownership.NewContext(ownership.Config{Owner: metadata.Owner, ResourceRoot: metadata.ResourceRoot, Scopes: []ownership.Scope{metadata.ControlNamespace.Ref.Scope}}); err != nil {
		return Bootstrap{}, fail("LocalBootstrapInvalid")
	}
	if !targetID.MatchString(metadata.Target.ProjectUID) || !targetID.MatchString(metadata.Target.Environment) || !targetID.MatchString(metadata.Target.Installation) || metadata.ControlNamespace.CreationOperationID == "" || metadata.ControlNamespace.CreationIdempotencyKey == "" {
		return Bootstrap{}, fail("LocalBootstrapInvalid")
	}
	endpoint, err := url.Parse(metadata.Endpoint)
	if err != nil {
		return Bootstrap{}, fail("LocalBootstrapInvalid")
	}
	port, err := strconv.Atoi(endpoint.Port())
	ip := net.ParseIP(endpoint.Hostname())
	if err != nil || port < 1 || port > 65535 || ip == nil || !ip.IsLoopback() && !ip.IsPrivate() || endpoint.Scheme != "https" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.ForceQuery || endpoint.Fragment != "" || endpoint.Path != "" && endpoint.Path != "/" || endpoint.RawPath != "" {
		return Bootstrap{}, fail("LocalBootstrapInvalid")
	}
	parent := ownership.NamespaceParent{ClusterID: metadata.Cluster.ID, Name: metadata.ControlNamespace.Ref.Name, UID: metadata.ControlNamespace.Ref.ProviderID, Owner: metadata.Owner, CreationOperationID: metadata.ControlNamespace.CreationOperationID, CreationIdempotencyKey: metadata.ControlNamespace.CreationIdempotencyKey}
	scope := ownership.Scope{Provider: ownership.Kubernetes, ClusterID: metadata.Cluster.ID, Namespace: parent.Name}
	subject := rolloutSubject(metadata.Target)
	for _, entry := range []struct {
		record         ownership.NamespacedRecord
		name, revision string
	}{{metadata.Journal, kubejournal.Name(subject), kubejournal.Revision}, {metadata.Desired, c.DesiredCellName(metadata.Target), c.DesiredRevision}, {metadata.Policy, c.PolicyCellName(metadata.Target), c.PolicyRevision}} {
		r := entry.record.Record
		if entry.record.Parent != parent || r.Owner != metadata.Owner || r.Ref.Scope != scope || r.Ref.Purpose != "configmap" || r.Ref.Name != entry.name || r.Ref.ProviderID == "" || r.Revision != entry.revision || r.SpecFingerprint == "" || r.CreationOperationID == "" || r.CreationIdempotencyKey == "" {
			return Bootstrap{}, fail("LocalBootstrapInvalid")
		}
	}
	// Reuse the full session configuration validator without any API read or
	// accepting a synthetic grant. The actual command supplies the live policy.
	key, err := c.NewSessionKey(secrets.private.sessionKey)
	if err != nil {
		return Bootstrap{}, fail("LocalBootstrapInvalid")
	}
	defer key.Close()
	if _, err = c.NewSessionAuthorizer(c.SessionConfig{Environment: metadata.Target.Environment, Installation: metadata.Target.Installation, Issuer: metadata.Issuer, Audience: metadata.Audience, AllowedDomain: metadata.AllowedDomain, MaxSessionLifetime: time.Duration(metadata.SessionSeconds) * time.Second, Now: time.Now, Key: key, Policy: deniedPolicy{}}); err != nil {
		return Bootstrap{}, fail("LocalBootstrapInvalid")
	}
	// Metadata contains records/scalars only; copy by value. Credential state is
	// already private and constructor-copied, never borrowed caller slices.
	copy, err := NewSecretInputs(secrets.private.ca, secrets.private.certificate, secrets.private.key, secrets.private.sessionKey, secrets.private.cookie)
	if err != nil {
		return Bootstrap{}, err
	}
	return Bootstrap{&bootstrapValue{metadata: metadata, secrets: copy}}, nil
}

type bootstrapWire struct {
	Metadata    Metadata `json:"metadata"`
	CA          []byte   `json:"ca"`
	Certificate []byte   `json:"certificate"`
	Key         []byte   `json:"key"`
	SessionKey  []byte   `json:"sessionKey"`
	Cookie      string   `json:"cookie"`
}

// WritePrivate is for the trusted launcher's owned anonymous pipe only. The
// launcher must START the consumer before writing: input may exceed pipe size.
// It is deliberately not a general fmt/JSON/log/history serialization method.
func (b Bootstrap) WritePrivate(out io.Writer) error {
	if b.private == nil {
		return fail("LocalBootstrapInvalid")
	}
	b.private.mu.Lock()
	defer b.private.mu.Unlock()
	if b.private.secrets.private == nil {
		return fail("LocalBootstrapInvalid")
	}
	s := b.private.secrets.private
	wire := bootstrapWire{b.private.metadata, s.ca, s.certificate, s.key, s.sessionKey, s.cookie}
	encoded, err := json.Marshal(wire)
	if err != nil || len(encoded) > MaxBootstrapBytes {
		return fail("LocalBootstrapInvalid")
	}
	defer clear(encoded)
	if n, err := out.Write(encoded); err != nil || n != len(encoded) {
		return fail("LocalBootstrapWriteFailed")
	}
	return nil
}

func DecodeBootstrap(in io.Reader) (Bootstrap, error) {
	data, err := io.ReadAll(io.LimitReader(in, MaxBootstrapBytes+1))
	if err != nil || len(data) > MaxBootstrapBytes {
		return Bootstrap{}, fail("LocalBootstrapReadFailed")
	}
	defer clear(data)
	var wire bootstrapWire
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&wire) != nil || decoder.Decode(new(any)) != io.EOF {
		return Bootstrap{}, fail("LocalBootstrapInvalid")
	}
	canonical, err := json.Marshal(wire)
	if err != nil || !bytes.Equal(canonical, bytes.TrimSpace(data)) {
		clear(canonical)
		return Bootstrap{}, fail("LocalBootstrapInvalid")
	}
	clear(canonical)
	secrets, err := NewSecretInputs(wire.CA, wire.Certificate, wire.Key, wire.SessionKey, wire.Cookie)
	clear(wire.CA)
	clear(wire.Certificate)
	clear(wire.Key)
	clear(wire.SessionKey)
	if err != nil {
		return Bootstrap{}, err
	}
	defer secrets.Close()
	return NewBootstrap(wire.Metadata, secrets)
}

func (b Bootstrap) Close() {
	if b.private == nil {
		return
	}
	b.private.mu.Lock()
	defer b.private.mu.Unlock()
	if b.private.secrets.private == nil {
		return
	}
	b.private.secrets.Close()
	b.private.secrets.private = nil
}
