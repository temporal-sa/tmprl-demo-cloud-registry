package catalogdiagnostics

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"reflect"
	"regexp"
	"sort"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const PolicyDataKey = "diagnostic-policy.json"
const PolicyRevision = "diagnostic-policy-schema-1"
const policyCellLabel = "registry.tmprl-demo.cloud/diagnostic-policy-cell"
const LocalPinnedPolicy = "local_pinned_diagnostic_policy"

var sessionFingerprint = regexp.MustCompile(`^[a-f0-9]{64}$`)

// ProjectReadPolicy is an explicit project ACL, never a default employee grant.
// Denied subjects and session fingerprints take precedence. AuthenticationAt
// permits revoking old sessions for an otherwise still-authorized principal.
// Policy expiry is intentional, bounded by seven days from IssuedAt.
type ProjectReadPolicy struct {
	SchemaVersion   int             `json:"schemaVersion"`
	Target          Target          `json:"target"`
	Issuer          string          `json:"issuer"`
	IssuedAt        time.Time       `json:"issuedAt"`
	ExpiresAt       time.Time       `json:"expiresAt"`
	Subjects        []PolicySubject `json:"subjects"`
	RevokedSessions []string        `json:"revokedSessions,omitempty"`
}
type PolicySubject struct {
	Subject                string    `json:"subject"`
	Allowed                bool      `json:"allowed"`
	AuthenticatedAtOrAfter time.Time `json:"authenticatedAtOrAfter"`
}

func (p ProjectReadPolicy) JSON() ([]byte, error) {
	if p.SchemaVersion != 1 || !p.Target.valid() || p.Issuer == "" || len(p.Issuer) > 512 || p.IssuedAt.IsZero() || !p.ExpiresAt.After(p.IssuedAt) || p.ExpiresAt.Sub(p.IssuedAt) > 7*24*time.Hour || len(p.Subjects) > 256 || len(p.RevokedSessions) > 256 {
		return nil, ErrInvalidSource
	}
	previous := ""
	for _, subject := range p.Subjects {
		if !googleSubject.MatchString(subject.Subject) || subject.Subject <= previous || subject.AuthenticatedAtOrAfter.IsZero() || subject.AuthenticatedAtOrAfter.After(p.ExpiresAt) {
			return nil, ErrInvalidSource
		}
		previous = subject.Subject
	}
	previous = ""
	for _, fingerprint := range p.RevokedSessions {
		if !sessionFingerprint.MatchString(fingerprint) || fingerprint <= previous {
			return nil, ErrInvalidSource
		}
		previous = fingerprint
	}
	encoded, err := json.Marshal(p)
	if err != nil || len(encoded) > 65536 {
		return nil, ErrInvalidSource
	}
	return encoded, nil
}

func PolicyCellName(target Target) string {
	data, _ := json.Marshal(target)
	digest := sha256.Sum256(data)
	return "project-read-policy-" + hex.EncodeToString(digest[:16])
}
func PolicyBootstrapObject(namespace string, policy ProjectReadPolicy) (*corev1.ConfigMap, error) {
	data, err := policy.JSON()
	if err != nil || namespace == "" || len(namespace) > 63 {
		return nil, ErrInvalidSource
	}
	return &corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}, ObjectMeta: metav1.ObjectMeta{Name: PolicyCellName(policy.Target), Namespace: namespace, Labels: map[string]string{policyCellLabel: "true"}}, Data: map[string]string{PolicyDataKey: string(data)}}, nil
}

type PolicyCell struct {
	Target    Target
	Namespace DesiredBinding
	Pinned    ownership.NamespacedRecord
}
type PinnedPolicyConfig struct {
	Mode                      string
	Environment, Installation string
	Cells                     []PolicyCell
	Now                       func() time.Time
}
type PinnedReadPolicy struct {
	environment, installation string
	cells                     map[Target]PolicyCell
	now                       func() time.Time
}

func NewPinnedReadPolicy(config PinnedPolicyConfig) (*PinnedReadPolicy, error) {
	if config.Mode != LocalPinnedPolicy || !scopeID.MatchString(config.Environment) || !scopeID.MatchString(config.Installation) || config.Now == nil || len(config.Cells) > 4096 {
		return nil, ErrInvalidSource
	}
	p := &PinnedReadPolicy{environment: config.Environment, installation: config.Installation, cells: make(map[Target]PolicyCell, len(config.Cells)), now: config.Now}
	for _, cell := range config.Cells {
		if cell.Namespace == nil || !cell.Target.valid() || cell.Target.Environment != p.environment || cell.Target.Installation != p.installation {
			return nil, ErrInvalidSource
		}
		c := cell.Namespace.Context()
		r := cell.Pinned.Record
		owner := c.Owner()
		if c.ParentPurpose() != "namespace" || owner.Lifetime != ownership.Disposable || owner.RunID == "" || owner.EnvironmentID != p.environment || owner.InstallationID != p.installation || cell.Pinned.Parent != c.Parent() || r.Owner != owner || r.Ref.Scope != c.Scope() || r.Ref.Purpose != "configmap" || r.Ref.Name != PolicyCellName(cell.Target) || r.Ref.ProviderID == "" || r.Revision != PolicyRevision || r.SpecFingerprint == "" || r.CreationOperationID == "" || r.CreationIdempotencyKey == "" {
			return nil, ErrInvalidSource
		}
		if _, exists := p.cells[cell.Target]; exists {
			return nil, ErrInvalidSource
		}
		p.cells[cell.Target] = cell
	}
	return p, nil
}

// CheckDiagnosticRead reads the current policy on every call. It has no cache,
// project-existence discovery, writer method or implicit membership fallback.
// Its identity argument is a verifier result; calling it with an invented DTO
// does not authenticate a caller. HTTP route wiring must use the verified
// SessionAuthorizer; the exported IssueReadGrant constructor is a trusted seam.
func (p *PinnedReadPolicy) CheckDiagnosticRead(ctx context.Context, identity SessionIdentity, target Target) (ReadDecision, error) {
	denied := ReadDecision{Allowed: false}
	if !target.valid() || target.Environment != p.environment || target.Installation != p.installation || !googleSubject.MatchString(identity.Subject) || identity.Issuer == "" || !sessionFingerprint.MatchString(identity.Fingerprint) {
		return denied, nil
	}
	cell, ok := p.cells[target]
	if !ok {
		return denied, nil
	}
	if ctx.Err() != nil {
		return ReadDecision{}, ErrAuthorizationUnavailable
	}
	snapshot, err := cell.Namespace.ObserveConfigMap(ctx, cell.Pinned)
	if err != nil {
		return ReadDecision{}, ErrAuthorizationUnavailable
	}
	o := snapshot.Object
	r := cell.Pinned.Record
	observed := snapshot.Observation.Observation
	if o == nil || o.Name != r.Ref.Name || o.Namespace != r.Ref.Scope.Namespace || string(o.UID) != r.Ref.ProviderID || o.ResourceVersion == "" || o.DeletionTimestamp != nil || observed.Ref != r.Ref || observed.Owner != r.Owner || observed.CreationIdempotencyKey != r.CreationIdempotencyKey || snapshot.Observation.ParentUID != cell.Pinned.Parent.UID || observed.Revision != PolicyRevision || o.Annotations[kube.CreationOperationAnnotation] != r.CreationOperationID || len(o.Data) != 1 || len(o.BinaryData) != 0 || o.Immutable != nil {
		return ReadDecision{}, ErrAuthorizationUnavailable
	}
	data, ok := o.Data[PolicyDataKey]
	if !ok || len(data) > 65536 {
		return ReadDecision{}, ErrAuthorizationUnavailable
	}
	var policy ProjectReadPolicy
	d := json.NewDecoder(bytes.NewReader([]byte(data)))
	d.DisallowUnknownFields()
	if d.Decode(&policy) != nil || d.Decode(new(any)) != io.EOF {
		return ReadDecision{}, ErrAuthorizationUnavailable
	}
	canonical, err := policy.JSON()
	if err != nil || !bytes.Equal(canonical, bytes.TrimSpace([]byte(data))) || policy.Target != target {
		return ReadDecision{}, ErrAuthorizationUnavailable
	}
	expected, err := PolicyBootstrapObject(o.Namespace, policy)
	if err != nil {
		return ReadDecision{}, ErrAuthorizationUnavailable
	}
	fingerprint, err := kube.ConfigMapFingerprint(expected)
	if err != nil || fingerprint != observed.SpecFingerprint || !reflect.DeepEqual(expected.Labels, o.Labels) {
		return ReadDecision{}, ErrAuthorizationUnavailable
	}
	now := p.now()
	if ctx.Err() != nil {
		return ReadDecision{}, ErrAuthorizationUnavailable
	}
	if policy.Issuer != identity.Issuer || now.Before(policy.IssuedAt) || !now.Before(policy.ExpiresAt) || now.Before(identity.IssuedAt) || !now.Before(identity.ExpiresAt) {
		return denied, nil
	}
	index := sort.SearchStrings(policy.RevokedSessions, identity.Fingerprint)
	if index < len(policy.RevokedSessions) && policy.RevokedSessions[index] == identity.Fingerprint {
		return denied, nil
	}
	index = sort.Search(len(policy.Subjects), func(i int) bool { return policy.Subjects[i].Subject >= identity.Subject })
	if index == len(policy.Subjects) || policy.Subjects[index].Subject != identity.Subject || !policy.Subjects[index].Allowed || identity.AuthenticatedAt.Before(policy.Subjects[index].AuthenticatedAtOrAfter) {
		return denied, nil
	}
	// Bind grants to the exact policy incarnation, provider version and payload.
	// Even metadata-only policy writes require a fresh HTTP authorization.
	id := sha256.Sum256([]byte(string(o.UID) + "\x00" + o.ResourceVersion + "\x00" + string(canonical)))
	expiry := now.Add(MaxGrantLifetime)
	if policy.ExpiresAt.Before(expiry) {
		expiry = policy.ExpiresAt
	}
	if identity.ExpiresAt.Before(expiry) {
		expiry = identity.ExpiresAt
	}
	return ReadDecision{Allowed: true, PolicyID: "policy-" + hex.EncodeToString(id[:]), ValidUntil: expiry}, nil
}
