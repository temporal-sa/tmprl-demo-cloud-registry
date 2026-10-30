// Package ownership defines local ownership checks and provider preconditions.
// It does not authenticate observations, acquire locks, or enforce provider IAM.
// Adapters must obtain observations independently of untrusted project input.
package ownership

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"unicode"
)

type Provider string

const (
	AWS        Provider = "aws"
	Kubernetes Provider = "kubernetes"
	Temporal   Provider = "temporal"
)

type Lifetime string

const (
	Stable     Lifetime = "stable"
	Disposable Lifetime = "disposable"
)

// Scope is a logical authority boundary, obtained from trusted installation
// configuration. All fields participate in equality and name derivation.
// AWS uses partition/account/region, Kubernetes uses cluster/namespace, and
// Temporal uses account/namespace. Cluster-scoped Kubernetes resources use an
// empty namespace; a separate context is required for another authority scope.
type Scope struct {
	Provider  Provider `json:"provider"`
	Partition string   `json:"partition,omitempty"`
	AccountID string   `json:"accountId,omitempty"`
	Region    string   `json:"region,omitempty"`
	ClusterID string   `json:"clusterId,omitempty"`
	Namespace string   `json:"namespace,omitempty"`
}

type Owner struct {
	EnvironmentID  string   `json:"environmentId"`
	InstallationID string   `json:"installationId"`
	ProjectUID     string   `json:"projectUid"`
	Lifetime       Lifetime `json:"lifetime"`
	RunID          string   `json:"runId,omitempty"`
}

type Config struct {
	Owner        Owner
	ResourceRoot string
	Scopes       []Scope
}

// Context deliberately keeps its configuration private and copies scope input.
// Construct it at the process boundary, never directly from a project document.
type Context struct {
	owner  Owner
	root   string
	scopes []Scope
}

var slugPattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$`)
var pathSegmentPattern = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*$`)
var accountPattern = regexp.MustCompile(`^[0-9]{12}$`)

func NewContext(config Config) (Context, error) {
	for _, field := range []struct{ key, value string }{
		{"environment ID", config.Owner.EnvironmentID},
		{"installation ID", config.Owner.InstallationID},
		{"project UID", config.Owner.ProjectUID},
	} {
		if !validID(field.value) {
			return Context{}, invalid("invalid %s", field.key)
		}
	}
	switch config.Owner.Lifetime {
	case Stable:
		if config.Owner.RunID != "" {
			return Context{}, invalid("stable ownership cannot contain an ephemeral run ID")
		}
	case Disposable:
		if !validID(config.Owner.RunID) {
			return Context{}, invalid("disposable ownership requires a run ID")
		}
	default:
		return Context{}, invalid("explicit stable or disposable lifetime is required")
	}
	if !slugPattern.MatchString(config.ResourceRoot) || len(config.ResourceRoot) > 16 {
		return Context{}, invalid("resource root must be a lowercase DNS label of at most 16 characters")
	}
	if strings.HasPrefix(config.ResourceRoot, "tmprl-dem-cld") ||
		strings.HasPrefix("tmprl-dem-cld", config.ResourceRoot+"-") {
		return Context{}, invalid("resource root overlaps the legacy production root")
	}
	if len(config.Scopes) == 0 {
		return Context{}, invalid("at least one explicit provider scope is required")
	}
	seen := make(map[Provider]bool)
	for _, scope := range config.Scopes {
		if err := validateScope(scope); err != nil {
			return Context{}, err
		}
		if seen[scope.Provider] {
			return Context{}, invalid("duplicate provider scope %q", scope.Provider)
		}
		seen[scope.Provider] = true
	}
	return Context{owner: config.Owner, root: config.ResourceRoot, scopes: append([]Scope(nil), config.Scopes...)}, nil
}

func (c Context) Owner() Owner { return c.owner }

func (c Context) Scope(provider Provider) (Scope, error) {
	for _, scope := range c.scopes {
		if scope.Provider == provider {
			return scope, nil
		}
	}
	return Scope{}, invalid("provider %q has no configured authority", provider)
}

// Name always preserves the disjoint root and a 128-bit digest of the complete
// owner, provider scope, purpose and logical ID, including truncated characters.
// Reconcile IDs are absent: stable names survive process restarts and retries.
func (c Context) Name(provider Provider, purpose, logicalID string, limit int) (string, error) {
	scope, err := c.Scope(provider)
	if err != nil {
		return "", err
	}
	if !slugPattern.MatchString(purpose) || len(purpose) > 64 || !validID(logicalID) {
		return "", invalid("purpose must be a lowercase label and logical ID must be explicit")
	}
	// Keep the first 16 digest bytes; 39-character Temporal names still retain
	// the entire root plus a complete digest when the root is short enough.
	digest := digestOf(c.owner, scope, c.root, purpose, logicalID)[:32]
	minimum := len(c.root) + 1 + len(digest)
	if limit < minimum {
		return "", invalid("name limit %d cannot preserve root and 128-bit digest (minimum %d)", limit, minimum)
	}
	readable := slug(purpose + "-" + logicalID)
	available := limit - minimum - 1
	if available <= 0 {
		return c.root + "-" + digest, nil
	}
	if len(readable) > available {
		readable = strings.TrimRight(readable[:available], "-")
	}
	if readable == "" {
		return c.root + "-" + digest, nil
	}
	return c.root + "-" + readable + "-" + digest, nil
}

// Path is suitable for slash-based secret/repository identities. The owner
// digest prevents path traversal and retains opaque owner identity in the seed.
func (c Context) Path(provider Provider, purpose, logicalID string) (string, error) {
	name, err := c.Name(provider, purpose, logicalID, 96)
	if err != nil {
		return "", err
	}
	return c.root + "/" + digestOf(c.owner)[:32] + "/" + name, nil
}

func validateScope(scope Scope) error {
	switch scope.Provider {
	case AWS:
		if !accountPattern.MatchString(scope.AccountID) || !validID(scope.Region) ||
			(scope.Partition != "aws" && scope.Partition != "aws-us-gov" && scope.Partition != "aws-cn") ||
			scope.ClusterID != "" || scope.Namespace != "" {
			return invalid("AWS scope requires explicit partition, 12-digit account and region only")
		}
	case Kubernetes:
		if !validID(scope.ClusterID) || scope.AccountID != "" || scope.Partition != "" || scope.Region != "" {
			return invalid("Kubernetes scope requires explicit cluster identity and optional namespace only")
		}
		if scope.Namespace != "" && (!slugPattern.MatchString(scope.Namespace) || len(scope.Namespace) > 63) {
			return invalid("invalid Kubernetes namespace")
		}
	case Temporal:
		if !validID(scope.AccountID) || !validID(scope.Namespace) || scope.Partition != "" || scope.Region != "" || scope.ClusterID != "" {
			return invalid("Temporal scope requires explicit account and namespace only")
		}
	default:
		return invalid("unknown provider %q", scope.Provider)
	}
	return nil
}

func validID(value string) bool {
	return value != "" && len(value) <= 256 && strings.TrimSpace(value) == value &&
		!strings.ContainsFunc(value, unicode.IsControl)
}

func slug(value string) string {
	var out strings.Builder
	for _, char := range strings.ToLower(value) {
		if char >= 'a' && char <= 'z' || char >= '0' && char <= '9' {
			out.WriteRune(char)
		} else {
			out.WriteByte('-')
		}
	}
	return strings.Trim(out.String(), "-")
}

func digestOf(values ...any) string {
	data, _ := json.Marshal(values) // All call sites supply only concrete serializable values.
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func invalid(format string, args ...any) error {
	return &Error{Code: InvalidContext, Detail: fmt.Sprintf(format, args...)}
}
