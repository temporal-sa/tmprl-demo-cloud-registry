// Package catalogdiagnostics serves explicitly authorized, materialized
// operation views. It has no default authenticator, credential loader, cloud
// client, worker query, project enumeration or production route registration.
package catalogdiagnostics

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"time"
)

var (
	ErrUnauthenticated   = errors.New("diagnostic authentication required")
	ErrForbidden         = errors.New("diagnostic access denied")
	ErrNotFound          = errors.New("diagnostic project not found")
	ErrSourceUnavailable = errors.New("diagnostic source unavailable")
	ErrInvalidSource     = errors.New("diagnostic source invalid")
)
var scopeID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

const MaxGrantLifetime = 5 * time.Minute

type Target struct {
	Environment  string `json:"environment"`
	Installation string `json:"installation"`
	ProjectUID   string `json:"projectUID"`
}

func (t Target) valid() bool {
	return scopeID.MatchString(t.Environment) && scopeID.MatchString(t.Installation) && scopeID.MatchString(t.ProjectUID)
}

// ReadGrant is an opaque, short-lived policy result for one project. It conveys
// no evidence-download permission or authority over a different environment.
type ReadGrant struct {
	target      Target
	principalID string
	policyID    string
	issuedAt    time.Time
	expiresAt   time.Time
	// Set only by adapters that require live revocation/project policy. It is
	// deliberately private so a caller cannot detach the policy from a grant.
	recheck func(context.Context) error
}

func (g ReadGrant) Target() Target { return g.target }

// IssueReadGrant constructs a trusted policy adapter's result AFTER credential
// verification and project authorization. It does not authenticate a session,
// token, forwarded header or claimed subject. Construction never substitutes
// for a real ReadAuthorizer implementation.
func IssueReadGrant(principalID, policyID string, target Target, issuedAt, expiresAt time.Time) (ReadGrant, error) {
	if principalID == "" || len(principalID) > 256 || policyID == "" || len(policyID) > 128 || !target.valid() || issuedAt.IsZero() || !expiresAt.After(issuedAt) || expiresAt.Sub(issuedAt) > MaxGrantLifetime {
		return ReadGrant{}, ErrForbidden
	}
	return ReadGrant{target: target, principalID: principalID, policyID: policyID, issuedAt: issuedAt, expiresAt: expiresAt}, nil
}
func (g ReadGrant) valid(target Target, now time.Time) bool {
	return g.target == target && g.target.valid() && g.principalID != "" && g.policyID != "" && !now.Before(g.issuedAt) && now.Before(g.expiresAt) && g.expiresAt.Sub(g.issuedAt) <= MaxGrantLifetime
}

func (g ReadGrant) authorize(ctx context.Context, target Target, now time.Time) error {
	if !g.valid(target, now) {
		return ErrForbidden
	}
	if g.recheck != nil {
		return g.recheck(ctx)
	}
	return nil
}

// ReadAuthorizer must authenticate the request with a verified session/OIDC
// adapter AND apply project policy before issuing a grant. A claimed email or
// forwarded header is never proof of identity. Anonymous/denied requests must
// not look up project existence in StatusSource.
type ReadAuthorizer interface {
	AuthorizeRead(context.Context, *http.Request, Target) (ReadGrant, error)
}
