package catalogdiagnostics

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/mail"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const sessionCookie = "temporal_demo_auth"
const maxSessionBytes = 8192

var ErrAuthorizationUnavailable = errors.New("diagnostic authorization unavailable")

// SessionKey is supplied explicitly by trusted bootstrap. This package never
// loads environment, files or a cloud secret. Length is checked; entropy must
// be established by provisioning. Rotation constructs a new authorizer.
type SessionKey struct{ secret *sessionKeyValue }
type sessionKeyValue struct {
	mu    sync.Mutex
	value []byte
}

func NewSessionKey(value []byte) (*SessionKey, error) {
	if len(value) < 32 || len(value) > 4096 {
		return nil, ErrUnauthenticated
	}
	return &SessionKey{secret: &sessionKeyValue{value: bytes.Clone(value)}}, nil
}
func (SessionKey) String() string   { return "[session verification key]" }
func (SessionKey) GoString() string { return "[session verification key]" }
func (SessionKey) Format(s fmt.State, _ rune) {
	_, _ = io.WriteString(s, "[session verification key]")
}
func (SessionKey) MarshalJSON() ([]byte, error) { return nil, ErrUnauthenticated }
func (SessionKey) MarshalText() ([]byte, error) { return nil, ErrUnauthenticated }
func (k *SessionKey) Close() {
	if k == nil || k.secret == nil {
		return
	}
	k.secret.mu.Lock()
	defer k.secret.mu.Unlock()
	clear(k.secret.value)
	k.secret.value = nil
}
func (k *SessionKey) verify(input string, signature []byte) bool {
	if k == nil || k.secret == nil {
		return false
	}
	k.secret.mu.Lock()
	defer k.secret.mu.Unlock()
	if len(k.secret.value) < 32 {
		return false
	}
	h := hmac.New(sha256.New, k.secret.value)
	_, _ = io.WriteString(h, input)
	return hmac.Equal(signature, h.Sum(nil))
}
func (k *SessionKey) active() bool {
	if k == nil || k.secret == nil {
		return false
	}
	k.secret.mu.Lock()
	defer k.secret.mu.Unlock()
	return len(k.secret.value) >= 32
}

// SessionIdentity contains verified claims plus a one-way session fingerprint
// for exact session revocation. Policy must use issuer+subject as identity;
// email/domain alone never confer project access. No bearer token is exposed.
type SessionIdentity struct {
	Issuer, Subject, Email, Domain, Fingerprint string
	IssuedAt, AuthenticatedAt, ExpiresAt        time.Time
}

type ReadDecision struct {
	Allowed    bool
	PolicyID   string
	ValidUntil time.Time
}

// SessionReadPolicy must consult authoritative current project membership and
// session/principal revocation on every call, with no bootstrap-allow path.
// It must return a bounded validity horizon. This interface is not a default
// policy implementation or permission to treat all employees as authorized.
type SessionReadPolicy interface {
	CheckDiagnosticRead(context.Context, SessionIdentity, Target) (ReadDecision, error)
}

type SessionConfig struct {
	Environment, Installation       string
	Issuer, Audience, AllowedDomain string
	MaxSessionLifetime              time.Duration
	Now                             func() time.Time
	Key                             *SessionKey
	Policy                          SessionReadPolicy
}
type SessionAuthorizer struct{ config *SessionConfig }

func (SessionAuthorizer) Format(s fmt.State, _ rune) {
	_, _ = io.WriteString(s, "[session authorizer]")
}
func (SessionAuthorizer) MarshalJSON() ([]byte, error) { return nil, ErrUnauthenticated }
func (SessionAuthorizer) MarshalText() ([]byte, error) { return nil, ErrUnauthenticated }

var domainPattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?$`)
var googleSubject = regexp.MustCompile(`^google:[A-Za-z0-9_-]{1,128}$`)

func NewSessionAuthorizer(config SessionConfig) (*SessionAuthorizer, error) {
	u, err := url.Parse(config.Issuer)
	if !scopeID.MatchString(config.Environment) || !scopeID.MatchString(config.Installation) || err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || len(config.Issuer) > 512 || config.Audience == "" || len(config.Audience) > 256 || !domainPattern.MatchString(config.AllowedDomain) || !strings.Contains(config.AllowedDomain, ".") || strings.Contains(config.AllowedDomain, "..") || config.MaxSessionLifetime <= 0 || config.MaxSessionLifetime > 7*24*time.Hour || config.Now == nil || config.Key == nil || config.Policy == nil {
		return nil, ErrUnauthenticated
	}
	// Pin the opaque shared state, not the caller's replaceable wrapper. Close
	// still retires that state; assigning a fresh key to the old wrapper cannot
	// revive grants authenticated with the retired key.
	key := *config.Key
	config.Key = &key
	return &SessionAuthorizer{config: &config}, nil
}

// A value receiver pins this authorizer's private configuration in each grant;
// reassigning a caller's exported wrapper cannot retarget an existing grant.
func (a SessionAuthorizer) AuthorizeRead(ctx context.Context, request *http.Request, target Target) (ReadGrant, error) {
	if a.config == nil {
		return ReadGrant{}, ErrUnauthenticated
	}
	if !target.valid() || target.Environment != a.config.Environment || target.Installation != a.config.Installation {
		return ReadGrant{}, ErrForbidden
	}
	if request == nil {
		return ReadGrant{}, ErrUnauthenticated
	}
	// Reject duplicate cookie names, including across Cookie headers. Forwarded
	// headers, Authorization and query parameters are never identity inputs.
	var token string
	count, size := 0, 0
	for _, value := range request.Header.Values("Cookie") {
		size += len(value)
	}
	if size > 16384 {
		return ReadGrant{}, ErrUnauthenticated
	}
	for _, cookie := range request.Cookies() {
		if cookie.Name == sessionCookie {
			count++
			token = cookie.Value
		}
	}
	if count != 1 {
		return ReadGrant{}, ErrUnauthenticated
	}
	identity, err := a.verify(token)
	if err != nil {
		return ReadGrant{}, ErrUnauthenticated
	}
	decision, err := a.check(ctx, identity, target)
	if err != nil {
		return ReadGrant{}, err
	}
	now := a.config.Now()
	expiry := now.Add(MaxGrantLifetime)
	if identity.ExpiresAt.Before(expiry) {
		expiry = identity.ExpiresAt
	}
	if decision.ValidUntil.Before(expiry) {
		expiry = decision.ValidUntil
	}
	grant, err := IssueReadGrant(identity.Subject, decision.PolicyID, target, now, expiry)
	if err != nil {
		return ReadGrant{}, ErrForbidden
	}
	grant.recheck = func(ctx context.Context) error {
		current, err := a.check(ctx, identity, target)
		if err != nil {
			return err
		}
		finished := a.config.Now()
		if current.PolicyID != decision.PolicyID || finished.Before(now) || !finished.Before(expiry) {
			return ErrForbidden
		}
		return nil
	}
	return grant, nil
}

func (a *SessionAuthorizer) check(ctx context.Context, identity SessionIdentity, target Target) (ReadDecision, error) {
	if ctx.Err() != nil {
		return ReadDecision{}, ErrAuthorizationUnavailable
	}
	now := a.config.Now()
	if !a.config.Key.active() || now.Before(identity.IssuedAt) || !now.Before(identity.ExpiresAt) {
		return ReadDecision{}, ErrForbidden
	}
	d, err := a.config.Policy.CheckDiagnosticRead(ctx, identity, target)
	if err != nil || ctx.Err() != nil {
		return ReadDecision{}, ErrAuthorizationUnavailable
	}
	if !d.Allowed {
		return ReadDecision{}, ErrForbidden
	}
	// A policy version change requires a fresh request. Unbounded/invalid policy
	// results cannot be used to mint a capability.
	finished := a.config.Now()
	if !a.config.Key.active() || !scopeID.MatchString(d.PolicyID) || !finished.Before(d.ValidUntil) || !finished.Before(identity.ExpiresAt) || finished.Before(identity.IssuedAt) {
		return ReadDecision{}, ErrForbidden
	}
	return d, nil
}

func (a *SessionAuthorizer) verify(token string) (SessionIdentity, error) {
	invalid := func() (SessionIdentity, error) { return SessionIdentity{}, ErrUnauthenticated }
	if len(token) == 0 || len(token) > maxSessionBytes {
		return invalid()
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return invalid()
	}
	decode := func(value string) ([]byte, error) {
		if value == "" || strings.ContainsAny(value, "=\r\n\t ") {
			return nil, ErrUnauthenticated
		}
		return base64.RawURLEncoding.Strict().DecodeString(value)
	}
	header, err := decode(parts[0])
	if err != nil {
		return invalid()
	}
	signature, err := decode(parts[2])
	if err != nil || len(signature) != sha256.Size {
		return invalid()
	}
	h, err := flatObject(header)
	if err != nil || len(h) != 2 || stringField(h, "alg") != "HS256" || stringField(h, "typ") != "JWT" {
		return invalid()
	}
	if !a.config.Key.verify(parts[0]+"."+parts[1], signature) {
		return invalid()
	}
	payload, err := decode(parts[1])
	if err != nil {
		return invalid()
	}
	p, err := flatObject(payload)
	if err != nil {
		return invalid()
	}
	allowed := map[string]bool{"typ": true, "iss": true, "aud": true, "sub": true, "email": true, "name": true, "hd": true, "iat": true, "exp": true, "auth_time": true, "nbf": true}
	for name := range p {
		if !allowed[name] {
			return invalid()
		}
	}
	if name, ok := p["name"]; ok {
		var s string
		if json.Unmarshal(name, &s) != nil || len(s) > 512 {
			return invalid()
		}
	}
	identity := SessionIdentity{Issuer: stringField(p, "iss"), Subject: stringField(p, "sub"), Email: stringField(p, "email"), Domain: stringField(p, "hd")}
	if stringField(p, "typ") != "session" || identity.Issuer != a.config.Issuer || stringField(p, "aud") != a.config.Audience || !googleSubject.MatchString(identity.Subject) || identity.Domain != a.config.AllowedDomain {
		return invalid()
	}
	email, err := mail.ParseAddress(identity.Email)
	if err != nil || email.Address != identity.Email || len(identity.Email) > 320 || strings.ContainsAny(identity.Email, "\r\n\x00") {
		return invalid()
	}
	at := strings.LastIndexByte(identity.Email, '@')
	if at <= 0 || identity.Email[at+1:] != a.config.AllowedDomain {
		return invalid()
	}
	iat, ok1 := integerField(p, "iat")
	exp, ok2 := integerField(p, "exp")
	auth, ok3 := integerField(p, "auth_time")
	// Python create_session_token sets auth_time and iat from the same clock
	// sample. Accept only that profile, so a reissued/sliding token cannot extend
	// authentication age beyond the configured session lifetime.
	if !ok1 || !ok2 || !ok3 || iat <= 0 || auth != iat || exp <= iat || exp-iat > int64(a.config.MaxSessionLifetime/time.Second) {
		return invalid()
	}
	identity.IssuedAt, identity.AuthenticatedAt, identity.ExpiresAt = time.Unix(iat, 0), time.Unix(auth, 0), time.Unix(exp, 0)
	now := a.config.Now()
	if now.Before(identity.IssuedAt) || !now.Before(identity.ExpiresAt) || now.Sub(identity.AuthenticatedAt) > a.config.MaxSessionLifetime {
		return invalid()
	}
	if _, ok := p["nbf"]; ok {
		nbf, valid := integerField(p, "nbf")
		if !valid || nbf <= 0 || nbf > iat || now.Before(time.Unix(nbf, 0)) {
			return invalid()
		}
	}
	digest := sha256.Sum256([]byte(token))
	identity.Fingerprint = hex.EncodeToString(digest[:])
	return identity, nil
}

// The legacy profile is flat. Reject duplicate/case-aliased, unknown and
// non-scalar fields rather than silently letting encoding/json pick a value.
func flatObject(data []byte) (map[string]json.RawMessage, error) {
	if len(data) > maxSessionBytes || !utf8.Valid(data) {
		return nil, ErrUnauthenticated
	}
	d := json.NewDecoder(bytes.NewReader(data))
	t, err := d.Token()
	if err != nil || t != json.Delim('{') {
		return nil, ErrUnauthenticated
	}
	result := make(map[string]json.RawMessage)
	for d.More() {
		t, err = d.Token()
		name, ok := t.(string)
		if err != nil || !ok || len(name) > 64 {
			return nil, ErrUnauthenticated
		}
		if _, exists := result[name]; exists {
			return nil, ErrUnauthenticated
		}
		var value json.RawMessage
		if d.Decode(&value) != nil || len(value) == 0 || value[0] == '{' || value[0] == '[' || bytes.Equal(value, []byte("null")) {
			return nil, ErrUnauthenticated
		}
		result[name] = value
	}
	if t, err = d.Token(); err != nil || t != json.Delim('}') {
		return nil, ErrUnauthenticated
	}
	if d.Decode(new(any)) != io.EOF {
		return nil, ErrUnauthenticated
	}
	return result, nil
}
func stringField(p map[string]json.RawMessage, name string) string {
	var s string
	if json.Unmarshal(p[name], &s) != nil {
		return ""
	}
	return s
}
func integerField(p map[string]json.RawMessage, name string) (int64, bool) {
	var n int64
	err := json.Unmarshal(p[name], &n)
	return n, err == nil
}
