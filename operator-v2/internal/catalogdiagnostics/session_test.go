package catalogdiagnostics

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

var sessionTestKey = []byte("synthetic-session-signing-key-1234567890")

type policyFunc func(context.Context, SessionIdentity, Target) (ReadDecision, error)

func (p policyFunc) CheckDiagnosticRead(c context.Context, i SessionIdentity, t Target) (ReadDecision, error) {
	return p(c, i, t)
}
func sessionClaims() map[string]any {
	return map[string]any{"typ": "session", "iss": "https://catalog.example.test", "aud": "catalog.example.test", "sub": "google:123456789", "email": "michael@example.test", "name": "Michael", "hd": "example.test", "iat": testNow.Add(-time.Minute).Unix(), "auth_time": testNow.Add(-time.Minute).Unix(), "exp": testNow.Add(time.Hour).Unix()}
}
func signSession(header, payload string) string {
	enc := base64.RawURLEncoding.EncodeToString
	input := enc([]byte(header)) + "." + enc([]byte(payload))
	h := hmac.New(sha256.New, sessionTestKey)
	_, _ = h.Write([]byte(input))
	return input + "." + enc(h.Sum(nil))
}
func tokenFor(claims map[string]any) string {
	data, _ := json.Marshal(claims)
	return signSession(`{"alg":"HS256","typ":"JWT"}`, string(data))
}
func sessionRequest(token string) *http.Request {
	r := httptest.NewRequest("GET", "/api/v2/projects/"+target.ProjectUID+"/diagnostics", nil)
	r.AddCookie(&http.Cookie{Name: sessionCookie, Value: token})
	return r
}
func sessionAuthorizer(t *testing.T, p SessionReadPolicy) *SessionAuthorizer {
	t.Helper()
	key, err := NewSessionKey(sessionTestKey)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(key.Close)
	a, err := NewSessionAuthorizer(SessionConfig{Environment: target.Environment, Installation: target.Installation, Issuer: "https://catalog.example.test", Audience: "catalog.example.test", AllowedDomain: "example.test", MaxSessionLifetime: 7 * 24 * time.Hour, Now: func() time.Time { return testNow }, Key: key, Policy: p})
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func allowSession(context.Context, SessionIdentity, Target) (ReadDecision, error) {
	return ReadDecision{Allowed: true, PolicyID: "policy-v1", ValidUntil: testNow.Add(time.Minute)}, nil
}

func TestVerifiedSessionBeforeLiveScopedPolicy(t *testing.T) {
	calls := 0
	a := sessionAuthorizer(t, policyFunc(func(_ context.Context, i SessionIdentity, scope Target) (ReadDecision, error) {
		calls++
		if scope != target || i.Subject != "google:123456789" || i.Issuer != "https://catalog.example.test" || len(i.Fingerprint) != 64 || i.Email != "michael@example.test" {
			t.Fatal("unverified identity/scope")
		}
		return allowSession(context.Background(), i, scope)
	}))
	r := sessionRequest(tokenFor(sessionClaims()))
	r.Header.Set("X-Temporal-Auth-Email", "admin@other.test")
	r.Header.Set("X-Forwarded-User", "administrator")
	g, err := a.AuthorizeRead(context.Background(), r, target)
	if err != nil || g.principalID != "google:123456789" || g.expiresAt != testNow.Add(time.Minute) {
		t.Fatalf("grant: %v", err)
	}
	reads := 0
	m := model(t, sourceFunc(func(context.Context, ReadGrant) (Snapshot, error) { reads++; return snapshot(t), nil }))
	if _, err = m.Read(context.Background(), g, target); err != nil {
		t.Fatal(err)
	}
	if calls != 3 || reads != 1 {
		t.Fatalf("policy must run before issue, before source, after source: %d/%d", calls, reads)
	}
	foreign := target
	foreign.Installation = "production"
	if _, err = a.AuthorizeRead(context.Background(), r, foreign); !errors.Is(err, ErrForbidden) || calls != 3 {
		t.Fatal("foreign scope consulted policy")
	}
}

func TestSessionProfileRejectsMalformedAndWrongKindBeforePolicy(t *testing.T) {
	calls := 0
	a := sessionAuthorizer(t, policyFunc(func(context.Context, SessionIdentity, Target) (ReadDecision, error) {
		calls++
		return ReadDecision{}, nil
	}))
	mutations := map[string]func(map[string]any){
		"downstream": func(p map[string]any) { p["typ"] = "downstream" }, "share": func(p map[string]any) { p["typ"] = "workspace-share" }, "issuer": func(p map[string]any) { p["iss"] = "https://other.test" }, "audience": func(p map[string]any) { p["aud"] = "other" }, "array audience": func(p map[string]any) { p["aud"] = []string{"catalog.example.test"} }, "domain": func(p map[string]any) { p["hd"] = "other.test" }, "email domain": func(p map[string]any) { p["email"] = "michael@other.test" }, "display email": func(p map[string]any) { p["email"] = "Michael <michael@example.test>" }, "subject": func(p map[string]any) { p["sub"] = "email:michael@example.test" }, "expired": func(p map[string]any) { p["exp"] = testNow.Unix() }, "future": func(p map[string]any) { p["iat"] = testNow.Add(time.Second).Unix() }, "old authentication": func(p map[string]any) { p["auth_time"] = testNow.Add(-8 * 24 * time.Hour).Unix() }, "missing authentication": func(p map[string]any) { delete(p, "auth_time") }, "fraction time": func(p map[string]any) { p["iat"] = 1.5 }, "boolean time": func(p map[string]any) { p["iat"] = true }, "null": func(p map[string]any) { p["email"] = nil }, "case alias": func(p map[string]any) { p["SUB"] = "google:evil" }, "excess lifetime": func(p map[string]any) { p["exp"] = testNow.Add(8 * 24 * time.Hour).Unix() }, "notbefore": func(p map[string]any) { p["nbf"] = testNow.Add(time.Minute).Unix() },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			p := sessionClaims()
			mutate(p)
			if _, err := a.AuthorizeRead(context.Background(), sessionRequest(tokenFor(p)), target); !errors.Is(err, ErrUnauthenticated) {
				t.Fatalf("accepted: %v", err)
			}
		})
	}
	payload, _ := json.Marshal(sessionClaims())
	bad := []string{
		signSession(`{"alg":"none","typ":"JWT"}`, string(payload)),
		signSession(`{"alg":"HS256","typ":"other"}`, string(payload)),
		signSession(`{"alg":"HS256","typ":"JWT","jku":"https://evil.test"}`, string(payload)),
		signSession(`{"alg":"HS256","typ":"JWT","alg":"HS256"}`, string(payload)),
		signSession(`{"alg":"HS256","typ":"JWT"}`, strings.TrimSuffix(string(payload), "}")+`,"sub":"google:other"}`),
		tokenFor(sessionClaims()) + "=", strings.Repeat("x", maxSessionBytes+1), "a.b.c", "",
	}
	good := tokenFor(sessionClaims())
	parts := strings.Split(good, ".")
	bad = append(bad, parts[0]+"."+parts[1]+"."+base64.RawURLEncoding.EncodeToString(make([]byte, 32)))
	for _, token := range bad {
		if _, err := a.AuthorizeRead(context.Background(), sessionRequest(token), target); !errors.Is(err, ErrUnauthenticated) {
			t.Fatalf("malformed accepted: %v", err)
		}
	}
	r := sessionRequest(good)
	r.AddCookie(&http.Cookie{Name: sessionCookie, Value: good})
	if _, err := a.AuthorizeRead(context.Background(), r, target); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("duplicate cookie accepted")
	}
	r = httptest.NewRequest("GET", "/", nil)
	r.Header.Set("X-Temporal-Auth-Email", "michael@example.test")
	r.Header.Set("Authorization", "Bearer "+good)
	if _, err := a.AuthorizeRead(context.Background(), r, target); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("forwarded/bearer identity accepted")
	}
	if calls != 0 {
		t.Fatalf("invalid session reached policy %d times", calls)
	}
}

func TestRevocationAndPolicyFailureAroundDurableSource(t *testing.T) {
	for _, afterRead := range []bool{false, true} {
		t.Run(fmt.Sprint(afterRead), func(t *testing.T) {
			revoked := false
			calls := 0
			a := sessionAuthorizer(t, policyFunc(func(c context.Context, i SessionIdentity, scope Target) (ReadDecision, error) {
				calls++
				d, _ := allowSession(c, i, scope)
				d.Allowed = !revoked
				return d, nil
			}))
			g, err := a.AuthorizeRead(context.Background(), sessionRequest(tokenFor(sessionClaims())), target)
			if err != nil {
				t.Fatal(err)
			}
			reads := 0
			m := model(t, sourceFunc(func(context.Context, ReadGrant) (Snapshot, error) { reads++; revoked = true; return snapshot(t), nil }))
			if !afterRead {
				revoked = true
			}
			if _, err := m.Read(context.Background(), g, target); !errors.Is(err, ErrForbidden) {
				t.Fatalf("revoked read: %v", err)
			}
			if (!afterRead && reads != 0) || (afterRead && reads != 1) {
				t.Fatal("source access ordering")
			}
		})
	}
	t.Run("safe unavailable", func(t *testing.T) {
		a := sessionAuthorizer(t, policyFunc(func(context.Context, SessionIdentity, Target) (ReadDecision, error) {
			return ReadDecision{}, errors.New("password=PRIVATE-SENTINEL")
		}))
		reads := 0
		h := handler(t, model(t, sourceFunc(func(context.Context, ReadGrant) (Snapshot, error) { reads++; return Snapshot{}, nil })), a)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, sessionRequest(tokenFor(sessionClaims())))
		if w.Code != 503 || reads != 0 || strings.Contains(w.Body.String(), "PRIVATE") || w.Header().Get("Cache-Control") != "no-store, private" {
			t.Fatalf("unsafe auth failure %d %s", w.Code, w.Body)
		}
	})
}

func TestSessionKeyIsPrivateCopiedAndCloseable(t *testing.T) {
	raw := append([]byte(nil), sessionTestKey...)
	k, err := NewSessionKey(raw)
	if err != nil {
		t.Fatal(err)
	}
	clear(raw)
	h := hmac.New(sha256.New, sessionTestKey)
	_, _ = h.Write([]byte("message"))
	sig := h.Sum(nil)
	if !k.verify("message", sig) {
		t.Fatal("key aliased caller input")
	}
	for _, value := range []any{k, *k, struct{ Key SessionKey }{*k}} {
		for _, format := range []string{"%v", "%+v", "%#v", "%s", "%x", "%d", "%p", "%#p", "%t", "%z"} {
			if strings.Contains(fmt.Sprintf(format, value), "synthetic-session") {
				t.Fatal("key formatting leaked")
			}
		}
	}
	if _, err := json.Marshal(k); err == nil {
		t.Fatal("key serialized")
	}
	k.Close()
	if k.verify("message", sig) {
		t.Fatal("closed key accepted")
	}
	if _, err := NewSessionKey([]byte("short")); err == nil {
		t.Fatal("short key accepted")
	}
}

func TestLivePolicyVersionExpiryAndKeyRetirementDenyGrantReplay(t *testing.T) {
	for _, change := range []string{"different project", "policy version", "policy expired", "key retired", "session expired", "policy unavailable"} {
		t.Run(change, func(t *testing.T) {
			now := testNow
			changed := false
			a := sessionAuthorizer(t, policyFunc(func(c context.Context, i SessionIdentity, scope Target) (ReadDecision, error) {
				d, _ := allowSession(c, i, scope)
				if scope != target {
					d.Allowed = false
				}
				if changed {
					switch change {
					case "policy version":
						d.PolicyID = "policy-v2"
					case "policy expired":
						d.ValidUntil = now
					case "policy unavailable":
						return ReadDecision{}, errors.New("private backend error")
					}
				}
				return d, nil
			}))
			a.config.Now = func() time.Time { return now }
			r := sessionRequest(tokenFor(sessionClaims()))
			g, err := a.AuthorizeRead(context.Background(), r, target)
			if err != nil {
				t.Fatal(err)
			}
			changed = true
			if change == "key retired" {
				a.config.Key.Close()
			}
			if change == "session expired" {
				now = testNow.Add(2 * time.Hour)
			}
			reads := 0
			m := model(t, sourceFunc(func(context.Context, ReadGrant) (Snapshot, error) { reads++; return snapshot(t), nil }))
			m.config.Now = func() time.Time { return now }
			scope := target
			if change == "different project" {
				scope.ProjectUID = "other-project"
			}
			if _, err := m.Read(context.Background(), g, scope); err == nil || reads != 0 {
				t.Fatalf("grant replay crossed source: %v, reads=%d", err, reads)
			}
			if change == "different project" {
				if _, err := a.AuthorizeRead(context.Background(), r, scope); !errors.Is(err, ErrForbidden) {
					t.Fatal("policy did not deny foreign project")
				}
			}
			for _, value := range []any{a, *a} {
				for _, verb := range []string{"%v", "%#v", "%d", "%x", "%p", "%z"} {
					if strings.Contains(fmt.Sprintf(verb, value), "synthetic-session") {
						t.Fatal("authorizer formatting exposed key")
					}
				}
			}
		})
	}
}

func TestPolicyCompletionCannotOutliveGrantOrRetiredVerificationKey(t *testing.T) {
	for _, phase := range []int{1, 2, 3} {
		for _, action := range []string{"key closes", "grant expires"} {
			t.Run(fmt.Sprintf("%d/%s", phase, action), func(t *testing.T) {
				now := testNow
				calls, reads := 0, 0
				var a *SessionAuthorizer
				a = sessionAuthorizer(t, policyFunc(func(context.Context, SessionIdentity, Target) (ReadDecision, error) {
					calls++
					if calls == phase {
						if action == "key closes" {
							a.config.Key.Close()
						} else {
							now = now.Add(6 * time.Minute)
						}
					}
					return ReadDecision{Allowed: true, PolicyID: "policy-v1", ValidUntil: now.Add(time.Minute)}, nil
				}))
				a.config.Now = func() time.Time { return now }
				g, err := a.AuthorizeRead(context.Background(), sessionRequest(tokenFor(sessionClaims())), target)
				if phase == 1 && action == "key closes" {
					if err == nil {
						t.Fatal("retired key authorized after policy")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				// Issue-time policy latency may yield a new grant after that latency;
				// later checks must honor its original, fixed expiration.
				if phase == 1 {
					return
				}
				m := model(t, sourceFunc(func(context.Context, ReadGrant) (Snapshot, error) { reads++; return snapshot(t), nil }))
				m.config.Now = func() time.Time { return now }
				if _, err := m.Read(context.Background(), g, target); err == nil {
					t.Fatal("policy completion authorized expired/revoked grant")
				}
				if (phase == 2 && reads != 0) || (phase == 3 && reads != 1) {
					t.Fatalf("source access ordering: %d", reads)
				}
			})
		}
	}
}

func TestReplacingPublicWrappersCannotReviveRetiredKeyGrant(t *testing.T) {
	key, err := NewSessionKey(sessionTestKey)
	if err != nil {
		t.Fatal(err)
	}
	config := SessionConfig{Environment: target.Environment, Installation: target.Installation, Issuer: "https://catalog.example.test", Audience: "catalog.example.test", AllowedDomain: "example.test", MaxSessionLifetime: 7 * 24 * time.Hour, Now: func() time.Time { return testNow }, Key: key, Policy: policyFunc(allowSession)}
	a, err := NewSessionAuthorizer(config)
	if err != nil {
		t.Fatal(err)
	}
	g, err := a.AuthorizeRead(context.Background(), sessionRequest(tokenFor(sessionClaims())), target)
	if err != nil {
		t.Fatal(err)
	}
	key.Close()
	freshKey, err := NewSessionKey(sessionTestKey)
	if err != nil {
		t.Fatal(err)
	}
	defer freshKey.Close()
	*key = *freshKey
	if err := g.authorize(context.Background(), target, testNow); !errors.Is(err, ErrForbidden) {
		t.Fatal("key wrapper replacement revived grant")
	}
	config.Key = freshKey
	freshAuthorizer, err := NewSessionAuthorizer(config)
	if err != nil {
		t.Fatal(err)
	}
	*a = *freshAuthorizer
	if err := g.authorize(context.Background(), target, testNow); !errors.Is(err, ErrForbidden) {
		t.Fatal("authorizer wrapper replacement revived grant")
	}
	if _, err := a.AuthorizeRead(context.Background(), sessionRequest(tokenFor(sessionClaims())), target); err != nil {
		t.Fatal("new authorizer rejected fresh verification")
	}
	var zero SessionAuthorizer
	if _, err := zero.AuthorizeRead(context.Background(), nil, target); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("zero authorizer did not fail closed")
	}
}
