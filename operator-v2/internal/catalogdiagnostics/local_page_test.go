package catalogdiagnostics

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func pageAssets() LocalPageAssets {
	return LocalPageAssets{HTML: []byte("public shell"), Module: []byte("page module"), Stylesheet: []byte("page styles"), Panel: []byte("existing panel")}
}

func pageRequest(h http.Handler, method, path string, signed bool) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, nil)
	r.Header.Set("X-Temporal-Auth-Email", "michael@example.test")
	if signed {
		r.AddCookie(&http.Cookie{Name: sessionCookie, Value: tokenFor(sessionClaims())})
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestLocalPageDisabledAndFinitePublicAssets(t *testing.T) {
	disabled, err := NewLocalPage(LocalPageConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/diagnostics/", LocalBindingPath, "/api/v2/projects/" + target.ProjectUID + "/diagnostics"} {
		if w := pageRequest(disabled, "GET", path, true); w.Code != 404 {
			t.Fatal("disabled page registered diagnostic resource")
		}
	}
	var zero *LocalPage
	if pageRequest(zero, "GET", LocalBindingPath, false).Code != 404 {
		t.Fatal("nil page did not deny")
	}
	reads, authCalls := 0, 0
	h := handler(t, model(t, sourceFunc(func(context.Context, ReadGrant) (Snapshot, error) {
		reads++
		return snapshot(t), nil
	})), authFunc(func(context.Context, *http.Request, Target) (ReadGrant, error) {
		authCalls++
		return ReadGrant{}, ErrUnauthenticated
	}))
	assets := pageAssets()
	p, err := NewLocalPage(LocalPageConfig{Enabled: true, Target: target, Handler: h, Assets: assets})
	if err != nil {
		t.Fatal(err)
	}
	assets.HTML[0] = 'X'
	for path, mime := range map[string]string{"/diagnostics/": "text/html", "/diagnostics/diagnostic-page.mjs": "text/javascript", "/diagnostics/diagnostic-page.css": "text/css", "/diagnostics/diagnostics.mjs": "text/javascript"} {
		w := pageRequest(p, "GET", path, false)
		if w.Code != 200 || !strings.HasPrefix(w.Header().Get("Content-Type"), mime) || w.Header().Get("Cache-Control") != "no-store, private" || w.Header().Get("Content-Security-Policy") != "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'" || w.Header().Get("X-Content-Type-Options") != "nosniff" || w.Header().Get("Referrer-Policy") != "no-referrer" || w.Header().Get("Cross-Origin-Resource-Policy") != "same-origin" {
			t.Fatalf("public asset contract failed for %s", path)
		}
		if path == "/diagnostics/" && w.Body.String() != "public shell" {
			t.Fatal("caller changed copied public shell")
		}
		if head := pageRequest(p, "HEAD", path, false); head.Code != 200 || head.Body.Len() != 0 {
			t.Fatal("static HEAD returned a body")
		}
	}
	if reads != 0 || authCalls != 0 {
		t.Fatal("public assets queried protected identity/source")
	}
	for path, code := range map[string]int{"/diagnostics": 404, "/diagnostics/../private": 404, "/diagnostics/private.key": 404, "/diagnostics/?": 400, "/diagnostics/?project=foreign": 400, "/diagnostics/%64iagnostic-page.mjs": 400} {
		if w := pageRequest(p, "GET", path, false); w.Code != code {
			t.Fatalf("unexpected route status %s: %d", path, w.Code)
		}
	}
	if w := pageRequest(p, "POST", "/diagnostics/", false); w.Code != 405 || w.Header().Get("Allow") != "GET, HEAD" {
		t.Fatal("static route accepted mutation method")
	}
}

func TestLocalPageBindingVerifiesSessionAndRechecksWithoutSource(t *testing.T) {
	reads, checks := 0, 0
	revoked := false
	a := sessionAuthorizer(t, policyFunc(func(ctx context.Context, identity SessionIdentity, scope Target) (ReadDecision, error) {
		checks++
		if scope != target || revoked {
			return ReadDecision{}, nil
		}
		return allowSession(ctx, identity, scope)
	}))
	h := handler(t, model(t, sourceFunc(func(context.Context, ReadGrant) (Snapshot, error) {
		reads++
		return snapshot(t), nil
	})), a)
	p, err := NewLocalPage(LocalPageConfig{Enabled: true, Target: target, Handler: h, Assets: pageAssets()})
	if err != nil {
		t.Fatal(err)
	}
	if w := pageRequest(p, "GET", LocalBindingPath, false); w.Code != 401 || checks != 0 || reads != 0 || strings.Contains(w.Body.String(), target.ProjectUID) {
		t.Fatal("header-only identity reached current policy/source or revealed target")
	}
	known := pageRequest(p, "GET", "/api/v2/projects/"+target.ProjectUID+"/diagnostics", false)
	foreign := pageRequest(p, "GET", "/api/v2/projects/foreign-project/diagnostics", false)
	if known.Code != 401 || foreign.Code != known.Code || foreign.Body.String() != known.Body.String() || checks != 0 || reads != 0 {
		t.Fatal("anonymous route responses distinguished configured and foreign project UIDs")
	}
	w := pageRequest(p, "GET", LocalBindingPath, true)
	var b struct {
		Enabled bool   `json:"enabled"`
		Target  Target `json:"target"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &b) != nil || !b.Enabled || b.Target != target || w.Body.Len() > 1024 || checks != 2 || reads != 0 {
		t.Fatal("binding was not scoped and rechecked without source read")
	}
	if head := pageRequest(p, "HEAD", LocalBindingPath, true); head.Code != 200 || head.Body.Len() != 0 || checks != 4 || reads != 0 {
		t.Fatal("binding HEAD skipped authorization or returned body")
	}
	if w := pageRequest(p, "GET", "/api/v2/projects/foreign-project/diagnostics", true); w.Code != 404 || reads != 0 || checks != 6 {
		t.Fatal("foreign project reached configured handler")
	}
	if w := pageRequest(p, "GET", "/api/v2/projects/"+target.ProjectUID+"/diagnostics", true); w.Code != 200 || reads != 1 {
		t.Fatal("fixed diagnostic read did not use existing handler")
	}
	revoked = true
	if w := pageRequest(p, "GET", LocalBindingPath, true); w.Code != 404 || strings.Contains(w.Body.String(), target.ProjectUID) || reads != 1 {
		t.Fatal("current revocation failed to hide binding")
	}
}

func TestLocalPageBindingCannotPublishAfterGrantRevocation(t *testing.T) {
	for _, fault := range []error{ErrForbidden, errors.New("private-provider-failure")} {
		checks, reads := 0, 0
		a := authFunc(func(context.Context, *http.Request, Target) (ReadGrant, error) {
			g := grant(t)
			g.recheck = func(context.Context) error { checks++; return fault }
			return g, nil
		})
		p, err := NewLocalPage(LocalPageConfig{Enabled: true, Target: target, Handler: handler(t, model(t, sourceFunc(func(context.Context, ReadGrant) (Snapshot, error) {
			reads++
			return snapshot(t), nil
		})), a), Assets: pageAssets()})
		if err != nil {
			t.Fatal(err)
		}
		w := pageRequest(p, "GET", LocalBindingPath, true)
		if w.Code == 200 || checks != 1 || reads != 0 || strings.Contains(w.Body.String(), target.ProjectUID) || strings.Contains(w.Body.String(), fault.Error()) {
			t.Fatal("revoked/unavailable grant published binding or raw cause")
		}
	}
}

func TestLocalPageRejectsInvalidScopeAssetsAndPinsWrappers(t *testing.T) {
	h := handler(t, model(t, sourceFunc(func(context.Context, ReadGrant) (Snapshot, error) { return snapshot(t), nil })), allowed(t))
	valid := LocalPageConfig{Enabled: true, Target: target, Handler: h, Assets: pageAssets()}
	for _, mutate := range []func(*LocalPageConfig){
		func(c *LocalPageConfig) { c.Target.ProjectUID = "" },
		func(c *LocalPageConfig) { c.Target.Environment = "other" },
		func(c *LocalPageConfig) { c.Target.Installation = "other" },
		func(c *LocalPageConfig) { c.Handler = nil },
		func(c *LocalPageConfig) {
			copy := *h
			copy.model = &Model{}
			c.Handler = &copy
		},
		func(c *LocalPageConfig) {
			copy := *h
			m := *h.model
			m.config.MaxDesiredAge = 0
			copy.model = &m
			c.Handler = &copy
		},
		func(c *LocalPageConfig) { c.Assets.Panel = nil },
		func(c *LocalPageConfig) { c.Assets.HTML = []byte{0xff} },
		func(c *LocalPageConfig) { c.Assets.Module = make([]byte, 64*1024+1) },
	} {
		c := valid
		mutate(&c)
		if _, err := NewLocalPage(c); err == nil {
			t.Fatal("invalid scope/asset accepted")
		}
	}
	p, err := NewLocalPage(valid)
	if err != nil {
		t.Fatal(err)
	}
	*h = Handler{}
	if pageRequest(p, "GET", LocalBindingPath, true).Code != 200 {
		t.Fatal("caller replaced pinned handler wrapper")
	}
}
