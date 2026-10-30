package catalogdiagnostics

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"unicode/utf8"
)

const LocalBindingPath = "/api/v2/diagnostics/binding"

// LocalPageAssets are reviewed public bytes supplied by trusted bootstrap.
// The handler copies them and serves exactly four paths; it never opens files.
type LocalPageAssets struct {
	HTML, Module, Stylesheet, Panel []byte
}

type LocalPageConfig struct {
	Enabled bool
	Target  Target
	Handler *Handler
	Assets  LocalPageAssets
}

type localAsset struct {
	contentType string
	body        []byte
}

// LocalPage is an opt-in, single-project diagnostic page. It does not install
// a listener, issue sessions, discover projects, or register with the catalog.
// Concrete bootstrap must supply the pinned journal, desired and live policy
// readers behind Handler. The binding itself confers no read permission.
type LocalPage struct {
	enabled bool
	target  Target
	handler Handler
	assets  map[string]localAsset
}

func NewLocalPage(c LocalPageConfig) (*LocalPage, error) {
	if !c.Enabled {
		return &LocalPage{}, nil
	}
	if !c.Target.valid() || c.Handler == nil || c.Handler.model == nil || c.Handler.auth == nil || c.Target.Environment != c.Handler.environment || c.Target.Installation != c.Handler.installation {
		return nil, ErrInvalidSource
	}
	model, err := NewModel(c.Handler.model.source, c.Handler.model.config)
	if err != nil {
		return nil, ErrInvalidSource
	}
	p := &LocalPage{enabled: true, target: c.Target, handler: *c.Handler, assets: map[string]localAsset{}}
	// Pin the caller's wrappers while retaining current reads from their actual
	// sources. A live policy update still takes effect on every authorization.
	p.handler.model = model
	if auth, ok := c.Handler.auth.(*SessionAuthorizer); ok {
		if auth == nil {
			return nil, ErrInvalidSource
		}
		copy := *auth
		p.handler.auth = &copy
	}
	for path, asset := range map[string]localAsset{
		"/diagnostics/":                    {"text/html; charset=utf-8", c.Assets.HTML},
		"/diagnostics/diagnostic-page.mjs": {"text/javascript; charset=utf-8", c.Assets.Module},
		"/diagnostics/diagnostic-page.css": {"text/css; charset=utf-8", c.Assets.Stylesheet},
		"/diagnostics/diagnostics.mjs":     {"text/javascript; charset=utf-8", c.Assets.Panel},
	} {
		if len(asset.body) == 0 || len(asset.body) > 64*1024 || !utf8.Valid(asset.body) {
			return nil, ErrInvalidSource
		}
		asset.body = bytes.Clone(asset.body)
		p.assets[path] = asset
	}
	return p, nil
}

func localPageHeaders(w http.ResponseWriter) {
	w.Header().Set("Cache-Control", "no-store, private")
	w.Header().Set("Vary", "Authorization, Cookie")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Cross-Origin-Resource-Policy", "same-origin")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'")
}

func localPageFailure(w http.ResponseWriter, r *http.Request, status int, code string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		_, _ = w.Write([]byte(`{"error":"` + code + `"}`))
	}
}

func localPageAuthorizationFailure(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, ErrUnauthenticated):
		localPageFailure(w, r, http.StatusUnauthorized, "AuthenticationRequired")
	case errors.Is(err, ErrForbidden):
		localPageFailure(w, r, http.StatusNotFound, "NotFound")
	default:
		localPageFailure(w, r, http.StatusServiceUnavailable, "AuthorizationUnavailable")
	}
}

func (p *LocalPage) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	localPageHeaders(w)
	if p == nil || !p.enabled {
		localPageFailure(w, r, http.StatusNotFound, "NotFound")
		return
	}
	if r.URL.RawQuery != "" || r.URL.ForceQuery || r.URL.EscapedPath() != r.URL.Path {
		localPageFailure(w, r, http.StatusBadRequest, "InvalidRequest")
		return
	}
	asset, static := p.assets[r.URL.Path]
	readPath := "/api/v2/projects/" + p.target.ProjectUID + "/diagnostics"
	uid := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v2/projects/"), "/diagnostics")
	readRoute := strings.HasPrefix(r.URL.Path, "/api/v2/projects/") && strings.HasSuffix(r.URL.Path, "/diagnostics") && scopeID.MatchString(uid)
	if !static && r.URL.Path != LocalBindingPath && !readRoute {
		localPageFailure(w, r, http.StatusNotFound, "NotFound")
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		localPageFailure(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed")
		return
	}
	if static {
		w.Header().Set("Content-Type", asset.contentType)
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			_, _ = w.Write(asset.body)
		}
		return
	}
	if r.URL.Path == readPath {
		p.handler.ServeHTTP(w, r)
		return
	}
	// For syntactically valid foreign project paths, verify access to this
	// configured page before returning absence. Anonymous callers see the same
	// response for known/foreign UIDs; neither path discovers a source.
	if r.Context().Err() != nil {
		localPageFailure(w, r, http.StatusServiceUnavailable, "AuthorizationUnavailable")
		return
	}
	grant, err := p.handler.auth.AuthorizeRead(r.Context(), r, p.target)
	if err != nil {
		localPageAuthorizationFailure(w, r, err)
		return
	}
	if readRoute {
		if err = grant.authorize(r.Context(), p.target, p.handler.model.config.Now()); err != nil {
			localPageAuthorizationFailure(w, r, err)
			return
		}
		localPageFailure(w, r, http.StatusNotFound, "NotFound")
		return
	}
	data, err := json.Marshal(struct {
		Enabled bool   `json:"enabled"`
		Target  Target `json:"target"`
	}{true, p.target})
	if err != nil || len(data) > 1024 {
		localPageFailure(w, r, http.StatusServiceUnavailable, "DiagnosticBindingUnavailable")
		return
	}
	// Recheck the same scoped grant immediately before publishing the binding;
	// authorization never requires an existence/journal read.
	if err = grant.authorize(r.Context(), p.target, p.handler.model.config.Now()); err != nil {
		localPageAuthorizationFailure(w, r, err)
		return
	}
	if r.Context().Err() != nil {
		localPageFailure(w, r, http.StatusServiceUnavailable, "AuthorizationUnavailable")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(data)
	}
}
