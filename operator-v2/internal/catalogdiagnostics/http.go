package catalogdiagnostics

import (
	"errors"
	"net/http"
	"strings"
)

// Handler is an opt-in transport seam, not an installed production endpoint.
// The environment and installation are trusted server configuration. Request
// headers cannot select a different scope or authenticate themselves.
type Handler struct {
	environment, installation string
	auth                      ReadAuthorizer
	model                     *Model
}

func NewHandler(environment, installation string, auth ReadAuthorizer, model *Model) (*Handler, error) {
	if !scopeID.MatchString(environment) || !scopeID.MatchString(installation) || auth == nil || model == nil {
		return nil, errors.New("diagnostic handler requires trusted scope, authorizer and model")
	}
	return &Handler{environment: environment, installation: installation, auth: auth, model: model}, nil
}
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store, private")
	w.Header().Set("Vary", "Authorization, Cookie")
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	fail := func(status int, code string) {
		w.WriteHeader(status)
		if r.Method != http.MethodHead {
			_, _ = w.Write([]byte(`{"error":"` + code + `"}`))
		}
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		fail(http.StatusMethodNotAllowed, "MethodNotAllowed")
		return
	}
	const prefix = "/api/v2/projects/"
	const suffix = "/diagnostics"
	if !strings.HasPrefix(r.URL.Path, prefix) || !strings.HasSuffix(r.URL.Path, suffix) {
		fail(http.StatusNotFound, "NotFound")
		return
	}
	uid := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, prefix), suffix)
	if !scopeID.MatchString(uid) || r.URL.RawQuery != "" || r.URL.ForceQuery {
		fail(http.StatusBadRequest, "InvalidRequest")
		return
	}
	target := Target{Environment: h.environment, Installation: h.installation, ProjectUID: uid}
	grant, err := h.auth.AuthorizeRead(r.Context(), r, target)
	if err != nil {
		switch {
		case errors.Is(err, ErrUnauthenticated):
			fail(http.StatusUnauthorized, "AuthenticationRequired")
		case errors.Is(err, ErrForbidden):
			fail(http.StatusNotFound, "NotFound")
		default:
			fail(http.StatusServiceUnavailable, "AuthorizationUnavailable")
		}
		return
	}
	view, err := h.model.Read(r.Context(), grant, target)
	if err != nil {
		switch {
		case errors.Is(err, ErrForbidden), errors.Is(err, ErrNotFound):
			fail(http.StatusNotFound, "NotFound")
		case errors.Is(err, ErrInvalidSource):
			fail(http.StatusBadGateway, "InvalidDiagnosticSource")
		default:
			fail(http.StatusServiceUnavailable, "DiagnosticSourceUnavailable")
		}
		return
	}
	data, err := view.JSON()
	if err != nil {
		fail(http.StatusBadGateway, "InvalidDiagnosticSource")
		return
	}
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(data)
	}
}
