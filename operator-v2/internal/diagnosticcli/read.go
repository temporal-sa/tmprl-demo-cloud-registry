package diagnosticcli

import (
	"context"
	"errors"
	"net/http"
	"time"

	c "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/catalogdiagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubejournal"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
)

// ReadLocal never opens a listener, mints a session, accepts ambient config,
// acquires writer admission, or calls any provider mutation method.
func ReadLocal(ctx context.Context, bootstrap Bootstrap) (c.View, error) {
	if bootstrap.private == nil {
		return c.View{}, fail("LocalBootstrapInvalid")
	}
	bootstrap.private.mu.Lock()
	defer bootstrap.private.mu.Unlock()
	if bootstrap.private.secrets.private == nil {
		return c.View{}, fail("LocalBootstrapInvalid")
	}
	m, secret := bootstrap.private.metadata, bootstrap.private.secrets.private
	credentials, err := kube.NewCredentials(secret.ca, secret.certificate, secret.key)
	if err != nil {
		return c.View{}, fail("LocalBootstrapInvalid")
	}
	client, err := kube.New(ctx, kube.Connection{Mode: kube.LocalSerialized, Endpoint: m.Endpoint, Identity: m.Cluster, Credentials: credentials, CallTimeout: 5 * time.Second})
	if err != nil {
		return c.View{}, fail("LocalDiagnosticSourceUnavailable")
	}
	base, err := ownership.NewContext(ownership.Config{Owner: m.Owner, ResourceRoot: m.ResourceRoot, Scopes: []ownership.Scope{m.ControlNamespace.Ref.Scope}})
	if err != nil {
		return c.View{}, fail("LocalBootstrapInvalid")
	}
	binding, err := client.BindNamespace(ctx, base, m.ControlNamespace, m.ControlAttempt)
	if err != nil {
		return c.View{}, fail("LocalDiagnosticSourceUnavailable")
	}
	subject := rolloutSubject(m.Target)
	cell, err := kubejournal.NewReadOnly(kubejournal.ReadConfig{Namespace: binding, Pinned: m.Journal, Subject: subject})
	if err != nil {
		return c.View{}, fail("LocalBootstrapInvalid")
	}
	repo, err := journal.New(cell, subject, m.Journal.Record.Ref.ProviderID)
	if err != nil {
		return c.View{}, fail("LocalBootstrapInvalid")
	}
	desired, err := c.NewPinnedDesired(c.PinnedDesiredConfig{Mode: c.LocalPinnedDesired, Target: m.Target, Namespace: binding, Pinned: m.Desired, Now: time.Now})
	if err != nil {
		return c.View{}, fail("LocalBootstrapInvalid")
	}
	policy, err := c.NewPinnedReadPolicy(c.PinnedPolicyConfig{Mode: c.LocalPinnedPolicy, Environment: m.Target.Environment, Installation: m.Target.Installation, Cells: []c.PolicyCell{{Target: m.Target, Namespace: binding, Pinned: m.Policy}}, Now: time.Now})
	if err != nil {
		return c.View{}, fail("LocalBootstrapInvalid")
	}
	key, err := c.NewSessionKey(secret.sessionKey)
	if err != nil {
		return c.View{}, fail("LocalBootstrapInvalid")
	}
	defer key.Close()
	auth, err := c.NewSessionAuthorizer(c.SessionConfig{Environment: m.Target.Environment, Installation: m.Target.Installation, Issuer: m.Issuer, Audience: m.Audience, AllowedDomain: m.AllowedDomain, MaxSessionLifetime: time.Duration(m.SessionSeconds) * time.Second, Now: time.Now, Key: key, Policy: policy})
	if err != nil {
		return c.View{}, fail("LocalBootstrapInvalid")
	}
	source, err := c.NewJournalSource(m.Target.Environment, m.Target.Installation, []c.JournalBinding{{Target: m.Target, Repository: repo, Origin: c.Materialized}}, desired, time.Now)
	if err != nil {
		return c.View{}, fail("LocalBootstrapInvalid")
	}
	model, err := c.NewModel(source, c.Config{Now: time.Now, MaxProgressAge: 5 * time.Minute, MaxDesiredAge: 2 * time.Minute, MaxAvailabilityAge: 2 * time.Minute})
	if err != nil {
		return c.View{}, fail("LocalBootstrapInvalid")
	}
	return readAuthorized(ctx, auth, model, m.Target, secret.cookie)
}

func rolloutSubject(target c.Target) rollout.Subject {
	return rollout.Subject{Environment: target.Environment, Installation: target.Installation, ProjectUID: target.ProjectUID}
}

func readAuthorized(ctx context.Context, auth c.ReadAuthorizer, model *c.Model, target c.Target, cookie string) (c.View, error) {
	// This is an in-process verifier input, not a network request or an HTTP
	// authentication bypass. The same legacy JWT verifier and live policy run.
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://operator-local.invalid/api/v2/projects/"+target.ProjectUID+"/diagnostics", nil)
	if err != nil {
		return c.View{}, fail("LocalBootstrapInvalid")
	}
	request.AddCookie(&http.Cookie{Name: "temporal_demo_auth", Value: cookie})
	grant, err := auth.AuthorizeRead(ctx, request, target)
	if err != nil {
		return c.View{}, readError(err)
	}
	view, err := model.Read(ctx, grant, target)
	if err != nil {
		return c.View{}, readError(err)
	}
	return view, nil
}

func readError(err error) error {
	if errors.Is(err, c.ErrUnauthenticated) || errors.Is(err, c.ErrForbidden) {
		return fail("LocalDiagnosticAccessDenied")
	}
	if errors.Is(err, c.ErrAuthorizationUnavailable) {
		return fail("LocalDiagnosticAuthorizationUnavailable")
	}
	return fail("LocalDiagnosticSourceUnavailable")
}
