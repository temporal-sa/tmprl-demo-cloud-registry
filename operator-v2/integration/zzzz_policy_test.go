//go:build integration

package integration

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
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/catalogdiagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubejournal"
	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
)

type unknownPolicyDesired struct{}

func (unknownPolicyDesired) ObserveDesired(context.Context, catalogdiagnostics.Target) (catalogdiagnostics.DesiredObservation, error) {
	return catalogdiagnostics.DesiredObservation{}, catalogdiagnostics.ErrSourceUnavailable
}

type policyProcessInput struct {
	Pin      runtimePin
	Cell     ownership.NamespacedRecord
	Target   catalogdiagnostics.Target
	Identity catalogdiagnostics.SessionIdentity
	Now      time.Time
	Allowed  bool
}

func newActualReadPolicy(t *testing.T, pin runtimePin, cell ownership.NamespacedRecord, target catalogdiagnostics.Target, now time.Time) *catalogdiagnostics.PinnedReadPolicy {
	t.Helper()
	adapter, _ := typedClient(t)
	base, err := ownership.NewContext(pin.Control)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := adapter.BindNamespace(context.Background(), base, pin.Parent, pin.Parent.CreationOperationID)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := catalogdiagnostics.NewPinnedReadPolicy(catalogdiagnostics.PinnedPolicyConfig{Mode: catalogdiagnostics.LocalPinnedPolicy, Environment: target.Environment, Installation: target.Installation, Cells: []catalogdiagnostics.PolicyCell{{Target: target, Namespace: bound, Pinned: cell}}, Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}
func runPolicyReadProcess(t *testing.T, input policyProcessInput) {
	t.Helper()
	data, _ := json.Marshal(input)
	command := exec.Command(os.Args[0], "-test.run=^TestPinnedProjectPolicyAcrossProcessRestart$", "-test.v", "-test.timeout=40s")
	command.Env = []string{"OPERATOR_LOCAL_KUBE_CONNECTION=" + os.Getenv("OPERATOR_LOCAL_KUBE_CONNECTION"), "OPERATOR_LOCAL_CLUSTER_RUN=" + local.RunID, "OPERATOR_POLICY_CHILD=1"}
	command.Stdin = bytes.NewReader(data)
	output, err := command.CombinedOutput()
	if err != nil || command.ProcessState == nil || !command.ProcessState.Exited() || !command.ProcessState.Success() || !strings.Contains(string(output), "POLICY-CHILD-VERIFIED") {
		t.Fatalf("owned read-only policy child failed: %v\n%s", err, output)
	}
}

func TestPinnedProjectPolicyAcrossProcessRestart(t *testing.T) {
	if os.Getenv("OPERATOR_POLICY_CHILD") == "1" {
		var input policyProcessInput
		d := json.NewDecoder(io.LimitReader(os.Stdin, 65537))
		d.DisallowUnknownFields()
		if d.Decode(&input) != nil || d.Decode(new(any)) != io.EOF {
			t.Fatal("invalid owned policy child input")
		}
		TestOwnedAPIAndNetworkBoundary(t)
		policy := newActualReadPolicy(t, input.Pin, input.Cell, input.Target, input.Now)
		decision, err := policy.CheckDiagnosticRead(context.Background(), input.Identity, input.Target)
		if err != nil || decision.Allowed != input.Allowed {
			t.Fatalf("durable policy decision mismatch: %v", err)
		}
		fmt.Println("POLICY-CHILD-VERIFIED")
		return
	}
	pin := runtimeControlPin(t, "policy")
	adapter, _ := typedClient(t)
	base, err := ownership.NewContext(pin.Control)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := adapter.BindNamespace(context.Background(), base, pin.Parent, pin.Parent.CreationOperationID)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	target := catalogdiagnostics.Target{Environment: pin.Subject.Environment, Installation: pin.Subject.Installation, ProjectUID: pin.Subject.ProjectUID}
	record := catalogdiagnostics.ProjectReadPolicy{SchemaVersion: 1, Target: target, Issuer: "https://owned-catalog.example.test", IssuedAt: now.Add(-time.Hour), ExpiresAt: now.Add(time.Hour), Subjects: []catalogdiagnostics.PolicySubject{{Subject: "google:owned-fixture-user", Allowed: true, AuthenticatedAtOrAfter: now.Add(-time.Hour)}}}
	object, err := catalogdiagnostics.PolicyBootstrapObject(bound.Context().NamespaceName(), record)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, _ := provider.ConfigMapFingerprint(object)
	intent, err := bound.Context().Intent("configmap", object.Name, catalogdiagnostics.PolicyRevision, "bootstrap-read-policy", fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	writer := ownership.Writer{ID: "control-writer", Owner: base.Owner(), Epoch: 1, ExpiresAt: time.Now().Add(time.Hour)}
	pinned, err := bound.CreateConfigMap(context.Background(), provider.ConfigMapCreate{Desired: object, Intent: intent, Writer: writer})
	if err != nil {
		t.Fatal(err)
	}
	keyBytes := []byte("owned-local-policy-signing-key-synthetic-12345")
	key, err := catalogdiagnostics.NewSessionKey(keyBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer key.Close()
	claims, _ := json.Marshal(map[string]any{"typ": "session", "iss": record.Issuer, "aud": "owned-catalog", "sub": "google:owned-fixture-user", "email": "fixture@example.test", "hd": "example.test", "iat": now.Unix(), "auth_time": now.Unix(), "exp": now.Add(time.Hour).Unix()})
	encode := base64.RawURLEncoding.EncodeToString
	input := encode([]byte(`{"alg":"HS256","typ":"JWT"}`)) + "." + encode(claims)
	mac := hmac.New(sha256.New, keyBytes)
	_, _ = mac.Write([]byte(input))
	token := input + "." + encode(mac.Sum(nil))
	tokenHash := sha256.Sum256([]byte(token))
	identity := catalogdiagnostics.SessionIdentity{Issuer: record.Issuer, Subject: "google:owned-fixture-user", Fingerprint: hex.EncodeToString(tokenHash[:]), IssuedAt: now, AuthenticatedAt: now, ExpiresAt: now.Add(time.Hour)}
	child := policyProcessInput{Pin: pin, Cell: pinned, Target: target, Identity: identity, Now: now, Allowed: true}
	runPolicyReadProcess(t, child)
	policy := newActualReadPolicy(t, pin, pinned, target, now)
	authorizer, err := catalogdiagnostics.NewSessionAuthorizer(catalogdiagnostics.SessionConfig{Environment: target.Environment, Installation: target.Installation, Issuer: record.Issuer, Audience: "owned-catalog", AllowedDomain: "example.test", MaxSessionLifetime: time.Hour, Now: func() time.Time { return now }, Key: key, Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	repoConfig := kubejournal.Config{Namespace: bound, Pinned: pin.Cell, Subject: pin.Subject, Writer: func() ownership.Writer { return writer }}
	source, err := catalogdiagnostics.NewJournalSource(target.Environment, target.Installation, []catalogdiagnostics.JournalBinding{{Target: target, Repository: openRepository(t, repoConfig), Origin: catalogdiagnostics.Archived}}, unknownPolicyDesired{}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	counted := &countedDiagnosticSource{source: source}
	model, err := catalogdiagnostics.NewModel(counted, catalogdiagnostics.Config{Now: func() time.Time { return now }, MaxProgressAge: time.Minute, MaxDesiredAge: time.Minute, MaxAvailabilityAge: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := catalogdiagnostics.NewHandler(target.Environment, target.Installation, authorizer, model)
	if err != nil {
		t.Fatal(err)
	}
	requestFor := func(project string, cookie bool) *http.Request {
		r := httptest.NewRequest("GET", "/api/v2/projects/"+project+"/diagnostics", nil)
		r.Header.Set("X-Temporal-Auth-Email", "fixture@example.test")
		if cookie {
			r.AddCookie(&http.Cookie{Name: "temporal_demo_auth", Value: token})
		}
		return r
	}
	read := func(project string, cookie bool) int {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, requestFor(project, cookie))
		return w.Code
	}
	if read(target.ProjectUID, false) != 401 || read("foreign", true) != 404 || counted.reads != 0 {
		t.Fatal("unverified or foreign access reached source")
	}
	if read(target.ProjectUID, true) != 200 || counted.reads != 1 {
		t.Fatal("authorized durable status read failed")
	}
	grant, err := authorizer.AuthorizeRead(context.Background(), requestFor(target.ProjectUID, true), target)
	if err != nil {
		t.Fatal(err)
	}
	path := "/api/v1/namespaces/" + object.Namespace + "/configmaps/" + object.Name
	actual := request(t, "GET", path, nil, 200)
	field(metadata(actual), "annotations")["registry.tmprl-demo.cloud/policy-audit"] = "metadata-only-change"
	request(t, "PUT", path, actual, 200)
	if _, err = model.Read(context.Background(), grant, target); !errors.Is(err, catalogdiagnostics.ErrForbidden) || counted.reads != 1 {
		t.Fatal("old grant survived metadata-only policy version change")
	}
	if read(target.ProjectUID, true) != 200 || counted.reads != 2 {
		t.Fatal("fresh verification failed after metadata-only change")
	}
	// Revoke through a real UID/RV conditional metadata-cell mutation.
	observed, err := bound.ObserveConfigMap(context.Background(), pinned)
	if err != nil {
		t.Fatal(err)
	}
	record.Subjects[0].Allowed = false
	desired, err := catalogdiagnostics.PolicyBootstrapObject(object.Namespace, record)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, _ = provider.ConfigMapFingerprint(desired)
	updateIntent, err := bound.Context().Intent("configmap", object.Name, catalogdiagnostics.PolicyRevision, "revoke-read-policy", fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	current := pinned
	current.Record.Revision = observed.Observation.Observation.Revision
	current.Record.SpecFingerprint = observed.Observation.Observation.SpecFingerprint
	current.Record.WriterEpoch = observed.Observation.Observation.WriterEpoch
	if _, err = bound.UpdateConfigMap(context.Background(), provider.ConfigMapUpdate{ConfigMapMutation: provider.ConfigMapMutation{OperationID: updateIntent.Intent.OperationID, Record: current, ExpectedVersion: observed.Object.ResourceVersion, Writer: writer}, Desired: desired, Intent: updateIntent}); err != nil {
		t.Fatal(err)
	}
	if read(target.ProjectUID, true) != 404 || counted.reads != 2 {
		t.Fatal("revoked policy touched diagnostic source")
	}
	child.Allowed = false
	runPolicyReadProcess(t, child)
	// An admin fixture deletes/replaces this exact UID with copied metadata. The
	// new object's otherwise matching data cannot become the pinned policy.
	actual = request(t, "GET", path, nil, 200)
	request(t, "DELETE", path, objectMapDelete(pinned.Record.Ref.ProviderID, text(metadata(actual), "resourceVersion")), 200)
	request(t, "POST", "/api/v1/namespaces/"+object.Namespace+"/configmaps", map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": object.Name, "namespace": object.Namespace, "annotations": field(metadata(actual), "annotations"), "labels": field(metadata(actual), "labels"), "ownerReferences": metadata(actual)["ownerReferences"]}, "data": field(actual, "data")}, 201)
	if read(target.ProjectUID, true) != 503 || counted.reads != 2 {
		t.Fatal("copied replacement policy was adopted or treated as ordinary denial")
	}
}

func objectMapDelete(uid, version string) map[string]any {
	return map[string]any{"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": map[string]any{"uid": uid, "resourceVersion": version}}
}
