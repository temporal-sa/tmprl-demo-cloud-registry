package runtimeinputs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func serviceAccountCapture(c Capture) ServiceAccountCapture {
	no := false
	o := &corev1.ServiceAccount{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"}, ObjectMeta: metav1.ObjectMeta{Name: c.Target.Name, Namespace: c.Target.Parent.Name, Labels: map[string]string{"private-input": "private-sa-sentinel"}}, AutomountServiceAccountToken: &no}
	target := c.Target
	target.Fingerprint = hash(encode(o))
	return ServiceAccountCapture{Target: target, Desired: o}
}

func TestServiceAccountInputRequiresFreshExplicitManifestAndPreservesOldBytes(t *testing.T) {
	ctx := context.Background()
	old, pin, root, c := fixture(t, 2)
	// Independently retain the complete pre-extension Config encoding.
	legacy := struct {
		BootstrapUID   string
		Owner          ownership.Owner
		CellUID        string
		MaximumEntries int
	}{old.config.BootstrapUID, old.config.Owner, old.config.CellUID, old.config.MaximumEntries}
	if !bytes.Equal(encode(old.config), encode(legacy)) {
		t.Fatal("zero opt-in changed historical config bytes")
	}
	for _, schema := range []string{"private-deployment-inputs-v1", "private-render-inputs-v2"} {
		raw := encode(manifest{schema, old.config, pin.Directory, pin.Lock})
		if err := root.WriteFile("manifest.json", raw, 0600); err != nil {
			t.Fatal(err)
		}
		pin.ManifestDigest = hash(raw)
		opened, err := Open(ctx, root, pin, old.config)
		if err != nil {
			t.Fatal(err)
		}
		sa := serviceAccountCapture(c)
		if _, err := opened.RetainServiceAccount(ctx, sa); err != ErrInvalid {
			t.Fatal("legacy store gained SA capability", err)
		}
		if _, err := root.Lstat(opened.uidFor(sa.Target, serviceAccountFamily) + ".input.json"); !os.IsNotExist(err) {
			t.Fatal("unsupported family left an artifact", err)
		}
		upgraded := old.config
		upgraded.ServiceAccounts = true
		if _, err := Open(ctx, root, pin, upgraded); err != ErrUnproven {
			t.Fatal("old manifest upgraded at Open", err)
		}
		if got, err := root.ReadFile("manifest.json"); err != nil || !bytes.Equal(got, raw) {
			t.Fatal("historical manifest changed")
		}
	}
}

func TestServiceAccountOriginalPrivateBodySurvivesReopenWithSharedCapacity(t *testing.T) {
	ctx := context.Background()
	s, pin, root, c := fixtureWithServiceAccounts(t, 3, true)
	deployment, err := s.Retain(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	svc := serviceCapture(t, c)
	service, err := s.RetainService(ctx, svc)
	if err != nil {
		t.Fatal(err)
	}
	sa := serviceAccountCapture(c)
	ref, err := s.RetainServiceAccount(ctx, sa)
	if err != nil || !ref.MatchesKind("ServiceAccount") || ref.UID == service.UID || ref.UID == deployment.UID {
		t.Fatal("SA family not separately pinned", err)
	}
	if again, err := s.RetainServiceAccount(ctx, sa); err != nil || again != ref {
		t.Fatal("exact retention not idempotent", err)
	}
	fresh, err := Open(ctx, root, pin, s.config)
	if err != nil {
		t.Fatal(err)
	}
	sa.Desired.Labels["private-input"] = "current-authoring"
	payload, err := fresh.ResolveServiceAccount(ctx, ref, sa.Target)
	if err != nil || payload.ObjectForProvider().Labels["private-input"] != "private-sa-sentinel" {
		t.Fatal("original SA body lost", err)
	}
	copy := payload.ObjectForProvider()
	copy.Labels["private-input"] = "mutated-copy"
	if payload.ObjectForProvider().Labels["private-input"] != "private-sa-sentinel" {
		t.Fatal("opaque payload aliases execution copy")
	}
	for _, value := range []any{sa, &sa, payload, &payload} {
		for _, format := range []string{"%v", "%+v", "%#v", "%x", "%q", "%p"} {
			if strings.Contains(fmt.Sprintf(format, value), "private-sa") || strings.Contains(fmt.Sprintf(format, value), "current-authoring") {
				t.Fatal("private input escaped formatting")
			}
		}
		b, err := json.Marshal(value)
		if err != nil || bytes.Contains(b, []byte("private-sa")) || bytes.Contains(b, []byte("current-authoring")) {
			t.Fatal("private input escaped serialization")
		}
	}
	if _, err := fresh.ResolveServiceAccount(ctx, service, sa.Target); err != ErrInvalid {
		t.Fatal("Service reference crossed SA family")
	}
	if _, err := fresh.ResolveService(ctx, ref, svc.Target); err != ErrInvalid {
		t.Fatal("SA reference crossed Service family")
	}
	if _, err := fresh.Resolve(ctx, ref, c.Target); err != ErrInvalid {
		t.Fatal("SA reference crossed Deployment family")
	}
	wrong := sa.Target
	wrong.Parent.UID = "recreated-parent"
	if _, err := fresh.ResolveServiceAccount(ctx, ref, wrong); err != ErrInvalid {
		t.Fatal("changed parent admitted")
	}
	other := serviceAccountCapture(c)
	other.Desired.Name = "another"
	other.Target.Name = "another"
	other.Target.Fingerprint = hash(encode(other.Desired))
	if _, err := fresh.RetainServiceAccount(ctx, other); err != ErrCapacity {
		t.Fatal("SA escaped shared capacity", err)
	}
	if err := root.Remove(ref.UID + ".input.json"); err != nil {
		t.Fatal(err)
	}
	if _, err := fresh.ResolveServiceAccount(ctx, ref, sa.Target); err != ErrUnproven {
		t.Fatal("lost input was not unproven", err)
	}
	if _, err := fresh.RetainServiceAccount(ctx, serviceAccountCapture(c)); err == nil {
		t.Fatal("commit-only entry repaired")
	}
	if _, err := root.Lstat(ref.UID + ".input.json"); !os.IsNotExist(err) {
		t.Fatal("original body recreated")
	}
	if (ServiceAccountPayload{}).ObjectForProvider() != nil || ref.Contract != journal.ServiceAccountInputContract {
		t.Fatal("zero payload or contract invalid")
	}
}
