package runtimeinputs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

func serviceCapture(t *testing.T, c Capture) ServiceCapture {
	t.Helper()
	o := &corev1.Service{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"}, ObjectMeta: metav1.ObjectMeta{Name: c.Target.Name, Namespace: c.Target.Parent.Name}, Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, Selector: map[string]string{"private-selector": "private-service-sentinel"}, Ports: []corev1.ServicePort{{Name: "web", Protocol: corev1.ProtocolTCP, Port: 80, TargetPort: intstr.FromInt32(8080)}}}}
	target := c.Target
	var e error
	target.Fingerprint, e = kube.ServiceFingerprint(o)
	if e != nil {
		t.Fatal(e)
	}
	return ServiceCapture{Target: target, Desired: o}
}

func TestServiceOriginalInputIsPrivateDistinctAndSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	s, pin, root, d := fixture(t, 2)
	deployment, e := s.Retain(ctx, d)
	if e != nil {
		t.Fatal(e)
	}
	c := serviceCapture(t, d)
	ref, e := s.RetainService(ctx, c)
	if e != nil || !ref.MatchesKind("Service") || ref.UID == deployment.UID || ref.Contract == journal.ServiceSpecContract {
		t.Fatal("Service input confused with Deployment or allocated receipt", e)
	}
	fresh, e := Open(ctx, root, pin, s.config)
	if e != nil {
		t.Fatal(e)
	}
	c.Desired.Spec.Selector["private-selector"] = "current-mutated-authoring"
	payload, e := fresh.ResolveService(ctx, ref, c.Target)
	if e != nil || payload.ObjectForProvider().Spec.Selector["private-selector"] != "private-service-sentinel" {
		t.Fatal("restart lost original Service", e)
	}
	copy := payload.ObjectForProvider()
	copy.Spec.Selector["private-selector"] = "caller-mutated"
	if payload.ObjectForProvider().Spec.Selector["private-selector"] != "private-service-sentinel" {
		t.Fatal("private payload aliases caller")
	}
	for _, value := range []any{c, &c, payload, &payload} {
		for _, format := range []string{"%v", "%+v", "%#v", "%p", "%d", "%x", "%q"} {
			if strings.Contains(fmt.Sprintf(format, value), "selector") || strings.Contains(fmt.Sprintf(format, value), "authoring") {
				t.Fatal("private Service escaped formatting")
			}
		}
		data, _ := json.Marshal(value)
		if strings.Contains(string(data), "selector") || strings.Contains(string(data), "authoring") {
			t.Fatal("private Service escaped JSON")
		}
	}
	if _, e := fresh.Resolve(ctx, ref, d.Target); e != ErrInvalid {
		t.Fatal("Service reference used as Deployment", e)
	}
	if _, e := fresh.ResolveService(ctx, deployment, c.Target); e != ErrInvalid {
		t.Fatal("Deployment reference used as Service", e)
	}
	wrong := c.Target
	wrong.Parent.UID = "foreign-incarnation"
	if _, e := fresh.ResolveService(ctx, ref, wrong); e != ErrInvalid {
		t.Fatal("foreign Service target resolved", e)
	}
	if e := root.Remove(ref.UID + ".input.json"); e != nil {
		t.Fatal(e)
	}
	c = serviceCapture(t, d)
	for range 2 {
		if _, e := fresh.RetainService(ctx, c); e == nil {
			t.Fatal("Service input repaired beside existing commit")
		}
	}
	if _, e := root.Lstat(ref.UID + ".input.json"); !os.IsNotExist(e) {
		t.Fatal("lost Service body recreated", e)
	}
}

func TestLegacyDeploymentStoreNeverImplicitlyUpgradesToService(t *testing.T) {
	ctx := context.Background()
	s, pin, root, c := fixture(t, 2)
	ref, e := s.Retain(ctx, c)
	if e != nil {
		t.Fatal(e)
	}
	// Explicit fixture administrator installs the previous finite manifest and
	// independently supplies its correct pin; production Open never upgrades it.
	raw := encode(manifest{"private-deployment-inputs-v1", s.config, pin.Directory, pin.Lock})
	if e := root.WriteFile("manifest.json", raw, 0600); e != nil {
		t.Fatal(e)
	}
	pin.ManifestDigest = hash(raw)
	old, e := Open(ctx, root, pin, s.config)
	if e != nil {
		t.Fatal(e)
	}
	payload, e := old.Resolve(ctx, ref, c.Target)
	if e != nil || payload.ObjectForProvider() == nil {
		t.Fatal("legacy Deployment reference changed", e)
	}
	svc := serviceCapture(t, c)
	if _, e := old.RetainService(ctx, svc); e != ErrInvalid {
		t.Fatal("legacy root silently upgraded", e)
	}
	if _, e := root.Lstat(old.uidFor(svc.Target, serviceFamily) + ".input.json"); !os.IsNotExist(e) {
		t.Fatal("unsupported Service left artifact", e)
	}
	if got, e := root.ReadFile("manifest.json"); e != nil || string(got) != string(raw) {
		t.Fatal("legacy manifest changed")
	}
}

func TestBothInputFamiliesShareCapacityAndRetainIncompleteService(t *testing.T) {
	ctx := context.Background()
	s, _, root, d := fixture(t, 1)
	c := serviceCapture(t, d)
	s.fault = func(step string) error {
		if step == "after-input" {
			return ErrUncertain
		}
		return nil
	}
	if _, e := s.RetainService(ctx, c); e != ErrUncertain {
		t.Fatal("Service partial not retained", e)
	}
	s.fault = nil
	if _, e := s.RetainService(ctx, c); e != ErrUncertain {
		t.Fatal("Service partial completed", e)
	}
	uid := s.uidFor(c.Target, serviceFamily)
	if _, e := root.Lstat(uid + ".input.json"); e != nil {
		t.Fatal("partial body discarded", e)
	}
	if _, e := root.Lstat(uid + ".commit.json"); !os.IsNotExist(e) {
		t.Fatal("partial gained receipt", e)
	}
	if _, e := s.Retain(ctx, d); e != ErrCapacity {
		t.Fatal("Deployment bypassed partial Service capacity", e)
	}
}
