package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

func TestDeploymentCreationReceiptUsesPOSTRVBeforeIndependentRead(t *testing.T) {
	f := newDeploymentFixture(t)
	creation, err := f.binding.CreateDeploymentWithReceipt(context.Background(), f.request())
	if err != nil {
		t.Fatal(err)
	}
	if creation.Record.Record.Ref.ProviderID != "deployment-uid" || creation.CreationResourceVersion != "20" || f.posts != 1 || f.gets != 0 {
		t.Fatal("create receipt required another mutation/child GET or lost original response version")
	}
	// Controller/status activity can change a live RV. That later observation is
	// a distinct fact; it must never become or rewrite original create evidence.
	f.object.ResourceVersion = "21"
	snapshot, err := f.binding.ObserveDeployment(context.Background(), DeploymentRead{Desired: f.desired, Intent: f.intent, Record: creation.Record})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.View().ResourceVersion != "21" || creation.CreationResourceVersion != "20" || f.posts != 1 || f.gets != 1 {
		t.Fatal("historical creation receipt laundered from current read")
	}
	assertReceiptPrivate(t, creation)
}
func TestDeploymentLegacyCreateDelegatesExactlyOnePOST(t *testing.T) {
	f := newDeploymentFixture(t)
	record, err := f.binding.CreateDeployment(context.Background(), f.request())
	if err != nil {
		t.Fatal(err)
	}
	if record.Record.Ref.ProviderID != "deployment-uid" || f.posts != 1 || f.gets != 0 {
		t.Fatal("legacy wrapper dispatched twice or did a child GET")
	}
}
func TestDeploymentCreationMissingNativeProofRetainsUnknown(t *testing.T) {
	for _, tamper := range []func(*appsv1.Deployment){func(d *appsv1.Deployment) { d.ResourceVersion = "" }, func(d *appsv1.Deployment) { d.UID = "" }, func(d *appsv1.Deployment) { d.Spec.Template.Spec.Containers[0].Args = []string{"changed-response"} }} {
		f := newDeploymentFixture(t)
		f.tamper = tamper
		creation, err := f.binding.CreateDeploymentWithReceipt(context.Background(), f.request())
		requireCode(t, err, UnknownWriteOutcome)
		if creation != (DeploymentCreation{}) || f.posts != 1 || f.gets != 0 {
			t.Fatal("unverified creation exposed partial receipt or read to learn it")
		}
		requireCode(t, f.binding.client.lifecycle.drain(context.Background(), f.parent.Name, string(f.parent.UID), f.binding.attemptID), UnknownWriteOutcome)
	}
}
func TestServiceCreationReceiptRVIsIndependentOfAllocationAndLaterRead(t *testing.T) {
	f := newServiceFixture(t)
	creation, err := f.binding.CreateService(context.Background(), f.request())
	if err != nil {
		t.Fatal(err)
	}
	if creation.Resource.Record.Ref.ProviderID != "service-uid" || creation.CreationResourceVersion != "20" || creation.SpecContract != ServiceSpecContract || !serviceReceiptDigest.MatchString(creation.AllocatedSpecDigest) || f.posts != 1 || f.gets != 0 {
		t.Fatal("validated POST metadata/allocation proof missing or obtained through child GET")
	}
	digest := creation.AllocatedSpecDigest
	f.object.ResourceVersion = "21"
	snapshot, err := f.binding.ObserveService(context.Background(), ServiceRead{Desired: f.desired, Intent: f.intent, Record: creation})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.View().ResourceVersion != "21" || creation.CreationResourceVersion != "20" || creation.AllocatedSpecDigest != digest || f.posts != 1 || f.gets != 1 {
		t.Fatal("later metadata read changed original creation evidence")
	}
	assertReceiptPrivate(t, creation)
}
func TestServiceCreationMissingPOSTVersionCannotBeLearnedFromGET(t *testing.T) {
	f := newServiceFixture(t)
	f.tamper = func(d *corev1.Service) { d.ResourceVersion = "" }
	creation, err := f.binding.CreateService(context.Background(), f.request())
	requireCode(t, err, UnknownWriteOutcome)
	if creation != (ServiceRecord{}) || f.posts != 1 || f.gets != 0 {
		t.Fatal("missing POST version exposed receipt or adopted a read")
	}
	requireCode(t, f.binding.client.lifecycle.drain(context.Background(), f.parent.Name, string(f.parent.UID), f.binding.attemptID), UnknownWriteOutcome)
}
func assertReceiptPrivate(t *testing.T, value any) {
	t.Helper()
	for _, verb := range []string{"%v", "%+v", "%#v", "%p", "%x", "%q", "%d"} {
		text := fmt.Sprintf(verb, value)
		if strings.Contains(text, "PRIVATE_") || strings.Contains(text, "50524956415445") || strings.Contains(text, "10.96.") {
			t.Fatalf("raw desired/response spec leaked through receipt %s", verb)
		}
	}
	data, err := json.Marshal(value)
	if err != nil || strings.Contains(string(data), "PRIVATE_") || strings.Contains(string(data), "10.96.") {
		t.Fatal("raw provider/config body leaked through creation receipt", err)
	}
}
