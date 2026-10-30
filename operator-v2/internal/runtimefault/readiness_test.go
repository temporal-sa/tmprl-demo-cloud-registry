package runtimefault

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
)

func readinessContext() Context {
	c := inputContext(InputRetain)
	c.Step, c.Dispatch, c.WriteID = Observe, Applied, "create-deployment"
	return c
}
func failedReadiness() kube.DeploymentReadiness {
	return kube.DeploymentReadiness{State: "Failed", Code: "DeploymentReplicaFailure", Generation: 4, ObservedGeneration: 4, Desired: 1, Unavailable: 1}
}

func TestReadinessFailureMapsExplicitNativeFailureAndPreservesFacts(t *testing.T) {
	for _, tc := range []struct{ code, message string }{
		{"DeploymentReplicaFailure", "The Deployment controller reported a replica failure for the observed generation."},
		{"DeploymentProgressDeadlineExceeded", "The Deployment controller reported its progress deadline was exceeded for the observed generation."},
	} {
		t.Run(tc.code, func(t *testing.T) {
			observed := failedReadiness()
			observed.Code = tc.code
			fault, err := MapDeploymentReadiness(readinessContext(), observed)
			if err != nil {
				t.Fatal(err)
			}
			v := fault.View()
			if v.Failure.Code != tc.code || v.Failure.Message != tc.message || v.Failure.Category != diagnostics.Readiness || v.Failure.NextAction == "" || v.Step != Observe || v.Dispatch != Applied || v.Readiness == nil || v.Readiness.Generation != 4 || v.Readiness.ObservedGeneration != 4 || v.Condition != "" {
				t.Fatal("finite readiness failure lost independent context")
			}
			if v.JournalDelivery.State != NotAttempted || v.DiagnosticDelivery.State != NotAttempted || v.InputPersistence.State != NotAttempted {
				t.Fatal("readiness classification invented a persistence acknowledgement")
			}
			outcome, err := NewOutcome(identity(), "readiness-failed", 2, clock.Add(time.Second), Admitted, fault)
			if err != nil {
				t.Fatal(err)
			}
			event, err := outcome.Event()
			if err != nil || event.State != diagnostics.Failed || event.Failure.Code != tc.code || event.Availability != nil || event.Retry.Status != diagnostics.RetryUnknown || event.Retry.Authoritative {
				t.Fatal("readiness result invented retry, availability or operation success")
			}
			encoded, err := json.Marshal(outcome)
			if err != nil {
				t.Fatal(err)
			}
			restored, err := ParseOutcome(encoded)
			if err != nil || restored.View().Fault.Failure.Code != tc.code || restored.View().Fault.Readiness.Generation != 4 {
				t.Fatal("finite readiness code did not survive actual outcome JSON")
			}
		})
	}
}

func TestReadinessFactoryRejectsNonFailureUnobservedAndMalformedStatus(t *testing.T) {
	for _, mutate := range []func(*kube.DeploymentReadiness){
		func(r *kube.DeploymentReadiness) { r.State = "Ready" },
		func(r *kube.DeploymentReadiness) { r.State = "Waiting" },
		func(r *kube.DeploymentReadiness) { r.State = "Unknown" },
		func(r *kube.DeploymentReadiness) { r.State = "Retained" },
		func(r *kube.DeploymentReadiness) { r.State = "PRIVATE_RAW_STATUS" },
		func(r *kube.DeploymentReadiness) { r.Code = "PRIVATE_RAW_CONDITION" },
		func(r *kube.DeploymentReadiness) { r.Code = "DeploymentReplicasPending" },
		func(r *kube.DeploymentReadiness) { r.Generation = 0; r.ObservedGeneration = 0 },
		func(r *kube.DeploymentReadiness) { r.Generation = -1; r.ObservedGeneration = -1 },
		func(r *kube.DeploymentReadiness) { r.ObservedGeneration = 3 },
		func(r *kube.DeploymentReadiness) { r.ObservedGeneration = 5 },
		func(r *kube.DeploymentReadiness) { r.Desired = -1 },
		func(r *kube.DeploymentReadiness) { r.Replicas = -1 },
		func(r *kube.DeploymentReadiness) { r.Updated = -1 },
		func(r *kube.DeploymentReadiness) { r.Ready = -1 },
		func(r *kube.DeploymentReadiness) { r.Available = -1 },
		func(r *kube.DeploymentReadiness) { r.Unavailable = -1 },
		func(r *kube.DeploymentReadiness) { r.Terminating = -1 },
		func(r *kube.DeploymentReadiness) { r.CollisionCount = -1 },
	} {
		r := failedReadiness()
		mutate(&r)
		if _, err := MapDeploymentReadiness(readinessContext(), r); !errors.Is(err, ErrContext) || strings.Contains(err.Error(), "PRIVATE") {
			t.Fatal("invalid native status was coerced into readiness failure")
		}
	}
	// Counters alone can never turn a Waiting read into a known failure.
	waiting := failedReadiness()
	waiting.State, waiting.Unavailable = "Waiting", 100
	if _, err := MapDeploymentReadiness(readinessContext(), waiting); !errors.Is(err, ErrContext) {
		t.Fatal("nonconvergence counters invented a failed condition")
	}
}

func TestReadinessFactoryRequiresObserveDeploymentAndIndependentApplied(t *testing.T) {
	for _, mutate := range []func(*Context){
		func(c *Context) { c.Step = Create },
		func(c *Context) { c.Step = InputRetain },
		func(c *Context) { c.Kind, c.Purpose = Service, "service" },
		func(c *Context) { c.Kind, c.Purpose = Namespace, "runtime-namespace" },
		func(c *Context) { c.Dispatch = "" },
		func(c *Context) { c.Dispatch = DispatchUnknown },
		func(c *Context) { c.Dispatch = NotDispatched },
		func(c *Context) { c.Dispatch = Rejected },
		func(c *Context) { c.WriteID = "" },
		func(c *Context) { c.NamespaceUID = "" },
		func(c *Context) { c.OccurredAt = time.Time{} },
		func(c *Context) { c.Fingerprint = "unknown" },
	} {
		c := readinessContext()
		mutate(&c)
		if _, err := MapDeploymentReadiness(c, failedReadiness()); !errors.Is(err, ErrContext) {
			t.Fatal("unproven context became readiness failure")
		}
	}
}

func TestReadinessFaultRoundtripIsFinitePrivateAndCopySafe(t *testing.T) {
	fault, err := MapDeploymentReadiness(readinessContext(), failedReadiness())
	if err != nil {
		t.Fatal(err)
	}
	v := fault.View()
	v.Readiness.Generation = 100
	if fault.View().Readiness.Generation != 4 {
		t.Fatal("view retained mutable evidence alias")
	}
	wire, err := json.Marshal(fault)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseFault(wire)
	again, marshalErr := json.Marshal(parsed)
	if err != nil || marshalErr != nil || !bytes.Equal(wire, again) {
		t.Fatal("readiness fault lost finite facts in JSON roundtrip")
	}
	for _, forbidden := range []string{"desired", "replicas", "unavailable", "collision", "termination", "reason", "statusMessage"} {
		if strings.Contains(string(wire), `"`+forbidden+`"`) {
			t.Fatal("raw or unnecessary readiness payload entered history")
		}
	}
	for _, value := range []string{fmt.Sprintf("%v", fault), fmt.Sprintf("%#v", fault), fmt.Sprintf("%p", fault), fault.Error()} {
		if strings.Contains(value, "PRIVATE") {
			t.Fatal("unsafe readiness input escaped formatting")
		}
	}
	unknown, err := fault.WithDelivery(Journal, DeliveryUnknown, clock.Add(time.Second))
	if err != nil || unknown.View().Dispatch != Applied || unknown.View().Failure.Code != fault.View().Failure.Code || unknown.View().JournalDelivery.State != DeliveryUnknown {
		t.Fatal("delivery uncertainty replaced known readiness/creation facts")
	}
	outcome, err := NewOutcome(identity(), "readiness-wire", 2, clock.Add(2*time.Second), Admitted, unknown)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(outcome)
	restored, err := ParseOutcome(encoded)
	if err != nil {
		t.Fatal(err)
	}
	event, err := restored.Event()
	if err != nil || event.Failure.Code != "DeploymentReplicaFailure" || event.State != diagnostics.Failed || event.Retry.Status != diagnostics.RetryUnknown || event.Availability != nil {
		t.Fatal("history reconstruction lost readiness failure")
	}
}

func TestReadinessParserRejectsCanonicalByteInjectionsAndMissingEvidence(t *testing.T) {
	fault, _ := MapDeploymentReadiness(readinessContext(), failedReadiness())
	wire, _ := json.Marshal(fault)
	for _, change := range []struct{ from, to string }{
		{`"observedGeneration":4`, `"observedGeneration":5`},
		{`"generation":4`, `"generation":0`},
		{`"readiness":`, `"condition":"Forbidden","readiness":`},
		{`"readiness":{"generation":4,"observedGeneration":4},`, ""},
		{`"dispatch":"Applied"`, `"dispatch":"Unknown"`},
		{`"step":"runtime.provider.observe"`, `"step":"runtime.provider.create"`},
		{`"code":"DeploymentReplicaFailure"`, `"code":"DeploymentProgressDeadlineExceeded"`},
		{"The Deployment controller reported a replica failure for the observed generation.", "PRIVATE_RAW_CONDITION"},
		{fault.View().Failure.NextAction, "PRIVATE_DYNAMIC_ACTION"},
	} {
		mutated := bytes.Replace(wire, []byte(change.from), []byte(change.to), 1)
		if bytes.Equal(mutated, wire) {
			t.Fatalf("mutation fixture did not exercise %s", change.from)
		}
		if _, err := ParseFault(mutated); !errors.Is(err, ErrContext) {
			t.Fatal("injected readiness history accepted")
		}
	}
	old, _ := MapProvider(readinessContext(), &kube.Error{Code: kube.ChildUnavailable})
	oldWire, _ := json.Marshal(old)
	if bytes.Contains(oldWire, []byte(`"readiness"`)) {
		t.Fatal("legacy absent-readiness wire changed")
	}
	if _, err := ParseFault(oldWire); err != nil {
		t.Fatal("legacy provider fault no longer roundtrips")
	}
}
