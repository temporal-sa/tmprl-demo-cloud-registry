package runtimefault

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeinputs"
)

var clock = time.Date(2026, 10, 30, 5, 0, 0, 0, time.UTC)

func inputContext(step Step) Context {
	c := Context{Step: step, Kind: Deployment, Purpose: "deployment", OperationID: "attempt-one", Name: "backend", NamespaceUID: "namespace-uid", Fingerprint: strings.Repeat("a", 64), OccurredAt: clock}
	if step == InputResolve {
		c.Input = &journal.InputReference{Kind: "DeploymentRender", UID: "render-" + strings.Repeat("b", 64), Version: "1", Digest: strings.Repeat("c", 64), Contract: journal.DeploymentInputContract}
	}
	return c
}

func serviceAccountContext(step Step) Context {
	c := inputContext(step)
	c.Kind, c.Purpose = ServiceAccount, "service-account"
	c.NamespaceName = "candidate-runtime"
	c.WriteID = "write-sa-one"
	if step == InputResolve {
		c.Input = &journal.InputReference{Kind: "ServiceAccountRender", UID: "render-" + strings.Repeat("b", 64), Version: "1", Digest: strings.Repeat("c", 64), Contract: journal.ServiceAccountInputContract}
	}
	return c
}

func TestServiceAccountFiniteProviderFaultsSurviveCanonicalHistory(t *testing.T) {
	for _, tc := range []struct {
		code kube.Code
		step Step
	}{
		{kube.ServiceAccountUnsupportedSpec, Create},
		{kube.ServiceAccountUnsupportedSpec, Observe},
		{kube.ServiceAccountSpecMismatch, Create},
		{kube.ServiceAccountSpecMismatch, Observe},
		{kube.ServiceAccountIntentUnverified, Create},
	} {
		t.Run(string(tc.code)+string(tc.step), func(t *testing.T) {
			c := serviceAccountContext(tc.step)
			c.Dispatch = NotDispatched
			if tc.step == Observe {
				c.Dispatch = Applied
			}
			fault, err := MapProvider(c, &kube.Error{Code: tc.code, Recovery: "PRIVATE_PROVIDER_RECOVERY"})
			if err != nil {
				t.Fatal(err)
			}
			v := fault.View()
			if v.Failure.Code != string(tc.code) || v.Target.Kind != ServiceAccount || v.Target.Purpose != "service-account" || v.Dispatch != c.Dispatch || !strings.Contains(v.Failure.Message, "ServiceAccount") || v.Failure.NextAction == "" {
				t.Fatal("finite SA cause/context/dispatch lost")
			}
			history, err := NewOutcome(identity(), "sa-event", 1, clock.Add(time.Second), Admitted, fault)
			if err != nil {
				t.Fatal(err)
			}
			wire, err := json.Marshal(history)
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := ParseOutcome(wire)
			if err != nil {
				t.Fatal(err)
			}
			parsedWire, _ := parsed.MarshalJSON()
			if !bytes.Equal(wire, parsedWire) {
				t.Fatal("SA history canonical bytes changed")
			}
			event, err := parsed.Event()
			if err != nil || event.Failure == nil || event.Failure.Code != string(tc.code) || event.Retry == nil || event.Retry.Status != diagnostics.RetryUnknown || event.Retry.Authoritative || event.Availability != nil {
				t.Fatal("SA event changed cause or invented execution/health/retry")
			}
			rejectCanonicalMessage(t, wire, v.Failure.Message)
			checkPrivate(t, fault)
			checkPrivate(t, parsed)
		})
	}
}

func TestServiceAccountFaultFactoryBoundariesAndExactInputKind(t *testing.T) {
	for _, step := range []Step{InputRetain, InputResolve, ReceiptAcknowledge, DispatchAuthorize} {
		c := serviceAccountContext(step)
		var fault Fault
		var err error
		switch step {
		case InputRetain:
			fault, err = MapInput(c, runtimeinputs.ErrNotConfigured)
		case InputResolve:
			fault, err = MapInput(c, runtimeinputs.ErrUnproven)
		case ReceiptAcknowledge:
			c.Dispatch = Applied
			fault, err = MapReceiptAcknowledgement(c)
		case DispatchAuthorize:
			c.Dispatch = NotDispatched
			fault, err = MapAuthority(c, LeaseExpired)
		}
		if err != nil {
			t.Fatal("valid SA factory context rejected", step, err)
		}
		outcome, err := NewOutcome(identity(), "sa-factory-event", 1, clock.Add(time.Second), Admitted, fault)
		if err != nil {
			t.Fatal(err)
		}
		wire, _ := json.Marshal(outcome)
		parsed, err := ParseOutcome(wire)
		if err != nil || parsed.View().Fault.Target.Kind != ServiceAccount {
			t.Fatal("SA factory history lost finite context", err)
		}
		if step == InputResolve && (parsed.View().Fault.Input == nil || *parsed.View().Fault.Input != *c.Input) {
			t.Fatal("SA exact private input reference lost")
		}
		if step == ReceiptAcknowledge {
			event, _ := parsed.Event()
			if event.State != diagnostics.Waiting || parsed.View().Fault.Dispatch != Applied || parsed.View().Fault.JournalDelivery.State != DeliveryUnknown {
				t.Fatal("SA known create lost Applied/independent journal uncertainty")
			}
		}
	}
	c := serviceAccountContext(InputResolve)
	c.Input.Contract = journal.DeploymentInputContract
	if _, err := MapInput(c, runtimeinputs.ErrUnproven); !errors.Is(err, ErrContext) {
		t.Fatal("foreign input contract entered SA context")
	}
	c = serviceAccountContext(Create)
	c.Name = strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + "." + strings.Repeat("c", 63) + "." + strings.Repeat("d", 61)
	if _, err := MapProvider(c, &kube.Error{Code: kube.OwnershipRejected}); err != nil {
		t.Fatal("valid253-byte SA DNS subdomain rejected", err)
	}
	c.Name += "e"
	if _, err := MapProvider(c, &kube.Error{Code: kube.OwnershipRejected}); !errors.Is(err, ErrContext) {
		t.Fatal("oversized SA name accepted")
	}
}

func TestServiceAccountFaultRejectsCrossKindStepsAndReadiness(t *testing.T) {
	for _, change := range []func(*Context){
		func(c *Context) { c.Kind = Deployment; c.Purpose = "deployment" },
		func(c *Context) { c.Purpose = "service" },
		func(c *Context) { c.Step = Delete },
		func(c *Context) { c.Step = Finalize },
	} {
		c := serviceAccountContext(Create)
		change(&c)
		if _, err := MapProvider(c, &kube.Error{Code: kube.ServiceAccountUnsupportedSpec}); !errors.Is(err, ErrContext) {
			t.Fatal("SA code escaped finite kind/step scope")
		}
	}
	c := serviceAccountContext(Observe)
	if _, err := MapProvider(c, &kube.Error{Code: kube.ServiceAccountIntentUnverified}); !errors.Is(err, ErrContext) {
		t.Fatal("Pending create verification code entered SA observe")
	}
	c.Dispatch = Applied
	if _, err := MapDeploymentReadiness(c, kube.DeploymentReadiness{State: "Failed", Code: "DeploymentReplicaFailure", Generation: 1, ObservedGeneration: 1}); !errors.Is(err, ErrContext) {
		t.Fatal("Deployment readiness entered SA context")
	}
	fault, err := MapProvider(c, &kube.Error{Code: kube.ServiceAccountSpecMismatch})
	if err != nil {
		t.Fatal(err)
	}
	v := fault.View()
	v.Readiness = &ReadinessEvidence{Generation: 1, ObservedGeneration: 1}
	if _, err := faultFromView(v); !errors.Is(err, ErrContext) {
		t.Fatal("strict parser accepted injected SA readiness")
	}
}
func identity() diagnostics.Identity {
	return diagnostics.Identity{Environment: "lab", Installation: "local", ProjectUID: "project-uid", ProjectName: "example", OperationID: "attempt-one", ReconcileKey: "accepted-one", Generation: 1}
}
func TestInputFaultMappingsPreserveActionableFactsWithoutRawCause(t *testing.T) {
	for _, tc := range []struct {
		step     Step
		err      error
		code     string
		delivery Delivery
	}{
		{InputRetain, runtimeinputs.ErrUnproven, "AcceptedInputUnproven", DeliveryUnknown},
		{InputResolve, runtimeinputs.ErrUnproven, "AcceptedInputUnproven", NotAttempted},
		{InputRetain, runtimeinputs.ErrCapacity, "AcceptedInputCapacityReached", DeliveryRejected},
		{InputRetain, runtimeinputs.ErrUncertain, "AcceptedInputRetentionUncertain", DeliveryUnknown},
		{InputResolve, runtimeinputs.ErrUncertain, "AcceptedInputVerificationUncertain", DeliveryUnknown},
		{InputRetain, runtimeinputs.ErrInvalid, "AcceptedInputInvalid", DeliveryRejected},
		{InputRetain, errors.New("PRIVATE_PATH /home/user/.aws SECRET_BODY"), "AcceptedInputUnavailable", DeliveryUnknown},
	} {
		t.Run(string(tc.step)+tc.code, func(t *testing.T) {
			c := inputContext(tc.step)
			raw := fmt.Errorf("PRIVATE_PATH PRIVATE_TOKEN: %w", tc.err)
			f, err := MapInput(c, raw)
			if err != nil {
				t.Fatal(err)
			}
			v := f.View()
			if v.Failure.Code != tc.code || v.Failure.Step != string(tc.step) || v.Failure.NextAction == "" || v.Dispatch != NotDispatched || v.InputPersistence.State != tc.delivery || v.WriteID != "" || v.Target.Name != "backend" {
				t.Fatalf("incorrect actionable mapping: %+v", v)
			}
			if tc.step == InputRetain && v.Input != nil {
				t.Fatal("invented pre-Pending input reference")
			}
			if tc.step == InputResolve && (v.Input == nil || v.Input.UID != c.Input.UID) {
				t.Fatal("lost exact accepted input reference")
			}
			if errors.Is(f, raw) {
				t.Fatal("raw error retained through Unwrap")
			}
			checkPrivate(t, f)
		})
	}
}
func TestFaultContextRejectsCrossKindStepPurposeAndMissingInput(t *testing.T) {
	for _, change := range []func(*Context){func(c *Context) { c.Purpose = "service" }, func(c *Context) { c.Kind = Namespace }, func(c *Context) { c.Name = "PRIVATE_PATH/escape" }, func(c *Context) { c.Fingerprint = "not-a-digest" }, func(c *Context) { c.OperationID = "PRIVATE_TOKEN with spaces" }, func(c *Context) { c.Dispatch = Applied }, func(c *Context) {
		c.Input = &journal.InputReference{Kind: "ServiceRender", UID: "render-" + strings.Repeat("b", 64), Version: "1", Digest: strings.Repeat("c", 64), Contract: journal.ServiceInputContract}
	}} {
		c := inputContext(InputRetain)
		change(&c)
		_, err := MapInput(c, runtimeinputs.ErrUnproven)
		if !errors.Is(err, ErrContext) {
			t.Fatal("invalid context admitted")
		}
	}
	c := inputContext(InputResolve)
	c.Input = nil
	if _, err := MapInput(c, runtimeinputs.ErrUnproven); !errors.Is(err, ErrContext) {
		t.Fatal("Resolve accepted no exact input reference")
	}
	c = inputContext(InputResolve)
	if _, err := MapInput(c, runtimeinputs.ErrCapacity); !errors.Is(err, ErrContext) {
		t.Fatal("impossible Resolve capacity mapping admitted")
	}
}
func TestProviderFaultExactCodesDoNotPropagateProviderText(t *testing.T) {
	c := inputContext(Create)
	c.WriteID = "write-one"
	for _, code := range []kube.Code{kube.DeploymentSpecMismatch, kube.DeploymentIntentUnverified, kube.UnknownWriteOutcome, kube.OwnershipRejected} {
		f, err := MapProvider(c, fmt.Errorf("PRIVATE_PROVIDER_BODY: %w", &kube.Error{Code: code, Recovery: "PRIVATE_RECOVERY token=PRIVATE_TOKEN"}))
		if err != nil {
			t.Fatal(err)
		}
		if f.View().Failure.Code != string(code) || f.View().Failure.Provider.Code != "" {
			t.Fatal("typed code missing or raw provider ref propagated")
		}
		checkPrivate(t, f)
	}
	var nilProvider *kube.Error
	if fallback, err := MapProvider(c, nilProvider); err != nil || fallback.View().Failure.Code != "RuntimeProviderFailureUnclassified" {
		t.Fatal("typed nil provider error was not safely classified", err)
	}
	f, err := MapProvider(c, &kube.Error{Code: kube.Code("PRIVATE_UNREVIEWED_CODE"), Recovery: "PRIVATE_RECOVERY"})
	if err != nil || f.View().Failure.Code != "RuntimeProviderFailureUnclassified" || f.View().Dispatch != DispatchUnknown {
		t.Fatal("unknown provider code not safely classified", err)
	}
	checkPrivate(t, f)
	if _, err := MapProvider(c, &kube.Error{Code: kube.ServiceSpecMismatch}); !errors.Is(err, ErrContext) {
		t.Fatal("Service code entered Deployment context")
	}
	known := c
	known.Dispatch = NotDispatched
	preserved, err := MapProvider(known, errors.New("PRIVATE_UNCLASSIFIED_PROVIDER_BODY"))
	if err != nil || preserved.View().Dispatch != NotDispatched {
		t.Fatal("unknown error erased independent undispatched fact", err)
	}
	history, err := NewOutcome(identity(), "undispatched-event", 1, clock.Add(time.Second), Admitted, preserved)
	if err != nil {
		t.Fatal(err)
	}
	wire, _ := json.Marshal(history)
	parsed, err := ParseOutcome(wire)
	if err != nil || parsed.View().Fault.Dispatch != NotDispatched {
		t.Fatal("history roundtrip erased independent dispatch fact", err)
	}
	c.Step = Observe
	if _, err := MapProvider(c, &kube.Error{Code: kube.UnknownWriteOutcome}); !errors.Is(err, ErrContext) {
		t.Fatal("mutation uncertainty entered read-only step")
	}
	c.Step = Create
	c.Dispatch = Applied
	if _, err := MapProvider(c, &kube.Error{Code: kube.UnknownWriteOutcome}); !errors.Is(err, ErrContext) {
		t.Fatal("contradictory dispatch certainty admitted")
	}
}
func TestDeliveryUncertaintyCannotReplaceCauseOrMutateEarlierFault(t *testing.T) {
	f, err := MapInput(inputContext(InputResolve), runtimeinputs.ErrUnproven)
	if err != nil {
		t.Fatal(err)
	}
	original := f.View()
	next, err := f.WithDelivery(Journal, DeliveryUnknown, clock.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	next, err = next.WithDelivery(Diagnostic, DeliveryRejected, clock.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(next.View().Failure, original.Failure) || f.View().JournalDelivery.State != NotAttempted || next.View().JournalDelivery.Failure.Code != "JournalAcknowledgementUnknown" || next.View().DiagnosticDelivery.Failure.Code != "DiagnosticDeliveryRejected" || !next.View().JournalDelivery.ObservedAt.Equal(clock.Add(time.Second)) {
		t.Fatal("cause/delivery causality or clocks lost")
	}
	v := next.View()
	v.Failure.Resources[0].ID = "mutated"
	v.Input.UID = "mutated"
	v.JournalDelivery.Failure.Message = "mutated"
	if next.View().Failure.Resources[0].ID == "mutated" || next.View().Input.UID == "mutated" || next.View().JournalDelivery.Failure.Message == "mutated" {
		t.Fatal("View aliases immutable fault")
	}
	acknowledged, err := next.WithDelivery(Journal, Acknowledged, clock.Add(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := acknowledged.WithDelivery(Journal, DeliveryUnknown, clock.Add(4*time.Second)); !errors.Is(err, ErrContext) {
		t.Fatal("late unknown erased ACK")
	}
	if _, err := next.WithDelivery(Diagnostic, DeliveryUnknown, clock.Add(time.Second)); !errors.Is(err, ErrContext) {
		t.Fatal("out-of-order delivery clock accepted")
	}
	checkPrivate(t, next)
}
func TestOutcomeEventPreservesRootAndSeparatesCleanup(t *testing.T) {
	fault, err := MapInput(inputContext(InputRetain), runtimeinputs.ErrCapacity)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := NewOutcome(identity(), "event-one", 1, clock.Add(time.Second), Admitted, fault)
	if err != nil {
		t.Fatal(err)
	}
	event, err := outcome.Event()
	if err != nil {
		t.Fatal(err)
	}
	if event.Failure == nil || event.Failure.Code != "AcceptedInputCapacityReached" || event.Retry == nil || event.Retry.Status != diagnostics.RetryUnknown || event.Retry.Authoritative || event.Availability != nil {
		t.Fatal("event invented retry/availability")
	}
	projection, err := diagnostics.NewProjection(identity())
	if err != nil {
		t.Fatal(err)
	}
	projection, err = projection.Apply(event)
	if err != nil {
		t.Fatal(err)
	}
	c := Context{Step: Delete, Kind: Namespace, Purpose: "runtime-namespace", OperationID: "attempt-one", WriteID: "delete-one", Name: "runtime-one", NamespaceUID: "namespace-uid", Fingerprint: strings.Repeat("a", 64), OccurredAt: clock.Add(2 * time.Second)}
	cleanup, err := MapProvider(c, &kube.Error{Code: kube.UnknownWriteOutcome, Recovery: "PRIVATE_PROVIDER_BODY"})
	if err != nil {
		t.Fatal(err)
	}
	cleanupOutcome, err := NewOutcome(identity(), "cleanup-event", 2, clock.Add(3*time.Second), Admitted, cleanup)
	if err != nil {
		t.Fatal(err)
	}
	cleanupEvent, err := cleanupOutcome.Event()
	if err != nil {
		t.Fatal(err)
	}
	projection, err = projection.Apply(cleanupEvent)
	if err != nil {
		t.Fatal(err)
	}
	if projection.Failure.Code != "AcceptedInputCapacityReached" || len(projection.Recovery) != 1 || projection.Recovery[0].Failure.Code != string(kube.UnknownWriteOutcome) || projection.Recovery[0].State != diagnostics.OutcomeRetained {
		t.Fatal("cleanup replaced original cause")
	}
	checkPrivate(t, outcome)
	checkPrivate(t, cleanupOutcome)
}
func TestOutcomeBeforeAdmissionIsDataNotJournalAcknowledgement(t *testing.T) {
	fault, err := MapInput(inputContext(InputResolve), runtimeinputs.ErrUnproven)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := NewOutcome(identity(), "preflight-event", 1, clock.Add(time.Second), BeforeAdmission, fault)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(outcome)
	if err != nil || len(wire) > MaxJSONBytes || strings.Contains(string(wire), "writerEpoch") || strings.Contains(string(wire), "handoff") || strings.Contains(string(wire), "admissionSession") {
		t.Fatal("outcome acquired authority fields", err)
	}
	f, err := fault.WithDelivery(Journal, Acknowledged, clock.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewOutcome(identity(), "preflight-event", 1, clock.Add(2*time.Second), BeforeAdmission, f); !errors.Is(err, ErrContext) {
		t.Fatal("pre-admission envelope claims journal ACK")
	}
	wrong := identity()
	wrong.OperationID = "other"
	if _, err := NewOutcome(wrong, "event", 1, clock.Add(time.Second), Admitted, fault); !errors.Is(err, ErrContext) {
		t.Fatal("foreign operation envelope admitted")
	}
	c := inputContext(InputResolve)
	f, err = MapInput(c, runtimeinputs.ErrUnproven)
	if err != nil {
		t.Fatal(err)
	}
	c.Input.UID = "changed-after-construction"
	if f.View().Input.UID == c.Input.UID {
		t.Fatal("constructor retained input alias")
	}
	copy := outcome.View()
	copy.Fault.Input.UID = "changed-view"
	if outcome.View().Fault.Input.UID == "changed-view" {
		t.Fatal("outcome view aliases input")
	}
}
func checkPrivate(t *testing.T, value any) {
	t.Helper()
	for _, verb := range []string{"%v", "%+v", "%#v", "%p", "%x", "%q", "%d"} {
		out := fmt.Sprintf(verb, value)
		if strings.Contains(out, "PRIVATE_") || strings.Contains(out, "50524956415445") || strings.Contains(out, "SECRET_BODY") {
			t.Fatalf("unsafe fmt %s", verb)
		}
	}
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "PRIVATE_") || strings.Contains(string(data), "SECRET_BODY") {
		t.Fatal("raw cause entered JSON")
	}
}

func TestRuntimeNamesFollowPinnedKindsAndDeliveryCannotEraseAttempt(t *testing.T) {
	for _, kind := range []Kind{Deployment, ConfigMap} {
		c := inputContext(Create)
		c.Kind = kind
		c.Purpose = strings.ToLower(string(kind))
		c.Name = strings.Repeat("a", 63) + "." + strings.Repeat("b", 63) + ".workload"
		if _, err := MapProvider(c, &kube.Error{Code: kube.OwnershipRejected}); err != nil {
			t.Fatal("valid DNS subdomain rejected", kind, err)
		}
	}
	for _, kind := range []Kind{Namespace, Service} {
		c := inputContext(Create)
		c.Kind = kind
		c.Purpose = strings.ToLower(string(kind))
		if kind == Namespace {
			c.Purpose = "runtime-namespace"
		}
		c.Name = "dotted.name"
		if _, err := MapProvider(c, &kube.Error{Code: kube.OwnershipRejected}); !errors.Is(err, ErrContext) {
			t.Fatal("unsupported label-kind dotted name admitted")
		}
	}
	f, err := MapInput(inputContext(InputRetain), runtimeinputs.ErrCapacity)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []Delivery{DeliveryRejected, DeliveryUnknown} {
		attempted, err := f.WithDelivery(Journal, state, clock.Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = attempted.WithDelivery(Journal, NotAttempted, clock.Add(2*time.Second)); !errors.Is(err, ErrContext) {
			t.Fatal("known delivery attempt erased")
		}
	}
}
func TestSafeOutcomeJSONRoundtripReconstructsFiniteEventOnly(t *testing.T) {
	fault, err := MapInput(inputContext(InputResolve), runtimeinputs.ErrUnproven)
	if err != nil {
		t.Fatal(err)
	}
	fault, err = fault.WithDelivery(Diagnostic, DeliveryUnknown, clock.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	original, err := NewOutcome(identity(), "roundtrip-event", 1, clock.Add(2*time.Second), BeforeAdmission, fault)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseOutcome(data)
	if err != nil {
		t.Fatal(err)
	}
	var sdkDecoded Outcome
	if err = json.Unmarshal(data, &sdkDecoded); err != nil {
		t.Fatal("standard JSON activity decode lost opaque value", err)
	}
	event, err := sdkDecoded.Event()
	if err != nil || event.Failure == nil || event.Failure.Code != "AcceptedInputUnproven" || event.Retry.Status != diagnostics.RetryUnknown {
		t.Fatal("roundtrip lost finite actionable event", err)
	}
	// Mutate original CANONICAL bytes directly: rejection must exercise finite
	// message/code reconstruction, not merely a map re-marshal key-order change.
	for _, pair := range [][2]string{{fault.View().Failure.Message, "PRIVATE_CANONICAL_MESSAGE"}, {fault.View().Failure.Code, "PRIVATE_CANONICAL_CODE"}, {fault.View().Failure.NextAction, "PRIVATE_CANONICAL_ACTION"}, {fault.View().DiagnosticDelivery.Failure.Message, "PRIVATE_CANONICAL_DELIVERY"}} {
		old, _ := json.Marshal(pair[0])
		replacement, _ := json.Marshal(pair[1])
		if !bytes.Contains(data, old) {
			t.Fatal("canonical mutation target missing")
		}
		changed := bytes.Replace(data, old, replacement, 1)
		if _, err := ParseOutcome(changed); !errors.Is(err, ErrContext) {
			t.Fatal("canonical byte injection accepted")
		}
	}
	if !reflect.DeepEqual(parsed.View(), original.View()) {
		t.Fatal("roundtrip changed outcome facts")
	}
	faultBytes, _ := json.Marshal(fault)
	var decodedFault Fault
	if err = json.Unmarshal(faultBytes, &decodedFault); err != nil || !reflect.DeepEqual(decodedFault.View(), fault.View()) {
		t.Fatal("Fault JSON roundtrip failed", err)
	}
	for _, change := range []func(map[string]any){
		func(v map[string]any) { v["schemaVersion"] = 2 },
		func(v map[string]any) { v["unknownField"] = "PRIVATE_INJECTED" },
		func(v map[string]any) {
			v["fault"].(map[string]any)["failure"].(map[string]any)["message"] = "PRIVATE_INJECTED"
		},
		func(v map[string]any) {
			v["fault"].(map[string]any)["failure"].(map[string]any)["code"] = "PRIVATE_CODE"
		},
		func(v map[string]any) {
			v["fault"].(map[string]any)["failure"].(map[string]any)["nextAction"] = "PRIVATE_RECOVERY"
		},
		func(v map[string]any) {
			v["fault"].(map[string]any)["failure"].(map[string]any)["causes"] = []any{map[string]any{"type": "provider", "message": "PRIVATE_CAUSE"}}
		},
		func(v map[string]any) {
			v["fault"].(map[string]any)["diagnosticDelivery"].(map[string]any)["failure"].(map[string]any)["message"] = "PRIVATE_DELIVERY"
		},
	} {
		var value map[string]any
		if json.Unmarshal(data, &value) != nil {
			t.Fatal("test fixture decode")
		}
		change(value)
		wire, _ := json.Marshal(value)
		if _, err := ParseOutcome(wire); !errors.Is(err, ErrContext) {
			t.Fatal("injected wire accepted")
		}
	}
	duplicate := strings.Replace(string(data), `"schemaVersion":1`, `"schemaVersion":1,"schemaVersion":1`, 1)
	if _, err := ParseOutcome([]byte(duplicate)); !errors.Is(err, ErrContext) {
		t.Fatal("duplicate wire keys accepted")
	}
	alias := strings.Replace(string(data), `"schemaVersion":1`, `"SchemaVersion":1`, 1)
	if _, err := ParseOutcome([]byte(alias)); !errors.Is(err, ErrContext) {
		t.Fatal("case-aliased key accepted")
	}
	if _, err := ParseOutcome([]byte(strings.Repeat(" ", MaxJSONBytes) + string(data))); !errors.Is(err, ErrContext) {
		t.Fatal("wire byte cap failed")
	}
	// Parsed ACK remains asserted history data, not a callback or live grant.
	ack, err := fault.WithDelivery(Journal, Acknowledged, clock.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := NewOutcome(identity(), "ack-event", 2, clock.Add(3*time.Second), Admitted, ack)
	if err != nil {
		t.Fatal(err)
	}
	wire, _ := json.Marshal(admitted)
	recovered, err := ParseOutcome(wire)
	if err != nil || recovered.View().Fault.JournalDelivery.State != Acknowledged {
		t.Fatal("known history ACK did not roundtrip", err)
	}
	checkPrivate(t, recovered)
}

func TestOptionalNamespaceNameIsBoundedLocatingMetadata(t *testing.T) {
	c := inputContext(InputResolve)
	legacy, err := MapInput(c, runtimeinputs.ErrUnproven)
	if err != nil {
		t.Fatal(err)
	}
	legacyWire, _ := json.Marshal(legacy)
	if bytes.Contains(legacyWire, []byte("namespaceName")) {
		t.Fatal("legacy canonical wire changed")
	}
	c.NamespaceName = "owned-runtime-one"
	f, err := MapInput(c, runtimeinputs.ErrUnproven)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := NewOutcome(identity(), "namespace-event", 1, clock.Add(time.Second), BeforeAdmission, f)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(outcome)
	parsed, err := ParseOutcome(data)
	if err != nil || parsed.View().Fault.Target.NamespaceName != c.NamespaceName {
		t.Fatal("namespace locating metadata lost", err)
	}
	c.NamespaceName = "invalid.namespace"
	if _, err := MapInput(c, runtimeinputs.ErrUnproven); !errors.Is(err, ErrContext) {
		t.Fatal("unbounded namespace alias admitted")
	}
}
