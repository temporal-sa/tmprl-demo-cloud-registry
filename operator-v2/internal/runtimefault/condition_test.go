package runtimefault

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeinputs"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Synthetic error chaining models the provider's typed capability error plus
// native API cause. No HTTP calls, credentials or real mutation are involved.
func capabilityCause(code kube.Code, cause error) error {
	return errors.Join(&kube.Error{Code: code, Recovery: "PRIVATE_CAPABILITY_RECOVERY"}, cause)
}
func nativeConditionStatus(code int32, reason metav1.StatusReason) *apierrors.StatusError {
	return &apierrors.StatusError{ErrStatus: metav1.Status{Code: code, Reason: reason, Message: "PRIVATE_STATUS_MESSAGE token=PRIVATE_TOKEN", Details: &metav1.StatusDetails{Name: "PRIVATE_UNTRUSTED_TARGET", Kind: "PRIVATE_KIND", Group: "PRIVATE_GROUP", Causes: []metav1.StatusCause{{Type: "PRIVATE_REQUEST_ID", Message: "PRIVATE_CAUSE_BODY", Field: "PRIVATE_FIELD"}}}}}
}
func TestFiniteNativeConditionsPreserveCapabilityCodeAndIndependentDispatch(t *testing.T) {
	cases := []struct {
		name      string
		cause     error
		condition Condition
		sentence  string
	}{
		{"forbidden", nativeConditionStatus(403, metav1.StatusReasonForbidden), ConditionForbidden, "The Kubernetes API denied access."},
		{"unauthenticated", nativeConditionStatus(401, metav1.StatusReasonUnauthorized), ConditionUnauthenticated, "The Kubernetes API rejected authentication."},
		{"not-found", nativeConditionStatus(404, metav1.StatusReasonNotFound), ConditionNotFound, "The Kubernetes API reported the requested resource was not found."},
		{"deadline", context.DeadlineExceeded, ConditionDeadlineExceeded, "The API request exceeded its deadline."},
		{"canceled", context.Canceled, ConditionCanceled, "The API request was canceled."},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := inputContext(Observe)
			c.Dispatch = Applied
			c.NamespaceName = "owned-runtime-one"
			c.WriteID = "committed-create"
			f, err := MapProvider(c, capabilityCause(kube.ChildUnavailable, tc.cause))
			if err != nil {
				t.Fatal(err)
			}
			v := f.View()
			if v.Failure.Code != string(kube.ChildUnavailable) || v.Failure.Category != diagnostics.Unknown || v.Dispatch != Applied || v.Condition != tc.condition || !strings.Contains(v.Failure.Message, tc.sentence) || v.Failure.Provider.Code != "" || len(v.Failure.Causes) != 0 {
				t.Fatalf("condition replaced/leaked capability facts: %+v", v)
			}
			if v.Target.Name != "backend" || v.Target.NamespaceName != "owned-runtime-one" {
				t.Fatal("status details substituted untrusted target")
			}
			outcome, err := NewOutcome(identity(), "condition-event", 1, clock.Add(time.Second), Admitted, f)
			if err != nil {
				t.Fatal(err)
			}
			data, _ := json.Marshal(outcome)
			parsed, err := ParseOutcome(data)
			if err != nil || parsed.View().Fault.Condition != tc.condition || parsed.View().Fault.Dispatch != Applied {
				t.Fatal("strict condition roundtrip failed", err)
			}
			event, err := parsed.Event()
			if err != nil || !strings.Contains(event.Failure.Message, tc.sentence) || event.Failure.Code != string(kube.ChildUnavailable) || event.Retry.Authoritative || event.Retry.Status != diagnostics.RetryUnknown || event.Availability != nil {
				t.Fatal("existing UI event lost useful why or invented authority", err)
			}
			rejectCanonicalMessage(t, data, v.Failure.Message)
			old, _ := json.Marshal(string(tc.condition))
			bad, _ := json.Marshal("PRIVATE_DYNAMIC_CONDITION")
			if _, err := ParseOutcome(bytes.Replace(data, old, bad, 1)); !errors.Is(err, ErrContext) {
				t.Fatal("dynamic condition enum admitted")
			}
			// The known enum cannot be removed while retaining its augmented message.
			var changed map[string]any
			if json.Unmarshal(data, &changed) != nil {
				t.Fatal("test decode")
			}
			delete(changed["fault"].(map[string]any), "condition")
			wire, _ := json.Marshal(changed)
			if _, err := ParseOutcome(wire); !errors.Is(err, ErrContext) {
				t.Fatal("conditionless augmented text admitted")
			}
			checkPrivate(t, f)
			checkPrivate(t, outcome)
			checkPrivate(t, parsed)
		})
	}
}
func TestUnknownOrContradictoryStatusesDoNotGuessConditions(t *testing.T) {
	cases := []error{
		errors.New("PRIVATE_BODY 403 Forbidden DeadlineExceeded request canceled"),
		nativeConditionStatus(500, metav1.StatusReasonForbidden),
		nativeConditionStatus(403, metav1.StatusReasonNotFound),
		nativeConditionStatus(504, metav1.StatusReasonTimeout),
		apierrors.NewServerTimeout(schema.GroupResource{Resource: "PRIVATE_RESOURCE"}, "PRIVATE_ACTION", 1),
	}
	for _, cause := range cases {
		c := inputContext(Observe)
		c.Dispatch = Applied
		f, err := MapProvider(c, capabilityCause(kube.ChildUnavailable, cause))
		if err != nil || f.View().Condition != "" {
			t.Fatal("raw/unsupported status inferred finite condition", err)
		}
		wire, _ := json.Marshal(f)
		if bytes.Contains(wire, []byte(`"condition"`)) {
			t.Fatal("absent condition changed legacy wire")
		}
		parsed, err := ParseFault(wire)
		if err != nil || parsed.View().Condition != "" {
			t.Fatal("legacy conditionless roundtrip changed", err)
		}
		checkPrivate(t, f)
	}
	// A concrete response fact wins over incidental context state after it.
	c := inputContext(Observe)
	c.Dispatch = Applied
	f, err := MapProvider(c, capabilityCause(kube.ChildUnavailable, errors.Join(nativeConditionStatus(403, metav1.StatusReasonForbidden), context.Canceled)))
	if err != nil || f.View().Condition != ConditionForbidden {
		t.Fatal("context state replaced typed native response", err)
	}
}
func TestDeadlineDuringMutationCannotInferNoncommitOrRetry(t *testing.T) {
	c := inputContext(Create)
	fault, err := MapProvider(c, capabilityCause(kube.UnknownWriteOutcome, context.DeadlineExceeded))
	if err != nil {
		t.Fatal(err)
	}
	if fault.View().Failure.Code != string(kube.UnknownWriteOutcome) || fault.View().Dispatch != DispatchUnknown || fault.View().Condition != ConditionDeadlineExceeded || !strings.Contains(fault.View().Failure.NextAction, "does not establish mutation noncommit") {
		t.Fatal("deadline erased original mutation uncertainty")
	}
	outcome, err := NewOutcome(identity(), "deadline-event", 1, clock.Add(time.Second), Admitted, fault)
	if err != nil {
		t.Fatal(err)
	}
	event, err := outcome.Event()
	if err != nil || event.State != diagnostics.Waiting || event.Retry.Status != diagnostics.RetryUnknown || event.Retry.Authoritative {
		t.Fatal("deadline invented noncommit/retry policy", err)
	}
	c.Dispatch = Applied
	if _, err := MapProvider(c, capabilityCause(kube.UnknownWriteOutcome, context.DeadlineExceeded)); !errors.Is(err, ErrContext) {
		t.Fatal("typed unknown mutation accepted contradictory Applied fact")
	}
}
func TestConditionCannotEnterOtherFactoryStepsOrExistingPrimaryCause(t *testing.T) {
	input, err := MapInput(inputContext(InputResolve), runtimeinputs.ErrUnproven)
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := NewOutcome(identity(), "input-event", 1, clock.Add(time.Second), BeforeAdmission, input)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(outcome)
	needle := []byte(`"failure":`)
	injected := bytes.Replace(data, needle, []byte(`"condition":"Forbidden","failure":`), 1)
	if _, err := ParseOutcome(injected); !errors.Is(err, ErrContext) {
		t.Fatal("API condition injected into input factory")
	}
	projection, err := diagnostics.NewProjection(identity())
	if err != nil {
		t.Fatal(err)
	}
	first, _ := outcome.Event()
	projection, err = projection.Apply(first)
	if err != nil {
		t.Fatal(err)
	}
	c := inputContext(Observe)
	c.Dispatch = Applied
	later, err := MapProvider(c, capabilityCause(kube.ChildUnavailable, nativeConditionStatus(403, metav1.StatusReasonForbidden)))
	if err != nil {
		t.Fatal(err)
	}
	next, err := NewOutcome(identity(), "later-read", 2, clock.Add(2*time.Second), Admitted, later)
	if err != nil {
		t.Fatal(err)
	}
	event, _ := next.Event()
	projection, err = projection.Apply(event)
	if err != nil || projection.Failure.Code != "AcceptedInputUnproven" || strings.Contains(projection.Failure.Message, "API denied access") {
		t.Fatal("later condition replaced immutable primary cause", err)
	}
}

type nilCapabilityWrapper struct{ cause error }

func (nilCapabilityWrapper) Error() string   { return "PRIVATE_TYPED_NIL_WRAPPER" }
func (w nilCapabilityWrapper) Unwrap() error { return w.cause }
func TestTypedNilCapabilitiesDoNotTraverseSecondaryCause(t *testing.T) {
	var typedNil *kube.Error
	for _, cause := range []error{typedNil, nilCapabilityWrapper{cause: typedNil}} {
		c := inputContext(Observe)
		c.Dispatch = Applied
		fault, err := MapProvider(c, cause)
		if err != nil || fault.View().Failure.Code != "RuntimeProviderFailureUnclassified" || fault.View().Condition != "" || fault.View().Dispatch != Applied {
			t.Fatal("typed nil capability failed safe classification", err)
		}
		wire, _ := json.Marshal(fault)
		parsed, err := ParseFault(wire)
		if err != nil || parsed.View().Dispatch != Applied {
			t.Fatal("typed nil fallback failed strict roundtrip", err)
		}
		checkPrivate(t, fault)
	}
}
