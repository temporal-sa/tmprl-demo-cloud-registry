package runtimefault

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeinputs"
)

func TestConfirmedMissingInputStoreIsNotAnInvalidInputOrAttemptedWrite(t *testing.T) {
	for _, step := range []Step{InputRetain, InputResolve} {
		c := inputContext(step)
		c.NamespaceName = "owned-runtime-one"
		fault, err := MapInput(c, runtimeinputs.ErrNotConfigured)
		if err != nil {
			t.Fatal(err)
		}
		v := fault.View()
		if v.Failure.Code != "AcceptedInputStoreNotConfigured" || v.Failure.Category != diagnostics.Configuration || v.Dispatch != NotDispatched || v.InputPersistence.State != NotAttempted || !v.InputPersistence.ObservedAt.IsZero() {
			t.Fatalf("missing dependency misclassified: %+v", v)
		}
		phase := Admitted
		if step == InputResolve {
			phase = BeforeAdmission
		}
		outcome, err := NewOutcome(identity(), "missing-store", 1, clock.Add(time.Second), phase, fault)
		if err != nil {
			t.Fatal(err)
		}
		data, _ := json.Marshal(outcome)
		parsed, err := ParseOutcome(data)
		if err != nil || parsed.View().Fault.InputPersistence.State != NotAttempted {
			t.Fatal("missing store roundtrip failed", err)
		}
		event, err := parsed.Event()
		if err != nil || event.Failure.Code != "AcceptedInputStoreNotConfigured" || event.Retry.Status != diagnostics.RetryUnknown {
			t.Fatal("missing store event lost why", err)
		}
		rejectCanonicalMessage(t, data, v.Failure.Message)
		if step == InputRetain && v.Input != nil {
			t.Fatal("pre-Pending missing store invented input reference")
		}
	}
}
func TestLocalAuthorityDenialHasClosedReasonAndIndependentUndispatchedFact(t *testing.T) {
	c := inputContext(DispatchAuthorize)
	c.Dispatch = NotDispatched
	c.WriteID = "pending-write"
	cases := []struct {
		reason AuthorityReason
		code   string
	}{
		{AuthorityMismatch, "RuntimeAuthorityMismatch"}, {OwnerMismatch, "RuntimeOwnerMismatch"}, {EpochMismatch, "RuntimeEpochMismatch"}, {LeaseExpired, "RuntimeLeaseExpired"}, {ActorClosed, "RuntimeActorClosed"}, {StateChanged, "RuntimeStateChanged"}, {UnresolvedWrites, "RuntimeUnresolvedWrites"}, {AuthorityUnclassified, "RuntimeAuthorityUnclassified"},
	}
	if len(authorityDescriptions) != len(cases) {
		t.Fatal("authority contract changed without independent expected mapping")
	}
	for _, test := range cases {
		t.Run(string(test.reason), func(t *testing.T) {
			fault, err := MapAuthority(c, test.reason)
			if err != nil {
				t.Fatal(err)
			}
			if fault.View().Failure.Code != test.code || fault.View().Step != DispatchAuthorize || fault.View().Dispatch != NotDispatched || strings.Contains(fault.View().Failure.Message, "provider returned") {
				t.Fatal("local denial blamed uninvoked provider")
			}
			outcome, err := NewOutcome(identity(), "authority-event", 1, clock.Add(time.Second), BeforeAdmission, fault)
			if err != nil {
				t.Fatal(err)
			}
			data, _ := json.Marshal(outcome)
			parsed, err := ParseOutcome(data)
			if err != nil || parsed.View().Fault.Failure.Code != test.code {
				t.Fatal("authority reason did not roundtrip", err)
			}
			rejectCanonicalMessage(t, data, fault.View().Failure.Message)
			checkPrivate(t, outcome)
		})
	}
	for _, dispatch := range []Dispatch{"", DispatchUnknown, Applied, Rejected} {
		invalid := c
		invalid.Dispatch = dispatch
		if _, err := MapAuthority(invalid, LeaseExpired); !errors.Is(err, ErrContext) {
			t.Fatal("reason inferred undispatched without independent fact")
		}
	}
	if _, err := MapAuthority(c, AuthorityReason("PRIVATE_DYNAMIC_CODE")); !errors.Is(err, ErrContext) {
		t.Fatal("raw authority reason entered factory")
	}
	c.Step = Create
	if _, err := MapAuthority(c, LeaseExpired); !errors.Is(err, ErrContext) {
		t.Fatal("authority factory accepted provider step")
	}
	c.Step = DispatchAuthorize
	if _, err := MapProvider(c, errors.New("PRIVATE_RUNTIME_ERROR")); !errors.Is(err, ErrContext) {
		t.Fatal("raw runtime error passed through provider classifier at authority step")
	}
}
func TestKnownAppliedCreateWithUnconfirmedReceiptHasIndependentJournalFault(t *testing.T) {
	for _, kind := range []Kind{Deployment, Service} {
		c := inputContext(ReceiptAcknowledge)
		c.Kind = kind
		c.Purpose = strings.ToLower(string(kind))
		c.Dispatch = Applied
		c.WriteID = "create-write"
		c.NamespaceName = "owned-runtime-one"
		fault, err := MapReceiptAcknowledgement(c)
		if err != nil {
			t.Fatal(err)
		}
		v := fault.View()
		if v.Failure.Code != "CanonicalCreateReceiptUnconfirmed" || v.Failure.Category != diagnostics.Publication || v.Dispatch != Applied || v.JournalDelivery.State != DeliveryUnknown || v.JournalDelivery.Failure.Code != "JournalAcknowledgementUnknown" || v.DiagnosticDelivery.State != NotAttempted {
			t.Fatal("known applied fact conflated with unknown provider outcome")
		}
		outcome, err := NewOutcome(identity(), "receipt-ack", 1, clock.Add(time.Second), Admitted, fault)
		if err != nil {
			t.Fatal(err)
		}
		data, _ := json.Marshal(outcome)
		parsed, err := ParseOutcome(data)
		if err != nil {
			t.Fatal(err)
		}
		event, err := parsed.Event()
		if err != nil || event.State != diagnostics.Waiting || event.Failure.Code != "CanonicalCreateReceiptUnconfirmed" || event.Retry.Authoritative || event.Retry.Status != diagnostics.RetryUnknown || event.Availability != nil {
			t.Fatal("unconfirmed receipt invented provider failure/retry/route", err)
		}
		rejectCanonicalMessage(t, data, v.Failure.Message)
		checkPrivate(t, parsed)
		if _, err := NewOutcome(identity(), "receipt-ack", 1, clock.Add(time.Second), BeforeAdmission, fault); !errors.Is(err, ErrContext) {
			t.Fatal("pre-admission receipt commit fabricated authority")
		}
		for _, dispatch := range []Dispatch{"", DispatchUnknown, NotDispatched, Rejected} {
			bad := c
			bad.Dispatch = dispatch
			if _, err := MapReceiptAcknowledgement(bad); !errors.Is(err, ErrContext) {
				t.Fatal("receipt acknowledgement inferred Applied")
			}
		}
		bad := c
		bad.WriteID = ""
		if _, err := MapReceiptAcknowledgement(bad); !errors.Is(err, ErrContext) {
			t.Fatal("unconfirmed receipt lost exact write identity")
		}
	}
}
func rejectCanonicalMessage(t *testing.T, data []byte, message string) {
	t.Helper()
	old, _ := json.Marshal(message)
	injected, _ := json.Marshal("PRIVATE_CANONICAL_MESSAGE")
	if !bytes.Contains(data, old) {
		t.Fatal("canonical message target missing")
	}
	if _, err := ParseOutcome(bytes.Replace(data, old, injected, 1)); !errors.Is(err, ErrContext) {
		t.Fatal("dynamic canonical message accepted")
	}
}
