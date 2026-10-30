// Package runtimefault describes actual runtime failures without retaining raw
// causes. It has no storage, provider calls, scheduler, admission or retry loop.
package runtimefault

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
)

var ErrContext = errors.New("invalid safe runtime fault context")

const MaxJSONBytes = 8 * 1024

type Step string

const (
	InputRetain        Step = "runtime.inputs.retain"
	InputResolve       Step = "runtime.inputs.resolve"
	Create             Step = "runtime.provider.create"
	Observe            Step = "runtime.provider.observe"
	Delete             Step = "runtime.namespace.delete"
	Finalize           Step = "runtime.namespace.finalize"
	DispatchAuthorize  Step = "runtime.dispatch.authorize"
	ReceiptAcknowledge Step = "runtime.journal.commit_receipt"
)

type Kind string

const (
	Namespace      Kind = "Namespace"
	ConfigMap      Kind = "ConfigMap"
	Deployment     Kind = "Deployment"
	Service        Kind = "Service"
	ServiceAccount Kind = "ServiceAccount"
)

type Dispatch string

const (
	DispatchUnknown Dispatch = "Unknown"
	NotDispatched   Dispatch = "NotDispatched"
	Rejected        Dispatch = "Rejected"
	Applied         Dispatch = "Applied"
)

type Delivery string

const (
	NotAttempted     Delivery = "NotAttempted"
	Acknowledged     Delivery = "Acknowledged"
	DeliveryRejected Delivery = "Rejected"
	DeliveryUnknown  Delivery = "Unknown"
)

type Sink string

const (
	Journal    Sink = "Journal"
	Diagnostic Sink = "Diagnostic"
)

// Context comes from accepted actor state, never parsed provider error text.
// It is execution input, not authority or a durable receipt. Dispatch certainty
// is an independently observed fact; a provider code alone cannot tell whether
// a denied request was dispatched (for example a conflict after a POST).
type Context struct {
	Step                                                                 Step
	Kind                                                                 Kind
	Purpose                                                              string
	OperationID, WriteID, Name, NamespaceUID, NamespaceName, Fingerprint string
	Input                                                                *journal.InputReference
	OccurredAt                                                           time.Time
	Dispatch                                                             Dispatch
}

func (Context) MarshalJSON() ([]byte, error) { return json.Marshal("execution-only") }
func (Context) String() string               { return "RuntimeFaultContext{execution-only}" }
func (c Context) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, c.String()) }

var stableID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,127}$`)
var label = regexp.MustCompile(`^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$`)
var digest = regexp.MustCompile(`^[a-f0-9]{64}$`)

func validName(kind Kind, name string) bool {
	if kind == Namespace || kind == Service {
		return label.MatchString(name)
	}
	if kind != Deployment && kind != ConfigMap && kind != ServiceAccount || len(name) > 253 {
		return false
	}
	for _, part := range strings.Split(name, ".") {
		if !label.MatchString(part) {
			return false
		}
	}
	return true
}

func validContext(c Context) bool {
	purpose := map[Kind]string{Namespace: "runtime-namespace", ConfigMap: "configmap", Deployment: "deployment", Service: "service", ServiceAccount: "service-account"}[c.Kind]
	if c.NamespaceName != "" && (!label.MatchString(c.NamespaceName) || c.Kind == Namespace && c.NamespaceName != c.Name) {
		return false
	}
	if purpose == "" || c.Purpose != purpose || !stableID.MatchString(c.OperationID) || c.WriteID != "" && !stableID.MatchString(c.WriteID) || c.OccurredAt.IsZero() || !digest.MatchString(c.Fingerprint) || !validName(c.Kind, c.Name) || c.Kind != Namespace && !stableID.MatchString(c.NamespaceUID) || c.Kind == Namespace && c.NamespaceUID != "" && !stableID.MatchString(c.NamespaceUID) {
		return false
	}
	switch c.Step {
	case InputRetain, InputResolve:
		if c.Kind != Deployment && c.Kind != Service && c.Kind != ServiceAccount {
			return false
		}
	case ReceiptAcknowledge:
		if c.Kind != Deployment && c.Kind != Service && c.Kind != ServiceAccount {
			return false
		}
	case Create, Observe, DispatchAuthorize:
	case Delete, Finalize:
		if c.Kind != Namespace {
			return false
		}
	default:
		return false
	}
	if c.Step == InputResolve && c.Input == nil {
		return false
	}
	if c.Input != nil && !c.Input.MatchesKind(string(c.Kind)) {
		return false
	}
	return c.Dispatch == "" || c.Dispatch == DispatchUnknown || c.Dispatch == NotDispatched || c.Dispatch == Rejected || c.Dispatch == Applied
}
func cloneContext(c Context) Context {
	if c.Input != nil {
		r := *c.Input
		c.Input = &r
	}
	return c
}

type Target struct {
	Kind          Kind   `json:"kind"`
	Purpose       string `json:"purpose"`
	Name          string `json:"name"`
	NamespaceUID  string `json:"namespaceUID,omitempty"`
	NamespaceName string `json:"namespaceName,omitempty"`
	Fingerprint   string `json:"fingerprint"`
}
type DeliveryReport struct {
	State      Delivery                   `json:"state"`
	ObservedAt time.Time                  `json:"observedAt"`
	Failure    *diagnostics.DomainFailure `json:"failure,omitempty"`
}

type View struct {
	SchemaVersion      int                       `json:"schemaVersion"`
	OperationID        string                    `json:"operationID"`
	WriteID            string                    `json:"writeID,omitempty"`
	Step               Step                      `json:"step"`
	Target             Target                    `json:"target"`
	Input              *journal.InputReference   `json:"input,omitempty"`
	Condition          Condition                 `json:"condition,omitempty"`
	Readiness          *ReadinessEvidence        `json:"readiness,omitempty"`
	Failure            diagnostics.DomainFailure `json:"failure"`
	Dispatch           Dispatch                  `json:"dispatch"`
	InputPersistence   DeliveryReport            `json:"inputPersistence"`
	JournalDelivery    DeliveryReport            `json:"journalDelivery"`
	DiagnosticDelivery DeliveryReport            `json:"diagnosticDelivery"`
}

// Fault's private pointer prevents fmt fallback verbs from exposing causes or
// caller inputs. No raw error is stored, wrapped or accessible through Unwrap.
type Fault struct{ private *View }

func (f Fault) View() View {
	if f.private == nil {
		return View{SchemaVersion: 1, Dispatch: DispatchUnknown, InputPersistence: DeliveryReport{State: DeliveryUnknown}, JournalDelivery: DeliveryReport{State: DeliveryUnknown}, DiagnosticDelivery: DeliveryReport{State: DeliveryUnknown}}
	}
	v := *f.private
	if v.Readiness != nil {
		evidence := *v.Readiness
		v.Readiness = &evidence
	}
	v.Failure = diagnostics.SafeFailure(v.Failure)
	for _, report := range []*DeliveryReport{&v.InputPersistence, &v.JournalDelivery, &v.DiagnosticDelivery} {
		if report.Failure != nil {
			safe := diagnostics.SafeFailure(*report.Failure)
			report.Failure = &safe
		}
	}
	if v.Input != nil {
		r := *v.Input
		v.Input = &r
	}
	return v
}
func (f Fault) MarshalJSON() ([]byte, error) {
	data, err := json.Marshal(f.View())
	if err != nil || len(data) > MaxJSONBytes {
		return nil, ErrContext
	}
	return data, nil
}
func (f Fault) String() string { data, _ := f.MarshalJSON(); return string(data) }
func (f Fault) Error() string {
	if f.private == nil {
		return "runtime failure is unverified"
	}
	return f.private.Failure.Code + ": " + f.private.Failure.Message
}
func (f Fault) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, f.String()) }
func (f Fault) Valid() bool                { return f.private != nil }

// WithDelivery changes only delivery knowledge, never primary cause/dispatch.
// It grants no ACK by itself: the caller supplies independently observed sink
// results. A sink cannot durably prove its own failed/unknown write this way.
// Acknowledged is monotone; delayed reports cannot erase an observed ACK.
func (f Fault) WithDelivery(sink Sink, state Delivery, at time.Time) (Fault, error) {
	if !f.Valid() || at.IsZero() || at.Before(f.private.Failure.OccurredAt) || state != NotAttempted && state != Acknowledged && state != DeliveryRejected && state != DeliveryUnknown {
		return Fault{}, ErrContext
	}
	v := f.View()
	var old *DeliveryReport
	switch sink {
	case Journal:
		old = &v.JournalDelivery
	case Diagnostic:
		old = &v.DiagnosticDelivery
	default:
		return Fault{}, ErrContext
	}
	if old.State == Acknowledged && state != Acknowledged || state == NotAttempted && old.State != NotAttempted || at.Before(old.ObservedAt) {
		return Fault{}, ErrContext
	}
	report := DeliveryReport{State: state, ObservedAt: at.UTC()}
	if state == DeliveryUnknown || state == DeliveryRejected {
		step := "runtime.journal.persist"
		code := "JournalAcknowledgementUnknown"
		message := "The operation journal acknowledgement is unknown."
		action := "Confirm the exact retained journal proposal before allowing another provider action."
		if sink == Diagnostic {
			step = "runtime.diagnostics.publish"
			code = "DiagnosticAcknowledgementUnknown"
			message = "Diagnostic publication acknowledgement is unknown."
			action = "Repair or retry diagnostic publication separately; do not replay the provider operation."
		}
		if state == DeliveryRejected {
			code = "JournalDeliveryRejected"
			message = "The operation journal update was rejected."
			if sink == Diagnostic {
				code = "DiagnosticDeliveryRejected"
				message = "Diagnostic publication was rejected."
			}
		}
		failure := diagnostics.SafeFailure(diagnostics.DomainFailure{ID: diagnosticID(v.OperationID + "/" + step), Code: code, Category: diagnostics.Publication, Message: message, Step: step, OccurredAt: at.UTC(), NextAction: action})
		report.Failure = &failure
	}
	*old = report
	return Fault{private: &v}, nil
}
func safeFailure(c Context, code string, category diagnostics.Category, message, action string) Fault {
	c = cloneContext(c)
	id := c.OperationID + "/" + string(c.Kind) + "/" + string(c.Step) + "/" + c.Name + "/" + c.Fingerprint + "/" + c.WriteID
	sum := diagnosticID(id)
	failure := diagnostics.SafeFailure(diagnostics.DomainFailure{ID: sum, Code: code, Category: category, Message: message, Step: string(c.Step), OccurredAt: c.OccurredAt.UTC(), NextAction: action, Resources: []diagnostics.ResourceRef{{Provider: "kubernetes", Kind: string(c.Kind), ID: c.Name}}})
	dispatch := c.Dispatch
	if dispatch == "" {
		dispatch = DispatchUnknown
	}
	v := View{SchemaVersion: 1, OperationID: c.OperationID, WriteID: c.WriteID, Step: c.Step, Target: Target{Kind: c.Kind, Purpose: c.Purpose, Name: c.Name, NamespaceUID: c.NamespaceUID, NamespaceName: c.NamespaceName, Fingerprint: c.Fingerprint}, Input: c.Input, Failure: failure, Dispatch: dispatch, InputPersistence: DeliveryReport{State: NotAttempted}, JournalDelivery: DeliveryReport{State: NotAttempted}, DiagnosticDelivery: DeliveryReport{State: NotAttempted}}
	return Fault{private: &v}
}
func cleanupStep(step Step) bool { return step == Delete || step == Finalize }
func validIdentity(identity diagnostics.Identity) bool {
	_, err := diagnostics.NewProjection(identity)
	return err == nil
}
func noUnsafeID(value string) bool {
	return stableID.MatchString(value) && !strings.Contains(value, "//")
}
