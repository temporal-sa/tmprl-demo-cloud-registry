package runtimefault

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeinputs"
)

// ParseOutcome accepts only the bounded canonical producer wire form and
// reconstructs every message/code/action from finite mappings. Parsed claims
// remain HISTORY DATA: an encoded ACK, identity or phase is not independent
// storage proof, admission, authorization or permission to replay a provider.
// Exact canonical matching also rejects duplicate/case-aliased/missing fields,
// dynamic error strings, injected causes/recovery and alternative wire schemas.
func ParseOutcome(data []byte) (Outcome, error) {
	var v OutcomeView
	if decodeCanonicalShape(data, &v) != nil || v.SchemaVersion != 1 {
		return Outcome{}, ErrContext
	}
	fault, err := faultFromView(v.Fault)
	if err != nil {
		return Outcome{}, ErrContext
	}
	result, err := NewOutcome(v.Identity, v.EventID, v.Sequence, v.ObservedAt, v.Phase, fault)
	if err != nil {
		return Outcome{}, ErrContext
	}
	canonical, err := result.MarshalJSON()
	if err != nil || !bytes.Equal(bytes.TrimSpace(data), canonical) {
		return Outcome{}, ErrContext
	}
	return result, nil
}
func ParseFault(data []byte) (Fault, error) {
	var v View
	if decodeCanonicalShape(data, &v) != nil {
		return Fault{}, ErrContext
	}
	result, err := faultFromView(v)
	if err != nil {
		return Fault{}, ErrContext
	}
	canonical, err := result.MarshalJSON()
	if err != nil || !bytes.Equal(bytes.TrimSpace(data), canonical) {
		return Fault{}, ErrContext
	}
	return result, nil
}
func decodeCanonicalShape(data []byte, value any) error {
	if len(data) == 0 || len(data) > MaxJSONBytes {
		return ErrContext
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(value) != nil || d.Decode(new(any)) != io.EOF {
		return ErrContext
	}
	return nil
}
func faultFromView(v View) (Fault, error) {
	if v.SchemaVersion != 1 {
		return Fault{}, ErrContext
	}
	c := Context{Step: v.Step, Kind: v.Target.Kind, Purpose: v.Target.Purpose, OperationID: v.OperationID, WriteID: v.WriteID, Name: v.Target.Name, NamespaceUID: v.Target.NamespaceUID, NamespaceName: v.Target.NamespaceName, Fingerprint: v.Target.Fingerprint, Input: v.Input, OccurredAt: v.Failure.OccurredAt, Dispatch: v.Dispatch}
	var result Fault
	var err error
	if v.Condition != "" && c.Step != Create && c.Step != Observe && !cleanupStep(c.Step) {
		return Fault{}, ErrContext
	}
	if v.Readiness != nil {
		if v.Condition != "" || c.Kind != Deployment {
			return Fault{}, ErrContext
		}
		result, err = MapDeploymentReadiness(c, kube.DeploymentReadiness{State: "Failed", Code: v.Failure.Code, Generation: v.Readiness.Generation, ObservedGeneration: v.Readiness.ObservedGeneration})
	} else if c.Step == InputRetain || c.Step == InputResolve {
		var cause error
		switch v.Failure.Code {
		case "AcceptedInputStoreNotConfigured":
			cause = runtimeinputs.ErrNotConfigured
		case "AcceptedInputUnproven":
			cause = runtimeinputs.ErrUnproven
		case "AcceptedInputInvalid":
			cause = runtimeinputs.ErrInvalid
		case "AcceptedInputCapacityReached":
			cause = runtimeinputs.ErrCapacity
		case "AcceptedInputRetentionUncertain", "AcceptedInputVerificationUncertain":
			cause = runtimeinputs.ErrUncertain
		case "AcceptedInputUnavailable":
			cause = errors.New("unclassified accepted input failure")
		default:
			return Fault{}, ErrContext
		}
		result, err = MapInput(c, cause)
	} else if c.Step == DispatchAuthorize {
		found := false
		for reason, description := range authorityDescriptions {
			if description.code == v.Failure.Code {
				result, err = MapAuthority(c, reason)
				found = true
				break
			}
		}
		if !found {
			return Fault{}, ErrContext
		}
	} else if c.Step == ReceiptAcknowledge {
		if v.Failure.Code != "CanonicalCreateReceiptUnconfirmed" {
			return Fault{}, ErrContext
		}
		result, err = MapReceiptAcknowledgement(c)
	} else {
		code := kube.Code(v.Failure.Code)
		if _, known := providerDescriptions[code]; known {
			result, err = providerFault(c, code, v.Condition)
		} else if v.Failure.Code == "RuntimeProviderFailureUnclassified" {
			result, err = providerFault(c, code, v.Condition)
		} else {
			return Fault{}, ErrContext
		}
	}
	if err != nil {
		return Fault{}, ErrContext
	}
	for _, delivery := range []struct {
		sink   Sink
		report DeliveryReport
	}{{Journal, v.JournalDelivery}, {Diagnostic, v.DiagnosticDelivery}} {
		if delivery.report.State == NotAttempted && delivery.report.ObservedAt.IsZero() {
			continue
		}
		result, err = result.WithDelivery(delivery.sink, delivery.report.State, delivery.report.ObservedAt)
		if err != nil {
			return Fault{}, ErrContext
		}
	}
	// Compare canonical reconstructed views, not caller-controlled message text.
	// DomainFailure's defensive MarshalJSON redaction cannot turn a different
	// input into accepted wire because Parse additionally checks original bytes.
	expected, e1 := result.MarshalJSON()
	actual, e2 := json.Marshal(v)
	if e1 != nil || e2 != nil || !bytes.Equal(expected, actual) {
		return Fault{}, ErrContext
	}
	return result, nil
}
func (f *Fault) UnmarshalJSON(data []byte) error {
	parsed, err := ParseFault(data)
	if err != nil {
		return err
	}
	*f = parsed
	return nil
}
func (o *Outcome) UnmarshalJSON(data []byte) error {
	parsed, err := ParseOutcome(data)
	if err != nil {
		return err
	}
	*o = parsed
	return nil
}
