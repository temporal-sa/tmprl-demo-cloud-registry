package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/catalogdiagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimefault"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type diagnosticWireContractSource struct{ snapshot catalogdiagnostics.Snapshot }

func (s diagnosticWireContractSource) Read(context.Context, catalogdiagnostics.ReadGrant) (catalogdiagnostics.Snapshot, error) {
	return s.snapshot, nil
}

// Pure serialization contract; no provider/client/process calls. This catches
// wire representation differences before an expensive owned API bootstrap.
func TestDiagnosticCanonicalFailureWireRoundtrip(t *testing.T) {
	at := time.Date(2026, 10, 30, 6, 0, 0, 123456789, time.UTC)
	fault, e := runtimefault.MapProvider(runtimefault.Context{Step: runtimefault.Observe, Kind: runtimefault.Service, Purpose: "service", OperationID: "owned-operation", WriteID: "owned-service", Name: "app", NamespaceUID: "owned-namespace-uid", NamespaceName: "owned-namespace", Fingerprint: fmt.Sprintf("%064x", 1), OccurredAt: at, Dispatch: runtimefault.Applied}, errors.Join(&provider.Error{Code: provider.ChildUnavailable}, apierrors.NewNotFound(schema.GroupResource{Resource: "services"}, "app")))
	if e != nil {
		t.Fatal(e)
	}
	if fault.View().Condition != runtimefault.ConditionNotFound {
		t.Fatal("typed NotFound condition missing")
	}
	live := fault.View().Failure
	canonical, e := json.Marshal(live)
	if e != nil {
		t.Fatal(e)
	}
	var decoded diagnostics.DomainFailure
	if json.Unmarshal(canonical, &decoded) != nil {
		t.Fatal("failure wire decode failed")
	}
	returned, e := json.Marshal(decoded)
	if e != nil {
		t.Fatal(e)
	}
	sumA, sumB := sha256.Sum256(canonical), sha256.Sum256(returned)
	evidence := map[string]any{"idEqual": decoded.ID == live.ID, "codeEqual": decoded.Code == live.Code, "wireEqual": bytes.Equal(canonical, returned), "liveSHA256": hex.EncodeToString(sumA[:]), "decodedSHA256": hex.EncodeToString(sumB[:]), "timestampEqual": decoded.OccurredAt.Equal(live.OccurredAt), "liveCausesNil": live.Causes == nil, "decodedCausesNil": decoded.Causes == nil, "liveEvidenceIDsNil": live.EvidenceIDs == nil, "decodedEvidenceIDsNil": decoded.EvidenceIDs == nil, "liveResourcesNil": live.Resources == nil, "decodedResourcesNil": decoded.Resources == nil, "reflectEqual": reflect.DeepEqual(live, decoded)}
	if !bytes.Equal(canonical, returned) || !decoded.OccurredAt.Equal(live.OccurredAt) {
		t.Fatal("canonical failure wire changed")
	}
	id := diagnostics.Identity{Environment: "lab", Installation: "owned-installation", ProjectUID: "owned-project", ProjectName: "demo", OperationID: "owned-operation", ReconcileKey: "accepted-key", Generation: 1}
	projection, e := diagnostics.NewProjection(id)
	if e != nil {
		t.Fatal(e)
	}
	projection, e = projection.Apply(diagnostics.OperationEvent{Identity: id, Sequence: 1, OccurredAt: at, State: diagnostics.Failed, Step: string(runtimefault.InputRetain), Failure: &live})
	if e != nil {
		t.Fatal(e)
	}
	target := catalogdiagnostics.Target{Environment: id.Environment, Installation: id.Installation, ProjectUID: id.ProjectUID}
	now := at.Add(time.Second)
	model, e := catalogdiagnostics.NewModel(diagnosticWireContractSource{catalogdiagnostics.Snapshot{Target: target, Origin: catalogdiagnostics.Materialized, Operation: &projection, DesiredKnown: true, Desired: diagnostics.DesiredState{Generation: 1, ReconcileKey: id.ReconcileKey}, DesiredObservedAt: now}}, catalogdiagnostics.Config{Now: func() time.Time { return now }, MaxProgressAge: time.Minute, MaxDesiredAge: time.Minute, MaxAvailabilityAge: time.Minute})
	if e != nil {
		t.Fatal(e)
	}
	grant, e := catalogdiagnostics.IssueReadGrant("owned-subject", "owned-policy", target, at, now.Add(time.Minute))
	if e != nil {
		t.Fatal(e)
	}
	view, e := model.Read(context.Background(), grant, target)
	if e != nil {
		t.Fatal(e)
	}
	raw, e := view.JSON()
	if e != nil {
		t.Fatal(e)
	}
	var wire catalogdiagnostics.View
	if json.Unmarshal(raw, &wire) != nil || wire.Operation == nil {
		t.Fatal("read-model wire missing")
	}
	evidence["modelRoute"], evidence["wireRoute"], evidence["wireStatus"], evidence["retry"] = view.Operation.Availability.RouteState, wire.Operation.Availability.RouteState, wire.Status, wire.Operation.Retry.Status
	if wire.Status != "Failed" || wire.Operation.Retry.Status != diagnostics.RetryUnknown || wire.Operation.Retry.Authoritative {
		t.Fatal("initial unknown availability/retry wire changed")
	}
	safe, _ := json.Marshal(evidence)
	fmt.Println("SAFE-DIAGNOSTIC-WIRE " + string(safe))
}

// Compare precisely what persistence/public diagnostic transport acknowledges;
// nil-vs-empty omitempty slices have one canonical DomainFailure wire meaning.
func canonicalFailureWireEqual(left, right diagnostics.DomainFailure) bool {
	a, errA := json.Marshal(left)
	b, errB := json.Marshal(right)
	return errA == nil && errB == nil && bytes.Equal(a, b)
}
