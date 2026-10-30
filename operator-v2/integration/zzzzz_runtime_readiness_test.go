//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimeconfig"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimefault"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

const readinessQuotaMarker = "private-readiness-quota"

type readinessProcessOutput struct {
	Checkpoint []byte
	Receipt    runtimeconfig.DeploymentReceipt
	Fault      []byte
	Write      journal.Write
	Primary    diagnostics.DomainFailure
}

func runtimeReadinessProcess(t *testing.T, phase string) {
	t.Helper()
	TestOwnedAPIAndNetworkBoundary(t)
	var input privateRuntimeProcessInput
	d := json.NewDecoder(io.LimitReader(os.Stdin, 32768))
	d.DisallowUnknownFields()
	if d.Decode(&input) != nil || d.Decode(new(any)) != io.EOF || phase != "initial" && phase != "inspect" {
		t.Fatal("invalid owned readiness process input")
	}
	config, repository := retirementConfig(t, input.Pin, input.Endpoint, input.ProxyCA)
	config.Diagnostics = &runtimeconfig.DiagnosticConfig{ProjectName: "runtime-example"}
	store, _ := openRuntimeInputs(t, input)
	config.DeploymentInputs = store
	var worker *runtimeconfig.Worker
	var err error
	if phase == "initial" {
		worker, err = runtimeconfig.OpenInitial(context.Background(), config)
	} else {
		checkpoint, e := runtimeconfig.ParseCheckpoint(input.Checkpoint)
		if e != nil {
			t.Fatal(e)
		}
		// The real controller still reports Failed during preflight. This
		// must not turn clean read admission into a readiness requirement.
		worker, err = runtimeconfig.OpenClean(context.Background(), config, checkpoint)
	}
	if err != nil {
		t.Fatal("readiness worker admission failed", err)
	}
	desired := desiredRuntimeDeployment(input.Pin)
	var session *runtimeconfig.Session
	if phase == "initial" {
		session, err = worker.EnsureNamespace(context.Background(), desired, runtimeAttempt(input.Pin))
	} else {
		session, err = worker.ObserveNamespace(context.Background(), desired, runtimeAttempt(input.Pin))
	}
	if err != nil {
		t.Fatal(err)
	}
	var output readinessProcessOutput
	if phase == "initial" {
		namespace := session.Context().NamespaceName()
		// These are explicit fixture-administrator prerequisites, not operator
		// ServiceAccount/ResourceQuota mutation capabilities.
		request(t, "POST", "/api/v1/namespaces/"+namespace+"/serviceaccounts", object{"apiVersion": "v1", "kind": "ServiceAccount", "metadata": object{"name": "app", "labels": object{"operator.local/run-id": local.RunID}}, "automountServiceAccountToken": false}, 201)
		quotaPath := "/api/v1/namespaces/" + namespace + "/resourcequotas"
		request(t, "POST", quotaPath, object{"apiVersion": "v1", "kind": "ResourceQuota", "metadata": object{"name": readinessQuotaMarker, "labels": object{"operator.local/run-id": local.RunID}}, "spec": object{"hard": object{"pods": "0"}}}, 201)
		wait(t, func() bool {
			quota := request(t, "GET", quotaPath+"/"+readinessQuotaMarker, nil, 200)
			return text(field(field(quota, "status"), "hard"), "pods") == "0"
		})
		probe := request(t, "POST", "/api/v1/namespaces/"+namespace+"/pods", object{"apiVersion": "v1", "kind": "Pod", "metadata": object{"name": "quota-denial-probe"}, "spec": object{"serviceAccountName": "app", "automountServiceAccountToken": false, "restartPolicy": "Never", "containers": []any{object{"name": "probe", "image": local.ImageDigest}}}}, 403)
		if text(probe, "reason") != "Forbidden" || !strings.Contains(text(probe, "message"), "exceeded quota") || !strings.Contains(text(probe, "message"), readinessQuotaMarker) {
			t.Fatal("owned quota admission denial was not established")
		}
		object := deploymentFromFixture(t, session, input.Pin, false)
		output.Receipt, err = session.ApplyDeployment(context.Background(), object)
		if err == nil {
			wait(t, func() bool {
				output.Receipt, err = session.ObserveDeployment(context.Background(), object)
				return err != nil
			})
		}
	} else {
		write := nativeWrite(t, repository, runtimefault.Deployment)
		object := nativeStoredObject(t, nativeProcessInput{Base: input, Kind: runtimefault.Deployment}, store, session, write)
		output.Receipt, err = session.ObserveDeployment(context.Background(), object)
		_, blocked := session.ApplyDeployment(context.Background(), object)
		requireNativeMutationBlocked(t, blocked)
	}
	var failure *runtimeconfig.Error
	if !errors.As(err, &failure) || failure.Code != runtimeconfig.Unavailable || failure.Fault == nil {
		t.Fatal("real failed controller result did not become a safe readiness fault", err)
	}
	v := failure.Fault.View()
	if output.Receipt.Readiness.State != "Failed" || output.Receipt.Readiness.Code != "DeploymentReplicaFailure" || output.Receipt.Record.Record.Ref.ProviderID == "" || output.Receipt.ResourceVersion == "" || v.Failure.Code != "DeploymentReplicaFailure" || v.Dispatch != runtimefault.Applied || v.Readiness == nil || v.Readiness.Generation != output.Receipt.Readiness.Generation || v.Readiness.ObservedGeneration != output.Receipt.Readiness.Generation || v.JournalDelivery.State != runtimefault.Acknowledged || v.DiagnosticDelivery.State != runtimefault.Acknowledged {
		t.Fatal("known failed readiness lost canonical receipt, generation or delivery facts")
	}
	output.Fault, err = json.Marshal(failure.Fault)
	if err != nil || bytes.Contains(output.Fault, []byte(readinessQuotaMarker)) || bytes.Contains(output.Fault, []byte(runtimeDeploymentPrivateMarker)) {
		t.Fatal("raw quota/controller or private input entered readiness fault")
	}
	output.Write = nativeWrite(t, repository, runtimefault.Deployment)
	snapshot, err := repository.Load(context.Background())
	if err != nil || snapshot.Envelope.Projection == nil || snapshot.Envelope.Projection.Failure == nil || snapshot.Envelope.Projection.Failure.Code != "DeploymentReplicaFailure" || output.Receipt.JournalVersion == snapshot.Version || output.Write.ResultUID != output.Receipt.Record.Record.Ref.ProviderID {
		t.Fatal("readiness cause not independently persisted after the known read", err)
	}
	output.Primary = *snapshot.Envelope.Projection.Failure
	// Independently inspect the actual controller's condition, not a patched
	// status or proxy response. Its raw message is checked only inside fixture.
	name := output.Receipt.Record.Record.Ref.Name
	namespace := output.Receipt.Record.Record.Ref.Scope.Namespace
	raw := request(t, "GET", "/apis/apps/v1/namespaces/"+namespace+"/deployments/"+name, nil, 200)
	encoded, _ := json.Marshal(raw)
	var deployment appsv1.Deployment
	if json.Unmarshal(encoded, &deployment) != nil || deployment.Status.ObservedGeneration != deployment.Generation {
		t.Fatal("real controller generation was not observed")
	}
	quotaFailure := false
	for _, condition := range deployment.Status.Conditions {
		if condition.Type == appsv1.DeploymentReplicaFailure && condition.Status == corev1.ConditionTrue && strings.Contains(condition.Message, readinessQuotaMarker) {
			quotaFailure = true
		}
	}
	if !quotaFailure {
		t.Fatal("controller did not independently report the controlled quota failure")
	}
	checkpoint, err := worker.Quiesce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	output.Checkpoint, err = checkpoint.JSON()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err = json.Marshal(output)
	if err != nil || len(encoded) > 32768 {
		t.Fatal("owned readiness receipt exceeds bound")
	}
	fmt.Println("READINESS-PROCESS " + string(encoded))
}

func TestRuntimeControllerReadinessFailureAcrossCleanProcesses(t *testing.T) {
	if phase := os.Getenv("OPERATOR_READINESS_PROCESS"); phase != "" {
		runtimeReadinessProcess(t, phase)
		return
	}
	input, store, _ := deploymentProcessFixture(t, "readiness")
	endpoint, ca, posts := deploymentDispatchProxy(t, input, store, false)
	input.Endpoint, input.ProxyCA = endpoint, ca
	var first, second readinessProcessOutput
	runPrivateRuntimeProcess(t, "TestRuntimeControllerReadinessFailureAcrossCleanProcesses", "OPERATOR_READINESS_PROCESS", "READINESS-PROCESS ", "initial", input, &first, input.Directory, readinessQuotaMarker, runtimeDeploymentPrivateMarker)
	if posts.Load() != 1 || len(first.Checkpoint) == 0 {
		t.Fatal("initial readiness failure repeated creation or lost clean handoff")
	}
	// The helper has synchronously waited/reaped the first child before this
	// successor can open the private store or consume the Ready checkpoint.
	input.Checkpoint = first.Checkpoint
	runPrivateRuntimeProcess(t, "TestRuntimeControllerReadinessFailureAcrossCleanProcesses", "OPERATOR_READINESS_PROCESS", "READINESS-PROCESS ", "inspect", input, &second, input.Directory, readinessQuotaMarker, runtimeDeploymentPrivateMarker)
	if posts.Load() != 1 || !reflect.DeepEqual(first.Write, second.Write) || !reflect.DeepEqual(first.Primary, second.Primary) || first.Receipt.Record.Record.Ref != second.Receipt.Record.Record.Ref || len(second.Checkpoint) == 0 {
		t.Fatal("clean inspection changed committed creation/cause or replayed POST")
	}
	_, repository := retirementConfig(t, input.Pin, "", nil)
	persisted, err := repository.Load(context.Background())
	if err != nil || persisted.Envelope.Projection == nil || persisted.Envelope.Projection.Failure == nil || persisted.Envelope.Projection.Failure.Code != "DeploymentReplicaFailure" || persisted.Envelope.Projection.Retry.Status != diagnostics.RetryUnknown || persisted.Envelope.Projection.Availability.RouteState != diagnostics.RouteUnknown {
		t.Fatal("exited worker cause or honest retry/availability was lost", err)
	}
}
