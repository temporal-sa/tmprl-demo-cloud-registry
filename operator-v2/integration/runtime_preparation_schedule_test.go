//go:build integration && preparation_integration

package integration

// This is an explicit fixture-admin scheduling gate on the ONE owned kind
// node, not a runtime Node capability or production scheduling permission.
import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type preparationSchedulingGate struct {
	name, uid     string
	taint         corev1.Taint
	ownedPossible bool
}

func preparationNode(t *testing.T, name string) *corev1.Node {
	t.Helper()
	if !boundaryVerified || name != local.RunID+"-control-plane" {
		t.Fatal("PreparationNodeScopeUnverified")
	}
	response, err := client.Get(local.Server + "/api/v1/nodes/" + name)
	if err != nil {
		t.Fatal("PreparationNodeReadUnavailable")
	}
	defer response.Body.Close()
	var node corev1.Node
	if response.StatusCode != 200 || json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&node) != nil || node.Name != name || node.UID == "" || node.ResourceVersion == "" {
		t.Fatal("PreparationNodeIdentityInvalid")
	}
	return &node
}

func preparationNodeCAS(node *corev1.Node) (*corev1.Node, int) {
	body, err := json.Marshal(node)
	if err != nil {
		return nil, 0
	}
	request, err := http.NewRequest(http.MethodPut, local.Server+"/api/v1/nodes/"+node.Name, bytes.NewReader(body))
	if err != nil {
		return nil, 0
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return nil, 0 // Unknown fixture write; caller stops and runs cleanup.
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<20))
		return nil, response.StatusCode
	}
	var actual corev1.Node
	if json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&actual) != nil {
		return nil, 0
	}
	return &actual, 200
}

func preparationNodeResponseMatches(desired, actual *corev1.Node) bool {
	if actual == nil || actual.Name != desired.Name || actual.UID != desired.UID || actual.ResourceVersion == "" || actual.ResourceVersion == desired.ResourceVersion {
		return false
	}
	// Compare canonical wire semantics, preserving all unrelated Node spec and
	// caller-owned metadata. Status/server-owned metadata can independently move.
	freeze := func(node *corev1.Node) []byte {
		data, _ := json.Marshal(struct {
			Spec        corev1.NodeSpec
			Labels      map[string]string
			Annotations map[string]string
			Finalizers  []string
			Owners      []metav1.OwnerReference
		}{node.Spec, node.Labels, node.Annotations, node.Finalizers, node.OwnerReferences})
		return data
	}
	return bytes.Equal(freeze(desired), freeze(actual))
}

func blockPreparationScheduling(t *testing.T) *preparationSchedulingGate {
	t.Helper()
	name := local.RunID + "-control-plane"
	first := preparationNode(t, name)
	gate := &preparationSchedulingGate{name: name, uid: string(first.UID), taint: corev1.Taint{Key: "operator.local/preparation-wait", Value: local.RunID, Effect: corev1.TaintEffectNoSchedule}}
	for _, taint := range first.Spec.Taints {
		if taint.Key == gate.taint.Key {
			t.Fatal("PreparationSchedulingTagAlreadyPresent")
		}
	}
	// Even an ambiguous test-admin PUT may have applied this exact unique tag.
	// Cleanup independently verifies the pinned UID/current owned tag, without
	// relabeling a runtime Unknown write or granting any production authority.
	t.Cleanup(func() { gate.remove(t) })
	for attempt := 0; attempt < 3; attempt++ {
		node := first
		if attempt > 0 {
			node = preparationNode(t, name)
		}
		if string(node.UID) != gate.uid {
			t.Fatal("PreparationNodeIdentityChanged")
		}
		for _, taint := range node.Spec.Taints {
			if taint.Key == gate.taint.Key {
				t.Fatal("PreparationSchedulingTagAlreadyPresent")
			}
		}
		node.Spec.Taints = append(node.Spec.Taints, gate.taint)
		gate.ownedPossible = true
		actual, code := preparationNodeCAS(node)
		if code == 409 {
			gate.ownedPossible = false // Known rejection never adopts a later tag.
			continue                   // Known rejected CAS only; explicit fresh same-UID read.
		}
		if code == 400 || code == 401 || code == 403 || code == 404 || code == 422 {
			gate.ownedPossible = false // A known rejected PUT grants no cleanup ownership.
		}
		if code != 200 || !preparationNodeResponseMatches(node, actual) {
			t.Fatal("PreparationSchedulingGateUncertain")
		}
		return gate
	}
	t.Fatal("PreparationSchedulingGateConflict")
	return nil
}

func (gate *preparationSchedulingGate) remove(t *testing.T) {
	t.Helper()
	if !gate.ownedPossible {
		return
	}
	for attempt := 0; attempt < 3; attempt++ {
		node := preparationNode(t, gate.name)
		if string(node.UID) != gate.uid {
			t.Error("PreparationSchedulingCleanupNodeChanged")
			return
		}
		found := false
		taints := make([]corev1.Taint, 0, len(node.Spec.Taints))
		for _, taint := range node.Spec.Taints {
			if taint.Key != gate.taint.Key {
				taints = append(taints, taint)
				continue
			}
			if taint.Value != gate.taint.Value || taint.Effect != gate.taint.Effect || !taint.TimeAdded.Equal(gate.taint.TimeAdded) || found {
				t.Error("PreparationSchedulingCleanupTagChanged")
				return
			}
			found = true
		}
		if !found {
			gate.ownedPossible = false
			return // Independently confirmed goal; no guessed runtime RPC result.
		}
		node.Spec.Taints = taints
		actual, code := preparationNodeCAS(node)
		if code == 409 {
			continue
		}
		if code != 200 || !preparationNodeResponseMatches(node, actual) {
			t.Error("PreparationSchedulingCleanupUncertain")
			return
		}
		// Independent final GET, same UID and exact absence of our key.
		confirmed := preparationNode(t, gate.name)
		if string(confirmed.UID) != gate.uid {
			t.Error("PreparationSchedulingCleanupNodeChanged")
			return
		}
		for _, taint := range confirmed.Spec.Taints {
			if taint.Key == gate.taint.Key {
				t.Error("PreparationSchedulingCleanupStillPresent")
				return
			}
		}
		gate.ownedPossible = false
		return
	}
	t.Error("PreparationSchedulingCleanupConflict")
}
