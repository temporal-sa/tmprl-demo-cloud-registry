package fixtureplan

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

func syntheticProjectBinding(t *testing.T) NamespaceProjectBinding {
	t.Helper()
	raw, review, err := ProjectCreateReview()
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(raw)
	return NamespaceProjectBinding{ProjectID: "11111111111111111111111111111111", CreatedTime: "2026-10-30T02:35:03.281016994Z", ResourceVersion: "5c1130d8-2094-4328-b550-4082f6a3662a", AsyncOperationID: review["asyncOperationID"].(string), ApprovedRequestSHA256: hex.EncodeToString(hash[:]), State: "RESOURCE_STATE_ACTIVE", AsyncState: "STATE_FULFILLED", OperationInputExactSpecAndAsyncVerified: true, DeleteProtectionEnabled: true}
}

func TestNamespaceRequestsAreBoundProtectedSDKContracts(t *testing.T) {
	binding := syntheticProjectBinding(t)
	drafts, review, err := NamespaceCreateReview(binding)
	if err != nil || len(drafts) != 3 || review["createAuthorized"] != false {
		t.Fatal("inert three-namespace review missing")
	}
	names, asyncs := map[string]bool{}, map[string]bool{}
	for _, draft := range drafts {
		var req cloudservice.CreateNamespaceRequest
		if err := protojson.Unmarshal(draft.RequestJSON, &req); err != nil {
			t.Fatal(err)
		}
		if req.ProjectId != binding.ProjectID || req.Spec.Name+"."+TemporalAccount != draft.NamespaceID || req.Spec.RetentionDays != 1 || len(req.Spec.Replicas) != 1 || req.Spec.Replicas[0].Region != TemporalNamespaceRegion || !req.Spec.GetApiKeyAuth().GetEnabled() || !req.Spec.GetLifecycle().GetEnableDeleteProtection() || req.Spec.MtlsAuth != nil || len(req.Spec.Regions) != 0 || req.Spec.CapacitySpec != nil || len(req.Tags) != 0 || len(req.Spec.SearchAttributes) != 0 || len(req.Spec.ConnectivityRuleIds) != 0 {
			t.Fatal("namespace requested a foreign/unbounded/global capability")
		}
		if names[draft.NamespaceID] || asyncs[draft.AsyncID] || req.AsyncOperationId != draft.AsyncID {
			t.Fatal("namespace attempt identities collide")
		}
		names[draft.NamespaceID], asyncs[draft.AsyncID] = true, true
		hash := sha256.Sum256(draft.RequestJSON)
		if draft.RequestSHA256 != hex.EncodeToString(hash[:]) {
			t.Fatal("request review hash differs")
		}
	}
	again, _, _ := NamespaceCreateReview(binding)
	for i := range drafts {
		if !bytes.Equal(again[i].RequestJSON, drafts[i].RequestJSON) {
			t.Fatal("process retry changed inert reviewed request")
		}
	}
	view, _ := json.Marshal(review)
	if !bytes.Contains(view, []byte("no Project resourceVersion precondition")) || !bytes.Contains(view, []byte("not proof")) || !bytes.Contains(view, []byte("CreateProject-only")) {
		t.Fatal("review hid provider/transport/provenance compatibility gaps")
	}
}

func TestNamespaceReviewRejectsUnprovenOrProductionProjectBindings(t *testing.T) {
	for _, mutate := range []func(*NamespaceProjectBinding){
		func(b *NamespaceProjectBinding) { b.ProjectID = "" },
		func(b *NamespaceProjectBinding) { b.ProjectID = "eca773c382124db79bbfab0b429faa5a" },
		func(b *NamespaceProjectBinding) { b.CreatedTime = "" },
		func(b *NamespaceProjectBinding) { b.ResourceVersion = "" },
		func(b *NamespaceProjectBinding) { b.State = "RESOURCE_STATE_DELETED" },
		func(b *NamespaceProjectBinding) { b.AsyncState = "STATE_FAILED" },
		func(b *NamespaceProjectBinding) { b.AsyncOperationID = "other-create" },
		func(b *NamespaceProjectBinding) { b.ApprovedRequestSHA256 = "other-request" },
		func(b *NamespaceProjectBinding) { b.OperationInputExactSpecAndAsyncVerified = false },
		func(b *NamespaceProjectBinding) { b.DeleteProtectionEnabled = false },
	} {
		binding := syntheticProjectBinding(t)
		mutate(&binding)
		if drafts, _, err := NamespaceCreateReview(binding); err == nil || len(drafts) != 0 {
			t.Fatal("unproven/production parent escaped review gate")
		}
	}
}
