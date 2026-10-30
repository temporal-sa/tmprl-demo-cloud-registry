//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	provider "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	corev1 "k8s.io/api/core/v1"
)

func TestLiteralWorkloadCurrentDeclarationAndAPIMediatedServing(t *testing.T) {
	if !boundaryVerified {
		TestOwnedAPIAndNetworkBoundary(t)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	f := newLiteralRuntimeFixture(t, ctx)
	before, version := literalJournalBytes(t, ctx, f)
	var observed provider.LiteralWorkloadSnapshot
	for {
		readiness, err := f.session.ObserveDeployment(ctx, f.objects["Deployment"])
		if err != nil {
			t.Fatal("LiteralCanonicalReadinessUnavailable")
		}
		observed, err = f.binding.ObserveLiteralWorkload(ctx, f.read)
		if err != nil {
			t.Fatal("LiteralObservationInvalid")
		}
		v := observed.View()
		if readiness.Readiness.State == "Ready" && v.Complete && v.DeclarationsEqual && v.Pods == 1 && v.Publication.PublicationPresent && v.Publication.Counts.ReadyPods == 1 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatalf("LiteralConvergenceTimeout state=%s code=%s", v.State, v.Code)
		case <-time.After(250 * time.Millisecond):
		}
	}
	v := observed.View()
	if v.CreatorOriginVerified || v.ProcessValuesVerified || v.PlacementAttested || v.PromotionAuthorized || v.Publication.PayloadVerified {
		t.Fatal("LiteralEvidenceUpgradedAuthority")
	}
	literalAPIReady(t, ctx, f.binding.Parent().Name)
	postProbe, err := f.binding.ObserveLiteralWorkload(ctx, f.read)
	if err != nil || !postProbe.View().Complete || !postProbe.View().DeclarationsEqual || !observed.SamePodIdentities(postProbe) || postProbe.View().Publication.Counts.ReadyPods != 1 {
		t.Fatal("LiteralPostProbeSampleChanged")
	}
	encoded, _ := json.Marshal(postProbe)
	for _, private := range []string{"PRIVATE_RUNTIME_INPUT_FIXTURE_ONLY", "runtime-deployment", "/fixture", "10.244.", "PodSpec", "addresses"} {
		if strings.Contains(string(encoded), private) {
			t.Fatal("LiteralPrivatePayloadEscaped")
		}
	}
	// One exact owned fixture-administrator mutation, NOT an operator apply
	// capability. Pod env is immutable; additive tolerations are API-supported.
	// Current owner/hash/image and published readiness cannot conceal this drift.
	var list corev1.PodList
	get, err := http.NewRequestWithContext(ctx, "GET", local.Server+"/api/v1/namespaces/"+f.binding.Parent().Name+"/pods", nil)
	if err != nil {
		t.Fatal("LiteralDriftReadInvalid")
	}
	response, err := client.Do(get)
	if err != nil {
		t.Fatal("LiteralDriftReadUnavailable")
	}
	err = json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&list)
	response.Body.Close()
	if err != nil || response.StatusCode != 200 || len(list.Items) != 1 {
		t.Fatal("LiteralDriftPodAmbiguous")
	}
	pod := list.Items[0].DeepCopy()
	uid, rv := pod.UID, pod.ResourceVersion
	if uid == "" || rv == "" || pod.DeletionTimestamp != nil {
		t.Fatal("LiteralDriftPodChanged")
	}
	pod.Spec.Tolerations = append(pod.Spec.Tolerations, corev1.Toleration{Key: "operator.local/literal-fixture-drift", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule})
	body, _ := json.Marshal(pod)
	put, err := http.NewRequestWithContext(ctx, "PUT", local.Server+"/api/v1/namespaces/"+pod.Namespace+"/pods/"+pod.Name, bytes.NewReader(body))
	if err != nil {
		t.Fatal("LiteralDriftWriteInvalid")
	}
	put.Header.Set("Content-Type", "application/json")
	response, err = client.Do(put)
	if err != nil {
		t.Fatal("LiteralDriftWriteUnavailable")
	}
	var changed corev1.Pod
	err = json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&changed)
	response.Body.Close()
	if err != nil || response.StatusCode != 200 || changed.UID != uid || changed.ResourceVersion == "" || changed.ResourceVersion == rv {
		t.Fatal("LiteralDriftWriteUnconfirmed")
	}
	negative, err := f.binding.ObserveLiteralWorkload(ctx, f.read)
	if err != nil || negative.View().Complete || negative.View().DeclarationsEqual || negative.View().Code != "LiteralPodChanged" {
		t.Fatal("LiteralSameUIDSpecDriftAccepted")
	}
	after, afterVersion := literalJournalBytes(t, ctx, f)
	if !bytes.Equal(before, after) || version != afterVersion {
		t.Fatal("LiteralReadOrProbeMutatedJournal")
	}
	f.posts.mu.Lock()
	for _, kind := range []string{"Namespace", "ServiceAccount", "Deployment", "Service"} {
		if f.posts.values[kind] != 1 {
			f.posts.mu.Unlock()
			t.Fatal("LiteralManagedPOSTCountChanged")
		}
	}
	f.posts.mu.Unlock()
	// These observations did not create or change any operator resource. The
	// real API proxy served the accepted literal through the sampled backend;
	// this is NOT ClusterIP/kube-proxy, DNS, ingress or promotion proof.
	t.Log("LiteralWorkload declarations=equal apiMediatedReady=observed sameUIDDrift=denied managedCreates=NS1-SA1-D1-S1 promotion=false")
}
