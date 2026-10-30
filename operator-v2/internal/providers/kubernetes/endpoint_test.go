package kubernetes

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	appsclient "k8s.io/client-go/kubernetes/typed/apps/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	discoveryclient "k8s.io/client-go/kubernetes/typed/discovery/v1"
	"k8s.io/client-go/rest"
)

// All API reads in this unit fixture are a synthetic in-memory transport; it
// proves observer decisions, not real controller behavior, network isolation,
// durable journal inputs or Pod payload provenance.
type endpointFixture struct {
	binding                      *NamespaceBinding
	request                      EndpointRead
	service                      *corev1.Service
	deployment                   *appsv1.Deployment
	parent                       *corev1.Namespace
	pod                          *corev1.Pod
	rs                           *appsv1.ReplicaSet
	pages                        map[string]*discoveryv1.EndpointSliceList
	listCalls, podCalls, rsCalls int
	sourceCalls                  int
	beforeList                   func(*http.Request)
	afterList                    func()
	calls                        []string
}

func newEndpointFixture(t *testing.T) *endpointFixture {
	t.Helper()
	d := newDeploymentFixture(t)
	dr, err := d.binding.CreateDeployment(context.Background(), d.request())
	if err != nil {
		t.Fatal(err)
	}
	s := newServiceFixture(t)
	s.desired.Spec.Selector = map[string]string{"app": "backend"}
	s.refreshIntent(t)
	sr, err := s.binding.CreateService(context.Background(), s.request())
	if err != nil {
		t.Fatal(err)
	}
	f := &endpointFixture{binding: d.binding, parent: d.parent, service: s.object.DeepCopy(), deployment: d.object.DeepCopy(), request: EndpointRead{Service: ServiceRead{Desired: s.desired, Intent: s.intent, Record: sr}, Deployment: DeploymentRead{Desired: d.desired, Intent: d.intent, Record: dr}}}
	owner := func(kind, version, name, uid string) []metav1.OwnerReference {
		return []metav1.OwnerReference{{APIVersion: version, Kind: kind, Name: name, UID: types.UID(uid), Controller: deploymentPtr(true), BlockOwnerDeletion: deploymentPtr(true)}}
	}
	f.rs = &appsv1.ReplicaSet{TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "ReplicaSet"}, ObjectMeta: metav1.ObjectMeta{Name: "backend-rs", Namespace: f.parent.Name, UID: "rs-uid", ResourceVersion: "30", OwnerReferences: owner("Deployment", "apps/v1", f.deployment.Name, string(f.deployment.UID))}, Spec: appsv1.ReplicaSetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "backend", "pod-template-hash": "fixturehash"}}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "backend", "pod-template-hash": "fixturehash"}}}}}
	f.pod = &corev1.Pod{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}, ObjectMeta: metav1.ObjectMeta{Name: "backend-pod", Namespace: f.parent.Name, UID: "pod-uid", ResourceVersion: "40", OwnerReferences: owner("ReplicaSet", "apps/v1", f.rs.Name, string(f.rs.UID)), Labels: map[string]string{"app": "backend", "pod-template-hash": "fixturehash"}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "backend", Image: "PRIVATE_UNVERIFIED_PAYLOAD"}}}, Status: corev1.PodStatus{Phase: corev1.PodRunning, PodIP: "10.244.0.2", PodIPs: []corev1.PodIP{{IP: "10.244.0.2"}}, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue, Message: "PRIVATE_PROVIDER_MESSAGE"}}}}
	slice := discoveryv1.EndpointSlice{TypeMeta: metav1.TypeMeta{APIVersion: "discovery.k8s.io/v1", Kind: "EndpointSlice"}, ObjectMeta: metav1.ObjectMeta{Name: "backend-slice", Namespace: f.parent.Name, UID: "slice-uid", ResourceVersion: "50", OwnerReferences: owner("Service", "v1", f.service.Name, string(f.service.UID)), Labels: map[string]string{discoveryv1.LabelServiceName: f.service.Name, discoveryv1.LabelManagedBy: "endpointslice-controller.k8s.io"}}, AddressType: discoveryv1.AddressTypeIPv4, Ports: []discoveryv1.EndpointPort{{Name: deploymentPtr("http"), Port: deploymentPtr[int32](8080), Protocol: deploymentPtr(corev1.ProtocolTCP)}}, Endpoints: []discoveryv1.Endpoint{{Addresses: []string{"10.244.0.2"}, TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: f.parent.Name, Name: f.pod.Name, UID: f.pod.UID}, Conditions: discoveryv1.EndpointConditions{Ready: deploymentPtr(true), Serving: deploymentPtr(true), Terminating: deploymentPtr(false)}}}}
	f.pages = map[string]*discoveryv1.EndpointSliceList{"": {TypeMeta: metav1.TypeMeta{APIVersion: "discovery.k8s.io/v1", Kind: "EndpointSliceList"}, ListMeta: metav1.ListMeta{ResourceVersion: "100"}, Items: []discoveryv1.EndpointSlice{slice}}}
	transport := deploymentRoundTrip(func(r *http.Request) (*http.Response, error) {
		f.calls = append(f.calls, r.Method+" "+r.URL.Path)
		if r.Method != "GET" {
			return nil, fmt.Errorf("unexpected mutation")
		}
		var out any
		switch {
		case r.URL.Path == "/api/v1/namespaces/kube-system":
			out = &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "kube-system", UID: "cluster-uid"}}
		case r.URL.Path == "/api/v1/namespaces/"+f.parent.Name:
			out = f.parent.DeepCopy()
		case strings.HasSuffix(r.URL.Path, "/services/backend"):
			f.sourceCalls++
			out = f.service.DeepCopy()
		case strings.HasSuffix(r.URL.Path, "/deployments/backend"):
			out = f.deployment.DeepCopy()
		case strings.HasSuffix(r.URL.Path, "/endpointslices"):
			f.listCalls++
			if f.beforeList != nil {
				f.beforeList(r)
			}
			page := f.pages[r.URL.Query().Get("continue")]
			if page == nil {
				return nil, fmt.Errorf("PRIVATE_UNEXPECTED_TOKEN")
			}
			out = page.DeepCopy()
			if f.afterList != nil {
				f.afterList()
			}
		case strings.HasSuffix(r.URL.Path, "/pods/backend-pod"):
			f.podCalls++
			out = f.pod.DeepCopy()
		case strings.HasSuffix(r.URL.Path, "/replicasets/backend-rs"):
			f.rsCalls++
			out = f.rs.DeepCopy()
		default:
			return nil, fmt.Errorf("PRIVATE_UNEXPECTED_READ")
		}
		data, err := json.Marshal(out)
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(string(data)))}, err
	})
	config := &rest.Config{Host: "https://127.0.0.1:6443"}
	hc := &http.Client{Transport: transport}
	core, err := coreclient.NewForConfigAndClient(config, hc)
	if err != nil {
		t.Fatal(err)
	}
	apps, err := appsclient.NewForConfigAndClient(config, hc)
	if err != nil {
		t.Fatal(err)
	}
	discovery, err := discoveryclient.NewForConfigAndClient(config, hc)
	if err != nil {
		t.Fatal(err)
	}
	f.binding.client.core, f.binding.client.apps, f.binding.client.discovery = core, apps, discovery
	return f
}
func TestEndpointPublicationCurrentAncestryAndSafeWindow(t *testing.T) {
	f := newEndpointFixture(t)
	f.beforeList = func(r *http.Request) {
		q := r.URL.Query()
		if q.Get("labelSelector") != "kubernetes.io/service-name=backend" || q.Get("limit") != "16" || strings.Contains(q.Get("labelSelector"), "managed-by") {
			t.Fatal("discovery filter hides relevant foreign slices")
		}
	}
	snapshot, err := f.binding.ObserveEndpoints(context.Background(), f.request)
	if err != nil {
		t.Fatal(err)
	}
	v := snapshot.View()
	if !v.Complete || !v.PublicationPresent || v.State != "Observed" || v.Code != "EndpointPublicationObserved" || v.Counts.ReadyPods != 1 || v.Counts.VerifiedPods != 1 || v.Counts.Slices != 1 || v.PayloadVerified || v.PromotionAuthorized || v.Claim != "current_ancestry_publication" || v.StartedAt.IsZero() || v.CompletedAt.Before(v.StartedAt) || len(v.SampleDigest) != 64 {
		t.Fatalf("bad safe sample: %+v", v)
	}
	if f.listCalls != 1 || f.podCalls != 1 || f.rsCalls != 1 || f.sourceCalls != 2 {
		t.Fatal("fresh independent reads/end recheck missing")
	}
	for _, value := range []any{snapshot, &snapshot, f.request} {
		for _, verb := range []string{"%v", "%+v", "%#v", "%p", "%x", "%q"} {
			out := fmt.Sprintf(verb, value)
			if strings.Contains(out, "PRIVATE_") || strings.Contains(out, "50524956415445") || strings.Contains(out, "10.244.") || strings.Contains(out, "10.96.") {
				t.Fatal("private endpoint data escaped fmt")
			}
		}
		data, _ := json.Marshal(value)
		if strings.Contains(string(data), "PRIVATE_") || strings.Contains(string(data), "10.244.") || strings.Contains(string(data), "10.96.") {
			t.Fatal("private endpoint data escaped JSON")
		}
	}
	copy := snapshot.View()
	copy.Counts.ReadyPods = 99
	if snapshot.View().Counts.ReadyPods != 1 {
		t.Fatal("view aliases snapshot")
	}
	// A foreign payload is deliberately not authenticated by this ancestry-only
	// observer. The result must never claim payload equivalence or promotion.
	if snapshot.View().PayloadVerified || snapshot.View().PromotionAuthorized {
		t.Fatal("ancestry laundered into payload authority")
	}
}
func TestEndpointEmptyAndNotReadyArePublicationWaits(t *testing.T) {
	for _, kind := range []string{"no-slices", "empty-slice", "empty-placeholder", "not-ready", "terminating"} {
		t.Run(kind, func(t *testing.T) {
			f := newEndpointFixture(t)
			slice := &f.pages[""].Items[0]
			switch kind {
			case "no-slices":
				f.pages[""].Items = nil
			case "empty-slice":
				slice.Endpoints = nil
			case "empty-placeholder":
				slice.Endpoints = nil
				slice.Ports = nil
			case "not-ready":
				slice.Endpoints[0].Conditions.Ready = deploymentPtr(false)
				slice.Endpoints[0].Conditions.Serving = deploymentPtr(false)
				f.pod.Status.Conditions[0].Status = corev1.ConditionFalse
			case "terminating":
				now := metav1.Now()
				f.pod.DeletionTimestamp = &now
				slice.Endpoints[0].Conditions.Ready = deploymentPtr(false)
				slice.Endpoints[0].Conditions.Terminating = deploymentPtr(true)
			}
			snap, err := f.binding.ObserveEndpoints(context.Background(), f.request)
			if err != nil {
				t.Fatal(err)
			}
			v := snap.View()
			if !v.Complete || v.PublicationPresent || v.State != "Waiting" || v.Counts.ReadyPods != 0 {
				t.Fatalf("wait conflated with failure: %+v", v)
			}
		})
	}
}
func TestEndpointConditionNilDefaultsAndIndependentPodChecks(t *testing.T) {
	effective, defaulted := effectiveEndpointConditions(discoveryv1.EndpointConditions{})
	if !effective.ready || !effective.serving || effective.terminating || !defaulted {
		t.Fatal("official nil defaults changed")
	}
	f := newEndpointFixture(t)
	f.pages[""].Items[0].Endpoints[0].Conditions = discoveryv1.EndpointConditions{}
	snap, err := f.binding.ObserveEndpoints(context.Background(), f.request)
	if err != nil || !snap.View().Complete || snap.View().Counts.DefaultedConditionPods != 1 {
		t.Fatal("nil condition presence/default semantics differ", err)
	}
	f = newEndpointFixture(t)
	f.pages[""].Items[0].Endpoints[0].Conditions = discoveryv1.EndpointConditions{}
	f.pod.Status.Conditions[0].Status = corev1.ConditionFalse
	snap, err = f.binding.ObserveEndpoints(context.Background(), f.request)
	if err != nil || snap.View().Complete || snap.View().PublicationPresent || snap.View().Code != "EndpointConditionConflict" {
		t.Fatal("implicit true ignored fresh PodReady", err)
	}
}
func TestEndpointForeignAndStaleAncestryCannotBeHidden(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*endpointFixture)
	}{
		{"foreign-service-owner", func(f *endpointFixture) { f.pages[""].Items[0].OwnerReferences[0].UID = "foreign" }},
		{"foreign-controller-source", func(f *endpointFixture) { f.pages[""].Items[0].Labels[discoveryv1.LabelManagedBy] = "other" }},
		{"pod-recreated", func(f *endpointFixture) { f.pod.UID = "replacement" }},
		{"rs-recreated", func(f *endpointFixture) { f.rs.UID = "replacement" }},
		{"foreign-deployment", func(f *endpointFixture) { f.rs.OwnerReferences[0].UID = "foreign" }},
		{"copied-labels-no-owner", func(f *endpointFixture) { f.pod.OwnerReferences = nil }},
		{"address-not-pod", func(f *endpointFixture) { f.pages[""].Items[0].Endpoints[0].Addresses[0] = "10.244.0.99" }},
		{"wrong-target-port", func(f *endpointFixture) { f.pages[""].Items[0].Ports[0].Port = deploymentPtr[int32](9999) }},
		{"wrong-template-label", func(f *endpointFixture) { f.rs.Spec.Template.Labels["app"] = "foreign" }},
		{"slice-ready-contradiction", func(f *endpointFixture) { f.pages[""].Items[0].Endpoints[0].Conditions.Serving = deploymentPtr(false) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newEndpointFixture(t)
			tc.change(f)
			snap, err := f.binding.ObserveEndpoints(context.Background(), f.request)
			if err != nil {
				t.Fatal(err)
			}
			if snap.View().Complete || snap.View().PublicationPresent || snap.View().Counts.ReadyPods != 0 {
				t.Fatal("unverified ancestry counted as healthy")
			}
		})
	}
}
func TestEndpointPagingDeduplicatesOrFailsClosed(t *testing.T) {
	for _, kind := range []string{"identical", "conflict", "rv-change", "repeat-token", "page-bound"} {
		t.Run(kind, func(t *testing.T) {
			f := newEndpointFixture(t)
			page := f.pages[""].DeepCopy()
			page.Items[0].Name = "second-slice"
			page.Items[0].UID = "second-slice-uid"
			f.pages[""].Continue = "next"
			f.pages["next"] = page
			switch kind {
			case "conflict":
				page.Items[0].Endpoints[0].Conditions.Ready = deploymentPtr(false)
				page.Items[0].Endpoints[0].Conditions.Serving = deploymentPtr(false)
			case "rv-change":
				page.ResourceVersion = "101"
			case "repeat-token":
				page.Continue = "next"
			case "page-bound":
				l, _ := endpointLimits(EndpointLimits{})
				l.MaxPages = 1
				f.request.Limits = l
			}
			snap, err := f.binding.ObserveEndpoints(context.Background(), f.request)
			if err != nil {
				t.Fatal(err)
			}
			v := snap.View()
			if kind == "identical" {
				if !v.Complete || v.Counts.ReadyPods != 1 || v.Counts.VerifiedPods != 1 || v.Counts.DuplicateEntries != 1 || f.podCalls != 1 || f.rsCalls != 1 {
					t.Fatalf("duplicate inflated/repeated proof: %+v", v)
				}
			} else if v.Complete || v.PublicationPresent || v.Counts.ReadyPods != 0 {
				t.Fatal("partial/conflicting pages reported publication")
			}
		})
	}
}
func TestEndpointEndRechecksAndFrozenDesired(t *testing.T) {
	f := newEndpointFixture(t)
	f.beforeList = func(_ *http.Request) {
		f.request.Service.Desired.Spec.Selector["app"] = "changed-after-freeze"
		f.request.Deployment.Desired.Spec.Template.Spec.Containers[0].Image = "changed-after-freeze"
	}
	snap, err := f.binding.ObserveEndpoints(context.Background(), f.request)
	if err != nil || !snap.View().Complete {
		t.Fatal("caller mutation escaped frozen desired", err)
	}
	for _, kind := range []string{"service-body", "deployment-body", "parent-uid", "deployment-generation"} {
		t.Run(kind, func(t *testing.T) {
			f := newEndpointFixture(t)
			f.afterList = func() {
				switch kind {
				case "service-body":
					f.service.Spec.Selector["app"] = "tampered"
				case "deployment-body":
					f.deployment.Spec.Template.Spec.Containers[0].Image = "tampered"
				case "parent-uid":
					f.parent.UID = "replacement"
				case "deployment-generation":
					f.deployment.Generation++
				}
			}
			snap, err := f.binding.ObserveEndpoints(context.Background(), f.request)
			if err != nil || snap.View().Complete || snap.View().PublicationPresent || snap.View().Code != "EndpointSampleChanged" {
				t.Fatal("changed source passed end recheck", err)
			}
		})
	}
}
func TestEndpointBoundsAndMissingSourceNeverPublish(t *testing.T) {
	f := newEndpointFixture(t)
	f.request.Service.Record.AllocatedSpecDigest = ""
	snap, err := f.binding.ObserveEndpoints(context.Background(), f.request)
	if err != nil || snap.View().Complete || f.listCalls != 0 {
		t.Fatal("missing source receipt reached descendants", err)
	}
	f = newEndpointFixture(t)
	l, _ := endpointLimits(EndpointLimits{})
	l.MaxBytes = 1
	f.request.Limits = l
	snap, err = f.binding.ObserveEndpoints(context.Background(), f.request)
	if err != nil || snap.View().Complete || snap.View().State != "Incomplete" || f.listCalls != 0 {
		t.Fatal("byte bound failed", err)
	}
	f = newEndpointFixture(t)
	f.request.Limits = EndpointLimits{MaxPages: 100}
	_, err = f.binding.ObserveEndpoints(context.Background(), f.request)
	requireCode(t, err, EndpointInvalid)
	f = newEndpointFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	snap, err = f.binding.ObserveEndpoints(ctx, f.request)
	if err != nil || snap.View().Complete || snap.View().PublicationPresent {
		t.Fatal("cancelled sample published", err)
	}
}
