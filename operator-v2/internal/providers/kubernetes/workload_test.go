package kubernetes

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

func literalObjects(t *testing.T) (*appsv1.Deployment, *appsv1.ReplicaSet, *corev1.Pod, NodeBinding) {
	t.Helper()
	f := newDeploymentFixture(t)
	d, ok := literalDeployment(f.desired)
	if !ok {
		t.Fatal("fixture literal contract invalid")
	}
	d.UID = "deployment-canonical"
	rs := &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "backend-abc123", Namespace: d.Namespace, UID: "rs-canonical", ResourceVersion: "20", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: d.Name, UID: d.UID, Controller: deploymentPtr(true), BlockOwnerDeletion: deploymentPtr(true)}}}, Spec: appsv1.ReplicaSetSpec{Selector: d.Spec.Selector.DeepCopy(), Template: *d.Spec.Template.DeepCopy(), MinReadySeconds: d.Spec.MinReadySeconds}}
	rs.Spec.Selector.MatchLabels[appsv1.DefaultDeploymentUniqueLabelKey] = "abc123"
	rs.Spec.Template.Labels[appsv1.DefaultDeploymentUniqueLabelKey] = "abc123"
	rs.Labels = map[string]string{}
	for k, v := range rs.Spec.Template.Labels {
		rs.Labels[k] = v
	}
	node := NodeBinding{Name: "owned-control-plane", UID: "node-canonical"}
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: rs.Name + "-abcde", GenerateName: rs.Name + "-", Namespace: d.Namespace, UID: "pod-canonical", ResourceVersion: "30", Labels: rs.Spec.Template.DeepCopy().Labels, OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: deploymentPtr(true), BlockOwnerDeletion: deploymentPtr(true)}}}, Spec: literalPodSpec(d, node.Name)}
	return d, rs, p, node
}

func TestLiteralWorkloadFullTemplateAndPodDeclaration(t *testing.T) {
	d, rs, p, node := literalObjects(t)
	if !literalReplicaSet(rs, d) || !literalPod(p, rs, d, node) {
		t.Fatal("closed literal baseline rejected")
	}
	before, _ := json.Marshal(d)
	for name, change := range map[string]func(*corev1.Pod){
		"env": func(p *corev1.Pod) {
			p.Spec.Containers[0].Env = append(p.Spec.Containers[0].Env, corev1.EnvVar{Name: "INJECTED", Value: "PRIVATE_LITERAL"})
		},
		"command": func(p *corev1.Pod) { p.Spec.Containers[0].Command = []string{"injected"} },
		"image":   func(p *corev1.Pod) { p.Spec.Containers[0].Image = "other@sha256:" + strings.Repeat("f", 64) },
		"sidecar": func(p *corev1.Pod) { p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: "sidecar"}) },
		"volume":  func(p *corev1.Pod) { p.Spec.Volumes = []corev1.Volume{{Name: "injected"}} },
		"toleration": func(p *corev1.Pod) {
			p.Spec.Tolerations = append(p.Spec.Tolerations, corev1.Toleration{Key: "injected", Operator: corev1.TolerationOpExists})
		},
		"node":     func(p *corev1.Pod) { p.Spec.NodeName = "untrusted-node" },
		"owner":    func(p *corev1.Pod) { p.OwnerReferences[0].UID = "copied-foreign-owner" },
		"label":    func(p *corev1.Pod) { p.Labels["injected"] = "yes" },
		"security": func(p *corev1.Pod) { p.Spec.HostNetwork = true },
	} {
		t.Run(name, func(t *testing.T) {
			changed := p.DeepCopy()
			change(changed)
			if literalPod(changed, rs, d, node) {
				t.Fatal("full Pod deviation accepted")
			}
		})
	}
	changed := rs.DeepCopy()
	changed.Spec.Template.Spec.Containers[0].Args = []string{"adopted-payload"}
	if literalReplicaSet(changed, d) {
		t.Fatal("matching ancestry hid adopted RS payload")
	}
	// An empty foreign RS can retain a candidate-matching template while its
	// top-level labels differ. It remains relevant and must fail owner/template
	// verification rather than disappear from the namespace sample.
	foreign := rs.DeepCopy()
	foreign.OwnerReferences[0].UID = "foreign-deployment"
	foreign.Labels = map[string]string{"unrelated": "label"}
	if !literalRelevantReplicaSet(foreign, string(d.UID), labels.SelectorFromSet(d.Spec.Selector.MatchLabels)) || literalReplicaSet(foreign, d) {
		t.Fatal("foreign matching RS template concealed by top-level labels")
	}
	after, _ := json.Marshal(d)
	if string(before) != string(after) {
		t.Fatal("comparison mutated original input")
	}
}

func TestLiteralWorkloadRejectsDependenciesBeforeObservation(t *testing.T) {
	d, _, _, _ := literalObjects(t)
	for name, change := range map[string]func(*appsv1.Deployment){
		"secret": func(d *appsv1.Deployment) {
			d.Spec.Template.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "REF", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "secret"}, Key: "value"}}}}
		},
		"field": func(d *appsv1.Deployment) {
			d.Spec.Template.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "REF", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}}}
		},
		"envFrom": func(d *appsv1.Deployment) {
			d.Spec.Template.Spec.Containers[0].EnvFrom = []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "secret"}}}}
		},
		"volume":    func(d *appsv1.Deployment) { d.Spec.Template.Spec.Volumes = []corev1.Volume{{Name: "secret"}} },
		"scheduler": func(d *appsv1.Deployment) { d.Spec.Template.Spec.SchedulerName = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			copy := d.DeepCopy()
			change(copy)
			if _, ok := literalDeployment(copy); ok {
				t.Fatal("dependency or custom spec accepted")
			}
		})
	}
}

func TestLiteralWorkloadPodOnlyDefaultsAreExact(t *testing.T) {
	d, rs, p, node := literalObjects(t)
	d.Spec.Template.Spec.Containers[0].Resources = corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("200m"), corev1.ResourceMemory: resource.MustParse("32Mi")}}
	p.Spec = literalPodSpec(d, node.Name)
	if p.Spec.Containers[0].Resources.Requests.Cpu().String() != "200m" || !literalPod(p, rs, d, node) {
		t.Fatal("requests did not follow explicit limits")
	}
	p.Spec.Tolerations[0].TolerationSeconds = deploymentPtr[int64](299)
	if literalPod(p, rs, d, node) {
		t.Fatal("arbitrary toleration default accepted")
	}
	p.Spec = literalPodSpec(d, "")
	if !literalPod(p, rs, d, node) {
		t.Fatal("unassigned declared Pod rejected")
	}
}

func TestLiteralWorkloadPrivateInputsAndNoAuthorityUpgrade(t *testing.T) {
	r := LiteralWorkloadRead{}
	r.Deployment.Desired = &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Env: []corev1.EnvVar{{Name: "PRIVATE_LITERAL"}}}}}}}}
	data, _ := json.Marshal(r)
	if strings.Contains(string(data)+fmt.Sprintf("%+v", r), "PRIVATE_LITERAL") {
		t.Fatal("private desired escaped")
	}
	v := literalResult(EndpointView{Complete: true}, &literalSample{pods: map[string]*corev1.Pod{"uid": {}}, sets: map[string]*appsv1.ReplicaSet{}}, "Observed", "LiteralObserved").View()
	if !v.DeclarationsEqual || v.CreatorOriginVerified || v.ProcessValuesVerified || v.PlacementAttested || v.PromotionAuthorized || v.Publication.PayloadVerified {
		t.Fatal("declaration proof upgraded authority")
	}
	if (LiteralWorkloadSnapshot{}).View().Complete {
		t.Fatal("zero snapshot asserted completion")
	}
	first := literalResult(EndpointView{Complete: true}, &literalSample{pods: map[string]*corev1.Pod{"first": {}}}, "Observed", "LiteralObserved")
	other := literalResult(EndpointView{Complete: true}, &literalSample{pods: map[string]*corev1.Pod{"other": {}}}, "Observed", "LiteralObserved")
	if first.SamePodIdentities(other) || !first.SamePodIdentities(first) || first.SamePodIdentities(LiteralWorkloadSnapshot{}) {
		t.Fatal("public counts or a missing snapshot substituted for private identity")
	}
}
