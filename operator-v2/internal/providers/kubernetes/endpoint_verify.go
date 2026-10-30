package kubernetes

import (
	"net/netip"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	validation "k8s.io/apimachinery/pkg/util/validation"
)

type endpointConditions struct{ ready, serving, terminating bool }

func effectiveEndpointConditions(c discoveryv1.EndpointConditions) (endpointConditions, bool) {
	result := endpointConditions{ready: true, serving: true}
	if c.Ready != nil {
		result.ready = *c.Ready
	}
	if c.Serving != nil {
		result.serving = *c.Serving
	}
	if c.Terminating != nil {
		result.terminating = *c.Terminating
	}
	return result, c.Ready == nil || c.Serving == nil || c.Terminating == nil
}
func endpointController(meta metav1.ObjectMeta, kind, version, name, uid string) bool {
	if len(meta.OwnerReferences) != 1 {
		return false
	}
	r := meta.OwnerReferences[0]
	return r.APIVersion == version && r.Kind == kind && r.Name == name && string(r.UID) == uid && r.Controller != nil && *r.Controller
}
func endpointSliceMatches(s *discoveryv1.EndpointSlice, service *corev1.Service) bool {
	if s == nil || s.Namespace != service.Namespace || s.Name == "" || s.UID == "" || s.ResourceVersion == "" || s.DeletionTimestamp != nil || s.APIVersion != "" && s.APIVersion != "discovery.k8s.io/v1" || s.Kind != "" && s.Kind != "EndpointSlice" || s.Labels[discoveryv1.LabelServiceName] != service.Name || s.Labels[discoveryv1.LabelManagedBy] != "endpointslice-controller.k8s.io" || !endpointController(s.ObjectMeta, "Service", "v1", service.Name, string(service.UID)) {
		return false
	}
	if len(service.Spec.IPFamilies) != 1 || string(s.AddressType) != string(service.Spec.IPFamilies[0]) {
		return false
	}
	// The pinned controller creates an empty zero-port placeholder when there
	// are no endpoint groups (v1.37 endpointslice/reconciler.go:241-249). It
	// proves no eligible endpoints; all source/owner/family checks above still
	// apply. Nonempty endpoints always require the full target-port set.
	if len(s.Endpoints) == 0 && len(s.Ports) == 0 {
		return true
	}
	if len(s.Ports) != len(service.Spec.Ports) {
		return false
	}
	expected := map[string]corev1.ServicePort{}
	for _, p := range service.Spec.Ports {
		expected[p.Name] = p
	}
	seen := map[string]bool{}
	for _, p := range s.Ports {
		if p.Name == nil || p.Protocol == nil || p.Port == nil || p.AppProtocol != nil || seen[*p.Name] {
			return false
		}
		want, ok := expected[*p.Name]
		if !ok || *p.Protocol != corev1.ProtocolTCP || *p.Port != want.TargetPort.IntVal {
			return false
		}
		seen[*p.Name] = true
	}
	return true
}
func endpointPodRef(e discoveryv1.Endpoint, namespace string) (string, string, bool) {
	r := e.TargetRef
	if r == nil || r.Kind != "Pod" || r.APIVersion != "" && r.APIVersion != "v1" || r.Namespace != namespace || r.UID == "" || len(validation.IsDNS1123Subdomain(r.Name)) != 0 || r.FieldPath != "" {
		return "", "", false
	}
	return r.Name, string(r.UID), true
}
func endpointPodAddress(e discoveryv1.Endpoint, pod *corev1.Pod, family corev1.IPFamily) bool {
	if len(e.Addresses) != 1 {
		return false
	}
	address, err := netip.ParseAddr(e.Addresses[0])
	if err != nil || !address.IsGlobalUnicast() || address.Is4In6() || address.String() != e.Addresses[0] || address.Is4() != (family == corev1.IPv4Protocol) {
		return false
	}
	count := 0
	for _, item := range pod.Status.PodIPs {
		ip, err := netip.ParseAddr(item.IP)
		if err != nil || ip.Is4In6() {
			return false
		}
		if ip.Is4() == address.Is4() {
			count++
			if ip != address {
				return false
			}
		}
	}
	return count == 1
}
func endpointPodReady(p *corev1.Pod) (bool, bool) {
	seen := map[corev1.PodConditionType]bool{}
	ready := false
	for _, c := range p.Status.Conditions {
		if seen[c.Type] {
			return false, false
		}
		seen[c.Type] = true
		if c.Type == corev1.PodReady {
			if c.Status != corev1.ConditionTrue && c.Status != corev1.ConditionFalse && c.Status != corev1.ConditionUnknown {
				return false, false
			}
			ready = c.Status == corev1.ConditionTrue
		}
	}
	if ready && p.Status.Phase != corev1.PodRunning {
		return false, false
	}
	return ready, true
}

// This checks current selector/template-LABEL relations only, never PodSpec or
// full descendant payload equivalence. Controllers may adopt objects; matching
// owner refs/hash labels are not authenticated proof of original creation.
func endpointReplicaSetMatches(rs *appsv1.ReplicaSet, d *appsv1.Deployment) bool {
	if rs == nil || rs.Namespace != d.Namespace || rs.UID == "" || rs.ResourceVersion == "" || rs.DeletionTimestamp != nil || rs.APIVersion != "" && rs.APIVersion != "apps/v1" || rs.Kind != "" && rs.Kind != "ReplicaSet" || !endpointController(rs.ObjectMeta, "Deployment", "apps/v1", d.Name, string(d.UID)) || rs.Spec.Selector == nil || len(rs.Spec.Selector.MatchExpressions) != 0 {
		return false
	}
	expected := map[string]string{}
	for k, v := range d.Spec.Selector.MatchLabels {
		expected[k] = v
	}
	hash := rs.Spec.Selector.MatchLabels[appsv1.DefaultDeploymentUniqueLabelKey]
	if hash == "" {
		return false
	}
	expected[appsv1.DefaultDeploymentUniqueLabelKey] = hash
	if !sameDeploymentJSON(expected, rs.Spec.Selector.MatchLabels) {
		return false
	}
	for k, v := range expected {
		if rs.Spec.Template.Labels[k] != v {
			return false
		}
	}
	return true
}
func endpointPodMembership(p *corev1.Pod, rs *appsv1.ReplicaSet, s *corev1.Service) bool {
	if p == nil || p.Namespace != s.Namespace || p.UID == "" || p.ResourceVersion == "" || p.APIVersion != "" && p.APIVersion != "v1" || p.Kind != "" && p.Kind != "Pod" || !endpointController(p.ObjectMeta, "ReplicaSet", "apps/v1", rs.Name, string(rs.UID)) {
		return false
	}
	for k, v := range s.Spec.Selector {
		if p.Labels[k] != v {
			return false
		}
	}
	for k, v := range rs.Spec.Selector.MatchLabels {
		if p.Labels[k] != v {
			return false
		}
	}
	return true
}

type endpointVersion struct{ Kind, UID, Version string }

func endpointSampleDigest(versions []endpointVersion) string {
	sort.Slice(versions, func(i, j int) bool {
		if versions[i].Kind != versions[j].Kind {
			return versions[i].Kind < versions[j].Kind
		}
		return versions[i].UID < versions[j].UID
	})
	sum, _ := serviceDigest(versions)
	return sum
}
