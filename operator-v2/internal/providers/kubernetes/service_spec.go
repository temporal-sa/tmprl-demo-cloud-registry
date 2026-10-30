package kubernetes

import (
	"net/netip"
	"regexp"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	validation "k8s.io/apimachinery/pkg/util/validation"
)

var serviceReceiptDigest = regexp.MustCompile(`^[a-f0-9]{64}$`)

// Closed v1.37 baseline. Defaults are reviewed from official core/v1/defaults.go
// SetDefaults_Service and registry/core/service/storage/alloc.go. No general
// defaulting, derivative comparison, externally routed/headless/dual-stack
// behavior or mutable Service support is implied. Known typed v1.37 spec
// fields outside this whitelist fail closed. The official SDK can discard wire
// fields unknown to its schema: this is proof for the pinned trusted v1.37 API,
// not arbitrary future wire contracts. Server/client upgrades require review
// and a new actual API proof before enabling the new contract.
func serviceBaseline(object *corev1.Service) (*corev1.Service, error) {
	d := object.DeepCopy()
	s := &d.Spec
	allowed := corev1.ServiceSpec{Type: s.Type, Selector: s.Selector, Ports: s.Ports, SessionAffinity: s.SessionAffinity, InternalTrafficPolicy: s.InternalTrafficPolicy, IPFamilyPolicy: s.IPFamilyPolicy}
	if !sameDeploymentJSON(allowed, *s) || s.Type != corev1.ServiceTypeClusterIP || len(s.Selector) == 0 || len(s.Selector) > 64 || len(s.Ports) == 0 || len(s.Ports) > 64 || s.SessionAffinity != "" && s.SessionAffinity != corev1.ServiceAffinityNone || s.InternalTrafficPolicy != nil && *s.InternalTrafficPolicy != corev1.ServiceInternalTrafficPolicyCluster || s.IPFamilyPolicy != nil && *s.IPFamilyPolicy != corev1.IPFamilyPolicySingleStack {
		return nil, serviceFailure(ServiceUnsupportedSpec)
	}
	for k, v := range s.Selector {
		if len(validation.IsQualifiedName(k)) > 0 || len(validation.IsValidLabelValue(v)) > 0 {
			return nil, serviceFailure(ServiceUnsupportedSpec)
		}
	}
	names := map[string]bool{}
	ports := map[int32]bool{}
	for i := range s.Ports {
		p := &s.Ports[i]
		if !sameDeploymentJSON(*p, corev1.ServicePort{Name: p.Name, Protocol: p.Protocol, Port: p.Port, TargetPort: p.TargetPort}) || len(validation.IsDNS1123Label(p.Name)) > 0 || names[p.Name] || ports[p.Port] || p.Port < 1 || p.Port > 65535 || p.Protocol != "" && p.Protocol != corev1.ProtocolTCP || p.TargetPort.Type != intstr.Int || p.TargetPort.IntVal < 1 || p.TargetPort.IntVal > 65535 || p.TargetPort.StrVal != "" {
			return nil, serviceFailure(ServiceUnsupportedSpec)
		}
		names[p.Name] = true
		ports[p.Port] = true
		p.Protocol = corev1.ProtocolTCP
	}
	s.SessionAffinity = corev1.ServiceAffinityNone
	policy := corev1.ServiceInternalTrafficPolicyCluster
	s.InternalTrafficPolicy = &policy
	familyPolicy := corev1.IPFamilyPolicySingleStack
	s.IPFamilyPolicy = &familyPolicy
	return d, nil
}

// Allow allocation only after a known POST; all nonallocation fields must still
// exactly equal the original defaulted desired body. Subsequent reads ALSO
// require the persisted full-spec digest; this shape check alone is not proof.
func serviceAllocatedMatches(actual, expected *corev1.Service) bool {
	if actual == nil || len(actual.Spec.ClusterIPs) != 1 || actual.Spec.ClusterIP != actual.Spec.ClusterIPs[0] || len(actual.Spec.IPFamilies) != 1 {
		return false
	}
	ip, err := netip.ParseAddr(actual.Spec.ClusterIP)
	if err != nil || !ip.IsGlobalUnicast() || ip.Is4In6() || ip.String() != actual.Spec.ClusterIP {
		return false
	}
	family := corev1.IPv6Protocol
	if ip.Is4() {
		family = corev1.IPv4Protocol
	}
	if actual.Spec.IPFamilies[0] != family {
		return false
	}
	copy := actual.Spec.DeepCopy()
	copy.ClusterIP = ""
	copy.ClusterIPs = nil
	copy.IPFamilies = nil
	return sameDeploymentJSON(*copy, expected.Spec)
}
func serviceMetadataMatches(actual, expected *corev1.Service) bool {
	return actual != nil && actual.Name == expected.Name && actual.Namespace == expected.Namespace && (actual.APIVersion == "" || actual.APIVersion == "v1") && (actual.Kind == "" || actual.Kind == "Service") && actual.UID != "" && actual.ResourceVersion != "" && actual.GenerateName == "" && actual.DeletionTimestamp == nil && len(actual.Finalizers) == 0 && sameDeploymentJSON(actual.Labels, expected.Labels) && sameDeploymentJSON(actual.Annotations, expected.Annotations) && sameDeploymentJSON(actual.OwnerReferences, expected.OwnerReferences)
}
