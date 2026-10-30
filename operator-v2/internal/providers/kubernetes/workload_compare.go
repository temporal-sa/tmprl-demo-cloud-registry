package kubernetes

import (
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

const literalServerCommit = "f54c212e3a2f75d674b717a9b29052b20b60aefc"

func literalDeployment(d *appsv1.Deployment) (*appsv1.Deployment, bool) {
	if d == nil {
		return nil, false
	}
	normal, err := deploymentBaseline(d)
	if err != nil {
		return nil, false
	}
	p := &normal.Spec.Template.Spec
	if len(normal.Spec.Template.Annotations) != 0 {
		return nil, false
	}
	for _, c := range p.Containers {
		if len(c.EnvFrom) != 0 {
			return nil, false
		}
		for _, e := range c.Env {
			if e.ValueFrom != nil {
				return nil, false
			}
		}
	}
	return normal, true
}

func literalReplicaSet(rs *appsv1.ReplicaSet, d *appsv1.Deployment) bool {
	if !endpointReplicaSetMatches(rs, d) || rs.Spec.MinReadySeconds != d.Spec.MinReadySeconds || rs.Spec.Template.Labels == nil || rs.OwnerReferences[0].BlockOwnerDeletion == nil || !*rs.OwnerReferences[0].BlockOwnerDeletion || !sameDeploymentJSON(rs.Labels, rs.Spec.Template.Labels) {
		return false
	}
	template := rs.Spec.Template.DeepCopy()
	hash := rs.Spec.Selector.MatchLabels[appsv1.DefaultDeploymentUniqueLabelKey]
	if template.Labels[appsv1.DefaultDeploymentUniqueLabelKey] != hash || len(hash) > 63 || strings.Trim(hash, "abcdefghijklmnopqrstuvwxyz0123456789") != "" {
		return false
	}
	delete(template.Labels, appsv1.DefaultDeploymentUniqueLabelKey)
	return sameDeploymentJSON(*template, d.Spec.Template)
}

// Closed Pod-only transforms for Kubernetes v1.37.0, commit above:
// pkg/apis/core/v1/defaults.go (requests from limits, enableServiceLinks),
// plugin/pkg/admission/priority (zero/PreemptLowerPriority),
// plugin/pkg/admission/defaulttolerationseconds (the two 300s tolerations).
// This is a comparison contract, NOT an attestation of installed admission.
// Actual full-spec equality rejects other defaults/mutations; unknown features
// and external dependencies cannot silently widen this literal-only baseline.
func literalPodSpec(d *appsv1.Deployment, nodeName string) corev1.PodSpec {
	p := *d.Spec.Template.Spec.DeepCopy()
	p.EnableServiceLinks = deploymentPtr(true)
	p.Priority = deploymentPtr[int32](0)
	p.PreemptionPolicy = deploymentPtr(corev1.PreemptLowerPriority)
	p.Tolerations = []corev1.Toleration{
		{Key: corev1.TaintNodeNotReady, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: deploymentPtr[int64](300)},
		{Key: corev1.TaintNodeUnreachable, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: deploymentPtr[int64](300)},
	}
	p.NodeName = nodeName
	for i := range p.Containers {
		c := &p.Containers[i]
		if c.Resources.Requests == nil && len(c.Resources.Limits) > 0 {
			c.Resources.Requests = corev1.ResourceList{}
		}
		for k, v := range c.Resources.Limits {
			if _, ok := c.Resources.Requests[k]; !ok {
				c.Resources.Requests[k] = v.DeepCopy()
			}
		}
	}
	return p
}
func literalPod(p *corev1.Pod, rs *appsv1.ReplicaSet, d *appsv1.Deployment, node NodeBinding) bool {
	if p == nil || p.DeletionTimestamp != nil || p.UID == "" || p.ResourceVersion == "" || p.Namespace != d.Namespace || !endpointController(p.ObjectMeta, "ReplicaSet", "apps/v1", rs.Name, string(rs.UID)) || p.OwnerReferences[0].BlockOwnerDeletion == nil || !*p.OwnerReferences[0].BlockOwnerDeletion || p.GenerateName != rs.Name+"-" || !strings.HasPrefix(p.Name, p.GenerateName) || !sameDeploymentJSON(p.Labels, rs.Spec.Template.Labels) || !sameDeploymentJSON(p.Annotations, d.Spec.Template.Annotations) || !sameDeploymentJSON(p.Finalizers, d.Spec.Template.Finalizers) {
		return false
	}
	if p.Spec.NodeName != "" && p.Spec.NodeName != node.Name {
		return false
	}
	return sameDeploymentJSON(p.Spec, literalPodSpec(d, p.Spec.NodeName))
}
