package kubernetes

import (
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func containerResources(c project.Component) (corev1.ResourceRequirements, error) {
	result := corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("256Mi")}}
	if c.Resources == nil {
		return result, nil
	}
	fill := func(values *project.ResourceQuantities, target corev1.ResourceList) error {
		if values == nil {
			return nil
		}
		for key, value := range map[corev1.ResourceName]*string{corev1.ResourceCPU: values.Cpu, corev1.ResourceMemory: values.Memory} {
			if value == nil {
				continue
			}
			quantity, err := resource.ParseQuantity(*value)
			if err != nil || quantity.Sign() < 0 {
				return renderError("InvalidResourceQuantity", "resources", "Resource quantities must be valid nonnegative Kubernetes quantities.")
			}
			target[key] = quantity
		}
		return nil
	}
	if err := fill(c.Resources.Requests, result.Requests); err != nil {
		return corev1.ResourceRequirements{}, err
	}
	if c.Resources.Limits != nil {
		result.Limits = corev1.ResourceList{}
		if err := fill(c.Resources.Limits, result.Limits); err != nil {
			return corev1.ResourceRequirements{}, err
		}
		for key, limit := range result.Limits {
			if request, ok := result.Requests[key]; ok && limit.Cmp(request) < 0 {
				return corev1.ResourceRequirements{}, renderError("ResourceLimitBelowRequest", "resources.limits", "A resource limit is below its effective request.")
			}
		}
	}
	return result, nil
}
