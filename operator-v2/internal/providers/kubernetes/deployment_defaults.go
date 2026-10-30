package kubernetes

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

var deploymentImage = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]*(?::[0-9]+)?(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)+@sha256:[a-f0-9]{64}$`)

// This is deliberately a CLOSED v1.37 PodTemplate baseline, not a Kubernetes
// defaulting engine. Sources: kubernetes/kubernetes v1.37.0 pkg/apis/apps/v1/
// defaults.go + zz_generated.defaults.go and pkg/apis/core/v1/defaults.go.
// Pod-only defaults (service links, admission tolerations, requests=limits,
// default SA/token volumes) are NOT PodTemplate defaults and are not excused.
// Upgrading server/client or adding any feature requires an actual API fixture.
// Compare complete JSON, not DeepDerivative or selected-field hash equality.
func deploymentBaseline(object *appsv1.Deployment) (*appsv1.Deployment, error) {
	d := object.DeepCopy()
	s := &d.Spec
	p := &s.Template.Spec
	if s.Paused {
		return nil, deploymentFailure(DeploymentUnsupportedSpec)
	}
	if s.Selector == nil || len(s.Selector.MatchLabels) == 0 || len(s.Selector.MatchExpressions) != 0 {
		return nil, deploymentFailure(DeploymentUnsupportedSpec)
	}
	for k, v := range s.Selector.MatchLabels {
		if s.Template.Labels[k] != v {
			return nil, deploymentFailure(DeploymentUnsupportedSpec)
		}
	}
	meta := metav1.ObjectMeta{Labels: s.Template.Labels, Annotations: s.Template.Annotations}
	if !sameDeploymentJSON(meta, s.Template.ObjectMeta) {
		return nil, deploymentFailure(DeploymentUnsupportedSpec)
	}
	if len(p.Containers) != 1 || p.ServiceAccountName == "" || p.AutomountServiceAccountToken == nil || *p.AutomountServiceAccountToken || p.DeprecatedServiceAccount != "" && p.DeprecatedServiceAccount != p.ServiceAccountName || p.SecurityContext != nil && !sameDeploymentJSON(p.SecurityContext, &corev1.PodSecurityContext{}) {
		return nil, deploymentFailure(DeploymentUnsupportedSpec)
	}
	allowed := corev1.PodSpec{Containers: p.Containers, ServiceAccountName: p.ServiceAccountName, DeprecatedServiceAccount: p.DeprecatedServiceAccount, AutomountServiceAccountToken: p.AutomountServiceAccountToken, DNSPolicy: p.DNSPolicy, DNSConfig: p.DNSConfig, RestartPolicy: p.RestartPolicy, SecurityContext: p.SecurityContext, TerminationGracePeriodSeconds: p.TerminationGracePeriodSeconds, SchedulerName: p.SchedulerName}
	if !sameDeploymentJSON(allowed, *p) || p.DNSPolicy != "" && p.DNSPolicy != corev1.DNSClusterFirst || p.RestartPolicy != "" && p.RestartPolicy != corev1.RestartPolicyAlways || p.SchedulerName != "" && p.SchedulerName != corev1.DefaultSchedulerName || p.TerminationGracePeriodSeconds != nil && *p.TerminationGracePeriodSeconds < 0 {
		return nil, deploymentFailure(DeploymentUnsupportedSpec)
	}
	c := &p.Containers[0]
	allowedContainer := corev1.Container{Name: c.Name, Image: c.Image, ImagePullPolicy: c.ImagePullPolicy, Command: c.Command, Args: c.Args, WorkingDir: c.WorkingDir, Env: c.Env, EnvFrom: c.EnvFrom, Resources: c.Resources, Ports: c.Ports, ReadinessProbe: c.ReadinessProbe, TerminationMessagePath: c.TerminationMessagePath, TerminationMessagePolicy: c.TerminationMessagePolicy}
	// Image identity is immutable. The trusted caller independently verifies its
	// registry/build provenance; an arbitrary digest string is not that proof.
	if !sameDeploymentJSON(allowedContainer, *c) || c.Name == "" || !deploymentImage.MatchString(c.Image) || c.ImagePullPolicy != corev1.PullIfNotPresent && c.ImagePullPolicy != corev1.PullAlways && c.ImagePullPolicy != corev1.PullNever || !sameDeploymentJSON(c.Resources, corev1.ResourceRequirements{Requests: c.Resources.Requests, Limits: c.Resources.Limits}) {
		return nil, deploymentFailure(DeploymentUnsupportedSpec)
	}
	for _, list := range []corev1.ResourceList{c.Resources.Requests, c.Resources.Limits} {
		for key, val := range list {
			if key != corev1.ResourceCPU && key != corev1.ResourceMemory || val.Sign() < 0 {
				return nil, deploymentFailure(DeploymentUnsupportedSpec)
			}
		}
	}
	for _, port := range c.Ports {
		if port.HostIP != "" || port.HostPort != 0 || port.Protocol != "" && port.Protocol != corev1.ProtocolTCP {
			return nil, deploymentFailure(DeploymentUnsupportedSpec)
		}
	}
	for _, env := range c.Env {
		if env.ValueFrom != nil {
			source := env.ValueFrom
			count := 0
			if source.SecretKeyRef != nil {
				count++
			}
			if source.ConfigMapKeyRef != nil {
				count++
			}
			if source.FieldRef != nil {
				count++
				if source.FieldRef.APIVersion != "" && source.FieldRef.APIVersion != "v1" {
					return nil, deploymentFailure(DeploymentUnsupportedSpec)
				}
			}
			if count != 1 || source.ResourceFieldRef != nil || source.FileKeyRef != nil || env.Value != "" {
				return nil, deploymentFailure(DeploymentUnsupportedSpec)
			}
		}
	}
	for _, env := range c.EnvFrom {
		if (env.SecretRef == nil) == (env.ConfigMapRef == nil) {
			return nil, deploymentFailure(DeploymentUnsupportedSpec)
		}
	}
	if probe := c.ReadinessProbe; probe != nil {
		if probe.TCPSocket == nil || probe.Exec != nil || probe.HTTPGet != nil || probe.GRPC != nil || probe.TCPSocket.Host != "" || probe.TerminationGracePeriodSeconds != nil || probe.InitialDelaySeconds < 0 || probe.TimeoutSeconds < 0 || probe.PeriodSeconds < 0 || probe.SuccessThreshold < 0 || probe.FailureThreshold < 0 {
			return nil, deploymentFailure(DeploymentUnsupportedSpec)
		}
	}
	if s.Strategy.Type != "" && s.Strategy.Type != appsv1.RollingUpdateDeploymentStrategyType {
		return nil, deploymentFailure(DeploymentUnsupportedSpec)
	}
	if s.Replicas == nil {
		s.Replicas = deploymentPtr[int32](1)
	}
	if *s.Replicas < 0 || s.MinReadySeconds < 0 {
		return nil, deploymentFailure(DeploymentUnsupportedSpec)
	}
	s.Strategy.Type = appsv1.RollingUpdateDeploymentStrategyType
	if s.Strategy.RollingUpdate == nil {
		s.Strategy.RollingUpdate = &appsv1.RollingUpdateDeployment{}
	}
	if s.Strategy.RollingUpdate.MaxUnavailable == nil {
		s.Strategy.RollingUpdate.MaxUnavailable = deploymentPtr(intstr.FromString("25%"))
	}
	if s.Strategy.RollingUpdate.MaxSurge == nil {
		s.Strategy.RollingUpdate.MaxSurge = deploymentPtr(intstr.FromString("25%"))
	}
	if s.RevisionHistoryLimit == nil {
		s.RevisionHistoryLimit = deploymentPtr[int32](10)
	}
	if s.ProgressDeadlineSeconds == nil {
		s.ProgressDeadlineSeconds = deploymentPtr[int32](600)
	}
	if p.DNSPolicy == "" {
		p.DNSPolicy = corev1.DNSClusterFirst
	}
	if p.RestartPolicy == "" {
		p.RestartPolicy = corev1.RestartPolicyAlways
	}
	if p.SecurityContext == nil {
		p.SecurityContext = &corev1.PodSecurityContext{}
	}
	if p.TerminationGracePeriodSeconds == nil {
		p.TerminationGracePeriodSeconds = deploymentPtr[int64](30)
	}
	if p.SchedulerName == "" {
		p.SchedulerName = corev1.DefaultSchedulerName
	}
	p.DeprecatedServiceAccount = p.ServiceAccountName
	if c.TerminationMessagePath == "" {
		c.TerminationMessagePath = corev1.TerminationMessagePathDefault
	}
	if c.TerminationMessagePolicy == "" {
		c.TerminationMessagePolicy = corev1.TerminationMessageReadFile
	}
	for i := range c.Ports {
		if c.Ports[i].Protocol == "" {
			c.Ports[i].Protocol = corev1.ProtocolTCP
		}
	}
	for i := range c.Env {
		if from := c.Env[i].ValueFrom; from != nil && from.FieldRef != nil && from.FieldRef.APIVersion == "" {
			from.FieldRef.APIVersion = "v1"
		}
	}
	for _, list := range []corev1.ResourceList{c.Resources.Requests, c.Resources.Limits} {
		for key, val := range list {
			val.RoundUp(-3)
			list[key] = val
		}
	}
	if probe := c.ReadinessProbe; probe != nil {
		if probe.TimeoutSeconds == 0 {
			probe.TimeoutSeconds = 1
		}
		if probe.PeriodSeconds == 0 {
			probe.PeriodSeconds = 10
		}
		if probe.SuccessThreshold == 0 {
			probe.SuccessThreshold = 1
		}
		if probe.FailureThreshold == 0 {
			probe.FailureThreshold = 3
		}
	}
	return d, nil
}

func deploymentPtr[T any](v T) *T { return &v }
func sameDeploymentJSON(a, b any) bool {
	left, le := json.Marshal(a)
	right, re := json.Marshal(b)
	return le == nil && re == nil && bytes.Equal(left, right)
}

func deploymentMetadataMatches(actual, expected *appsv1.Deployment) bool {
	// Official typed codecs can clear TypeMeta after decoding. The exact apps/v1
	// resource/client supplies type authority; an explicit conflicting GVK fails.
	if actual == nil || actual.Name != expected.Name || actual.Namespace != expected.Namespace || actual.APIVersion != "" && actual.APIVersion != "apps/v1" || actual.Kind != "" && actual.Kind != "Deployment" || actual.UID == "" || actual.ResourceVersion == "" || actual.Generation <= 0 || actual.GenerateName != "" || len(actual.Finalizers) != 0 || !sameDeploymentJSON(actual.Labels, expected.Labels) || !sameDeploymentJSON(actual.OwnerReferences, expected.OwnerReferences) {
		return false
	}
	annotations := actual.Annotations
	copy := map[string]string{}
	for k, v := range annotations {
		if k == "deployment.kubernetes.io/revision" {
			if v == "" || strings.Trim(v, "0123456789") != "" {
				return false
			}
			continue
		}
		copy[k] = v
	}
	return sameDeploymentJSON(copy, expected.Annotations)
}
