package kubernetes

import (
	"encoding/json"
	"math"
	"regexp"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation"
)

var immutableImage = regexp.MustCompile(`^[a-z0-9][a-z0-9./:_-]*(?:@sha256:[a-f0-9]{64}|:git-[a-f0-9]{7,40})$`)

// RenderCandidate emits isolated workload resources only. Namespace bootstrap
// and traffic promotion are deliberately separate phases. The returned intents
// authorize no mutation by themselves: provider adapters must validate the
// current parent UID and enforce the NamespacedContext preconditions.
func RenderCandidate(authored project.Document, b Bindings) (Bundle, error) {
	if len(authored.Spec.Components) > 256 {
		return Bundle{}, renderError("RenderingLimit", "spec.components", "A single runtime plan may contain at most 256 components.")
	}
	document, err := project.Prepare(authored)
	if err != nil {
		return Bundle{}, renderError("InvalidProject", "project", "Project authoring validation failed; inspect the separate project validation issues.")
	}
	if err = validateBinding(document, b); err != nil {
		return Bundle{}, err
	}
	if document.Metadata.Annotations["registry.tmprl-demo.cloud/catalog"] == "true" {
		return Bundle{}, unsupported("platform.selfManagement")
	}
	if document.Spec.Rollout != nil {
		for build, image := range document.Spec.Rollout.ImageOverrides {
			if b.Images[build] != image {
				return Bundle{}, renderError("RequestedImageBindingMismatch", "spec.rollout.imageOverrides", "A requested image override differs from its explicit trusted resolved image binding.")
			}
		}
	}
	for _, name := range keys(document.Spec.Components) {
		c := document.Spec.Components[name]
		field := "spec.components." + name
		if c.Replicas != nil && (*c.Replicas < 0 || int64(*c.Replicas) > math.MaxInt32) {
			return Bundle{}, renderError("ReplicaRange", "spec.components."+name+".replicas", "Replica counts must fit Kubernetes's nonnegative int32 field.")
		}
		if c.Serverless != nil {
			return Bundle{}, unsupported(field + ".serverless")
		}
		if c.IsolatedExecutor != nil && *c.IsolatedExecutor {
			return Bundle{}, unsupported(field + ".isolatedExecutor")
		}
		if versioned(c) && b.WorkerController == nil {
			return Bundle{}, unsupported(field + ".worker")
		}
		if needsAutoscaling(authored, authored.Spec.Components[name]) && !b.AllowAutoscaling {
			return Bundle{}, unsupported(field + ".autoscaling")
		}
	}
	namespace, parentUID, owner := b.Namespace.Scope().Namespace, b.Namespace.Parent().UID, b.Namespace.Owner()
	bundle := Bundle{DeferredPromotion: document.Spec.Ingress != nil}
	seenObjects := map[string]string{}
	add := func(object Object, purpose string) error {
		desired, err := desired(object, purpose, b)
		if err != nil {
			return err
		}
		key := object.GetObjectKind().GroupVersionKind().String() + "/" + object.GetNamespace() + "/" + object.GetName()
		if fingerprint, exists := seenObjects[key]; exists {
			if fingerprint != desired.Intent.Intent.SpecFingerprint {
				return renderError("ConflictingResourceIntent", "objects", "Two capabilities rendered incompatible desired state for one object.")
			}
			return nil
		}
		seenObjects[key] = desired.Intent.Intent.SpecFingerprint
		bundle.Objects = append(bundle.Objects, desired)
		return nil
	}
	data, _ := json.Marshal(map[string]string{"project": document.Metadata.Name, "namespace": namespace, "revision": b.Revision, "sourceRepo": document.Spec.Source.Repo})
	config := &corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}, ObjectMeta: metadata("platform-project-metadata", namespace, parentUID, owner, document, "", b), Data: map[string]string{"project.json": string(data)}}
	if err = add(config, "configmap"); err != nil {
		return Bundle{}, err
	}
	requirements := map[string]SecretRequirement{}
	for _, name := range keys(document.Spec.Components) {
		c := document.Spec.Components[name]
		if len(validation.IsDNS1123Label(name)) > 0 {
			return Bundle{}, renderError("InvalidObjectName", "spec.components", "A logical component name is not a valid Kubernetes object name.")
		}
		image, ok := b.Images[string(c.Build)]
		if !ok || len(image) > 512 || !immutableImage.MatchString(image) {
			return Bundle{}, renderError("ImmutableImageRequired", "spec.components."+name+".build", "Every Kubernetes component requires an explicit immutable build image binding.")
		}
		automount := c.AutomountServiceAccountToken != nil && *c.AutomountServiceAccountToken
		if automount && !b.ApprovedTokenAutomount[name] {
			return Bundle{}, renderError("UnapprovedServiceAccountToken", "spec.components."+name+".automountServiceAccountToken", "Service account token automount requires an explicit trusted component approval.")
		}
		sa := &corev1.ServiceAccount{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"}, ObjectMeta: metadata(name, namespace, parentUID, owner, document, name, b), AutomountServiceAccountToken: ptr(automount)}
		if c.AWSIAMRoleARN != nil {
			if b.ApprovedIAMRoles[name] != *c.AWSIAMRoleARN {
				return Bundle{}, renderError("UnapprovedIAMRole", "spec.components."+name+".awsIamRoleArn", "The requested IAM role does not match a trusted component role binding.")
			}
			sa.Annotations["eks.amazonaws.com/role-arn"] = *c.AWSIAMRoleARN
		}
		if err = add(sa, "service-account"); err != nil {
			return Bundle{}, err
		}
		env, envFrom, err := environment(document, name, c, b, requirements)
		if err != nil {
			return Bundle{}, err
		}
		resources, err := containerResources(c)
		if err != nil {
			return Bundle{}, err
		}
		container := corev1.Container{Name: name, Image: image, ImagePullPolicy: corev1.PullIfNotPresent, Command: append([]string(nil), c.Command...), Args: append([]string(nil), c.Args...), Env: env, EnvFrom: envFrom, Resources: resources}
		for _, port := range c.Ports {
			container.Ports = append(container.Ports, corev1.ContainerPort{Name: string(*port.Name), ContainerPort: int32(port.ContainerPort), Protocol: corev1.ProtocolTCP})
		}
		if len(c.Ports) > 0 {
			container.ReadinessProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt32(int32(c.Ports[0].ContainerPort))}}, InitialDelaySeconds: 5, PeriodSeconds: 5, FailureThreshold: 30}
		}
		pod := corev1.PodSpec{ServiceAccountName: name, AutomountServiceAccountToken: ptr(automount), Containers: []corev1.Container{container}, DNSConfig: &corev1.PodDNSConfig{Options: []corev1.PodDNSConfigOption{{Name: "ndots", Value: ptr("2")}, {Name: "single-request-reopen"}}}}
		if c.TerminationGracePeriodSeconds != nil {
			pod.TerminationGracePeriodSeconds = ptr(int64(*c.TerminationGracePeriodSeconds))
		}
		if err = decoratePod(document, name, c, &pod, b, requirements); err != nil {
			return Bundle{}, err
		}
		selector := labels(owner, document.Metadata.Name, name, b.Revision)
		replicas := int32(1)
		if c.Replicas != nil {
			replicas = int32(*c.Replicas)
		}
		deployment := &appsv1.Deployment{TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"}, ObjectMeta: metadata(name, namespace, parentUID, owner, document, name, b), Spec: appsv1.DeploymentSpec{Replicas: &replicas, Selector: &metav1.LabelSelector{MatchLabels: selector}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels(owner, document.Metadata.Name, name, b.Revision)}, Spec: pod}}}
		autoscaled := needsAutoscaling(authored, authored.Spec.Components[name])
		workerName := ""
		if versioned(c) {
			input, inputErr := validateGateInput(name, c, b, requirements)
			if inputErr != nil {
				return Bundle{}, inputErr
			}
			if input != nil {
				bundle.RequiredConfigInputs = append(bundle.RequiredConfigInputs, *input)
			}
			objects, assigned, workerErr := renderWorker(document, name, c, pod, b, requirements, autoscaled)
			if workerErr != nil {
				return Bundle{}, workerErr
			}
			workerName = assigned
			for _, object := range objects {
				purpose := "worker-deployment"
				if object.GetObjectKind().GroupVersionKind().Kind == "Connection" {
					purpose = "connection"
				}
				if err = add(object, purpose); err != nil {
					return Bundle{}, err
				}
			}
		} else {
			if autoscaled {
				deployment.Spec.Replicas = nil
			}
			if err = add(deployment, "deployment"); err != nil {
				return Bundle{}, err
			}
		}
		if autoscaled {
			scaled, scaleErr := renderAutoscaling(document, name, document.Spec.Components[name], workerName, b)
			if scaleErr != nil {
				return Bundle{}, scaleErr
			}
			purpose := "scaled-object"
			if workerName != "" {
				purpose = "worker-resource-template"
			}
			if err = add(scaled, purpose); err != nil {
				return Bundle{}, err
			}
		}
		if len(c.Ports) > 0 {
			serviceName := name
			if c.ServiceName != nil {
				serviceName = string(*c.ServiceName)
			}
			service := &corev1.Service{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"}, ObjectMeta: metadata(serviceName, namespace, parentUID, owner, document, name, b), Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP, Selector: labels(owner, document.Metadata.Name, name, b.Revision)}}
			for _, port := range c.Ports {
				service.Spec.Ports = append(service.Spec.Ports, corev1.ServicePort{Name: string(*port.Name), Port: int32(*port.ServicePort), TargetPort: intstr.FromInt32(int32(port.ContainerPort)), Protocol: corev1.ProtocolTCP})
			}
			if err = add(service, "service"); err != nil {
				return Bundle{}, err
			}
		}
	}
	for _, name := range keys(requirements) {
		bundle.RequiredSecrets = append(bundle.RequiredSecrets, requirements[name])
	}
	return bundle, nil
}

func needsAutoscaling(d project.Document, c project.Component) bool {
	if c.Replicas != nil {
		return false
	}
	if c.Autoscaling != nil {
		return c.Autoscaling.Enabled == nil || *c.Autoscaling.Enabled
	}
	mode := "apiKey"
	if d.Spec.Temporal != nil && d.Spec.Temporal.Auth != nil && d.Spec.Temporal.Auth.Mode != nil {
		mode = *d.Spec.Temporal.Auth.Mode
	}
	return c.Worker != nil && c.Worker.Enabled && project.TaskQueue(d, c) != "" && mode == "apiKey" && (c.TemporalProxy == nil || !c.TemporalProxy.Enabled)
}
