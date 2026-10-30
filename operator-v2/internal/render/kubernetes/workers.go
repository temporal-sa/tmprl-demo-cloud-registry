package kubernetes

import (
	"net"
	"strconv"
	"strings"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

const workerAPIVersion = "temporal.io/v1alpha1"

type connectionSpec struct {
	HostPort        string                    `json:"hostPort"`
	APIKeySecretRef *corev1.SecretKeySelector `json:"apiKeySecretRef"`
}
type workerOptions struct {
	ConnectionRef     corev1.LocalObjectReference `json:"connectionRef"`
	TemporalNamespace string                      `json:"temporalNamespace"`
}
type sunset struct {
	ScaledownDelay string `json:"scaledownDelay"`
	DeleteDelay    string `json:"deleteDelay"`
}
type workerSpec struct {
	Replicas      *int32                 `json:"replicas,omitempty"`
	WorkerOptions workerOptions          `json:"workerOptions"`
	Rollout       project.WorkerRollout  `json:"rollout"`
	Sunset        sunset                 `json:"sunset"`
	Template      corev1.PodTemplateSpec `json:"template"`
}

func versioned(c project.Component) bool {
	return c.Worker != nil && c.Worker.Enabled && c.Worker.Versioning != nil && c.Worker.Versioning.Enabled
}

func renderWorker(document project.Effective, name string, c project.Component, pod corev1.PodSpec, b Bindings, requirements map[string]SecretRequirement, autoscaled bool) ([]Object, string, error) {
	if b.WorkerController == nil {
		return nil, "", unsupported("worker.versioning")
	}
	deploymentName := b.WorkerController.DeploymentNames[name]
	if deploymentName == "" || len(validation.IsDNS1123Label(deploymentName)) > 0 {
		return nil, "", renderError("WorkerDeploymentBindingRequired", "worker.versioning", "A versioned worker requires an explicit valid trusted deployment identity.")
	}
	if configured := c.Worker.Versioning.DeploymentName; configured != nil && string(*configured) != deploymentName {
		return nil, "", renderError("WorkerDeploymentBindingMismatch", "worker.versioning.deploymentName", "The authored worker deployment name differs from its trusted assigned identity.")
	}
	connection, ok := b.Temporal["primary"]
	if !ok || connection.Mode != "apiKey" || connection.Address == "" || connection.CloudNamespace == "" {
		return nil, "", renderError("WorkerConnectionBindingRequired", "worker.versioning", "Worker Controller requires explicit API-key, endpoint and Temporal namespace bindings.")
	}
	host, port, err := net.SplitHostPort(connection.Address)
	numericPort, portErr := strconv.Atoi(port)
	if err != nil || host == "" || portErr != nil || numericPort < 1 || numericPort > 65535 || len(validation.IsDNS1123Subdomain(host)) > 0 || strings.ContainsAny(host, "/ \t\r\n") {
		return nil, "", renderError("InvalidWorkerConnection", "temporal.address", "Worker connection address must be a trusted host and port.")
	}
	key, ok := connection.Secret.Keys["temporal.apiKey"]
	if !ok {
		return nil, "", renderError("SecretPropertyBindingRequired", "worker.connection", "The worker connection API key has no local Secret key binding.")
	}
	if err = requireSecret(connection.Secret, key, b, requirements); err != nil {
		return nil, "", err
	}
	namespace, parentUID, owner := b.Namespace.NamespaceName(), b.Namespace.Parent().UID, b.Namespace.Owner()
	connectionObject, err := custom(workerAPIVersion, "Connection", metadata("temporal", namespace, parentUID, owner, document, "", b), connectionSpec{HostPort: connection.Address, APIKeySecretRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: connection.Secret.Name}, Key: key}})
	if err != nil {
		return nil, "", err
	}
	rollout := project.WorkerRollout{Strategy: ptr("AllAtOnce")}
	if c.Worker.Versioning.Rollout != nil {
		rollout = *c.Worker.Versioning.Rollout
		if rollout.Strategy == nil {
			rollout.Strategy = ptr("AllAtOnce")
		}
	}
	ending := sunset{ScaledownDelay: "1h", DeleteDelay: "24h"}
	if value := c.Worker.Versioning.Sunset; value != nil {
		if value.ScaledownDelay != nil {
			ending.ScaledownDelay = string(*value.ScaledownDelay)
		}
		if value.DeleteDelay != nil {
			ending.DeleteDelay = string(*value.DeleteDelay)
		}
	}
	var replicas *int32
	if !autoscaled {
		value := int32(1)
		if c.Replicas != nil {
			value = int32(*c.Replicas)
		}
		replicas = &value
	}
	templateLabels := labels(owner, document.Metadata.Name, name, b.Revision)
	templateLabels["temporal.io/worker-deployment"] = deploymentName
	worker, err := custom(workerAPIVersion, "WorkerDeployment", metadata(deploymentName, namespace, parentUID, owner, document, name, b), workerSpec{Replicas: replicas, WorkerOptions: workerOptions{ConnectionRef: corev1.LocalObjectReference{Name: "temporal"}, TemporalNamespace: connection.CloudNamespace}, Rollout: rollout, Sunset: ending, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: templateLabels}, Spec: pod}})
	if err != nil {
		return nil, "", err
	}
	return []Object{connectionObject, worker}, deploymentName, nil
}
