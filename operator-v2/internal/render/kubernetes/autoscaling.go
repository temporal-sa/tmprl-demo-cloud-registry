package kubernetes

import (
	"math"
	"strconv"
	"strings"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
)

type scaleTarget struct {
	Name string `json:"name,omitempty"`
}
type scalePolicy struct {
	Type          string `json:"type"`
	Value         int    `json:"value"`
	PeriodSeconds int    `json:"periodSeconds"`
}
type scaleDown struct {
	StabilizationWindowSeconds int           `json:"stabilizationWindowSeconds"`
	Policies                   []scalePolicy `json:"policies"`
}
type hpaBehavior struct {
	ScaleDown scaleDown `json:"scaleDown"`
}
type hpaConfig struct {
	Name     string      `json:"name,omitempty"`
	Behavior hpaBehavior `json:"behavior"`
}
type scaleAdvanced struct {
	HPA hpaConfig `json:"horizontalPodAutoscalerConfig"`
}
type scaleTrigger struct {
	Type             string            `json:"type"`
	UseCachedMetrics bool              `json:"useCachedMetrics"`
	Metadata         map[string]string `json:"metadata"`
}
type scaledObjectSpec struct {
	ScaleTargetRef  scaleTarget    `json:"scaleTargetRef"`
	MinReplicaCount int            `json:"minReplicaCount"`
	MaxReplicaCount int            `json:"maxReplicaCount"`
	PollingInterval int            `json:"pollingInterval"`
	CooldownPeriod  int            `json:"cooldownPeriod"`
	Advanced        scaleAdvanced  `json:"advanced"`
	Triggers        []scaleTrigger `json:"triggers"`
}
type embeddedScaledObject struct {
	APIVersion string           `json:"apiVersion"`
	Kind       string           `json:"kind"`
	Spec       scaledObjectSpec `json:"spec"`
}
type workerResourceSpec struct {
	WorkerDeploymentRef scaleTarget          `json:"workerDeploymentRef"`
	Template            embeddedScaledObject `json:"template"`
}

func renderAutoscaling(d project.Effective, name string, c project.Component, workerName string, b Bindings) (Object, error) {
	if !b.AllowAutoscaling {
		return nil, unsupported("worker.autoscaling")
	}
	minimum, maximum, target, cooldown := 1, 10, 5, 300
	queueTypes := []string{"workflow", "activity"}
	if a := c.Autoscaling; a != nil {
		if a.MinReplicas != nil {
			minimum = *a.MinReplicas
		}
		if a.MaxReplicas != nil {
			maximum = *a.MaxReplicas
		}
		if a.TargetQueueSize != nil {
			target = *a.TargetQueueSize
		}
		if a.CooldownPeriod != nil {
			cooldown = *a.CooldownPeriod
		}
		if a.QueueTypes != nil {
			queueTypes = append([]string(nil), a.QueueTypes...)
		}
	}
	if cooldown > math.MaxInt32 {
		return nil, renderError("AutoscalingRange", "autoscaling.cooldownPeriod", "Autoscaling cooldown must fit its Kubernetes int32 field.")
	}
	meta := map[string]string{"taskQueue": d.TaskQueue(c), "queueTypes": strings.Join(queueTypes, ","), "targetQueueSize": strconv.Itoa(target), "activationTargetQueueSize": "0", "apiKeyFromEnv": "TEMPORAL_API_KEY"}
	targetRef := scaleTarget{Name: name}
	if workerName == "" {
		meta["endpointFromEnv"] = "TEMPORAL_ADDRESS"
		meta["namespaceFromEnv"] = "TEMPORAL_NAMESPACE"
	} else {
		meta["endpoint"] = b.Temporal["primary"].Address
		meta["namespace"] = ""
		meta["workerDeploymentName"] = ""
		meta["workerDeploymentBuildId"] = ""
		targetRef = scaleTarget{}
	}
	spec := scaledObjectSpec{ScaleTargetRef: targetRef, MinReplicaCount: minimum, MaxReplicaCount: maximum, PollingInterval: 15, CooldownPeriod: cooldown, Advanced: scaleAdvanced{HPA: hpaConfig{Behavior: hpaBehavior{ScaleDown: scaleDown{StabilizationWindowSeconds: cooldown, Policies: []scalePolicy{{Type: "Pods", Value: 1, PeriodSeconds: 60}}}}}}, Triggers: []scaleTrigger{{Type: "temporal", UseCachedMetrics: true, Metadata: meta}}}
	if len("keda-hpa-"+name) > 63 {
		spec.Advanced.HPA.Name = "keda-" + digest(struct{ UID, Name string }{b.Namespace.Parent().UID, name})[:32]
	}
	objectMeta := metadata(name, b.Namespace.NamespaceName(), b.Namespace.Parent().UID, b.Namespace.Owner(), d, name, b)
	if workerName == "" {
		return custom("keda.sh/v1alpha1", "ScaledObject", objectMeta, spec)
	}
	objectMeta.Name = workerName
	return custom(workerAPIVersion, "WorkerResourceTemplate", objectMeta, workerResourceSpec{WorkerDeploymentRef: scaleTarget{Name: workerName}, Template: embeddedScaledObject{APIVersion: "keda.sh/v1alpha1", Kind: "ScaledObject", Spec: spec}})
}
