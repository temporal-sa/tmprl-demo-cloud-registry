package runtimefault

import (
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
)

// ReadinessEvidence records a finite observed generation pair, never raw Pod or
// controller status. It is diagnostic data, not proof of a canonical API read.
type ReadinessEvidence struct {
	Generation         int64 `json:"generation"`
	ObservedGeneration int64 `json:"observedGeneration"`
}

type readinessDescription struct{ message, action string }

var readinessDescriptions = map[string]readinessDescription{
	"DeploymentReplicaFailure": {
		"The Deployment controller reported a replica failure for the observed generation.",
		"Inspect the accepted Deployment configuration and separately authorized replica evidence; retain the candidate and do not repeat creation.",
	},
	"DeploymentProgressDeadlineExceeded": {
		"The Deployment controller reported its progress deadline was exceeded for the observed generation.",
		"Inspect the candidate's readiness and accepted configuration; retain the canonical Deployment before requesting any new accepted attempt.",
	},
}

// MapDeploymentReadiness maps only an already validated canonical observation's
// explicit Failed result. The Worker must independently establish that source,
// the generation and the Applied creation fact; this constructor grants none.
// Counters cannot create a failure classification and never enter its wire.
func MapDeploymentReadiness(c Context, observed kube.DeploymentReadiness) (Fault, error) {
	if !validContext(c) || c.Step != Observe || c.Kind != Deployment || c.Dispatch != Applied || c.WriteID == "" || observed.State != "Failed" || observed.Generation <= 0 || observed.ObservedGeneration != observed.Generation {
		return Fault{}, ErrContext
	}
	if observed.Desired < 0 || observed.Replicas < 0 || observed.Updated < 0 || observed.Ready < 0 || observed.Available < 0 || observed.Unavailable < 0 || observed.Terminating < 0 || observed.CollisionCount < 0 {
		return Fault{}, ErrContext
	}
	description, known := readinessDescriptions[observed.Code]
	if !known {
		return Fault{}, ErrContext
	}
	f := safeFailure(c, observed.Code, diagnostics.Readiness, description.message, description.action)
	f.private.Readiness = &ReadinessEvidence{Generation: observed.Generation, ObservedGeneration: observed.ObservedGeneration}
	return f, nil
}
