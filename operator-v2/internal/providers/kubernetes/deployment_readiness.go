package kubernetes

import (
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

func deploymentReadiness(d *appsv1.Deployment) DeploymentReadiness {
	s := d.Status
	r := DeploymentReadiness{State: "Waiting", Code: "DeploymentControllerPending", Generation: d.Generation, ObservedGeneration: s.ObservedGeneration, Desired: *d.Spec.Replicas, Replicas: s.Replicas, Updated: s.UpdatedReplicas, Ready: s.ReadyReplicas, Available: s.AvailableReplicas, Unavailable: s.UnavailableReplicas}
	if s.TerminatingReplicas != nil {
		r.Terminating = *s.TerminatingReplicas
		r.TerminatingKnown = true
	}
	if s.CollisionCount != nil {
		r.CollisionCount = *s.CollisionCount
		r.CollisionKnown = true
	}
	if d.DeletionTimestamp != nil {
		r.State = "Retained"
		r.Code = "DeploymentTerminating"
		return r
	}
	if s.ObservedGeneration != d.Generation {
		if s.ObservedGeneration > d.Generation {
			r.State = "Unknown"
			r.Code = "DeploymentInvalidGeneration"
		}
		return r
	}
	seen := map[appsv1.DeploymentConditionType]bool{}
	available := false
	for _, c := range s.Conditions {
		if seen[c.Type] {
			r.State = "Unknown"
			r.Code = "DeploymentInvalidConditions"
			return r
		}
		seen[c.Type] = true
		if c.Type == appsv1.DeploymentReplicaFailure && c.Status == corev1.ConditionTrue {
			r.State = "Failed"
			r.Code = "DeploymentReplicaFailure"
			return r
		}
		if c.Type == appsv1.DeploymentProgressing && c.Status == corev1.ConditionFalse && c.Reason == "ProgressDeadlineExceeded" {
			r.State = "Failed"
			r.Code = "DeploymentProgressDeadlineExceeded"
			return r
		}
		if c.Type == appsv1.DeploymentAvailable && c.Status == corev1.ConditionTrue {
			available = true
		}
	}
	// A positive counter records historical hash-collision avoidance; a resolved
	// collision does not invalidate currently converged replicas.
	if r.CollisionCount < 0 {
		r.State = "Unknown"
		r.Code = "DeploymentInvalidCollisionCount"
		return r
	}
	if r.Replicas < 0 || r.Updated < 0 || r.Ready < 0 || r.Available < 0 || r.Unavailable < 0 || r.Terminating < 0 {
		r.State = "Unknown"
		r.Code = "DeploymentInvalidReplicaCounts"
		return r
	}
	if r.Replicas != r.Desired || r.Updated != r.Desired || r.Ready != r.Desired || r.Available != r.Desired || r.Unavailable != 0 {
		r.Code = "DeploymentReplicasPending"
		return r
	}
	if !available && r.Desired != 0 {
		r.Code = "DeploymentAvailabilityPending"
		return r
	}
	if r.Terminating != 0 {
		r.Code = "DeploymentTerminatingReplicasPending"
		return r
	}
	r.State = "Ready"
	r.Code = "DeploymentReady"
	return r
}
