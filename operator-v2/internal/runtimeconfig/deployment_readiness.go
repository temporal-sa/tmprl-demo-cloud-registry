package runtimeconfig

import (
	"context"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/runtimefault"
)

// deploymentReadiness runs only after an admitted canonical resource read.
// Failed readiness is a successful API observation, not a create rejection or
// an unknown mutation. Preflight intentionally checks canonical input/identity
// without calling this helper: readiness does not deny clean read admission.
func (w *Worker) deploymentReadiness(ctx context.Context, write journal.Write, observed kube.DeploymentReadiness) (kube.DeploymentReadiness, error) {
	if observed.State != "Failed" {
		return observed, nil
	}
	if write.State != journal.Committed || write.Preconditions.Kind != "Deployment" {
		observed.State, observed.Code = "Unknown", "DeploymentReadinessUnproven"
		return observed, failure(Invalid)
	}
	f, err := runtimefault.MapDeploymentReadiness(w.faultContext(write, runtimefault.Observe, runtimefault.Applied), observed)
	if err != nil {
		// Unsupported/future/malformed status cannot become a known failure
		// merely because its string state says Failed. Preserve no raw code.
		observed.State, observed.Code = "Unknown", "DeploymentReadinessUnproven"
		return observed, failure(Uncertain)
	}
	return observed, w.recordReadFault(ctx, Unavailable, f)
}
