package candidateprep

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	render "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/render/kubernetes"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

// Pair contains PRIVATE execution objects, not receipts or a prepared result.
// The original plan must stay owned by the supervisor. Losing it fails closed;
// no method reconstructs it from a fingerprint or an observed live object.
type Pair struct{ state *pairState }
type pairState struct {
	view                View
	deployment, service render.DesiredObject
	serviceAccount      render.DesiredObject
}

// Prerequisite names the separate bootstrap requirement only. It does not pin,
// observe, create or authenticate a ServiceAccount. A native supervisor must
// independently verify its canonical UID/spec in the same parent namespace.
type Prerequisite struct {
	ServiceAccountName           string
	AutomountServiceAccountToken bool
}

// Render requires an independently verified namespace binding matching this
// accepted attempt. A synthetically constructed binding in a pure test is not
// proof of native creation, process fencing or a current parent lifecycle guard.
func (p Plan) Render(namespace ownership.NamespacedContext) (Pair, error) {
	if p.state == nil {
		return Pair{}, failure("PreparationPlanMissing", "The original private accepted preparation plan is unavailable.")
	}
	expected, err := p.Namespace()
	if err != nil {
		return Pair{}, err
	}
	parent := namespace.Parent()
	scope, _ := p.state.authority.Scope(ownership.Kubernetes)
	scope.Namespace = expected.Object.Name
	if namespace.Owner() != p.state.authority.Owner() || namespace.Scope() != scope || namespace.ParentRevision() != p.state.desired.Revision || namespace.ParentPurpose() != "runtime-namespace" || parent.UID == "" || parent.Owner != namespace.Owner() || parent.Name != expected.Object.Name || parent.ClusterID != scope.ClusterID || parent.CreationOperationID != p.state.attempt || parent.CreationIdempotencyKey != expected.Intent.IdempotencyKey {
		return Pair{}, failure("PreparationNamespaceMismatch", "The verified namespace does not match the accepted owner, cluster, attempt, revision and creation provenance.")
	}
	bundle, err := render.RenderCandidate(p.state.document, render.Bindings{Namespace: namespace, Revision: p.state.desired.Revision, OperationID: p.state.attempt, Images: p.state.desired.Images})
	if err != nil {
		// Renderer errors can contain authoring paths. Do not retain raw errors
		// or dynamically copy their fields into the safe acceptance boundary.
		return Pair{}, failure("PreparationRenderRejected", "The accepted literal preparation profile could not be rendered within its supported contract.")
	}
	if len(bundle.RequiredSecrets) != 0 || len(bundle.RequiredConfigInputs) != 0 || bundle.DeferredPromotion {
		return Pair{}, failure("PreparationDependencyUnsupported", "The selected pair requires runtime dependencies outside the literal preparation profile.")
	}
	return p.pairFromBundle(namespace, bundle)
}

func (p Plan) pairFromBundle(namespace ownership.NamespacedContext, bundle render.Bundle) (Pair, error) {
	var deployment, service, serviceAccount render.DesiredObject
	for _, desired := range bundle.Objects {
		switch desired.Object.(type) {
		case *appsv1.Deployment:
			if deployment.Object != nil {
				return Pair{}, pairMismatch()
			}
			deployment = desired
		case *corev1.Service:
			if service.Object != nil {
				return Pair{}, pairMismatch()
			}
			service = desired
		case *corev1.ServiceAccount:
			if serviceAccount.Object != nil {
				return Pair{}, pairMismatch()
			}
			serviceAccount = desired
		}
	}
	if err := p.validatePair(namespace, deployment, service, serviceAccount); err != nil {
		return Pair{}, err
	}
	return Pair{&pairState{view: p.View(), deployment: deployment, service: service, serviceAccount: serviceAccount}}, nil
}

func pairMismatch() error {
	return failure("PreparationPairMismatch", "Rendered Deployment and ClusterIP Service must identify the same accepted workload, ports, namespace and attempt.")
}

func (p Plan) validatePair(namespace ownership.NamespacedContext, deployment, service, serviceAccount render.DesiredObject) error {
	d, dok := deployment.Object.(*appsv1.Deployment)
	s, sok := service.Object.(*corev1.Service)
	sa, saok := serviceAccount.Object.(*corev1.ServiceAccount)
	if !dok || !sok || !saok || d == nil || s == nil || sa == nil {
		return pairMismatch()
	}
	name := p.state.component
	selected := p.state.document.Spec.Components[name]
	if sa.Name != name || sa.AutomountServiceAccountToken == nil || *sa.AutomountServiceAccountToken || len(sa.Secrets) != 0 || len(sa.ImagePullSecrets) != 0 || len(sa.Finalizers) != 0 {
		return pairMismatch()
	}
	serviceName := name
	if selected.ServiceName != nil {
		serviceName = string(*selected.ServiceName)
	}
	if d.Name != name || s.Name != serviceName || s.Spec.Type != corev1.ServiceTypeClusterIP || s.Spec.ClusterIP != "" || len(s.Spec.ClusterIPs) != 0 || len(s.Spec.ExternalIPs) != 0 || s.Spec.ExternalName != "" || len(d.Spec.Template.Spec.Containers) != 1 || len(d.Spec.Template.Spec.InitContainers) != 0 || len(d.Spec.Template.Spec.Volumes) != 0 || d.Spec.Template.Spec.ServiceAccountName != name || d.Spec.Template.Spec.AutomountServiceAccountToken == nil || *d.Spec.Template.Spec.AutomountServiceAccountToken || d.Spec.Selector == nil || len(d.Spec.Selector.MatchExpressions) != 0 || !reflect.DeepEqual(d.Spec.Selector.MatchLabels, d.Spec.Template.Labels) || !reflect.DeepEqual(s.Spec.Selector, d.Spec.Selector.MatchLabels) {
		return pairMismatch()
	}
	container := d.Spec.Template.Spec.Containers[0]
	if container.Name != name || container.Image != p.state.desired.Images[string(selected.Build)] || len(container.EnvFrom) != 0 || len(container.VolumeMounts) != 0 || len(container.VolumeDevices) != 0 || len(s.Spec.Ports) != len(container.Ports) || len(s.Spec.Ports) == 0 {
		return pairMismatch()
	}
	for _, env := range container.Env {
		if env.ValueFrom != nil {
			return pairMismatch()
		}
	}
	for index, port := range s.Spec.Ports {
		cp := container.Ports[index]
		if port.Name != cp.Name || port.Protocol != corev1.ProtocolTCP || cp.Protocol != corev1.ProtocolTCP || port.NodePort != 0 || port.AppProtocol != nil || port.TargetPort.Type != 0 || port.TargetPort.IntVal != cp.ContainerPort || port.Port < 1 {
			return pairMismatch()
		}
	}
	for _, desired := range []render.DesiredObject{deployment, service, serviceAccount} {
		i := desired.Intent
		encoded, err := json.Marshal(desired.Object)
		hash := sha256.Sum256(encoded)
		if err != nil || i.Parent != namespace.Parent() || i.Intent.Owner != namespace.Owner() || i.Intent.Scope != namespace.Scope() || i.Intent.Name != desired.Object.GetName() || desired.Object.GetNamespace() != namespace.NamespaceName() || i.Intent.OperationID != p.state.attempt || i.Intent.Revision != p.state.desired.Revision || i.Intent.SpecFingerprint != hex.EncodeToString(hash[:]) || desired.Object.GetAnnotations()[render.ParentUIDAnnotation] != namespace.Parent().UID {
			return pairMismatch()
		}
	}
	if deployment.Intent.Intent.Purpose != "deployment" || service.Intent.Intent.Purpose != "service" || serviceAccount.Intent.Intent.Purpose != "service-account" {
		return pairMismatch()
	}
	return nil
}

func (p Pair) View() View {
	if p.state == nil {
		return View{Promotion: PromotionNotAuthorized}
	}
	return p.state.view
}
func (p Pair) Prerequisite() (Prerequisite, error) {
	if p.state == nil {
		return Prerequisite{}, pairMismatch()
	}
	return Prerequisite{ServiceAccountName: p.state.view.Component, AutomountServiceAccountToken: false}, nil
}

// Deployment returns a fresh PRIVATE execution copy for Session.Apply/Observe.
// It is not an authenticated provider receipt or a history-safe DTO.
func (p Pair) Deployment() (render.DesiredObject, error) {
	if p.state == nil {
		return render.DesiredObject{}, pairMismatch()
	}
	return render.DesiredObject{Object: p.state.deployment.Object.(*appsv1.Deployment).DeepCopy(), Intent: p.state.deployment.Intent}, nil
}

// Service has the same private-copy contract as Deployment.
func (p Pair) Service() (render.DesiredObject, error) {
	if p.state == nil {
		return render.DesiredObject{}, pairMismatch()
	}
	return render.DesiredObject{Object: p.state.service.Object.(*corev1.Service).DeepCopy(), Intent: p.state.service.Intent}, nil
}

// ServiceAccount is a private desired copy only. A real native creation receipt
// and current policy read are still required before satisfying Prerequisite.
func (p Pair) ServiceAccount() (render.DesiredObject, error) {
	if p.state == nil {
		return render.DesiredObject{}, pairMismatch()
	}
	return render.DesiredObject{Object: p.state.serviceAccount.Object.(*corev1.ServiceAccount).DeepCopy(), Intent: p.state.serviceAccount.Intent}, nil
}
func (p Pair) Format(s fmt.State, verb rune) {
	_, _ = io.WriteString(s, "[private candidate preparation pair]")
}
func (p Pair) MarshalJSON() ([]byte, error) {
	return nil, failure("PreparationPrivateSerializationDenied", "Private preparation objects cannot be serialized; use the safe descriptive view.")
}
