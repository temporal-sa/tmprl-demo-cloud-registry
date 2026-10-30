package kubernetes

import (
	"context"
	"encoding/json"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	validation "k8s.io/apimachinery/pkg/util/validation"
)

type endpointPodState struct {
	name, address string
	conditions    endpointConditions
	defaulted     bool
	ready         bool
}
type endpointSample struct {
	view        EndpointView
	limits      EndpointLimits
	bytes       int
	versions    []endpointVersion
	pods        map[string]endpointPodState
	replicaSets map[string]*appsv1.ReplicaSet
	literal     *literalSample
}

func (s *endpointSample) account(value any) bool {
	data, err := json.Marshal(value)
	s.bytes += len(data)
	return err == nil && s.bytes <= s.limits.MaxBytes
}
func (s *endpointSample) finish(state, code string, complete bool) EndpointSnapshot {
	s.view.State, s.view.Code, s.view.Complete = state, code, complete
	s.view.PublicationPresent = complete && s.view.Counts.ReadyPods > 0
	s.view.CompletedAt = time.Now().UTC()
	s.view.SampleDigest = endpointSampleDigest(s.versions)
	copy := s.view
	return EndpointSnapshot{private: &copy}
}

// ObserveEndpoints only reads the pinned v1.37 API. Its owner chain establishes
// CURRENT API relationships, which can result from adoption. It does not verify
// descendant Pod payload/admission equivalence or creator identity. Even a
// complete positive sample is not promotion authority or network readiness.
// Trusted actor composition must independently pin original desired inputs and
// committed receipts; these DTOs do not prove their own journal authenticity.
func (b *NamespaceBinding) ObserveEndpoints(ctx context.Context, request EndpointRead) (EndpointSnapshot, error) {
	return b.observeEndpoints(ctx, request, nil)
}

func (b *NamespaceBinding) observeEndpoints(ctx context.Context, request EndpointRead, literal *literalSample) (EndpointSnapshot, error) {
	limits, valid := endpointLimits(request.Limits)
	if !valid || b == nil || b.client == nil || b.client.discovery == nil || b.client.apps == nil || b.client.core == nil {
		return EndpointSnapshot{}, &Error{Code: EndpointInvalid, Recovery: "Supply the explicit pinned local client, original accepted Service/Deployment inputs and finite endpoint observation limits."}
	}
	// Copy both raw desired bodies BEFORE any API wait/callback. Existing child
	// observers revalidate complete fingerprints/receipts against these copies.
	service, _, err := b.freezeService(request.Service.Desired, request.Service.Intent)
	if err != nil {
		return EndpointSnapshot{}, err
	}
	deployment, _, err := b.freezeDeployment(request.Deployment.Desired, request.Deployment.Intent)
	if err != nil {
		return EndpointSnapshot{}, err
	}
	request.Service.Desired, request.Deployment.Desired = service, deployment
	if deployment.Spec.Selector == nil || !sameDeploymentJSON(service.Spec.Selector, deployment.Spec.Selector.MatchLabels) || len(deployment.Spec.Selector.MatchExpressions) != 0 || request.Service.Intent.Intent.Revision != request.Deployment.Intent.Intent.Revision {
		return EndpointSnapshot{}, &Error{Code: EndpointInvalid, Recovery: "Bind this publication observation to one accepted Service selector and Deployment revision in the same verified namespace."}
	}
	sample := &endpointSample{literal: literal, limits: limits, pods: map[string]endpointPodState{}, replicaSets: map[string]*appsv1.ReplicaSet{}, view: EndpointView{Version: "endpoint-view-v1", Claim: "current_ancestry_publication", StartedAt: time.Now().UTC(), NamespaceUID: b.bound.Parent().UID}}
	call, cancel := context.WithTimeout(ctx, limits.Timeout)
	defer cancel()
	firstService, err := b.ObserveService(call, request.Service)
	if err != nil {
		return sample.finish("Unverified", "EndpointSourceUnverified", false), nil
	}
	firstDeployment, err := b.ObserveDeployment(call, request.Deployment)
	if err != nil {
		return sample.finish("Unverified", "EndpointSourceUnverified", false), nil
	}
	actualService, actualDeployment := firstService.private.object, firstDeployment.private.object
	sample.view.ServiceUID = string(actualService.UID)
	sample.view.DeploymentUID = string(actualDeployment.UID)
	sample.view.DeploymentGeneration = actualDeployment.Generation
	sample.versions = append(sample.versions, endpointVersion{"Service", string(actualService.UID), actualService.ResourceVersion}, endpointVersion{"Deployment", string(actualDeployment.UID), actualDeployment.ResourceVersion})
	if actualDeployment.DeletionTimestamp != nil {
		return sample.finish("Unverified", "EndpointSourceTerminating", false), nil
	}
	if !sample.account(actualService) || !sample.account(actualDeployment) {
		return sample.finish("Incomplete", "EndpointObservationBound", false), nil
	}
	if literal != nil {
		if code := literal.begin(call, b, sample); code != "" {
			return sample.finish("Unverified", code, false), nil
		}
	}
	token := ""
	tokens := map[string]bool{}
	seenSlices := map[string]bool{}
	listVersion := ""
	for page := 0; ; page++ {
		if page >= limits.MaxPages || call.Err() != nil {
			return sample.finish("Incomplete", "EndpointObservationBound", false), nil
		}
		// Service-name discovers ALL relevant slices, including conflicting sources.
		// A managed-by prefilter could conceal a slice the data plane still consumes.
		list, err := b.client.discovery.EndpointSlices(b.bound.NamespaceName()).List(call, metav1.ListOptions{LabelSelector: labels.Set{discoveryv1.LabelServiceName: actualService.Name}.AsSelector().String(), Limit: limits.PageSize, Continue: token})
		if err != nil {
			return sample.finish("Incomplete", "EndpointListUnavailable", false), nil
		}
		if list.ResourceVersion == "" || listVersion != "" && list.ResourceVersion != listVersion || !sample.account(list) {
			return sample.finish("Incomplete", "EndpointListInconsistent", false), nil
		}
		listVersion = list.ResourceVersion
		if len(list.Items) > int(limits.PageSize) || sample.view.Counts.Slices+len(list.Items) > limits.MaxSlices {
			return sample.finish("Incomplete", "EndpointObservationBound", false), nil
		}
		for i := range list.Items {
			slice := &list.Items[i]
			sample.view.Counts.Slices++
			if seenSlices[string(slice.UID)] || !endpointSliceMatches(slice, actualService) {
				sample.view.Counts.UnverifiedEntries++
				return sample.finish("Unverified", "EndpointSliceUnverified", false), nil
			}
			seenSlices[string(slice.UID)] = true
			sample.versions = append(sample.versions, endpointVersion{"EndpointSlice", string(slice.UID), slice.ResourceVersion})
			for _, endpoint := range slice.Endpoints {
				sample.view.Counts.EndpointEntries++
				if sample.view.Counts.EndpointEntries > limits.MaxEndpoints {
					return sample.finish("Incomplete", "EndpointObservationBound", false), nil
				}
				code := b.observeEndpointPod(call, sample, endpoint, actualService, actualDeployment)
				if code != "" {
					sample.view.Counts.UnverifiedEntries++
					if code == "EndpointObservationBound" {
						return sample.finish("Incomplete", code, false), nil
					}
					return sample.finish("Unverified", code, false), nil
				}
			}
		}
		token = list.Continue
		if token == "" {
			break
		}
		if len(token) > 4096 || tokens[token] {
			return sample.finish("Incomplete", "EndpointListInconsistent", false), nil
		}
		tokens[token] = true
	}
	// Rechecks catch changed parent/source identity/spec/generation at completion.
	// Cross-kind RVs are not a common clock. Pod/slice readiness may change after
	// its individual read; Complete explicitly describes only this sample interval.
	endService, err := b.ObserveService(call, request.Service)
	if err != nil {
		return sample.finish("Changed", "EndpointSampleChanged", false), nil
	}
	endDeployment, err := b.ObserveDeployment(call, request.Deployment)
	if err != nil || endService.View().UID != sample.view.ServiceUID || endService.View().AllocatedSpecDigest != firstService.View().AllocatedSpecDigest || endDeployment.View().UID != sample.view.DeploymentUID || endDeployment.View().Readiness.Generation != sample.view.DeploymentGeneration || call.Err() != nil {
		return sample.finish("Changed", "EndpointSampleChanged", false), nil
	}
	sample.versions = append(sample.versions, endpointVersion{"ServiceEnd", endService.View().UID, endService.View().ResourceVersion}, endpointVersion{"DeploymentEnd", endDeployment.View().UID, endDeployment.View().ResourceVersion}, endpointVersion{"EndpointList", sample.view.ServiceUID, listVersion})
	if literal != nil {
		if code := literal.end(call, b, sample); code != "" {
			return sample.finish("Changed", code, false), nil
		}
	}
	for _, pod := range sample.pods {
		sample.view.Counts.VerifiedPods++
		if pod.defaulted {
			sample.view.Counts.DefaultedConditionPods++
		}
		if pod.conditions.serving {
			sample.view.Counts.ServingPods++
		}
		if pod.conditions.terminating {
			sample.view.Counts.TerminatingPods++
		}
		if pod.ready {
			sample.view.Counts.ReadyPods++
		}
	}
	if sample.view.Counts.ReadyPods == 0 {
		return sample.finish("Waiting", "EndpointPublicationPending", true), nil
	}
	return sample.finish("Observed", "EndpointPublicationObserved", true), nil
}

func (b *NamespaceBinding) observeEndpointPod(ctx context.Context, sample *endpointSample, endpoint discoveryv1.Endpoint, service *corev1.Service, deployment *appsv1.Deployment) string {
	name, uid, valid := endpointPodRef(endpoint, service.Namespace)
	if !valid {
		return "EndpointTargetUnverified"
	}
	conditions, defaulted := effectiveEndpointConditions(endpoint.Conditions)
	if conditions.ready != (conditions.serving && !conditions.terminating) {
		sample.view.Counts.ConditionConflicts++
		return "EndpointConditionConflict"
	}
	if len(endpoint.Addresses) != 1 {
		return "EndpointAddressUnverified"
	}
	if old, ok := sample.pods[uid]; ok {
		if old.name != name || old.address != endpoint.Addresses[0] || old.conditions != conditions {
			return "EndpointDuplicateConflict"
		}
		old.defaulted = old.defaulted || defaulted
		sample.pods[uid] = old
		sample.view.Counts.DuplicateEntries++
		return ""
	}
	if len(sample.pods) >= sample.limits.MaxPods {
		return "EndpointObservationBound"
	}
	pod, err := b.client.core.Pods(service.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil || pod.Name != name || string(pod.UID) != uid {
		return "EndpointPodUnverified"
	}
	if !sample.account(pod) {
		return "EndpointObservationBound"
	}
	if len(pod.OwnerReferences) != 1 {
		return "EndpointAncestryUnverified"
	}
	owner := pod.OwnerReferences[0]
	if owner.APIVersion != "apps/v1" || owner.Kind != "ReplicaSet" || owner.UID == "" || len(validation.IsDNS1123Subdomain(owner.Name)) > 0 || owner.Controller == nil || !*owner.Controller {
		return "EndpointAncestryUnverified"
	}
	rs := sample.replicaSets[string(owner.UID)]
	if rs == nil {
		if len(sample.replicaSets) >= sample.limits.MaxReplicaSets {
			return "EndpointObservationBound"
		}
		rs, err = b.client.apps.ReplicaSets(service.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
		if err != nil || rs.Name != owner.Name || rs.UID != owner.UID || !endpointReplicaSetMatches(rs, deployment) {
			return "EndpointAncestryUnverified"
		}
		if !sample.account(rs) {
			return "EndpointObservationBound"
		}
		sample.replicaSets[string(rs.UID)] = rs
		sample.versions = append(sample.versions, endpointVersion{"ReplicaSet", string(rs.UID), rs.ResourceVersion})
	}
	if rs.Name != owner.Name || !endpointPodMembership(pod, rs, service) {
		return "EndpointAncestryUnverified"
	}
	if sample.literal != nil && !sample.literal.matches(pod, rs) {
		return "LiteralPodChanged"
	}
	if !endpointPodAddress(endpoint, pod, service.Spec.IPFamilies[0]) {
		return "EndpointAddressUnverified"
	}
	podReady, valid := endpointPodReady(pod)
	if !valid || conditions.serving != podReady || conditions.terminating != (pod.DeletionTimestamp != nil) {
		sample.view.Counts.ConditionConflicts++
		return "EndpointConditionConflict"
	}
	sample.pods[uid] = endpointPodState{name: name, address: endpoint.Addresses[0], conditions: conditions, defaulted: defaulted, ready: conditions.ready && conditions.serving && !conditions.terminating && podReady && pod.DeletionTimestamp == nil && rs.DeletionTimestamp == nil}
	sample.versions = append(sample.versions, endpointVersion{"Pod", string(pod.UID), pod.ResourceVersion})
	return ""
}
