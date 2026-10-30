package kubernetes

import (
	"context"
	"encoding/json"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	validation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/version"
)

type literalSample struct {
	request LiteralWorkloadRead
	desired *appsv1.Deployment
	pods    map[string]*corev1.Pod
	sets    map[string]*appsv1.ReplicaSet
}

func literalRelevantReplicaSet(rs *appsv1.ReplicaSet, deploymentUID string, selector labels.Selector) bool {
	for _, ref := range rs.OwnerReferences {
		if string(ref.UID) == deploymentUID {
			return true
		}
	}
	return selector.Matches(labels.Set(rs.Labels)) || selector.Matches(labels.Set(rs.Spec.Template.Labels))
}

// ObserveLiteralWorkload only compares current API declarations under the
// explicitly trusted local bootstrap. It never mutates, probes an arbitrary
// URL, attests installed admission or authorizes a promotion. Full typed-body
// comparison is bounded to the reviewed server/SDK version, not future fields.
func (b *NamespaceBinding) ObserveLiteralWorkload(ctx context.Context, r LiteralWorkloadRead) (LiteralWorkloadSnapshot, error) {
	if b == nil || b.client == nil || r.Node.UID == "" || len(validation.IsDNS1123Subdomain(r.Node.Name)) != 0 {
		return LiteralWorkloadSnapshot{}, failure(InvalidConfiguration, nil)
	}
	d, _, err := b.freezeDeployment(r.Deployment.Desired, r.Deployment.Intent)
	if err != nil {
		return LiteralWorkloadSnapshot{}, err
	}
	normal, valid := literalDeployment(d)
	if !valid {
		return literalResult(EndpointView{}, nil, "Unsupported", "LiteralDependencyOrSpecUnsupported"), nil
	}
	sa, err := b.freezeServiceAccount(r.ServiceAccount.Desired, r.ServiceAccount.Intent)
	if err != nil {
		return LiteralWorkloadSnapshot{}, err
	}
	if sa.Name != normal.Spec.Template.Spec.ServiceAccountName || r.ServiceAccount.Intent.Intent.Revision != r.Deployment.Intent.Intent.Revision {
		return LiteralWorkloadSnapshot{}, failure(InvalidConfiguration, nil)
	}
	r.Deployment.Desired, r.ServiceAccount.Desired = d, sa
	literal := &literalSample{request: r, desired: normal}
	publication, err := b.observeEndpoints(ctx, r.EndpointRead, literal)
	if err != nil {
		return LiteralWorkloadSnapshot{}, err
	}
	v := publication.View()
	return literalResult(v, literal, v.State, v.Code), nil
}
func literalResult(publication EndpointView, s *literalSample, state, code string) LiteralWorkloadSnapshot {
	v := LiteralWorkloadView{Version: "literal-workload-v1", Claim: "current_workload_api_declaration_v1", State: state, Code: code, Publication: publication, Complete: publication.Complete}
	if s != nil {
		v.Pods = len(s.pods)
		v.ReplicaSets = len(s.sets)
		v.DeclarationsEqual = publication.Complete && len(s.pods) > 0
	}
	pods := map[string]bool{}
	if s != nil {
		for uid := range s.pods {
			pods[uid] = true
		}
	}
	return LiteralWorkloadSnapshot{private: &literalSnapshotData{view: v, pods: pods}}
}
func (s *literalSample) anchors(ctx context.Context, b *NamespaceBinding, sample *endpointSample) bool {
	var build version.Info
	data, err := b.client.core.RESTClient().Get().AbsPath("/version").Do(ctx).Raw()
	if err != nil || len(data) > 4096 || json.Unmarshal(data, &build) != nil || build.GitVersion != "v1.37.0" || build.GitCommit != literalServerCommit {
		return false
	}
	node, err := b.client.core.Nodes().Get(ctx, s.request.Node.Name, metav1.GetOptions{})
	if err != nil || node.Name != s.request.Node.Name || string(node.UID) != s.request.Node.UID || node.DeletionTimestamp != nil || !sample.account(node) {
		return false
	}
	sa, err := b.ObserveServiceAccount(ctx, s.request.ServiceAccount)
	if err != nil {
		return false
	}
	sample.versions = append(sample.versions, endpointVersion{"Node", string(node.UID), node.ResourceVersion}, endpointVersion{"ServiceAccount", sa.View().UID, sa.View().ResourceVersion})
	return true
}

// Lists the entire finite namespace, so mutated selector labels cannot conceal
// an owned descendant. Every page has one list RV; object accounting remains a
// post-decode bound (as in the existing endpoint observer).
func (s *literalSample) collect(ctx context.Context, b *NamespaceBinding, sample *endpointSample) (map[string]*appsv1.ReplicaSet, map[string]*corev1.Pod, string) {
	sets := map[string]*appsv1.ReplicaSet{}
	pods := map[string]*corev1.Pod{}
	selector := labels.SelectorFromSet(s.desired.Spec.Selector.MatchLabels)
	token, rv := "", ""
	tokens := map[string]bool{}
	seen := map[string]bool{}
	count := 0
	for page := 0; ; page++ {
		if page >= sample.limits.MaxPages || ctx.Err() != nil {
			return nil, nil, "LiteralObservationBound"
		}
		list, err := b.client.apps.ReplicaSets(b.bound.NamespaceName()).List(ctx, metav1.ListOptions{Limit: sample.limits.PageSize, Continue: token})
		if err != nil {
			return nil, nil, "LiteralListUnavailable"
		}
		count += len(list.Items)
		if count > sample.limits.MaxReplicaSets || len(list.Items) > int(sample.limits.PageSize) || list.ResourceVersion == "" || rv != "" && rv != list.ResourceVersion || !sample.account(list) {
			return nil, nil, "LiteralListIncomplete"
		}
		rv = list.ResourceVersion
		for i := range list.Items {
			rs := &list.Items[i]
			uid := string(rs.UID)
			if uid == "" || seen[uid] {
				return nil, nil, "LiteralListIncomplete"
			}
			seen[uid] = true
			if !literalRelevantReplicaSet(rs, sample.view.DeploymentUID, selector) {
				continue
			}
			if !literalReplicaSet(rs, s.desired) {
				return nil, nil, "LiteralReplicaSetChanged"
			}
			sets[uid] = rs.DeepCopy()
		}
		token = list.Continue
		if token == "" {
			break
		}
		if len(token) > 4096 || tokens[token] {
			return nil, nil, "LiteralListIncomplete"
		}
		tokens[token] = true
	}
	token, rv = "", ""
	tokens = map[string]bool{}
	seen = map[string]bool{}
	count = 0
	for page := 0; ; page++ {
		if page >= sample.limits.MaxPages || ctx.Err() != nil {
			return nil, nil, "LiteralObservationBound"
		}
		list, err := b.client.core.Pods(b.bound.NamespaceName()).List(ctx, metav1.ListOptions{Limit: sample.limits.PageSize, Continue: token})
		if err != nil {
			return nil, nil, "LiteralListUnavailable"
		}
		count += len(list.Items)
		if count > sample.limits.MaxPods || len(list.Items) > int(sample.limits.PageSize) || list.ResourceVersion == "" || rv != "" && rv != list.ResourceVersion || !sample.account(list) {
			return nil, nil, "LiteralListIncomplete"
		}
		rv = list.ResourceVersion
		for i := range list.Items {
			p := &list.Items[i]
			uid := string(p.UID)
			if uid == "" || seen[uid] {
				return nil, nil, "LiteralListIncomplete"
			}
			seen[uid] = true
			var rs *appsv1.ReplicaSet
			for _, ref := range p.OwnerReferences {
				if sets[string(ref.UID)] != nil {
					rs = sets[string(ref.UID)]
				}
			}
			if rs == nil && !selector.Matches(labels.Set(p.Labels)) {
				continue
			}
			if rs == nil || !literalPod(p, rs, s.desired, s.request.Node) {
				return nil, nil, "LiteralPodChanged"
			}
			pods[uid] = p.DeepCopy()
		}
		token = list.Continue
		if token == "" {
			break
		}
		if len(token) > 4096 || tokens[token] {
			return nil, nil, "LiteralListIncomplete"
		}
		tokens[token] = true
	}
	return sets, pods, ""
}
func (s *literalSample) begin(ctx context.Context, b *NamespaceBinding, sample *endpointSample) string {
	if !s.anchors(ctx, b, sample) {
		return "LiteralAnchorUnverified"
	}
	// This UID was independently verified by ObserveDeployment against the
	// supplied canonical receipt. No live descendant supplies desired payload.
	s.desired.UID = types.UID(sample.view.DeploymentUID)
	var code string
	s.sets, s.pods, code = s.collect(ctx, b, sample)
	return code
}
func (s *literalSample) matches(p *corev1.Pod, rs *appsv1.ReplicaSet) bool {
	old := s.pods[string(p.UID)]
	return old != nil && s.sets[string(rs.UID)] != nil && literalReplicaSet(rs, s.desired) && literalPod(p, rs, s.desired, s.request.Node) && sameDeploymentJSON(old.Spec, p.Spec)
}
func (s *literalSample) end(ctx context.Context, b *NamespaceBinding, sample *endpointSample) string {
	if !s.anchors(ctx, b, sample) {
		return "LiteralAnchorChanged"
	}
	sets, pods, code := s.collect(ctx, b, sample)
	if code != "" {
		return code
	}
	if len(sets) != len(s.sets) || len(pods) != len(s.pods) || len(pods) != len(sample.pods) {
		return "LiteralSampleChanged"
	}
	for uid, rs := range sets {
		if s.sets[uid] == nil || !sameDeploymentJSON(rs.Spec.Template, s.sets[uid].Spec.Template) {
			return "LiteralSampleChanged"
		}
		sample.versions = append(sample.versions, endpointVersion{"ReplicaSetEnd", uid, rs.ResourceVersion})
	}
	for uid, p := range pods {
		if s.pods[uid] == nil || sample.pods[uid].name != p.Name || !sameDeploymentJSON(p.Spec, s.pods[uid].Spec) {
			return "LiteralSampleChanged"
		}
		sample.versions = append(sample.versions, endpointVersion{"PodEnd", uid, p.ResourceVersion})
	}
	return ""
}
