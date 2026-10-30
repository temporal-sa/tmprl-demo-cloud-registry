package kubernetes

import (
	corev1 "k8s.io/api/core/v1"
	validation "k8s.io/apimachinery/pkg/util/validation"
)

// This is the finite known typed Kubernetes v1.37 ServiceAccount contract, not
// a strict decoder for arbitrary future wire fields. Server/client upgrades
// require separate review and a native API proof. Explicit false automount is
// required; no secret refs, pull refs, token requests or identity hooks exist.
// The pinned ServiceAccount strategy only cleans Secret reference shapes; this
// baseline accepts none. Nil/empty reference lists have the same empty meaning.
func serviceAccountBaseline(object *corev1.ServiceAccount) (*corev1.ServiceAccount, error) {
	if object == nil {
		return nil, serviceAccountFailure(ServiceAccountUnsupportedSpec)
	}
	d := object.DeepCopy()
	allowed := corev1.ServiceAccount{TypeMeta: d.TypeMeta, ObjectMeta: d.ObjectMeta, AutomountServiceAccountToken: d.AutomountServiceAccountToken}
	if !sameDeploymentJSON(allowed, *d) || d.AutomountServiceAccountToken == nil || *d.AutomountServiceAccountToken || len(d.Secrets) != 0 || len(d.ImagePullSecrets) != 0 || len(validation.IsDNS1123Subdomain(d.Name)) != 0 || len(d.Labels) > 64 {
		return nil, serviceAccountFailure(ServiceAccountUnsupportedSpec)
	}
	for key, value := range d.Labels {
		if len(validation.IsQualifiedName(key)) != 0 || len(validation.IsValidLabelValue(value)) != 0 {
			return nil, serviceAccountFailure(ServiceAccountUnsupportedSpec)
		}
	}
	d.Secrets = nil
	d.ImagePullSecrets = nil
	return d, nil
}

func serviceAccountPolicyMatches(actual, expected *corev1.ServiceAccount) bool {
	normalized, err := serviceAccountBaseline(actual)
	return err == nil && expected != nil && expected.AutomountServiceAccountToken != nil && !*expected.AutomountServiceAccountToken && normalized.AutomountServiceAccountToken != nil && !*normalized.AutomountServiceAccountToken
}

// Only native system metadata is outside the exact desired comparison:
// UID/resourceVersion/creationTimestamp/managedFields. No admission annotations,
// finalizers, owner refs, generateName, deletion state or nonzero generation are
// silently accepted. SA strategy has no generation bump in this pinned API.
func serviceAccountMetadataMatches(actual, expected *corev1.ServiceAccount) bool {
	return actual != nil && expected != nil && actual.Name == expected.Name && actual.Namespace == expected.Namespace && (actual.APIVersion == "" || actual.APIVersion == "v1") && (actual.Kind == "" || actual.Kind == "ServiceAccount") && actual.UID != "" && actual.ResourceVersion != "" && !actual.CreationTimestamp.IsZero() && actual.Generation == 0 && actual.SelfLink == "" && actual.GenerateName == "" && actual.DeletionTimestamp == nil && actual.DeletionGracePeriodSeconds == nil && len(actual.Finalizers) == 0 && sameDeploymentJSON(actual.Labels, expected.Labels) && sameDeploymentJSON(actual.Annotations, expected.Annotations) && sameDeploymentJSON(actual.OwnerReferences, expected.OwnerReferences)
}
