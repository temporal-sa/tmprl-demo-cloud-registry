package kubernetes

import (
	"encoding/json"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// custom encodes a locally typed CRD spec through Kubernetes's JSON decoder,
// preserving integer types for unstructured serialization/deep-copy semantics.
func custom(apiVersion, kind string, metadata metav1.ObjectMeta, spec any) (*unstructured.Unstructured, error) {
	data, err := json.Marshal(struct {
		APIVersion string            `json:"apiVersion"`
		Kind       string            `json:"kind"`
		Metadata   metav1.ObjectMeta `json:"metadata"`
		Spec       any               `json:"spec"`
	}{apiVersion, kind, metadata, spec})
	if err != nil {
		return nil, renderError("InvalidCustomResource", kind, "The typed custom resource cannot be encoded.")
	}
	object, _, err := unstructured.UnstructuredJSONScheme.Decode(data, nil, nil)
	if err != nil {
		return nil, renderError("InvalidCustomResource", kind, "The typed custom resource cannot be decoded.")
	}
	result, ok := object.(*unstructured.Unstructured)
	if !ok {
		return nil, renderError("InvalidCustomResource", kind, "The encoded resource has an unsupported Kubernetes shape.")
	}
	return result, nil
}
