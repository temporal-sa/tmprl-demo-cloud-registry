package kubernetes

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
)

// Generated clients inherit a Retry-After retry ceiling. Mutations use the same
// official versioned CoreV1 REST client/typed codecs with retries explicitly
// disabled. The coordinator owns retry and unknown-outcome recovery policy.
func (c *Client) updateConfigMap(ctx context.Context, object *corev1.ConfigMap) (*corev1.ConfigMap, error) {
	result := &corev1.ConfigMap{}
	err := c.core.RESTClient().Put().Namespace(object.Namespace).Resource("configmaps").Name(object.Name).VersionedParams(&metav1.UpdateOptions{}, scheme.ParameterCodec).Body(object).MaxRetries(0).Do(ctx).Into(result)
	return result, err
}
func (c *Client) deleteConfigMap(ctx context.Context, namespace, name string, options metav1.DeleteOptions) error {
	return c.core.RESTClient().Delete().Namespace(namespace).Resource("configmaps").Name(name).Body(&options).MaxRetries(0).Do(ctx).Error()
}

func (c *Client) createNamespace(ctx context.Context, object *corev1.Namespace) (*corev1.Namespace, error) {
	result := &corev1.Namespace{}
	err := c.core.RESTClient().Post().Resource("namespaces").VersionedParams(&metav1.CreateOptions{}, scheme.ParameterCodec).Body(object).MaxRetries(0).Do(ctx).Into(result)
	return result, err
}
func (c *Client) updateNamespace(ctx context.Context, object *corev1.Namespace) (*corev1.Namespace, error) {
	result := &corev1.Namespace{}
	err := c.core.RESTClient().Put().Resource("namespaces").Name(object.Name).VersionedParams(&metav1.UpdateOptions{}, scheme.ParameterCodec).Body(object).MaxRetries(0).Do(ctx).Into(result)
	return result, err
}
func (c *Client) deleteNamespace(ctx context.Context, name string, options metav1.DeleteOptions) error {
	return c.core.RESTClient().Delete().Resource("namespaces").Name(name).Body(&options).MaxRetries(0).Do(ctx).Error()
}
func (c *Client) createConfigMap(ctx context.Context, object *corev1.ConfigMap) (*corev1.ConfigMap, error) {
	result := &corev1.ConfigMap{}
	err := c.core.RESTClient().Post().Namespace(object.Namespace).Resource("configmaps").VersionedParams(&metav1.CreateOptions{}, scheme.ParameterCodec).Body(object).MaxRetries(0).Do(ctx).Into(result)
	return result, err
}
