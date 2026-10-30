package kubernetes

import (
	"sort"
	"strings"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

// StandardTemporalKeys describes a local credential Secret's key contract. It
// supplies no names, provider paths, credentials, namespace or production target.
func StandardTemporalKeys() map[string]string {
	return map[string]string{"temporal.address": "TEMPORAL_ADDRESS", "temporal.namespace": "TEMPORAL_NAMESPACE", "temporal.apiKey": "TEMPORAL_API_KEY", "temporal.tls": "TEMPORAL_TLS", "temporal.caCert": "TEMPORAL_CA_CERT", "temporal.clientCert": "TEMPORAL_CLIENT_CERT", "temporal.clientKey": "TEMPORAL_CLIENT_KEY"}
}

func validSecret(secret SecretBinding, b Bindings) error {
	if secret.Owner != b.Namespace.Owner() || secret.Namespace != b.Namespace.Scope().Namespace || secret.NamespaceUID != b.Namespace.Parent().UID {
		return renderError("ForeignSecretBinding", "secrets", "A projected Secret must belong to the trusted owner and current runtime Namespace UID.")
	}
	if secret.Name == "" || len(validation.IsDNS1123Subdomain(secret.Name)) > 0 {
		return renderError("InvalidSecretBinding", "secrets", "A projected Secret requires a valid explicit local name.")
	}
	for _, key := range secret.Keys {
		if len(validation.IsConfigMapKey(key)) > 0 {
			return renderError("InvalidSecretKey", "secrets", "A projected Secret contains an invalid local key identifier.")
		}
	}
	return nil
}

func requireSecret(secret SecretBinding, key string, b Bindings, requirements map[string]SecretRequirement) error {
	if err := validSecret(secret, b); err != nil {
		return err
	}
	value := requirements[secret.Name]
	value.Namespace, value.NamespaceUID, value.Name = secret.Namespace, secret.NamespaceUID, secret.Name
	if key != "" {
		found := false
		for _, present := range value.Keys {
			found = found || present == key
		}
		if !found {
			value.Keys = append(value.Keys, key)
			sort.Strings(value.Keys)
		}
	}
	requirements[secret.Name] = value
	return nil
}

func secretSelector(secret SecretBinding, property string, b Bindings, requirements map[string]SecretRequirement) (*corev1.EnvVarSource, error) {
	key, ok := secret.Keys[property]
	if !ok || key == "" {
		return nil, renderError("SecretPropertyBindingRequired", "env.valueFrom.inputRef", "The input property has no explicit trusted local Secret key binding.")
	}
	if err := requireSecret(secret, key, b, requirements); err != nil {
		return nil, err
	}
	return &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: secret.Name}, Key: key}}, nil
}

func environment(document project.Effective, name string, c project.Component, b Bindings, requirements map[string]SecretRequirement) ([]corev1.EnvVar, []corev1.EnvFromSource, error) {
	var env []corev1.EnvVar
	var from []corev1.EnvFromSource
	label := "primary"
	if c.TemporalNamespace != nil {
		label = string(*c.TemporalNamespace)
	}
	temporal, hasTemporal := b.Temporal[label]
	mode := "apiKey"
	if document.Spec.Temporal != nil && document.Spec.Temporal.Auth != nil && document.Spec.Temporal.Auth.Mode != nil {
		mode = *document.Spec.Temporal.Auth.Mode
	}
	checkTemporal := func() error {
		if !hasTemporal {
			return renderError("TemporalBindingRequired", "spec.components."+name, "Temporal inputs require an explicit label-to-local credential Secret binding.")
		}
		if temporal.Mode != mode {
			return renderError("TemporalAuthBindingMismatch", "spec.temporal.auth.mode", "The trusted credential binding authentication mode differs from the project.")
		}
		return validSecret(temporal.Secret, b)
	}
	proxy := c.TemporalProxy != nil && c.TemporalProxy.Enabled
	if (c.Worker != nil && c.Worker.Enabled || c.TemporalAccess != nil && *c.TemporalAccess) && !proxy {
		if err := checkTemporal(); err != nil {
			return nil, nil, err
		}
		required := []string{"temporal.address", "temporal.namespace", "temporal.tls"}
		defaultSecret := temporal.Secret
		if versioned(c) {
			if temporal.RuntimeSecret == nil {
				return nil, nil, renderError("WorkerRuntimeSecretBindingRequired", "worker.versioning", "A versioned worker requires a separate trusted runtime API-key/TLS Secret binding.")
			}
			defaultSecret = *temporal.RuntimeSecret
			required = []string{"temporal.tls"}
		}
		if mode == "apiKey" {
			required = append(required, "temporal.apiKey")
		} else {
			required = append(required, "temporal.caCert", "temporal.clientCert", "temporal.clientKey")
		}
		standard := StandardTemporalKeys()
		for _, property := range required {
			if defaultSecret.Keys[property] != standard[property] {
				return nil, nil, renderError("DefaultTemporalSecretShape", "temporal", "Default Temporal environment projection requires the standard local Secret key contract.")
			}
			if err := requireSecret(defaultSecret, standard[property], b, requirements); err != nil {
				return nil, nil, err
			}
		}
		from = append(from, corev1.EnvFromSource{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: defaultSecret.Name}}})
	}
	for _, secretName := range sortedSecretNames(c.Secrets) {
		secret, ok := b.ProjectSecrets[secretName]
		if !ok {
			return nil, nil, renderError("ProjectSecretBindingRequired", "spec.components."+name+".secrets", "The declared project Secret has no explicit trusted local binding.")
		}
		for property, key := range secret.Keys {
			if property != key {
				return nil, nil, renderError("SecretEnvFromShape", "secrets", "Whole-Secret environment projection requires unchanged property names.")
			}
			if err := requireSecret(secret, key, b, requirements); err != nil {
				return nil, nil, err
			}
		}
		if err := requireSecret(secret, "", b, requirements); err != nil {
			return nil, nil, err
		}
		from = append(from, corev1.EnvFromSource{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: secret.Name}}})
	}
	for secretName, declared := range document.Spec.Secrets {
		secret, ok := b.ProjectSecrets[secretName]
		if !ok {
			return nil, nil, renderError("ProjectSecretBindingRequired", "spec.secrets", "Every declared project Secret requires an explicit local binding.")
		}
		for _, property := range declared.RequiredProperties {
			key, ok := secret.Keys[string(property)]
			if !ok {
				return nil, nil, renderError("SecretPropertyBindingRequired", "spec.secrets.requiredProperties", "A required project Secret property has no local key binding.")
			}
			if err := requireSecret(secret, key, b, requirements); err != nil {
				return nil, nil, err
			}
		}
	}
	seen := map[string]bool{}
	for _, declared := range c.Env {
		key := string(declared.Name)
		if seen[key] {
			return nil, nil, renderError("DuplicateEnvironmentName", "spec.components."+name+".env", "Container environment names must be unique for deterministic Kubernetes field ownership.")
		}
		seen[key] = true
		value := corev1.EnvVar{Name: key}
		if declared.Value != nil {
			value.Value = *declared.Value
		} else if declared.ValueFrom != nil {
			input := declared.ValueFrom.InputRef
			switch {
			case input == "platform.rolloutVersion":
				value.Value = b.Revision
			case strings.HasPrefix(input, "temporal."):
				if err := checkTemporal(); err != nil {
					return nil, nil, err
				}
				selected := temporal.Secret
				if versioned(c) && (input == "temporal.apiKey" || input == "temporal.tls") {
					if temporal.RuntimeSecret == nil {
						return nil, nil, renderError("WorkerRuntimeSecretBindingRequired", "worker.versioning", "Versioned worker credential inputs require a runtime Secret binding.")
					}
					selected = *temporal.RuntimeSecret
				}
				source, err := secretSelector(selected, input, b, requirements)
				if err != nil {
					return nil, nil, err
				}
				value.ValueFrom = source
			case strings.HasPrefix(input, "secrets."):
				parts := strings.Split(input, ".")
				if len(parts) != 3 {
					return nil, nil, renderError("InvalidInputReference", "env.valueFrom.inputRef", "A project Secret input requires one logical Secret and property.")
				}
				secret, ok := b.ProjectSecrets[parts[1]]
				if !ok {
					return nil, nil, renderError("ProjectSecretBindingRequired", "env.valueFrom.inputRef", "The input Secret has no local trusted binding.")
				}
				source, err := secretSelector(secret, parts[2], b, requirements)
				if err != nil {
					return nil, nil, err
				}
				value.ValueFrom = source
			default:
				return nil, nil, renderError("UnsupportedInputReference", "env.valueFrom.inputRef", "The input reference is not supported.")
			}
		}
		env = append(env, value)
	}
	return env, from, nil
}

func sortedSecretNames(values []project.DNSLabel) []string {
	result := make([]string, len(values))
	for i, value := range values {
		result[i] = string(value)
	}
	sort.Strings(result)
	return result
}
