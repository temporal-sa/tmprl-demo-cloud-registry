package kubernetes

import (
	"encoding/json"
	"strings"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
	"k8s.io/apimachinery/pkg/util/validation"
)

func validateGateInput(component string, c project.Component, b Bindings, requirements map[string]SecretRequirement) (*ConfigInputRequirement, error) {
	if !versioned(c) || c.Worker.Versioning.Rollout == nil || c.Worker.Versioning.Rollout.Gate == nil {
		return nil, nil
	}
	gate := c.Worker.Versioning.Rollout.Gate
	if gate.Input != nil {
		// Literal input is authoring configuration only. Never source values from
		// credential bindings; reject recognizable credential fields as well.
		encoded, err := json.Marshal(gate.Input)
		if err != nil {
			return nil, renderError("InvalidGateConfiguration", "worker.rollout.gate.input", "Gate configuration cannot be encoded.")
		}
		var value any
		if err = json.Unmarshal(encoded, &value); err != nil {
			return nil, renderError("InvalidGateConfiguration", "worker.rollout.gate.input", "Gate configuration cannot be decoded.")
		}
		if credentialField(value) {
			return nil, renderError("CredentialGateInputDenied", "worker.rollout.gate.input", "Gate arguments must contain configuration, not credential fields.")
		}
	}
	if gate.InputFrom == nil {
		return nil, nil
	}
	kind := "ConfigMap"
	var source *project.LocalObjectKeyRef
	if gate.InputFrom.ConfigMapKeyRef != nil {
		source = gate.InputFrom.ConfigMapKeyRef
	} else {
		kind = "Secret"
		source = gate.InputFrom.SecretKeyRef
	}
	if source == nil {
		return nil, renderError("GateInputBindingRequired", "worker.rollout.gate.inputFrom", "The gate input source is incomplete.")
	}
	binding, approved := b.GateInputs[component]
	if !approved || binding.Policy != NonCredentialGateConfig || binding.Owner != b.Namespace.Owner() || binding.NamespaceUID != b.Namespace.Parent().UID || binding.Component != component || binding.Kind != kind || binding.Name != string(source.Name) || binding.Key != source.Key {
		return nil, renderError("GateInputBindingRequired", "worker.rollout.gate.inputFrom", "Gate input requires a separate owner-, namespace-, component- and key-scoped noncredential configuration policy.")
	}
	if len(validation.IsDNS1123Subdomain(binding.Name)) > 0 || len(validation.IsConfigMapKey(binding.Key)) > 0 {
		return nil, renderError("InvalidGateInputReference", "worker.rollout.gate.inputFrom", "The gate source object/key identifier is invalid.")
	}
	if credentialKey(binding.Key) {
		return nil, renderError("CredentialGateInputDenied", "worker.rollout.gate.inputFrom", "A credential property cannot supply gate arguments.")
	}
	if kind == "Secret" {
		// A configuration assertion cannot reclassify any known runtime,
		// bootstrap or Temporal authentication source into workflow arguments.
		for _, temporal := range b.Temporal {
			if binding.Name == temporal.Secret.Name || temporal.RuntimeSecret != nil && binding.Name == temporal.RuntimeSecret.Name {
				return nil, renderError("CredentialGateInputDenied", "worker.rollout.gate.inputFrom", "Runtime or Temporal credential Secrets cannot supply gate arguments.")
			}
		}
		for logical, declared := range b.ProjectSecrets {
			if declared.Name == binding.Name && (strings.Contains(strings.ToLower(logical), "bootstrap") || strings.Contains(strings.ToLower(logical), "temporal")) {
				return nil, renderError("CredentialGateInputDenied", "worker.rollout.gate.inputFrom", "A known bootstrap/Temporal Secret cannot supply gate arguments.")
			}
		}
		secret := SecretBinding{Namespace: b.Namespace.NamespaceName(), NamespaceUID: binding.NamespaceUID, Owner: binding.Owner, Name: binding.Name, Keys: map[string]string{binding.Key: binding.Key}}
		if err := requireSecret(secret, binding.Key, b, requirements); err != nil {
			return nil, err
		}
	}
	return &ConfigInputRequirement{Owner: binding.Owner, Namespace: b.Namespace.NamespaceName(), NamespaceUID: binding.NamespaceUID, Component: component, Kind: kind, Name: binding.Name, Key: binding.Key, Policy: binding.Policy}, nil
}

func credentialKey(key string) bool {
	canonical := strings.ToLower(strings.NewReplacer("-", "", "_", "", ".", "").Replace(key))
	return canonical == "apikey" || canonical == "token" || canonical == "password" || canonical == "privatekey" || canonical == "clientkey" || canonical == "clientsecret" || canonical == "authorization" || canonical == "accesstoken" || canonical == "refreshtoken" || canonical == "secretaccesskey" || canonical == "temporalcloudapikey" || canonical == "temporalapikey"
}

func credentialField(value any) bool {
	switch value := value.(type) {
	case map[string]any:
		for key, item := range value {
			if text, ok := item.(string); ok && text != "" && credentialKey(key) {
				return true
			}
			if credentialField(item) {
				return true
			}
		}
	case []any:
		for _, item := range value {
			if credentialField(item) {
				return true
			}
		}
	}
	return false
}
