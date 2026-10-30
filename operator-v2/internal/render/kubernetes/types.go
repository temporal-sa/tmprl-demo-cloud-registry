// Package kubernetes renders desired objects without a client, credential
// loader, environment lookup, or provider I/O. Rendered intents are plans;
// adapters must enforce ownership and provider preconditions before applying.
package kubernetes

import (
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

type Object interface {
	runtime.Object
	metav1.Object
}

type SecretBinding struct {
	Namespace    string
	NamespaceUID string
	Owner        ownership.Owner
	Name         string
	// Keys maps authoring property/input identifiers to local Secret keys.
	// No secret values are accepted or rendered.
	Keys map[string]string
}

type TemporalBinding struct {
	Mode           string
	Secret         SecretBinding
	RuntimeSecret  *SecretBinding
	Address        string
	CloudNamespace string
}

type Bindings struct {
	Namespace              ownership.NamespacedContext
	Revision               string
	OperationID            string
	Images                 map[string]string
	Temporal               map[string]TemporalBinding
	ProjectSecrets         map[string]SecretBinding
	ApprovedIAMRoles       map[string]string
	ApprovedTokenAutomount map[string]bool
	WorkerController       *WorkerControllerBinding
	AllowAutoscaling       bool
	ProxyImage             string
	GateInputs             map[string]GateInputBinding
}

type WorkerControllerBinding struct {
	// DeploymentNames are explicitly assigned control-plane identities, not
	// inferred from a project's name or a production naming convention.
	DeploymentNames map[string]string
}

type SecretRequirement struct {
	Namespace    string   `json:"namespace"`
	NamespaceUID string   `json:"namespaceUID"`
	Name         string   `json:"name"`
	Keys         []string `json:"keys,omitempty"`
}

type DesiredObject struct {
	Object Object
	Intent ownership.NamespacedIntent
}

type Bundle struct {
	Objects         []DesiredObject
	RequiredSecrets []SecretRequirement
	// Candidate rendering never emits public traffic resources. A caller must
	// explicitly render and conditionally apply the promotion phase.
	DeferredPromotion    bool
	RequiredConfigInputs []ConfigInputRequirement
}

type GateInputPolicy string

const NonCredentialGateConfig GateInputPolicy = "noncredential-configuration"

// GateInputBinding is separate from ordinary pod Secret access. Its policy is
// a trusted producer assertion that the payload contains no credential values;
// an adapter must verify that assertion against its actual data source before
// sending any input to Temporal history. No encrypted-payload bypass is implied.
type GateInputBinding struct {
	Owner        ownership.Owner
	NamespaceUID string
	Component    string
	Kind         string
	Name         string
	Key          string
	Policy       GateInputPolicy
}
type ConfigInputRequirement struct {
	Owner        ownership.Owner `json:"owner"`
	Namespace    string          `json:"namespace"`
	NamespaceUID string          `json:"namespaceUID"`
	Component    string          `json:"component"`
	Kind         string          `json:"kind"`
	Name         string          `json:"name"`
	Key          string          `json:"key"`
	Policy       GateInputPolicy `json:"policy"`
}

type PromotionBindings struct {
	Namespace                  ownership.NamespacedContext
	Backend                    ownership.NamespacedContext
	Revision                   string
	OperationID                string
	BaseDomain                 string
	AuthURL                    string
	TLSSecret                  *SecretBinding
	UseDefaultTLSStore         bool
	AllowPublicIngress         bool
	AllowCrossNamespaceBackend bool
	Routing                    *RoutingCapability
}

// RoutingCapability is a trusted installation assertion scoped to one stable
// namespace incarnation and domain. It is not itself provider authorization.
type RoutingCapability struct {
	NamespaceUID string
	BaseDomain   string
}

type NamespaceRequest struct {
	Revision    string
	OperationID string
	Fingerprint string
}

type NamespacePlan struct {
	Object *corev1.Namespace
	Intent ownership.Intent
}
