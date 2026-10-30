package kubernetes

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	OwnerAnnotation     = "registry.tmprl-demo.cloud/owner"
	ParentUIDAnnotation = ownership.ParentNamespaceUIDAnnotation
	RevisionAnnotation  = "registry.tmprl-demo.cloud/revision"
	OperationAnnotation = "registry.tmprl-demo.cloud/operation"
	RevisionLabel       = "registry.tmprl-demo.cloud/revision"
	OwnerLabel          = "registry.tmprl-demo.cloud/owner"
)

func digest(value any) string {
	encoded, _ := json.Marshal(value)
	hash := sha256.Sum256(encoded)
	return hex.EncodeToString(hash[:])
}
func ptr[T any](value T) *T { return &value }
func keys[T any](values map[string]T) []string {
	result := make([]string, 0, len(values))
	for key := range values {
		result = append(result, key)
	}
	sort.Strings(result)
	return result
}

func renderError(code, field, message string) error {
	return diagnostics.NewError(diagnostics.DomainFailure{ID: "render-" + code, Code: code, Category: diagnostics.Configuration, Message: message, Step: field, NextAction: "Provide a reviewed trusted binding or correct the referenced authoring field before rendering."})
}

func labels(owner ownership.Owner, projectName, component, revision string) map[string]string {
	result := map[string]string{"app.kubernetes.io/name": projectName, "app.kubernetes.io/managed-by": "registry-operator", OwnerLabel: digest(owner)[:32], RevisionLabel: digest(revision)[:32]}
	if component != "" {
		result["app.kubernetes.io/component"] = component
	}
	return result
}

func metadata(name, namespace, namespaceUID string, owner ownership.Owner, document project.Effective, component string, b Bindings) metav1.ObjectMeta {
	encoded, _ := json.Marshal(owner)
	return metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels(owner, document.Metadata.Name, component, b.Revision), Annotations: map[string]string{OwnerAnnotation: string(encoded), ParentUIDAnnotation: namespaceUID, RevisionAnnotation: b.Revision, OperationAnnotation: b.OperationID}}
}

// RenderNamespace is the separate bootstrap phase. Its context must be
// cluster-scoped; no Namespace UID exists yet, and it emits no child objects.
func RenderNamespace(context ownership.Context, request NamespaceRequest) (NamespacePlan, error) {
	scope, err := context.Scope(ownership.Kubernetes)
	if err != nil {
		return NamespacePlan{}, err
	}
	if scope.Namespace != "" {
		return NamespacePlan{}, renderError("ClusterScopeRequired", "namespace", "Namespace bootstrap requires explicit cluster-scoped authority.")
	}
	// OperationID is the trusted durable accepted-intent attempt ID. Retrying an
	// attempt is stable; intentionally revisiting a source revision gets a new
	// attempt and therefore a new namespace name. The apply ledger, not this
	// pure renderer, must forbid recreation after a previously observed UID.
	logical := digest(struct{ Revision, Attempt string }{request.Revision, request.OperationID})
	intent, err := context.Intent(ownership.Kubernetes, "runtime-namespace", logical, request.Revision, request.OperationID, request.Fingerprint, 63)
	if err != nil {
		return NamespacePlan{}, err
	}
	owner := context.Owner()
	encoded, _ := json.Marshal(owner)
	object := &corev1.Namespace{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"}, ObjectMeta: metav1.ObjectMeta{Name: intent.Name, Labels: map[string]string{"app.kubernetes.io/managed-by": "registry-operator", OwnerLabel: digest(owner)[:32], RevisionLabel: digest(request.Revision)[:32]}, Annotations: map[string]string{OwnerAnnotation: string(encoded), RevisionAnnotation: request.Revision, OperationAnnotation: request.OperationID}}}
	return NamespacePlan{Object: object, Intent: intent}, nil
}

func validateBinding(document project.Effective, b Bindings) error {
	scope := b.Namespace.Scope()
	if scope.Provider != ownership.Kubernetes || scope.Namespace == "" || b.Namespace.Parent().UID == "" {
		return renderError("NamespaceAuthorityRequired", "namespace", "Runtime rendering requires a verified Namespace owner and canonical parent UID.")
	}
	if b.Revision != b.Namespace.ParentRevision() {
		return renderError("CandidateNamespaceRevisionMismatch", "namespace", "Candidate workloads must target the verified Namespace ledger revision.")
	}
	if b.Revision == "" || b.OperationID == "" {
		return renderError("OperationIdentityRequired", "revision", "Revision and operation identity must be explicit.")
	}
	if document.Metadata.UID != "" && document.Metadata.UID != b.Namespace.Owner().ProjectUID {
		return renderError("ProjectIdentityMismatch", "metadata.uid", "The project UID differs from the trusted namespace owner.")
	}
	if len(b.Revision) > 256 || len(b.OperationID) > 256 {
		return renderError("OperationIdentityInvalid", "revision", "Revision and operation identity exceed their bounds.")
	}
	return nil
}

func desired(object Object, purpose string, b Bindings) (DesiredObject, error) {
	encoded, err := json.Marshal(object)
	if err != nil || len(encoded) > project.MaxDocumentBytes {
		return DesiredObject{}, renderError("RenderedObjectLimit", "objects", "A desired Kubernetes object exceeds its safe encoding limit.")
	}
	hash := sha256.Sum256(encoded)
	fingerprint := hex.EncodeToString(hash[:])
	intent, err := b.Namespace.Intent(purpose, object.GetName(), b.Revision, b.OperationID, fingerprint)
	if err != nil {
		return DesiredObject{}, err
	}
	return DesiredObject{Object: object, Intent: intent}, nil
}

func unsupported(field string) error {
	return renderError("UnsupportedKubernetesCapability", field, fmt.Sprintf("The requested runtime capability at %s is not implemented by this rendering slice.", field))
}
