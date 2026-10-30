package catalogdiagnostics

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"reflect"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const DesiredDataKey = "accepted-intent.json"
const LocalPinnedDesired = "local_pinned_accepted_intent"
const DesiredRevision = "desired-schema-1"
const desiredCellLabel = "registry.tmprl-demo.cloud/desired-cell"

// AcceptedDesired is metadata written only by the trusted intent acceptance
// path. It is not raw authoring, a credential or proof of source authorization.
// The actual writer/admission integration is separate from this read capability.
type AcceptedDesired struct {
	SchemaVersion int    `json:"schemaVersion"`
	Target        Target `json:"target"`
	ReconcileKey  string `json:"reconcileKey"`
	Generation    int64  `json:"generation"`
	DecisionID    string `json:"decisionID"`
}

func (v AcceptedDesired) JSON() ([]byte, error) {
	if v.SchemaVersion != 1 || !v.Target.valid() || !validReconcileKey(v.ReconcileKey) || v.Generation < 0 || !validReconcileKey(v.DecisionID) {
		return nil, ErrInvalidSource
	}
	b, err := json.Marshal(v)
	if err != nil || len(b) > 4096 {
		return nil, ErrInvalidSource
	}
	return b, nil
}
func DesiredCellName(target Target) string {
	b, _ := json.Marshal(target)
	sum := sha256.Sum256(b)
	return "project-desired-" + hex.EncodeToString(sum[:16])
}

// DesiredBootstrapObject is a pure metadata constructor for a separately
// authorized bootstrap/update. It does not create a cell or confer ownership.
func DesiredBootstrapObject(namespace string, desired AcceptedDesired) (*corev1.ConfigMap, error) {
	data, err := desired.JSON()
	if err != nil || namespace == "" || len(namespace) > 63 {
		return nil, ErrInvalidSource
	}
	return &corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}, ObjectMeta: metav1.ObjectMeta{Name: DesiredCellName(desired.Target), Namespace: namespace, Labels: map[string]string{desiredCellLabel: "true"}}, Data: map[string]string{DesiredDataKey: string(data)}}, nil
}

type DesiredBinding interface {
	Context() ownership.NamespacedContext
	ObserveConfigMap(context.Context, ownership.NamespacedRecord) (kube.ConfigMapSnapshot, error)
}
type PinnedDesiredConfig struct {
	Mode      string
	Target    Target
	Namespace DesiredBinding
	Pinned    ownership.NamespacedRecord
	Now       func() time.Time
}
type PinnedDesired struct{ config PinnedDesiredConfig }

func NewPinnedDesired(config PinnedDesiredConfig) (*PinnedDesired, error) {
	if config.Mode != LocalPinnedDesired || config.Namespace == nil || config.Now == nil || !config.Target.valid() {
		return nil, ErrInvalidSource
	}
	c := config.Namespace.Context()
	r := config.Pinned.Record
	owner := c.Owner()
	if owner.Lifetime != ownership.Disposable || owner.RunID == "" || c.ParentPurpose() != "namespace" || owner.EnvironmentID != config.Target.Environment || owner.InstallationID != config.Target.Installation || config.Pinned.Parent != c.Parent() || r.Owner != owner || r.Ref.Scope != c.Scope() || r.Ref.Purpose != "configmap" || r.Ref.Name != DesiredCellName(config.Target) || r.Ref.ProviderID == "" || r.CreationOperationID == "" || r.CreationIdempotencyKey == "" || r.Revision != DesiredRevision || r.SpecFingerprint == "" {
		return nil, ErrInvalidSource
	}
	return &PinnedDesired{config: config}, nil
}

// ObserveDesired only reads the independently pinned control cell. An API read
// establishes observation time, not storage commit time. It never recreates a
// missing object, follows a name replacement, queries a worker or reads secrets.
func (s *PinnedDesired) ObserveDesired(ctx context.Context, target Target) (DesiredObservation, error) {
	if target != s.config.Target {
		return DesiredObservation{}, ErrForbidden
	}
	observed, err := s.config.Namespace.ObserveConfigMap(ctx, s.config.Pinned)
	if err != nil {
		return DesiredObservation{}, ErrSourceUnavailable
	}
	o := observed.Object
	r := s.config.Pinned.Record
	if o == nil || o.Name != r.Ref.Name || o.Namespace != r.Ref.Scope.Namespace || string(o.UID) != r.Ref.ProviderID || o.ResourceVersion == "" || o.DeletionTimestamp != nil || observed.Observation.Observation.Ref != r.Ref || observed.Observation.Observation.Owner != r.Owner || observed.Observation.ParentUID != s.config.Pinned.Parent.UID || observed.Observation.Observation.CreationIdempotencyKey != r.CreationIdempotencyKey || o.Annotations[kube.CreationOperationAnnotation] != r.CreationOperationID || len(o.Data) != 1 || len(o.BinaryData) != 0 {
		return DesiredObservation{}, ErrInvalidSource
	}
	data, ok := o.Data[DesiredDataKey]
	if !ok || len(data) > 4096 {
		return DesiredObservation{}, ErrInvalidSource
	}
	var desired AcceptedDesired
	d := json.NewDecoder(bytes.NewReader([]byte(data)))
	d.DisallowUnknownFields()
	if d.Decode(&desired) != nil || d.Decode(new(any)) != io.EOF {
		return DesiredObservation{}, ErrInvalidSource
	}
	canonical, err := desired.JSON()
	// The accepted writer uses this canonical metadata encoding. This rejects
	// duplicate/case-aliased fields as well as unrecognized payload members.
	if err != nil || !bytes.Equal(canonical, bytes.TrimSpace([]byte(data))) || desired.Target != target {
		return DesiredObservation{}, ErrInvalidSource
	}
	expected, err := DesiredBootstrapObject(o.Namespace, desired)
	if err != nil {
		return DesiredObservation{}, ErrInvalidSource
	}
	fingerprint, err := kube.ConfigMapFingerprint(expected)
	if err != nil || fingerprint != observed.Observation.Observation.SpecFingerprint || observed.Observation.Observation.Revision != DesiredRevision || o.Immutable != nil || !reflect.DeepEqual(o.Labels, expected.Labels) {
		return DesiredObservation{}, ErrInvalidSource
	}
	if ctx.Err() != nil {
		return DesiredObservation{}, ErrSourceUnavailable
	}
	return DesiredObservation{Desired: diagnostics.DesiredState{ReconcileKey: desired.ReconcileKey, Generation: desired.Generation}, Known: true, ObservedAt: s.config.Now()}, nil
}
