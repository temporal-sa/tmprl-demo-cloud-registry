package kubernetes

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// NamespaceCleanupDescription is a copy of frozen, safe journal preconditions.
// It grants no authority and contains neither credentials nor request bodies.
type NamespaceCleanupDescription struct {
	Namespace, UID, ResourceVersion, OperationID, BodyDigest string
	Terminating                                              bool
	ForeignFinalizers                                        []string
}

// NamespaceCleanupResult separates acknowledged mutation from observed goal.
// Delete acknowledgement alone does not prove canonical namespace absence.
type NamespaceCleanupResult struct {
	Acknowledged, ConfirmedAbsent    bool
	UID, ResourceVersion             string
	Terminating, OwnFinalizerPresent bool
	ForeignFinalizers                []string
}

// PreparedNamespaceDelete and PreparedNamespaceFinalize are single-use,
// same-client capabilities. Their exact mutation bytes cannot be changed after
// preparation. They do not acknowledge a durable journal on the caller's behalf.
type PreparedNamespaceDelete struct{ ticket *preparedNamespaceCleanup }
type PreparedNamespaceFinalize struct{ ticket *preparedNamespaceCleanup }
type preparedNamespaceCleanup struct {
	mu       sync.Mutex
	used     bool
	binding  *NamespaceBinding
	writer   ownership.Writer
	observed ownership.Observation
	info     NamespaceCleanupDescription
	body     []byte
	finalize bool
}

func (p *preparedNamespaceCleanup) description() NamespaceCleanupDescription {
	info := p.info
	info.ForeignFinalizers = append([]string(nil), info.ForeignFinalizers...)
	return info
}
func (p *PreparedNamespaceDelete) Description() NamespaceCleanupDescription {
	if p == nil || p.ticket == nil {
		return NamespaceCleanupDescription{}
	}
	return p.ticket.description()
}
func (p *PreparedNamespaceFinalize) Description() NamespaceCleanupDescription {
	if p == nil || p.ticket == nil {
		return NamespaceCleanupDescription{}
	}
	return p.ticket.description()
}

// ResumeDrainingNamespace restores only an irreversible cleanup lifecycle after
// the controlled supervisor has verified durable admission and drained the old
// process. It is not a lease or recovery of an unknown write. Calling this on a
// different client cannot resolve another client's uncertain operation.
func (c *Client) ResumeDrainingNamespace(ctx context.Context, base ownership.Context, parent ownership.Record, attemptID string) (*NamespaceBinding, error) {
	if c == nil || c.core == nil || attemptID == "" {
		return nil, failure(InvalidConfiguration, nil)
	}
	scope, err := base.Scope(ownership.Kubernetes)
	if err != nil || scope.Namespace != "" || scope.ClusterID != c.identity.ID || parent.Ref.Scope != scope {
		return nil, failure(OwnershipRejected, err)
	}
	current, raw, err := c.readParentState(ctx, parent, true)
	if err != nil {
		return nil, err
	}
	if raw.Annotations[AttemptAnnotation] != attemptID {
		return nil, failure(OwnershipRejected, nil)
	}
	bound, err := base.BindNamespace(parent, current)
	if err != nil {
		return nil, failure(OwnershipRejected, err)
	}
	if err := c.lifecycle.restoreDraining(parent.Ref.Name, parent.Ref.ProviderID, attemptID); err != nil {
		return nil, err
	}
	return &NamespaceBinding{client: c, base: base, parent: parent, bound: bound, attemptID: attemptID}, nil
}

// ObserveDrainingNamespace performs only read-only provider I/O, but changes
// the local lifecycle to irreversibly draining. Absence is an observed goal, never
// evidence that an ambiguous DELETE/UPDATE committed or did not commit.
func (b *NamespaceBinding) ObserveDrainingNamespace(ctx context.Context) (NamespaceCleanupResult, error) {
	if err := b.client.lifecycle.drain(ctx, b.parent.Ref.Name, b.parent.Ref.ProviderID, b.attemptID); err != nil {
		return NamespaceCleanupResult{}, err
	}
	_, object, err := b.refreshState(ctx, true)
	if apierrors.IsNotFound(err) {
		return NamespaceCleanupResult{ConfirmedAbsent: true}, nil
	}
	if err != nil {
		return NamespaceCleanupResult{}, err
	}
	return NamespaceCleanupResult{UID: string(object.UID), ResourceVersion: object.ResourceVersion,
		Terminating:         object.DeletionTimestamp != nil || object.Status.Phase == corev1.NamespaceTerminating,
		OwnFinalizerPresent: contains(object.Finalizers, NamespaceLifecycleFinalizer), ForeignFinalizers: foreignNamespaceFinalizers(object)}, nil
}

func foreignNamespaceFinalizers(object *corev1.Namespace) []string {
	var result []string
	for _, value := range object.Finalizers {
		if value != NamespaceLifecycleFinalizer {
			result = append(result, value)
		}
	}
	for _, value := range object.Spec.Finalizers {
		if value != "kubernetes" {
			result = append(result, string(value))
		}
	}
	return result
}

func (b *NamespaceBinding) prepareCleanup(ctx context.Context, writer ownership.Writer, operationID string, finalize bool) (*preparedNamespaceCleanup, error) {
	if operationID == "" || len(operationID) > 256 || strings.TrimSpace(operationID) != operationID {
		return nil, failure(InvalidConfiguration, nil)
	}
	if err := b.client.lifecycle.drain(ctx, b.parent.Ref.Name, b.parent.Ref.ProviderID, b.attemptID); err != nil {
		return nil, err
	}
	observed, object, err := b.refreshState(ctx, true)
	if err != nil {
		return nil, err
	}
	if _, err := b.base.Authorize(ownership.MutationRequest{Action: ownership.Cleanup, Record: b.parent, Observed: observed, ExpectedRevision: b.parent.Revision, Writer: writer, Now: time.Now(), Fencing: ownership.ProviderConditional}); err != nil {
		return nil, failure(OwnershipRejected, err)
	}
	info := NamespaceCleanupDescription{Namespace: object.Name, UID: string(object.UID), ResourceVersion: object.ResourceVersion, OperationID: operationID, Terminating: object.DeletionTimestamp != nil || object.Status.Phase == corev1.NamespaceTerminating}
	info.ForeignFinalizers = foreignNamespaceFinalizers(object)
	var body []byte
	if finalize {
		if !info.Terminating || !contains(object.Finalizers, NamespaceLifecycleFinalizer) {
			return nil, failure(ConditionalConflict, nil)
		}
		updated := object.DeepCopy()
		updated.TypeMeta = metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"}
		updated.Finalizers = nil
		for _, value := range object.Finalizers {
			if value != NamespaceLifecycleFinalizer {
				updated.Finalizers = append(updated.Finalizers, value)
			}
		}
		body, err = json.Marshal(updated)
	} else {
		uid, version := types.UID(info.UID), info.ResourceVersion
		body, err = json.Marshal(metav1.DeleteOptions{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "DeleteOptions"}, Preconditions: &metav1.Preconditions{UID: &uid, ResourceVersion: &version}})
	}
	if err != nil {
		return nil, failure(InvalidConfiguration, nil)
	}
	digest := sha256.Sum256(body)
	info.BodyDigest = "sha256:" + hex.EncodeToString(digest[:])
	return &preparedNamespaceCleanup{binding: b, writer: writer, observed: observed, info: info, body: body, finalize: finalize}, nil
}

func (b *NamespaceBinding) PrepareNamespaceDelete(ctx context.Context, writer ownership.Writer, operationID string) (*PreparedNamespaceDelete, error) {
	ticket, err := b.prepareCleanup(ctx, writer, operationID, false)
	if err != nil {
		return nil, err
	}
	return &PreparedNamespaceDelete{ticket: ticket}, nil
}
func (b *NamespaceBinding) PrepareOwnFinalizerRemoval(ctx context.Context, writer ownership.Writer, operationID string) (*PreparedNamespaceFinalize, error) {
	ticket, err := b.prepareCleanup(ctx, writer, operationID, true)
	if err != nil {
		return nil, err
	}
	return &PreparedNamespaceFinalize{ticket: ticket}, nil
}

func (p *PreparedNamespaceDelete) Execute(ctx context.Context) (NamespaceCleanupResult, error) {
	if p == nil || p.ticket == nil {
		return NamespaceCleanupResult{}, failure(InvalidConfiguration, nil)
	}
	return p.ticket.execute(ctx)
}
func (p *PreparedNamespaceFinalize) Execute(ctx context.Context) (NamespaceCleanupResult, error) {
	if p == nil || p.ticket == nil {
		return NamespaceCleanupResult{}, failure(InvalidConfiguration, nil)
	}
	return p.ticket.execute(ctx)
}

// finalizationResponseMatches checks the immediate response against precisely
// what this request preserved. Server-owned metadata/status may advance. This
// does not fence later independent controller/admin changes.
func finalizationResponseMatches(body []byte, actual *corev1.Namespace) bool {
	var expected corev1.Namespace
	if json.Unmarshal(body, &expected) != nil {
		return false
	}
	project := func(object *corev1.Namespace) []byte {
		data, _ := json.Marshal(struct {
			Metadata metav1.ObjectMeta    `json:"metadata"`
			Spec     corev1.NamespaceSpec `json:"spec"`
		}{Metadata: metav1.ObjectMeta{Name: object.Name, GenerateName: object.GenerateName, Namespace: object.Namespace, UID: object.UID, Labels: object.Labels, Annotations: object.Annotations, OwnerReferences: object.OwnerReferences, Finalizers: object.Finalizers}, Spec: object.Spec})
		return data
	}
	return bytes.Equal(project(&expected), project(actual))
}

func (p *preparedNamespaceCleanup) execute(ctx context.Context) (NamespaceCleanupResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.used {
		return NamespaceCleanupResult{}, failure(ConditionalConflict, nil)
	}
	p.used = true
	b := p.binding
	if _, err := b.base.Authorize(ownership.MutationRequest{Action: ownership.Cleanup, Record: b.parent, Observed: p.observed, ExpectedRevision: b.parent.Revision, Writer: p.writer, Now: time.Now(), Fencing: ownership.ProviderConditional}); err != nil {
		return NamespaceCleanupResult{}, failure(OwnershipRejected, err)
	}
	action := "delete-namespace"
	if p.finalize {
		action = "finalize-namespace"
	}
	op := OperationTombstone{ID: p.info.OperationID, Action: action, Name: p.info.Namespace, UID: p.info.UID, ExpectedVersion: p.info.ResourceVersion, TargetRevision: b.parent.Revision, TargetFingerprint: p.info.BodyDigest, WriterEpoch: p.writer.Epoch}
	result := NamespaceCleanupResult{}
	err := b.client.lifecycle.runCleanup(b.parent.Ref.Name, b.parent.Ref.ProviderID, b.attemptID, op, func() error {
		call, cancel := context.WithTimeout(ctx, b.client.timeout)
		defer cancel()
		request := b.client.core.RESTClient().Delete().Resource("namespaces").Name(p.info.Namespace)
		if p.finalize {
			request = b.client.core.RESTClient().Put().Resource("namespaces").Name(p.info.Namespace)
		}
		raw, err := request.SetHeader("Content-Type", "application/json").Body(p.body).MaxRetries(0).Do(call).Raw()
		if err != nil {
			return mutationError(err)
		}
		var kind metav1.TypeMeta
		if json.Unmarshal(raw, &kind) != nil || kind.APIVersion != "v1" {
			return failure(UnknownWriteOutcome, nil)
		}
		if kind.Kind == "Status" {
			var status metav1.Status
			if json.Unmarshal(raw, &status) != nil || status.Status != metav1.StatusSuccess {
				return failure(UnknownWriteOutcome, nil)
			}
		} else if kind.Kind == "Namespace" {
			var object corev1.Namespace
			if json.Unmarshal(raw, &object) != nil || string(object.UID) != p.info.UID || object.Name != p.info.Namespace {
				return failure(UnknownWriteOutcome, nil)
			}
			if p.finalize {
				if !finalizationResponseMatches(p.body, &object) {
					return failure(UnknownWriteOutcome, nil)
				}
				observed, err := b.client.observeMetadata(&object, "")
				if err != nil {
					return failure(UnknownWriteOutcome, err)
				}
				if _, err := b.base.BindNamespace(b.parent, observed); err != nil || object.Annotations[CreationOperationAnnotation] != b.parent.CreationOperationID || object.Annotations[AttemptAnnotation] != b.attemptID || contains(object.Finalizers, NamespaceLifecycleFinalizer) {
					return failure(UnknownWriteOutcome, err)
				}
				if object.ResourceVersion == p.info.ResourceVersion {
					// Kubernetes may return the exact updated Namespace with its
					// old version when removal deletes the object. This matching
					// 2xx is an acknowledgement; only same-callback independent
					// canonical absence completes this finite outcome. It never
					// resolves a previously returned uncertain operation.
					_, _, err := b.refreshState(ctx, true)
					if !apierrors.IsNotFound(err) {
						return failure(UnknownWriteOutcome, err)
					}
					result = NamespaceCleanupResult{Acknowledged: true, ConfirmedAbsent: true}
					return nil
				}
				result = NamespaceCleanupResult{Acknowledged: true, UID: string(object.UID), ResourceVersion: object.ResourceVersion}
				return nil
			}
		} else {
			return failure(UnknownWriteOutcome, nil)
		}
		result.Acknowledged = true
		if p.finalize {
			// A success Status carries no invented object version. Independently
			// prove canonical absence or the same-UID finalizer removal.
			_, object, err := b.refreshState(ctx, true)
			if apierrors.IsNotFound(err) {
				result.ConfirmedAbsent = true
				return nil
			}
			if err != nil || contains(object.Finalizers, NamespaceLifecycleFinalizer) || object.ResourceVersion == p.info.ResourceVersion || !finalizationResponseMatches(p.body, object) {
				return failure(UnknownWriteOutcome, err)
			}
			result.UID, result.ResourceVersion = string(object.UID), object.ResourceVersion
		}
		return nil
	})
	return result, err
}
