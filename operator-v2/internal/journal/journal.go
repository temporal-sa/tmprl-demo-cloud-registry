// Package journal persists bounded rollout facts before provider dispatch. CAS
// provides journal ordering, not fencing of arbitrary provider calls. Epochs
// here record authority established by a separate serialized writer guard.
package journal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"regexp"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
)

const SchemaVersion = 1
const MaxBytes = 512 * 1024
const MaxAttempts = 256
const MaxWrites = 1024

var (
	ErrInvalid     = errors.New("invalid operation journal contract")
	ErrConflict    = errors.New("operation journal compare-and-swap conflict")
	ErrUnknown     = errors.New("operation journal write outcome is unknown")
	ErrMissing     = errors.New("pinned operation journal is missing or replaced")
	ErrUnavailable = errors.New("operation journal unavailable")
)

type AttemptState string

const (
	Reserved AttemptState = "Reserved"
	Observed AttemptState = "Observed"
	Draining AttemptState = "Draining"
	Consumed AttemptState = "Consumed"
)

type DrainReason string

const (
	DrainCandidateFailed     DrainReason = "CandidateFailed"
	DrainSuperseded          DrainReason = "Superseded"
	DrainProjectFinalization DrainReason = "ProjectFinalization"
)

func validDrainReason(reason DrainReason) bool {
	return reason == DrainCandidateFailed || reason == DrainSuperseded || reason == DrainProjectFinalization
}

type WriteState string

const (
	Pending   WriteState = "Pending"
	Unknown   WriteState = "Unknown"
	Committed WriteState = "Committed"
	Rejected  WriteState = "Rejected"
)

type Attempt struct {
	Number        uint64       `json:"number"`
	OperationID   string       `json:"operationID"`
	NamespaceName string       `json:"namespaceName"`
	NamespaceUID  string       `json:"namespaceUID,omitempty"`
	State         AttemptState `json:"state"`
	Revision      string       `json:"revision"`
	DesiredDigest string       `json:"desiredDigest"`
	DrainReason   DrainReason  `json:"drainReason,omitempty"`
	// CanonicalAbsent records an independently observed cleanup goal, not proof
	// that any ambiguous RPC committed or was finally not committed.
	CanonicalAbsent bool `json:"canonicalAbsent,omitempty"`
}
type Preconditions struct {
	Kind                string `json:"kind"`   // finite reviewed Namespace and typed children
	Action              string `json:"action"` // Create, Update, Delete; Namespace-only Finalize
	Name                string `json:"name"`
	ParentUID           string `json:"parentUID,omitempty"`
	UID                 string `json:"uid,omitempty"`
	Version             string `json:"version,omitempty"`
	Fingerprint         string `json:"fingerprint"`
	TargetRevision      string `json:"targetRevision"`
	ProviderOperationID string `json:"providerOperationID"`
}
type Write struct {
	ID              string        `json:"id"`
	OperationID     string        `json:"operationID"`
	WriterEpoch     uint64        `json:"writerEpoch"`
	Preconditions   Preconditions `json:"preconditions"`
	State           WriteState    `json:"state"`
	ResultUID       string        `json:"resultUID,omitempty"`
	ResultVersion   string        `json:"resultVersion,omitempty"`
	ConfirmedAbsent bool          `json:"confirmedAbsent,omitempty"`
	// Input identifies an independently retained private accepted Deployment
	// body. It carries no object, environment value, path, credential or URL.
	// The finite provider-only prototype may omit it; composed durable runtimes
	// must require it before dispatch and independently verify its payload.
	Input InputReference `json:"input,omitzero"`
	// Service creation pins its entire verified allocated/defaulted spec once.
	// A later GET cannot supply a missing original creation receipt.
	ResultSpecDigest   string `json:"resultSpecDigest,omitempty"`
	ResultSpecContract string `json:"resultSpecContract,omitempty"`
}

type InputReference struct {
	Kind    string `json:"kind"`
	UID     string `json:"uid"`
	Version string `json:"version"`
	// Digest hashes the complete private canonical envelope. The write's
	// Preconditions.Fingerprint separately hashes its original object body.
	Digest   string `json:"digest"`
	Contract string `json:"contract"`
}

const DeploymentInputContract = "kubernetes-deployment-v1-k8s1.37"
const ServiceInputContract = "kubernetes-service-v1-k8s1.37"
const ServiceAccountInputContract = "kubernetes-service-account-v1-k8s1.37"
const ServiceSpecContract = "clusterip-service-v1-k8s1.37"

var inputUID = regexp.MustCompile(`^render-[a-f0-9]{64}$`)

func (r InputReference) Valid() bool {
	return (r.Kind == "DeploymentRender" && r.Contract == DeploymentInputContract || r.Kind == "ServiceRender" && r.Contract == ServiceInputContract || r.Kind == "ServiceAccountRender" && r.Contract == ServiceAccountInputContract) && inputUID.MatchString(r.UID) && r.Version == "1" && hash.MatchString(r.Digest)
}

// MatchesKind prevents a valid private input of another family from being
// attached to an otherwise valid write. Zero remains a provider-only seam.
func (r InputReference) MatchesKind(kind string) bool {
	return r.Valid() && (kind == "Deployment" && r.Kind == "DeploymentRender" || kind == "Service" && r.Kind == "ServiceRender" || kind == "ServiceAccount" && r.Kind == "ServiceAccountRender")
}

type AdmissionState string

const (
	AdmissionActive AdmissionState = "Active"
	AdmissionReady  AdmissionState = "HandoffReady"
)

// Admission records one controlled clean-handoff session. It is not a lease
// and does not by itself fence a provider RPC or prove a process has exited.
type Admission struct {
	Generation uint64         `json:"generation"`
	SessionID  string         `json:"sessionID"`
	State      AdmissionState `json:"state"`
	HandoffID  string         `json:"handoffID,omitempty"`
}
type Envelope struct {
	SchemaVersion  int                               `json:"schemaVersion"`
	Subject        rollout.Subject                   `json:"subject"`
	Sequence       uint64                            `json:"sequence"`
	PreviousDigest string                            `json:"previousDigest,omitempty"`
	Digest         string                            `json:"digest"`
	WriterEpoch    uint64                            `json:"writerEpoch"`
	Attempts       []Attempt                         `json:"attempts,omitempty"`
	Writes         []Write                           `json:"writes,omitempty"`
	Projection     *diagnostics.OperationProjection  `json:"projection,omitempty"`
	Archive        []diagnostics.OperationProjection `json:"archive,omitempty"`
	Admission      *Admission                        `json:"admission,omitempty"`
}
type Version struct {
	UID             string `json:"uid"`
	ResourceVersion string `json:"resourceVersion"`
}
type Snapshot struct {
	Envelope Envelope
	Version  Version
}

// Cell is already bootstrapped and pinned through independently trusted UID and
// creation provenance. It must never recreate a missing/replaced object. Its
// control-plane owner may differ from the project Subject in data.
type Cell interface {
	Read(context.Context) ([]byte, Version, error)
	CompareAndSwap(context.Context, Version, []byte, string) error
}

// CommitConfirmer resolves backend-local uncertainty for this exact previously
// proposed commit. Observing its digest alone may not release a client's gate.
// It must never infer final-not-committed or clear another write's tombstone.
type CommitConfirmer interface {
	ConfirmCommit(context.Context, Version, []byte, string) error
}
type Repository struct {
	cell      Cell
	subject   rollout.Subject
	pinnedUID string
}

// This remains a finite set of reviewed typed children. Unknown kinds cannot
// gain journal semantics or parent authority through a caller-supplied string.
func childKind(kind string) bool {
	return kind == "ConfigMap" || kind == "Deployment" || kind == "Service" || kind == "ServiceAccount"
}

func attemptTransition(old, next AttemptState) bool {
	switch old {
	case Reserved:
		return next == Reserved || next == Observed || next == Consumed
	case Observed:
		return next == Observed || next == Draining
	case Draining:
		return next == Draining || next == Consumed
	case Consumed:
		return next == Consumed
	}
	return false
}
func hasFailure(e Envelope, operation string) bool {
	if p := e.Projection; p != nil && p.Identity.OperationID == operation && p.Failure != nil {
		return true
	}
	for _, p := range e.Archive {
		if p.Identity.OperationID == operation && p.Failure != nil {
			return true
		}
	}
	return false
}

func New(cell Cell, subject rollout.Subject, pinnedUID string) (*Repository, error) {
	if cell == nil || !validSubject(subject) || !id.MatchString(pinnedUID) {
		return nil, ErrInvalid
	}
	return &Repository{cell: cell, subject: subject, pinnedUID: pinnedUID}, nil
}

// Initial is used only in a separately authorized storage bootstrap. Creating
// this value is not evidence that a ConfigMap or any provider resource is owned.
func Initial(subject rollout.Subject, epoch uint64) (Envelope, error) {
	e := Envelope{SchemaVersion: SchemaVersion, Subject: subject, Sequence: 1, WriterEpoch: epoch}
	e.Digest = checksum(e)
	if err := validate(e); err != nil {
		return Envelope{}, err
	}
	return e, nil
}

func Decode(data []byte) (Envelope, error) {
	if len(data) > MaxBytes {
		return Envelope{}, ErrInvalid
	}
	var e Envelope
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(&e) != nil || d.Decode(new(any)) != io.EOF {
		return Envelope{}, ErrInvalid
	}
	if err := validate(e); err != nil {
		return Envelope{}, err
	}
	return e, nil
}
func (e Envelope) JSON() ([]byte, error) {
	if err := validate(e); err != nil {
		return nil, err
	}
	data, err := json.Marshal(e)
	if err != nil || len(data) > MaxBytes {
		return nil, ErrInvalid
	}
	return data, nil
}

func (r *Repository) Load(ctx context.Context) (Snapshot, error) {
	data, v, err := r.cell.Read(ctx)
	if err != nil {
		return Snapshot{}, safeError(err)
	}
	if v.UID != r.pinnedUID {
		return Snapshot{}, ErrMissing
	}
	e, err := Decode(data)
	if err != nil || e.Subject != r.subject || !validVersion(v) {
		return Snapshot{}, ErrInvalid
	}
	return Snapshot{e, v}, nil
}

// Commit seals the next sequence/hash. A replay after a lost acknowledgement
// accepts only the exact previously proposed digest. Reading old data alone
// cannot resolve a possibly delayed write; that outcome remains Unknown.
func (r *Repository) Commit(ctx context.Context, previous Snapshot, next Envelope) (Snapshot, error) {
	if previous.Envelope.Subject != r.subject || next.Subject != r.subject || next.SchemaVersion != SchemaVersion || !validVersion(previous.Version) || validate(previous.Envelope) != nil {
		return Snapshot{}, ErrInvalid
	}
	next.SchemaVersion = SchemaVersion
	next.Subject = r.subject
	if previous.Envelope.Sequence == ^uint64(0) {
		return Snapshot{}, ErrInvalid
	}
	if len(next.Attempts) > MaxAttempts || len(next.Writes) > MaxWrites || len(next.Archive) > MaxAttempts {
		return Snapshot{}, ErrInvalid
	}
	if old := previous.Envelope.Projection; old != nil && (next.Projection == nil || next.Projection.Identity != old.Identity) {
		next.Archive = append(append([]diagnostics.OperationProjection(nil), next.Archive...), *old)
	}
	next.Sequence = previous.Envelope.Sequence + 1
	next.PreviousDigest = previous.Envelope.Digest
	next.Digest = checksum(next)
	if err := transition(previous.Envelope, next); err != nil {
		return Snapshot{}, err
	}
	data, err := next.JSON()
	if err != nil {
		return Snapshot{}, err
	}
	current, err := r.Load(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	if current.Version.UID != previous.Version.UID {
		return Snapshot{}, ErrMissing
	}
	if current.Envelope.Digest == next.Digest {
		if confirmer, ok := r.cell.(CommitConfirmer); ok {
			if err := confirmer.ConfirmCommit(ctx, previous.Version, data, next.Digest); err != nil {
				return Snapshot{}, ErrUnknown
			}
		}
		return current, nil
	}
	if current.Version != previous.Version || current.Envelope.Digest != previous.Envelope.Digest {
		return Snapshot{}, ErrConflict
	}
	err = r.cell.CompareAndSwap(ctx, previous.Version, data, next.Digest)
	if err != nil && !errors.Is(err, ErrUnknown) {
		return Snapshot{}, safeError(err)
	}
	confirmed, readErr := r.Load(ctx)
	if readErr == nil && confirmed.Version.UID == previous.Version.UID && confirmed.Envelope.Digest == next.Digest {
		if confirmer, ok := r.cell.(CommitConfirmer); ok {
			if err := confirmer.ConfirmCommit(ctx, previous.Version, data, next.Digest); err != nil {
				return Snapshot{}, ErrUnknown
			}
		}
		return confirmed, nil
	}
	// A provider might have committed even when the acknowledgement/read was
	// lost. Preserve the exact proposal for recovery; never report success here.
	return Snapshot{}, ErrUnknown
}

var id = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}$`)
var hash = regexp.MustCompile(`^[a-f0-9]{64}$`)
var dns = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

func validSubject(s rollout.Subject) bool {
	return id.MatchString(s.Environment) && len(s.Environment) <= 128 && id.MatchString(s.Installation) && len(s.Installation) <= 128 && id.MatchString(s.ProjectUID) && len(s.ProjectUID) <= 128
}
func validVersion(v Version) bool { return id.MatchString(v.UID) && id.MatchString(v.ResourceVersion) }
func checksum(e Envelope) string {
	e.Digest = ""
	data, _ := json.Marshal(e)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
func safeError(err error) error {
	for _, known := range []error{ErrInvalid, ErrConflict, ErrUnknown, ErrMissing} {
		if errors.Is(err, known) {
			return known
		}
	}
	return ErrUnavailable
}

func validate(e Envelope) error {
	if e.SchemaVersion != SchemaVersion || !validSubject(e.Subject) || e.Sequence == 0 || e.WriterEpoch == 0 || !hash.MatchString(e.Digest) || checksum(e) != e.Digest || e.Sequence == 1 && e.PreviousDigest != "" || e.Sequence > 1 && !hash.MatchString(e.PreviousDigest) || len(e.Attempts) > MaxAttempts || len(e.Writes) > MaxWrites || len(e.Archive) > MaxAttempts {
		return ErrInvalid
	}
	if a := e.Admission; a != nil {
		if a.Generation == 0 || !hash.MatchString(a.SessionID) || a.State != AdmissionActive && a.State != AdmissionReady || a.State == AdmissionActive && a.HandoffID != "" || a.State == AdmissionReady && !hash.MatchString(a.HandoffID) {
			return ErrInvalid
		}
	}
	operations, names := map[string]bool{}, map[string]bool{}
	attempts := map[string]Attempt{}
	var last uint64
	for _, a := range e.Attempts {
		if a.Number <= last || !id.MatchString(a.OperationID) || !id.MatchString(a.Revision) || !hash.MatchString(a.DesiredDigest) || !dns.MatchString(a.NamespaceName) || operations[a.OperationID] || names[a.NamespaceName] {
			return ErrInvalid
		}
		if a.State != Reserved && a.State != Observed && a.State != Draining && a.State != Consumed || a.State == Reserved && a.NamespaceUID != "" || (a.State == Observed || a.State == Draining) && !id.MatchString(a.NamespaceUID) || a.NamespaceUID != "" && !id.MatchString(a.NamespaceUID) {
			return ErrInvalid
		}
		if a.State == Reserved || a.State == Observed {
			if a.DrainReason != "" || a.CanonicalAbsent {
				return ErrInvalid
			}
		} else {
			if !validDrainReason(a.DrainReason) || a.State == Draining && a.CanonicalAbsent || a.State == Consumed && a.CanonicalAbsent != (a.NamespaceUID != "") {
				return ErrInvalid
			}
			if a.DrainReason == DrainCandidateFailed && !hasFailure(e, a.OperationID) {
				return ErrInvalid
			}
		}
		last = a.Number
		operations[a.OperationID] = true
		names[a.NamespaceName] = true
		attempts[a.OperationID] = a
	}
	writes := map[string]bool{}
	creates := map[string]bool{}
	namespaceBirths := map[string]string{}
	for _, w := range e.Writes {
		p := w.Preconditions
		if p.Kind == "ServiceAccount" && (p.Action != "Create" || !w.Input.MatchesKind("ServiceAccount")) {
			return ErrInvalid
		}
		if p.Kind == "Service" {
			if p.Action != "Create" {
				return ErrInvalid
			}
			if w.State == Committed {
				if !hash.MatchString(w.ResultSpecDigest) || w.ResultSpecContract != ServiceSpecContract {
					return ErrInvalid
				}
			} else if w.ResultSpecDigest != "" || w.ResultSpecContract != "" {
				return ErrInvalid
			}
		} else if w.ResultSpecDigest != "" || w.ResultSpecContract != "" {
			return ErrInvalid
		}
		if w.Input != (InputReference{}) && (p.Action != "Create" || !w.Input.MatchesKind(p.Kind)) {
			return ErrInvalid
		}
		if !id.MatchString(w.ID) || writes[w.ID] || !operations[w.OperationID] || w.WriterEpoch == 0 || w.WriterEpoch > e.WriterEpoch || !hash.MatchString(p.Fingerprint) || !dns.MatchString(p.Name) || p.Kind != "Namespace" && !childKind(p.Kind) || p.Kind == "Namespace" && p.ParentUID != "" || childKind(p.Kind) && !id.MatchString(p.ParentUID) {
			return ErrInvalid
		}
		if p.Action == "Create" {
			if p.UID != "" || p.Version != "" {
				return ErrInvalid
			}
			key := w.OperationID + "/" + p.Kind + "/" + p.Name
			if creates[key] {
				return ErrInvalid
			}
			creates[key] = true
		} else if p.Action == "Update" || p.Action == "Delete" || p.Action == "Finalize" && p.Kind == "Namespace" {
			if !id.MatchString(p.UID) || !id.MatchString(p.Version) {
				return ErrInvalid
			}
		} else {
			return ErrInvalid
		}
		a := attempts[w.OperationID]
		if p.Kind == "Namespace" && (p.Action == "Delete" || p.Action == "Finalize") && a.State != Draining && a.State != Consumed {
			return ErrInvalid
		}
		if (w.State == Pending || w.State == Unknown) && (a.State == Consumed || a.State == Draining && (p.Kind != "Namespace" || p.Action != "Delete" && p.Action != "Finalize")) {
			return ErrInvalid
		}
		if p.TargetRevision != a.Revision || !id.MatchString(p.ProviderOperationID) || p.Kind == "Namespace" && p.Action == "Create" && p.ProviderOperationID != a.OperationID {
			return ErrInvalid
		}
		if p.Kind == "Namespace" && (p.Name != a.NamespaceName || p.Action != "Create" && p.UID != a.NamespaceUID) || childKind(p.Kind) && p.ParentUID != a.NamespaceUID {
			return ErrInvalid
		}
		if w.State != Pending && w.State != Unknown && w.State != Committed && w.State != Rejected {
			return ErrInvalid
		}
		if w.State == Committed {
			if p.Action == "Delete" || p.Action == "Finalize" && w.ConfirmedAbsent {
				if !w.ConfirmedAbsent || w.ResultUID != "" || w.ResultVersion != "" {
					return ErrInvalid
				}
			} else if !id.MatchString(w.ResultUID) || !id.MatchString(w.ResultVersion) || w.ConfirmedAbsent || (p.Action == "Update" || p.Action == "Finalize") && w.ResultUID != p.UID {
				return ErrInvalid
			}
			if p.Action == "Finalize" && !w.ConfirmedAbsent && w.ResultVersion == p.Version {
				return ErrInvalid
			}
		} else if w.ResultUID != "" || w.ResultVersion != "" || w.ConfirmedAbsent {
			return ErrInvalid
		}
		if p.Kind == "Namespace" && w.State == Committed {
			if p.Action == "Create" && (a.NamespaceUID != w.ResultUID || a.State == Reserved) || (p.Action == "Delete" || p.Action == "Finalize" && w.ConfirmedAbsent) && (a.State != Consumed || !a.CanonicalAbsent) {
				return ErrInvalid
			}
			if p.Action == "Create" {
				namespaceBirths[w.OperationID] = w.ResultUID
			}
		}
		writes[w.ID] = true
	}
	for _, a := range e.Attempts {
		if a.NamespaceUID != "" && namespaceBirths[a.OperationID] != a.NamespaceUID {
			return ErrInvalid
		}
	}
	if e.Projection != nil {
		p := e.Projection
		if p.Identity.Environment != e.Subject.Environment || p.Identity.Installation != e.Subject.Installation || p.Identity.ProjectUID != e.Subject.ProjectUID || !operations[p.Identity.OperationID] || p.Identity.OperationID != e.Attempts[len(e.Attempts)-1].OperationID {
			return ErrInvalid
		}
		if _, err := p.JSON(); err != nil {
			return ErrInvalid
		}
	}
	archived := map[string]bool{}
	for _, p := range e.Archive {
		if p.Identity.Environment != e.Subject.Environment || p.Identity.Installation != e.Subject.Installation || p.Identity.ProjectUID != e.Subject.ProjectUID || !operations[p.Identity.OperationID] || archived[p.Identity.OperationID] {
			return ErrInvalid
		}
		if _, err := p.JSON(); err != nil {
			return ErrInvalid
		}
		archived[p.Identity.OperationID] = true
	}
	return nil
}

func transition(old, next Envelope) error {
	if validate(next) != nil || next.WriterEpoch < old.WriterEpoch || len(next.Attempts) < len(old.Attempts) || len(next.Writes) < len(old.Writes) || len(next.Archive) < len(old.Archive) {
		return ErrInvalid
	}
	if err := admissionTransition(old, next); err != nil {
		return err
	}
	for i, p := range old.Archive {
		if jsonHash(p) != jsonHash(next.Archive[i]) {
			return ErrInvalid
		}
	}
	for i, a := range old.Attempts {
		n := next.Attempts[i]
		if a.Number != n.Number || a.OperationID != n.OperationID || a.Revision != n.Revision || a.DesiredDigest != n.DesiredDigest || a.NamespaceName != n.NamespaceName || a.NamespaceUID != "" && a.NamespaceUID != n.NamespaceUID || a.State == Consumed && n != a || a.DrainReason != "" && a.DrainReason != n.DrainReason || !attemptTransition(a.State, n.State) {
			return ErrInvalid
		}
		if a.State == Reserved && n.State == Consumed && n.NamespaceUID != "" {
			return ErrInvalid
		}
	}
	if len(next.Attempts) > len(old.Attempts) {
		for _, a := range next.Attempts[:len(old.Attempts)] {
			if a.State == Draining {
				return ErrInvalid
			}
		}
	}
	for _, a := range next.Attempts[len(old.Attempts):] {
		if a.State != Reserved || a.NamespaceUID != "" {
			return ErrInvalid
		}
	}
	for i, w := range old.Writes {
		n := next.Writes[i]
		if w.ID != n.ID || w.OperationID != n.OperationID || w.WriterEpoch != n.WriterEpoch || w.Preconditions != n.Preconditions || w.Input != n.Input || w.State == Unknown && n.State == Pending || (w.State == Committed || w.State == Rejected) && n != w {
			return ErrInvalid
		}
		if w.State == Unknown && w.Preconditions.Kind == "Namespace" && (w.Preconditions.Action == "Delete" || w.Preconditions.Action == "Finalize") && n != w {
			return ErrInvalid
		}
		if next.WriterEpoch != old.WriterEpoch && (w.State == Pending || w.State == Unknown) {
			return ErrInvalid
		}
		if len(next.Attempts) > len(old.Attempts) && (w.State == Pending || w.State == Unknown) {
			return ErrInvalid
		}
	}
	for _, w := range next.Writes[len(old.Writes):] {
		if w.State != Pending || w.WriterEpoch != next.WriterEpoch {
			return ErrInvalid
		}
		for _, prior := range old.Writes {
			if prior.OperationID == w.OperationID && prior.State == Unknown && prior.Preconditions.Kind == "Namespace" && (prior.Preconditions.Action == "Delete" || prior.Preconditions.Action == "Finalize") {
				return ErrInvalid
			}
		}
		for _, a := range next.Attempts {
			if a.OperationID == w.OperationID && (a.State == Consumed || childKind(w.Preconditions.Kind) && a.State != Observed || w.Preconditions.Kind == "Namespace" && w.Preconditions.Action == "Create" && a.State != Reserved || w.Preconditions.Kind == "Namespace" && (w.Preconditions.Action == "Delete" || w.Preconditions.Action == "Finalize") && a.State != Draining || w.Preconditions.Kind == "Namespace" && w.Preconditions.Action == "Update" && a.State != Observed) {
				return ErrInvalid
			}
		}
		if w.Preconditions.Action == "Create" {
			for _, prior := range old.Writes {
				if prior.OperationID == w.OperationID && prior.Preconditions.Kind == w.Preconditions.Kind && prior.Preconditions.Name == w.Preconditions.Name && prior.Preconditions.Action == "Create" {
					return ErrInvalid
				}
			}
		}
	}
	if old.Projection != nil {
		if next.Projection == nil || next.Projection.Identity != old.Projection.Identity {
			// Commit appends this exact value automatically. Make the invariant
			// explicit here so future callers/refactors cannot rewrite its cause.
			if len(next.Archive) <= len(old.Archive) || jsonHash(next.Archive[len(next.Archive)-1]) != jsonHash(*old.Projection) {
				return ErrInvalid
			}
		}
		if next.Projection == nil {
			if len(next.Attempts) == len(old.Attempts) {
				return ErrInvalid
			}
		} else if old.Projection.Identity == next.Projection.Identity {
			if next.Projection.Sequence < old.Projection.Sequence || !sameCause(old.Projection.Failure, next.Projection.Failure) {
				return ErrInvalid
			}
			if next.Projection.Sequence == old.Projection.Sequence {
				// Validate even an apparently identical compacted hash: a new sink
				// might otherwise be silently dropped by JSON size compaction.
				// Storage CAS advances independently from domain progress. Only
				// finite monotone publication repair can change this projection
				// without a new domain event; all other facts remain byteequal.
				if err := diagnostics.ValidatePublicationRepair(*old.Projection, *next.Projection); err != nil {
					return errors.Join(ErrInvalid, err)
				}
			}
		} else if len(next.Attempts) == len(old.Attempts) {
			return ErrInvalid
		}
	}
	return nil
}

func admissionTransition(old, next Envelope) error {
	a, b := old.Admission, next.Admission
	if a == nil && b == nil {
		return nil
	}
	if b == nil {
		return ErrInvalid
	}
	if a == nil {
		if old.Sequence != 1 || len(old.Attempts) != 0 || len(old.Writes) != 0 || old.Projection != nil || len(old.Archive) != 0 || b.Generation != 1 || b.State != AdmissionActive || !sameFacts(old, next) {
			return ErrInvalid
		}
		return nil
	}
	if *a == *b {
		if a.State == AdmissionReady {
			return ErrInvalid
		}
		return nil
	}
	if !sameFacts(old, next) {
		return ErrInvalid
	}
	for _, w := range old.Writes {
		if w.State == Pending || w.State == Unknown {
			return ErrInvalid
		}
	}
	if a.State == AdmissionActive && b.State == AdmissionReady && a.Generation == b.Generation && a.SessionID == b.SessionID {
		return nil
	}
	if a.State == AdmissionReady && b.State == AdmissionActive && a.Generation != ^uint64(0) && b.Generation == a.Generation+1 && b.SessionID != a.SessionID {
		return nil
	}
	return ErrInvalid
}

func sameFacts(old, next Envelope) bool {
	old.Sequence, next.Sequence = 0, 0
	old.Digest, next.Digest = "", ""
	old.PreviousDigest, next.PreviousDigest = "", ""
	old.Admission, next.Admission = nil, nil
	return jsonHash(old) == jsonHash(next)
}

// Native execution references can become known after the original provider
// failure. Add missing references without rewriting any existing one or cause.
func sameCause(old, next *diagnostics.DomainFailure) bool {
	if old == nil {
		return true
	}
	if next == nil {
		return false
	}
	a, b := *old, *next
	x, y := a.Execution, b.Execution
	for _, pair := range [][2]string{{x.Namespace, y.Namespace}, {x.WorkflowID, y.WorkflowID}, {x.RunID, y.RunID}, {x.ActivityID, y.ActivityID}, {x.ActivityType, y.ActivityType}} {
		if pair[0] != "" && pair[0] != pair[1] {
			return false
		}
	}
	if x.ScheduledEventID != 0 && x.ScheduledEventID != y.ScheduledEventID {
		return false
	}
	a.Execution, b.Execution = diagnostics.ExecutionRef{}, diagnostics.ExecutionRef{}
	return jsonHash(a) == jsonHash(b)
}

func jsonHash(value any) string {
	data, _ := json.Marshal(value)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// DesiredDigest binds an attempt to the complete safe rollout intent. It does
// not authenticate the intent or replace source/provider ownership checks.
func DesiredDigest(desired rollout.Desired) string { return jsonHash(desired) }
