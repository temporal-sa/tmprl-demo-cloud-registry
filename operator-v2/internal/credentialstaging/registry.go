// Package credentialstaging implements a controlled disposable staging actor.
// Its immutable bootstrap reservations are authoritative only for managed,
// staging-only writers: there is no promotion API, external-writer fencing or
// production/distributed claim. Cooperative local filesystems must honor
// flock/fsync; restoring metadata backups requires a separate high-water witness.
package credentialstaging

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"reflect"
	"regexp"
	"sort"
	"syscall"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubejournal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/localmetadata"
	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
)

type Error string

func (e Error) Error() string { return string(e) }

const (
	Invalid       Error = "CredentialStagingInvalidContract"
	Changed       Error = "CredentialStagingPinnedStateChanged"
	Blocked       Error = "CredentialStagingRecoveryRequired"
	Closed        Error = "CredentialStagingActorClosed"
	Uncertain     Error = "CredentialStagingOutcomeUncertain"
	Reserved      Error = "CredentialStagingRuntimeIdentityReserved"
	NotAuthorized Error = "CredentialStagingInvocationDenied"
)

var safeID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/#-]{0,511}$`)
var hexDigest = regexp.MustCompile(`^[a-f0-9]{64}$`)

// Reservation is an independently inventoried runtime identity that staging
// must never use. CandidateID is its exact Secrets Manager name, not an ARN
// prefix; KeyID protects reused key identities even under another secret name.
// Empty inventories require an explicit bootstrap attestation, not an empty
// in-memory map or Active=false received from a provider.
type Reservation struct {
	StoreID, CandidateID, KeyID string
}

type RegistryConfig struct {
	BootstrapUID         string
	Owner                ownership.Owner
	Control              kubejournal.Identity
	Namespace            tc.Binding
	Metadata             localmetadata.Binding
	MetadataConfigDigest string
	StoreID              string
	// InventoryReceipt is independently retained bootstrap review evidence for
	// the complete reservation inventory, including deliberately empty cases.
	InventoryReceipt string
	Reservations     []Reservation
	// Omitted at zero to preserve exact stage-only v1 registry bytes. Read
	// settings require fresh explicit bootstrap, not in-place enable/disable.
	ObservationBindingDigest    string `json:",omitempty"`
	AuthObservationPolicyDigest string `json:",omitempty"`
}

// RegistryBinding must be pinned outside the root. Reopen never creates a
// missing manifest/lock or trusts an equivalent manifest in a different inode.
type RegistryBinding struct {
	BootstrapUID              string
	Directory, Lock, Manifest localmetadata.FileID
	ManifestDigest            string
}

type manifest struct {
	Schema          string
	Config          RegistryConfig
	Directory, Lock localmetadata.FileID
}

type registry struct {
	root    *os.Root
	binding RegistryBinding
	config  RegistryConfig
}

func sum(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func rawSum(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

func normalized(c RegistryConfig) (RegistryConfig, error) {
	if c.ObservationBindingDigest != "" && !hexDigest.MatchString(c.ObservationBindingDigest) {
		return RegistryConfig{}, Invalid
	}
	if c.AuthObservationPolicyDigest != "" && (!hexDigest.MatchString(c.AuthObservationPolicyDigest) || c.ObservationBindingDigest == "") {
		return RegistryConfig{}, Invalid
	}
	if !safeID.MatchString(c.BootstrapUID) || c.Owner.Lifetime != ownership.Disposable || c.Owner.RunID == "" || !safeID.MatchString(c.StoreID) || !safeID.MatchString(c.InventoryReceipt) || !hexDigest.MatchString(c.MetadataConfigDigest) || c.Metadata.ConfigSHA256 != c.MetadataConfigDigest || c.Metadata.BootstrapUID == "" || !hexDigest.MatchString(c.Metadata.IdentitySHA256) || len(c.Reservations) > 256 {
		return RegistryConfig{}, Invalid
	}
	subject := rollout.Subject{Environment: c.Owner.EnvironmentID, Installation: c.Owner.InstallationID, ProjectUID: c.Owner.ProjectUID}
	if _, e := journal.Initial(subject, 1); e != nil {
		return RegistryConfig{}, Invalid
	}
	if c.Control.ControlOwner.Lifetime != ownership.Stable || c.Control.ControlOwner.RunID != "" || c.Control.ControlParent.ClusterID == "" || c.Namespace.Record.Ref.Scope.Provider != ownership.Temporal || c.Namespace.Record.Ref.Purpose != "namespace" || c.Namespace.Record.CreationOperationID == "" || c.Namespace.Record.CreationIdempotencyKey == "" {
		return RegistryConfig{}, Invalid
	}
	if c.Control.Subject != subject || c.Control.CellUID == "" || c.Control.ControlParent.UID == "" || c.Control.ControlOwner.EnvironmentID != c.Owner.EnvironmentID || c.Control.ControlOwner.InstallationID != c.Owner.InstallationID || c.Namespace.Record.Owner != c.Owner || c.Namespace.CreatedAt.IsZero() || c.Namespace.Record.Ref.ProviderID != tc.IncarnationID(c.Namespace.NamespaceID, c.Namespace.CreatedAt) || c.Namespace.ProvenanceID == "" {
		return RegistryConfig{}, Invalid
	}
	c.Reservations = append([]Reservation(nil), c.Reservations...)
	for _, r := range c.Reservations {
		if !safeID.MatchString(r.StoreID) || (!safeID.MatchString(r.CandidateID) && r.CandidateID != "") || (!safeID.MatchString(r.KeyID) && r.KeyID != "") || r.CandidateID == "" && r.KeyID == "" {
			return RegistryConfig{}, Invalid
		}
	}
	sort.Slice(c.Reservations, func(i, j int) bool { return sum(c.Reservations[i]) < sum(c.Reservations[j]) })
	for i := 1; i < len(c.Reservations); i++ {
		if c.Reservations[i] == c.Reservations[i-1] {
			return RegistryConfig{}, Invalid
		}
	}
	return c, nil
}

func fileID(info os.FileInfo, dir bool) (localmetadata.FileID, bool) {
	if info == nil || info.IsDir() != dir || (!dir && !info.Mode().IsRegular()) || dir && info.Mode().Perm() != 0700 || !dir && info.Mode().Perm() != 0600 {
		return localmetadata.FileID{}, false
	}
	s, ok := info.Sys().(*syscall.Stat_t)
	if !ok || s.Uid != uint32(os.Getuid()) || !dir && s.Nlink != 1 {
		return localmetadata.FileID{}, false
	}
	return localmetadata.FileID{Device: uint64(s.Dev), Inode: uint64(s.Ino), UID: s.Uid}, true
}

func rootID(root *os.Root) (localmetadata.FileID, error) {
	if root == nil {
		return localmetadata.FileID{}, Invalid
	}
	f, e := root.Open(".")
	if e != nil {
		return localmetadata.FileID{}, Changed
	}
	defer f.Close()
	i, e := f.Stat()
	id, ok := fileID(i, true)
	if e != nil || !ok {
		return localmetadata.FileID{}, Changed
	}
	return id, nil
}

// BootstrapRegistry requires an explicit preexisting empty private directory.
// It creates only immutable metadata and a lock, fsyncs/readbacks it, and returns
// an independently retained binding. Any partial outcome is retained; no reset.
func BootstrapRegistry(ctx context.Context, root *os.Root, config RegistryConfig) (RegistryBinding, error) {
	c, e := normalized(config)
	if e != nil {
		return RegistryBinding{}, e
	}
	dir, e := rootID(root)
	if e != nil {
		return RegistryBinding{}, e
	}
	d, e := root.Open(".")
	if e != nil {
		return RegistryBinding{}, Changed
	}
	defer d.Close()
	entries, e := d.ReadDir(1)
	if len(entries) != 0 || e != nil && e != io.EOF {
		return RegistryBinding{}, Changed
	}
	if ctx.Err() != nil {
		return RegistryBinding{}, Uncertain
	}
	f, e := root.OpenFile("lock", os.O_CREATE|os.O_EXCL|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if e != nil {
		return RegistryBinding{}, Changed
	}
	defer f.Close()
	i, e := f.Stat()
	lock, ok := fileID(i, false)
	if e != nil || !ok || f.Sync() != nil {
		return RegistryBinding{}, Uncertain
	}
	raw, e := json.Marshal(manifest{Schema: registrySchema(c), Config: c, Directory: dir, Lock: lock})
	if e != nil || len(raw) > 131072 {
		return RegistryBinding{}, Invalid
	}
	m, e := root.OpenFile("reservations.json", os.O_CREATE|os.O_EXCL|os.O_WRONLY|syscall.O_NOFOLLOW, 0600)
	if e != nil {
		return RegistryBinding{}, Uncertain
	}
	mi, me := m.Stat()
	mid, mok := fileID(mi, false)
	if me != nil || !mok {
		_ = m.Close()
		return RegistryBinding{}, Uncertain
	}
	n, we := m.Write(raw)
	se := m.Sync()
	ce := m.Close()
	if we != nil || n != len(raw) || se != nil || ce != nil || d.Sync() != nil {
		return RegistryBinding{}, Uncertain
	}
	b := RegistryBinding{BootstrapUID: c.BootstrapUID, Directory: dir, Lock: lock, Manifest: mid, ManifestDigest: rawSum(raw)}
	r := registry{root: root, binding: b, config: c}
	if e := r.locked(ctx, func() error { return nil }); e != nil {
		return RegistryBinding{}, e
	}
	return b, nil
}

func (r *registry) check() error {
	d, e := rootID(r.root)
	if e != nil || d != r.binding.Directory {
		return Changed
	}
	lockInfo, lockError := r.root.Lstat("lock")
	lockID, lockOK := fileID(lockInfo, false)
	if lockError != nil || !lockOK || lockID != r.binding.Lock || lockInfo.Size() != 0 {
		return Changed
	}
	info, e := r.root.Lstat("reservations.json")
	before, ok := fileID(info, false)
	if e != nil || !ok || before != r.binding.Manifest || info.Size() > 131072 {
		return Changed
	}
	f, e := r.root.OpenFile("reservations.json", os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if e != nil {
		return Changed
	}
	defer f.Close()
	i, e := f.Stat()
	after, ok := fileID(i, false)
	if e != nil || !ok || before != after {
		return Changed
	}
	raw, e := io.ReadAll(io.LimitReader(f, 131073))
	if e != nil || len(raw) > 131072 || rawSum(raw) != r.binding.ManifestDigest {
		return Changed
	}
	var m manifest
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(&m) != nil || dec.Decode(new(any)) != io.EOF || m.Schema != registrySchema(r.config) || m.Directory != d || m.Lock != r.binding.Lock || m.Config.BootstrapUID != r.binding.BootstrapUID || !reflect.DeepEqual(m.Config, r.config) {
		return Changed
	}
	return nil
}

func registrySchema(c RegistryConfig) string {
	if c.AuthObservationPolicyDigest != "" {
		return "staging-auth-observation-receipts-v1"
	}
	if c.ObservationBindingDigest != "" {
		return "staging-observation-reservations-v1"
	}
	return "staging-reservations-v1"
}

// locked holds the pinned inode lock throughout the provider callback. The
// actor supplies its own lifecycle mutex; this lock also covers cooperative
// separately opened handles. It is not permission to adopt restored metadata.
func (r *registry) locked(ctx context.Context, fn func() error) error {
	if e := r.check(); e != nil {
		return e
	}
	f, e := r.root.OpenFile("lock", os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if e != nil {
		return Changed
	}
	defer f.Close()
	i, e := f.Stat()
	id, ok := fileID(i, false)
	if e != nil || !ok || id != r.binding.Lock || i.Size() != 0 {
		return Changed
	}
	for {
		e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if e == nil {
			break
		}
		if e != syscall.EWOULDBLOCK && e != syscall.EAGAIN {
			return Uncertain
		}
		select {
		case <-ctx.Done():
			return Uncertain
		case <-time.After(time.Millisecond):
		}
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	i, e = r.root.Lstat("lock")
	id, ok = fileID(i, false)
	if e != nil || !ok || id != r.binding.Lock {
		return Changed
	}
	if e = r.check(); e != nil {
		return e
	}
	if ctx.Err() != nil {
		return Uncertain
	}
	return fn()
}

func (r *registry) permits(w tc.CandidateWrite) error {
	for _, p := range r.config.Reservations {
		if p.KeyID != "" && p.KeyID == w.Key.ID || p.StoreID == r.config.StoreID && p.CandidateID != "" && p.CandidateID == w.Intent.CandidateID {
			return Reserved
		}
	}
	return nil
}
