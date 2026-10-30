//go:build darwin || linux

package runtimeinputs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"regexp"
	"syscall"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	kube "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/kubernetes"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

var hex64 = regexp.MustCompile(`^[a-f0-9]{64}$`)
var stableID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}$`)
var entryName = regexp.MustCompile(`^render-[a-f0-9]{64}\.(input|commit)\.json$`)

func hash(data []byte) string { d := sha256.Sum256(data); return hex.EncodeToString(d[:]) }
func encode(v any) []byte     { b, _ := json.Marshal(v); return b }
func identity(info os.FileInfo, directory bool) (FileID, bool) {
	if info == nil || directory && (!info.IsDir() || info.Mode().Perm() != 0700) || !directory && (!info.Mode().IsRegular() || info.Mode().Perm() != 0600) {
		return FileID{}, false
	}
	s, ok := info.Sys().(*syscall.Stat_t)
	if !ok || s.Uid != uint32(os.Getuid()) || !directory && s.Nlink != 1 {
		return FileID{}, false
	}
	return FileID{uint64(s.Dev), uint64(s.Ino), s.Uid}, true
}
func configured(root *os.Root, c Config) (*Store, error) {
	if root == nil || !hex64.MatchString(c.BootstrapUID) || !stableID.MatchString(c.CellUID) || c.Owner.Lifetime != ownership.Disposable || c.Owner.RunID == "" || c.Owner.EnvironmentID == "" || c.Owner.InstallationID == "" || c.Owner.ProjectUID == "" || c.MaximumEntries < 1 || c.MaximumEntries > 256 {
		return nil, ErrInvalid
	}
	for _, value := range []string{c.Owner.EnvironmentID, c.Owner.InstallationID, c.Owner.ProjectUID, c.Owner.RunID} {
		if !stableID.MatchString(value) || len(value) > 128 {
			return nil, ErrInvalid
		}
	}
	return &Store{root: root, config: c}, nil
}
func (s *Store) directory() (FileID, error) {
	i, e := s.root.Stat(".")
	id, ok := identity(i, true)
	if e != nil || !ok {
		return FileID{}, ErrUnproven
	}
	return id, nil
}
func (s *Store) syncDirectory() error {
	f, e := s.root.Open(".")
	if e != nil {
		return ErrUncertain
	}
	defer f.Close()
	i, e := f.Stat()
	id, ok := identity(i, true)
	if e != nil || !ok || id != s.pin.Directory || f.Sync() != nil {
		return ErrUncertain
	}
	return nil
}
func (s *Store) read(name string, limit int) ([]byte, error) {
	i, e := s.root.Lstat(name)
	id, ok := identity(i, false)
	if e != nil || !ok || i.Size() > int64(limit) {
		return nil, ErrUnproven
	}
	f, e := s.root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if e != nil {
		return nil, ErrUnproven
	}
	defer f.Close()
	opened, e := f.Stat()
	actual, ok := identity(opened, false)
	if e != nil || !ok || actual != id {
		return nil, ErrUnproven
	}
	b, e := io.ReadAll(io.LimitReader(f, int64(limit)+1))
	if e != nil || len(b) > limit {
		return nil, ErrUnproven
	}
	// An exact existing immutable entry may be the result of a previous lost
	// persistence acknowledgement. Reestablish file durability before allowing
	// any caller to acknowledge it; directory durability is checked separately.
	if f.Sync() != nil {
		return nil, ErrUncertain
	}
	return b, nil
}
func (s *Store) write(name string, data []byte) error {
	f, e := s.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if e != nil {
		return ErrUncertain
	}
	n, we := f.Write(data)
	se := f.Sync()
	ce := f.Close()
	if n != len(data) || we != nil || se != nil || ce != nil {
		return ErrUncertain
	}
	actual, e := s.read(name, len(data))
	if e != nil || !bytes.Equal(actual, data) {
		return ErrUncertain
	}
	return nil
}

// Bootstrap requires an explicit already-created empty private root. Partial
// bootstrap/artifacts are retained; Open never creates, repairs or adopts them.
func Bootstrap(ctx context.Context, root *os.Root, c Config) (*Store, Pin, error) {
	s, e := configured(root, c)
	if e != nil {
		return nil, Pin{}, e
	}
	dir, e := s.directory()
	if e != nil {
		return nil, Pin{}, e
	}
	f, e := root.Open(".")
	if e != nil {
		return nil, Pin{}, ErrUnproven
	}
	entries, re := f.ReadDir(1)
	_ = f.Close()
	if len(entries) != 0 || re != nil && re != io.EOF {
		return nil, Pin{}, ErrUnproven
	}
	if ctx.Err() != nil {
		return nil, Pin{}, ErrUncertain
	}
	lock, e := root.OpenFile("lock", os.O_CREATE|os.O_EXCL|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if e != nil {
		return nil, Pin{}, ErrUnproven
	}
	defer lock.Close()
	i, e := lock.Stat()
	lid, ok := identity(i, false)
	if e != nil || !ok || lock.Sync() != nil {
		return nil, Pin{}, ErrUncertain
	}
	s.pin = Pin{BootstrapUID: c.BootstrapUID, Directory: dir, Lock: lid}
	s.schema = "private-render-inputs-v2"
	if c.ServiceAccounts {
		s.schema = "private-render-inputs-v3"
	}
	raw := encode(manifest{s.schema, c, dir, lid})
	s.pin.ManifestDigest = hash(raw)
	if s.write("manifest.json", raw) != nil || s.syncDirectory() != nil {
		return nil, Pin{}, ErrUncertain
	}
	return s, s.pin, nil
}
func Open(ctx context.Context, root *os.Root, pin Pin, c Config) (*Store, error) {
	s, e := configured(root, c)
	if e != nil {
		return nil, e
	}
	s.pin = pin
	if e = s.locked(ctx, func() error { return nil }); e != nil {
		return nil, e
	}
	return s, nil
}
func (s *Store) check() error {
	d, e := s.directory()
	if e != nil || d != s.pin.Directory || s.pin.BootstrapUID != s.config.BootstrapUID {
		return ErrUnproven
	}
	raw, e := s.read("manifest.json", 4096)
	if e != nil || hash(raw) != s.pin.ManifestDigest {
		return ErrUnproven
	}
	schemas := []string{"private-deployment-inputs-v1", "private-render-inputs-v2"}
	if s.config.ServiceAccounts {
		schemas = []string{"private-render-inputs-v3"}
	}
	for _, schema := range schemas {
		if bytes.Equal(raw, encode(manifest{schema, s.config, s.pin.Directory, s.pin.Lock})) {
			s.schema = schema
			return nil
		}
	}
	return ErrUnproven
}
func (s *Store) locked(ctx context.Context, fn func() error) error {
	if s == nil || s.root == nil {
		return ErrInvalid
	}
	for !s.mu.TryLock() {
		select {
		case <-ctx.Done():
			return ErrUncertain
		case <-time.After(time.Millisecond):
		}
	}
	defer s.mu.Unlock()
	if e := s.check(); e != nil {
		return e
	}
	f, e := s.root.OpenFile("lock", os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if e != nil {
		return ErrUnproven
	}
	defer f.Close()
	i, e := f.Stat()
	id, ok := identity(i, false)
	if e != nil || !ok || id != s.pin.Lock || i.Size() != 0 {
		return ErrUnproven
	}
	for {
		e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if e == nil {
			break
		}
		if e != syscall.EWOULDBLOCK && e != syscall.EAGAIN {
			return ErrUncertain
		}
		select {
		case <-ctx.Done():
			return ErrUncertain
		case <-time.After(time.Millisecond):
		}
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	i, e = s.root.Lstat("lock")
	id, ok = identity(i, false)
	if e != nil || !ok || id != s.pin.Lock || s.check() != nil {
		return ErrUnproven
	}
	if ctx.Err() != nil {
		return ErrUncertain
	}
	return fn()
}
func (s *Store) valid(t Target) bool {
	if t.CellUID != s.config.CellUID {
		return false
	}
	p := t.Parent
	return p.Owner == s.config.Owner && stableID.MatchString(p.UID) && stableID.MatchString(p.ClusterID) && stableID.MatchString(p.Name) && p.CreationOperationID == t.AttemptID && stableID.MatchString(p.CreationIdempotencyKey) && stableID.MatchString(t.AttemptID) && stableID.MatchString(t.Revision) && stableID.MatchString(t.Name) && hex64.MatchString(t.DesiredDigest) && hex64.MatchString(t.Fingerprint)
}
func (s *Store) uid(t Target) string {
	return s.uidFor(t, deploymentFamily)
}

type family uint8

const (
	deploymentFamily family = iota
	serviceFamily
	serviceAccountFamily
)

func (f family) contract() string {
	if f == serviceAccountFamily {
		return journal.ServiceAccountInputContract
	}
	if f == serviceFamily {
		return journal.ServiceInputContract
	}
	return journal.DeploymentInputContract
}
func (f family) kind() string {
	if f == serviceAccountFamily {
		return "ServiceAccountRender"
	}
	if f == serviceFamily {
		return "ServiceRender"
	}
	return "DeploymentRender"
}
func (f family) schema() string {
	if f == serviceAccountFamily {
		return "private-service-account-input-v1"
	}
	if f == serviceFamily {
		return "private-service-input-v1"
	}
	return "private-deployment-input-v1"
}
func (f family) receiptSchema() string {
	if f == serviceAccountFamily {
		return "private-service-account-input-commit-v1"
	}
	if f == serviceFamily {
		return "private-service-input-commit-v1"
	}
	return "private-deployment-input-commit-v1"
}
func (s *Store) uidFor(t Target, f family) string {
	return "render-" + hash(encode(struct {
		Bootstrap, Cell, Contract string
		Owner                     ownership.Owner
		Target                    Target
	}{s.config.BootstrapUID, s.config.CellUID, f.contract(), s.config.Owner, t}))
}
func (s *Store) envelopeFor(t Target, body []byte, f family) envelope {
	o := s.config.Owner
	return envelope{f.schema(), s.config.BootstrapUID, s.config.CellUID, rollout.Subject{Environment: o.EnvironmentID, Installation: o.InstallationID, ProjectUID: o.ProjectUID}, o, t, f.contract(), body}
}
func (s *Store) capacity() error {
	f, e := s.root.Open(".")
	if e != nil {
		return ErrUnproven
	}
	defer f.Close()
	entries, e := f.ReadDir(s.config.MaximumEntries*2 + 3)
	if e != nil && e != io.EOF {
		return ErrUnproven
	}
	if len(entries) > s.config.MaximumEntries*2+2 {
		return ErrCapacity
	}
	slots := map[string]bool{}
	for _, entry := range entries {
		if entry.Name() == "lock" || entry.Name() == "manifest.json" {
			continue
		}
		if !entryName.MatchString(entry.Name()) {
			return ErrUnproven
		}
		info, e := s.root.Lstat(entry.Name())
		_, ok := identity(info, false)
		if e != nil || !ok || info.Size() > maxEnvelopeBytes {
			return ErrUnproven
		}
		// Both companions reserve the slot. Missing-body/commit-only evidence
		// cannot release capacity or permit another pair beyond the bound.
		slots[entry.Name()[:len("render-")+64]] = true
	}
	if len(slots) >= s.config.MaximumEntries {
		return ErrCapacity
	}
	return nil
}

// Retain freezes the original rendered body before any journal Pending/POST.
// Exact already-committed content may be acknowledged again; a partial entry
// is retained and never completed, overwritten, deleted or rerendered here.
// Call it only before the first durable write for this Deployment identity.
// For every existing journal row, use Resolve; filesystem absence cannot prove
// that an accepted row never existed. The external journal is the witness when
// both private companion files disappear. No filesystem rollback is inferred.
func (s *Store) Retain(ctx context.Context, c Capture) (journal.InputReference, error) {
	var ref journal.InputReference
	if s == nil || !s.valid(c.Target) || c.Desired == nil {
		return ref, ErrInvalid
	}
	o := c.Desired.DeepCopy()
	fp, e := kube.DeploymentFingerprint(o)
	if e != nil || fp != c.Target.Fingerprint || o.Name != c.Target.Name || o.Namespace != c.Target.Parent.Name || o.APIVersion != "apps/v1" || o.Kind != "Deployment" {
		return ref, ErrInvalid
	}
	return s.retain(ctx, c.Target, encode(o), deploymentFamily)
}

// RetainService preserves the original unallocated desired Service body. The
// provider's independently committed allocated-spec receipt is a separate fact.
func (s *Store) RetainService(ctx context.Context, c ServiceCapture) (journal.InputReference, error) {
	if s == nil || !s.valid(c.Target) || c.Desired == nil {
		return journal.InputReference{}, ErrInvalid
	}
	o := c.Desired.DeepCopy()
	fp, e := kube.ServiceFingerprint(o)
	if e != nil || fp != c.Target.Fingerprint || o.Name != c.Target.Name || o.Namespace != c.Target.Parent.Name || o.APIVersion != "v1" || o.Kind != "Service" {
		return journal.InputReference{}, ErrInvalid
	}
	return s.retain(ctx, c.Target, encode(o), serviceFamily)
}
func (s *Store) retain(ctx context.Context, target Target, body []byte, f family) (journal.InputReference, error) {
	var ref journal.InputReference
	raw := encode(s.envelopeFor(target, body, f))
	if len(raw) > maxEnvelopeBytes {
		return ref, ErrCapacity
	}
	ref = journal.InputReference{Kind: f.kind(), UID: s.uidFor(target, f), Version: "1", Digest: hash(raw), Contract: f.contract()}
	rawReceipt := encode(receipt{f.receiptSchema(), ref.UID, ref.Digest, ref.Contract})
	e := s.locked(ctx, func() error {
		if !s.supports(f) {
			return ErrInvalid
		}
		name := ref.UID + ".input.json"
		if _, e := s.root.Lstat(name); e == nil {
			actual, e := s.read(name, maxEnvelopeBytes)
			commit, e2 := s.read(ref.UID+".commit.json", 1024)
			if e != nil || e2 != nil || !bytes.Equal(raw, actual) || !bytes.Equal(rawReceipt, commit) {
				return ErrUncertain
			}
			return s.syncDirectory()
		} else if !os.IsNotExist(e) {
			return ErrUnproven
		}
		// Either companion proves this identity was already used. A missing
		// body beside an existing commit is lost evidence, never a fresh slot.
		// Check before creating anything so repeated Retain cannot repair it.
		if _, e := s.root.Lstat(ref.UID + ".commit.json"); e == nil || !os.IsNotExist(e) {
			return ErrUnproven
		}
		if e := s.capacity(); e != nil {
			return e
		}
		if ctx.Err() != nil {
			return ErrUncertain
		}
		if s.write(name, raw) != nil || s.syncDirectory() != nil || ctx.Err() != nil {
			return ErrUncertain
		}
		if s.fault != nil && s.fault("after-input") != nil {
			return ErrUncertain
		}
		if s.write(ref.UID+".commit.json", rawReceipt) != nil || s.syncDirectory() != nil || ctx.Err() != nil {
			return ErrUncertain
		}
		if s.fault != nil && s.fault("after-commit") != nil {
			return ErrUncertain
		}
		return nil
	})
	if e != nil {
		return journal.InputReference{}, e
	}
	return ref, nil
}

// Resolve only reads the exact reference selected by an acknowledged journal.
// It does not consult a provider, mutable current project, directory discovery
// or current renderer. Unsupported historical contracts fail closed.
func (s *Store) Resolve(ctx context.Context, ref journal.InputReference, t Target) (Payload, error) {
	var result Payload
	body, e := s.resolve(ctx, ref, t, deploymentFamily)
	if e != nil {
		return result, e
	}
	var object appsv1.Deployment
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if d.Decode(&object) != nil || d.Decode(new(any)) != io.EOF || !bytes.Equal(encode(&object), body) {
		return result, ErrUnproven
	}
	fp, e := kube.DeploymentFingerprint(&object)
	if e != nil || fp != t.Fingerprint || object.Name != t.Name || object.Namespace != t.Parent.Name || object.APIVersion != "apps/v1" || object.Kind != "Deployment" {
		return result, ErrUnproven
	}
	return Payload{&payload{object.DeepCopy()}}, nil
}
func (s *Store) ResolveService(ctx context.Context, ref journal.InputReference, t Target) (ServicePayload, error) {
	var result ServicePayload
	body, e := s.resolve(ctx, ref, t, serviceFamily)
	if e != nil {
		return result, e
	}
	var object corev1.Service
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if d.Decode(&object) != nil || d.Decode(new(any)) != io.EOF || !bytes.Equal(encode(&object), body) {
		return result, ErrUnproven
	}
	fp, e := kube.ServiceFingerprint(&object)
	if e != nil || fp != t.Fingerprint || object.Name != t.Name || object.Namespace != t.Parent.Name || object.APIVersion != "v1" || object.Kind != "Service" {
		return result, ErrUnproven
	}
	return ServicePayload{&servicePayload{object.DeepCopy()}}, nil
}
func (s *Store) resolve(ctx context.Context, ref journal.InputReference, t Target, f family) ([]byte, error) {
	if s == nil || !ref.Valid() || ref.Kind != f.kind() || ref.Contract != f.contract() || !s.valid(t) || ref.UID != s.uidFor(t, f) {
		return nil, ErrInvalid
	}
	var body []byte
	e := s.locked(ctx, func() error {
		if !s.supports(f) {
			return ErrInvalid
		}
		raw, e := s.read(ref.UID+".input.json", maxEnvelopeBytes)
		if e != nil || hash(raw) != ref.Digest {
			return ErrUnproven
		}
		commit, e := s.read(ref.UID+".commit.json", 1024)
		if e != nil || !bytes.Equal(commit, encode(receipt{f.receiptSchema(), ref.UID, ref.Digest, ref.Contract})) {
			return ErrUnproven
		}
		var v envelope
		d := json.NewDecoder(bytes.NewReader(raw))
		d.DisallowUnknownFields()
		if d.Decode(&v) != nil || d.Decode(new(any)) != io.EOF || !bytes.Equal(encode(s.envelopeFor(t, v.Object, f)), raw) {
			return ErrUnproven
		}
		body = append([]byte(nil), v.Object...)
		return nil
	})
	return body, e
}
