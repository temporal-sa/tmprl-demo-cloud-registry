//go:build darwin || linux

package sourceresolver

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"syscall"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/buildplan"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
)

var ErrStoreConflict = errors.New("owned source record version conflict")
var ErrStoreMissing = errors.New("pinned owned source record missing or replaced")
var ErrStoreUnknown = errors.New("owned source commit outcome is unknown; retain partial evidence")
var digestPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
var commitPattern = regexp.MustCompile(`^[a-f0-9]{40}$`)
var bindingPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var uidPattern = regexp.MustCompile(`^source-[a-f0-9]{32}$`)

const recordName = "source-record.json"
const lockName = ".source-lock"
const maxLocalVersions = 128
const maxLocalStoreBytes = 256 << 20

var archiveFilePattern = regexp.MustCompile(`^artifact-[a-f0-9]{64}\.tar$`)

type localRecord struct {
	UID        string
	Owner      ownership.Owner
	Repository string
	Version    uint64
	Facts      Facts
	Digest     string
}

// LocalStore owns no arbitrary host paths. The caller opens an explicit private
// os.Root and retains its lifetime; no HOME/config/credential directory lookup.
// Bootstrap and Open are distinct: an externally retained UID pin is mandatory
// on reopen. OS-owner-only files plus an OS cross-process lock protect this
// local capability. This is not distributed storage or a provider identity API.
type LocalStore struct {
	root            *os.Root
	owner           ownership.Owner
	repository, uid string
}

func BootstrapLocalStore(ctx context.Context, root *os.Root, owner ownership.Owner, repository string) (*LocalStore, string, error) {
	s := &LocalStore{root: root, owner: owner, repository: repository}
	if !s.validRoot() || !validRepository(repository) || owner.ProjectUID == "" || owner.InstallationID == "" || owner.EnvironmentID == "" {
		return nil, "", invalid("InvalidSourceStore", "Source storage requires a private owned root and exact owner/repository binding.")
	}
	f, err := root.OpenFile(lockName, os.O_CREATE|os.O_EXCL|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, "", ErrStoreMissing
	}
	defer f.Close()
	if syscall.Flock(int(f.Fd()), syscall.LOCK_EX) != nil {
		return nil, "", ErrStoreUnknown
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	if ctx.Err() != nil {
		return nil, "", ErrStoreUnknown
	}
	if _, err = root.Lstat(recordName); !os.IsNotExist(err) {
		return nil, "", ErrStoreMissing
	}
	var random [16]byte
	if _, err = rand.Read(random[:]); err != nil {
		return nil, "", ErrStoreUnknown
	}
	s.uid = "source-" + hex.EncodeToString(random[:])
	record := localRecord{UID: s.uid, Owner: owner, Repository: repository}
	record.Digest = recordDigest(record)
	data, _ := json.Marshal(record)
	if err = s.writeAtomic(recordName+".pending", recordName, data); err != nil {
		return nil, "", err
	}
	if err = f.Sync(); err != nil {
		return nil, "", ErrStoreUnknown
	}
	return s, s.uid, nil
}
func OpenLocalStore(ctx context.Context, root *os.Root, owner ownership.Owner, repository, expectedUID string) (*LocalStore, error) {
	s := &LocalStore{root: root, owner: owner, repository: repository, uid: expectedUID}
	if !s.validRoot() || !uidPattern.MatchString(expectedUID) || !validRepository(repository) {
		return nil, ErrStoreMissing
	}
	unlock, err := s.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if _, err = s.readRecord(); err != nil {
		return nil, err
	}
	return s, nil
}
func validRepository(value string) bool {
	u, err := url.Parse(value)
	return err == nil && u.Scheme == "https" && u.Host != "" && u.User == nil && u.RawQuery == "" && u.Fragment == "" && u.Path != ""
}
func (s *LocalStore) validRoot() bool {
	if s.root == nil {
		return false
	}
	info, err := s.root.Stat(".")
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}
func validPrivate(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && info.Mode().IsRegular() && info.Mode().Perm() == 0600 && stat.Uid == uint32(os.Geteuid()) && stat.Nlink == 1
}
func (s *LocalStore) openPrivate(name string) (*os.File, error) {
	if !s.validRoot() {
		return nil, ErrStoreMissing
	}
	f, err := s.root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, ErrStoreMissing
	}
	info, err := f.Stat()
	if err != nil || !validPrivate(info) {
		f.Close()
		return nil, ErrStoreMissing
	}
	return f, nil
}
func (s *LocalStore) lock(ctx context.Context) (func(), error) {
	if !s.validRoot() {
		return nil, ErrStoreMissing
	}
	f, err := s.root.OpenFile(lockName, os.O_RDWR|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, ErrStoreMissing
	}
	info, err := f.Stat()
	if err != nil || !validPrivate(info) {
		f.Close()
		return nil, ErrStoreMissing
	}
	for {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			break
		}
		if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
			f.Close()
			return nil, ErrStoreUnknown
		}
		select {
		case <-ctx.Done():
			f.Close()
			return nil, ErrStoreUnknown
		case <-time.After(5 * time.Millisecond):
		}
	}
	return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close() }, nil
}
func recordDigest(record localRecord) string {
	record.Digest = ""
	data, _ := json.Marshal(record)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
func (s *LocalStore) readRecord() (localRecord, error) {
	if _, err := s.checkFiles(); err != nil {
		return localRecord{}, err
	}
	if _, err := s.root.Lstat(recordName + ".pending"); !os.IsNotExist(err) {
		return localRecord{}, ErrStoreUnknown
	}
	f, err := s.openPrivate(recordName)
	if err != nil {
		return localRecord{}, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, 64*1024+1))
	if err != nil || len(data) > 64*1024 {
		return localRecord{}, ErrStoreMissing
	}
	var record localRecord
	if json.Unmarshal(data, &record) != nil || record.UID != s.uid || record.Owner != s.owner || record.Repository != s.repository || record.Digest != recordDigest(record) {
		return localRecord{}, ErrStoreMissing
	}
	if record.Version > 0 {
		if !s.validFacts(record.Facts) || s.verifyArchive(record.Facts) != nil {
			return localRecord{}, ErrStoreUnknown
		}
	}
	return record, nil
}
func (s *LocalStore) checkFiles() (int64, error) {
	directory, err := s.root.Open(".")
	if err != nil {
		return 0, ErrStoreMissing
	}
	defer directory.Close()
	entries, err := directory.ReadDir(maxLocalVersions + 5)
	if err != nil && err != io.EOF {
		return 0, ErrStoreUnknown
	}
	if len(entries) >= maxLocalVersions+5 {
		return 0, ErrStoreUnknown
	}
	var total int64
	for _, entry := range entries {
		name := entry.Name()
		if name != recordName && name != lockName && !archiveFilePattern.MatchString(name) {
			return 0, ErrStoreUnknown
		}
		info, err := s.root.Lstat(name)
		if err != nil || !validPrivate(info) {
			return 0, ErrStoreMissing
		}
		total += info.Size()
		if total > maxLocalStoreBytes {
			return 0, ErrStoreUnknown
		}
	}
	return total, nil
}
func (s *LocalStore) validFacts(f Facts) bool {
	return f.Owner == s.owner && f.CheckoutURL == s.repository && f.RequestedRef != "" && len(f.RequestedRef) <= 1024 && f.PolicyRevision != "" && len(f.PolicyRevision) <= 256 && bindingPattern.MatchString(f.BindingHash) && commitPattern.MatchString(f.Commit) && digestPattern.MatchString(f.ArtifactDigest) && digestPattern.MatchString(f.WatchSelectionDigest) && f.ArchiveFormat == ArchiveFormat && f.WatchProfile == WatchProfile && f.Files >= 0 && f.Files <= BoundedLimits().Files && f.ArchiveBytes > 0 && f.ArchiveBytes <= BoundedLimits().ArchiveBytes
}
func archiveName(f Facts) string { return "artifact-" + f.ArtifactDigest[len("sha256:"):] + ".tar" }
func (s *LocalStore) verifyArchive(facts Facts) error {
	f, err := s.openPrivate(archiveName(facts))
	if err != nil {
		return err
	}
	defer f.Close()
	hasher := sha256.New()
	n, err := io.Copy(hasher, io.LimitReader(f, facts.ArchiveBytes+1))
	if err != nil || n != facts.ArchiveBytes || "sha256:"+hex.EncodeToString(hasher.Sum(nil)) != facts.ArtifactDigest {
		return ErrStoreUnknown
	}
	return nil
}
func (s *LocalStore) Reserve(ctx context.Context, selection buildplan.SourceSelection) (Reservation, error) {
	if selection.Owner != s.owner || selection.Binding.CheckoutURL != s.repository || selection.Binding.Ref != selection.RequestedRef {
		return Reservation{}, ErrStoreMissing
	}
	unlock, err := s.lock(ctx)
	if err != nil {
		return Reservation{}, err
	}
	defer unlock()
	record, err := s.readRecord()
	if err != nil {
		return Reservation{}, err
	}
	return Reservation{SourceUID: s.uid, Version: record.Version, BindingHash: buildplan.SourceBindingHash(selection.Binding)}, nil
}
func (s *LocalStore) Commit(ctx context.Context, reservation Reservation, facts Facts, archive []byte) (Receipt, error) {
	if !s.validFacts(facts) || reservation.SourceUID != s.uid || reservation.BindingHash != facts.BindingHash || int64(len(archive)) != facts.ArchiveBytes {
		return Receipt{}, ErrStoreConflict
	}
	sum := sha256.Sum256(archive)
	if "sha256:"+hex.EncodeToString(sum[:]) != facts.ArtifactDigest {
		return Receipt{}, ErrStoreConflict
	}
	unlock, err := s.lock(ctx)
	if err != nil {
		return Receipt{}, err
	}
	defer unlock()
	record, err := s.readRecord()
	if err != nil {
		return Receipt{}, err
	}
	if record.Version > 0 && reflect.DeepEqual(record.Facts, facts) {
		if err = s.syncDirectory(); err != nil {
			return Receipt{}, err
		}
		return Receipt{SourceUID: s.uid, Version: record.Version, Facts: record.Facts}, nil
	}
	if record.Version != reservation.Version || record.Version >= maxLocalVersions {
		return Receipt{}, ErrStoreConflict
	}
	if _, err = s.root.Lstat(archiveName(facts)); os.IsNotExist(err) {
		total, checkErr := s.checkFiles()
		if checkErr != nil || total+int64(len(archive))+64*1024 > maxLocalStoreBytes {
			return Receipt{}, ErrStoreUnknown
		}
		if err = s.writeAtomic(archiveName(facts)+".pending", archiveName(facts), archive); err != nil {
			return Receipt{}, err
		}
	} else if err != nil {
		return Receipt{}, ErrStoreUnknown
	}
	if err = s.verifyArchive(facts); err != nil {
		return Receipt{}, ErrStoreUnknown
	}
	record.Version++
	record.Facts = facts
	record.Digest = recordDigest(record)
	data, _ := json.Marshal(record)
	if err = s.writeAtomic(recordName+".pending", recordName, data); err != nil {
		return Receipt{}, err
	}
	readback, err := s.readRecord()
	if err != nil || !bytes.Equal(data, mustJSON(readback)) {
		return Receipt{}, ErrStoreUnknown
	}
	return Receipt{SourceUID: s.uid, Version: readback.Version, Facts: readback.Facts}, nil
}
func mustJSON(v any) []byte { data, _ := json.Marshal(v); return data }

// Archive and metadata are deliberately ordered, not an atomic multi-file
// transaction. Archive is synced/read back before metadata replaces its version.
// Errors leave pending/orphan bytes as evidence; never overwrite a partial file
// or report a successful source receipt for an unknown write.
func (s *LocalStore) writeAtomic(pending, name string, data []byte) error {
	f, err := s.root.OpenFile(pending, os.O_CREATE|os.O_EXCL|os.O_WRONLY|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return ErrStoreUnknown
	}
	_, writeErr := io.Copy(f, bytes.NewReader(data))
	syncErr := f.Sync()
	closeErr := f.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		return ErrStoreUnknown
	}
	if err = s.root.Rename(pending, name); err != nil {
		return ErrStoreUnknown
	}
	return s.syncDirectory()
}
func (s *LocalStore) syncDirectory() error {
	f, err := s.root.Open(".")
	if err != nil {
		return ErrStoreUnknown
	}
	defer f.Close()
	if f.Sync() != nil {
		return ErrStoreUnknown
	}
	return nil
}
