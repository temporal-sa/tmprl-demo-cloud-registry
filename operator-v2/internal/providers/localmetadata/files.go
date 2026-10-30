package localmetadata

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"syscall"
	"time"
)

const identityName = "identity.json"
const lockName = "lock"

type identity struct {
	Schema       string
	BootstrapUID string
	ConfigSHA256 string
	Directory    FileID
	Lock         FileID
}

func digest(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func rawDigest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
func equal(a, b any) bool {
	left, right := digest(a), digest(b)
	return left != "" && right != "" && left == right
}
func encoded(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	return append(b, '\n')
}

func fileID(info os.FileInfo, directory bool) (FileID, bool) {
	if info == nil || (directory && !info.IsDir()) || (!directory && !info.Mode().IsRegular()) || info.Mode().Perm()&0077 != 0 {
		return FileID{}, false
	}
	if (directory && info.Mode().Perm() != 0700) || (!directory && info.Mode().Perm() != 0600) {
		return FileID{}, false
	}
	v, ok := info.Sys().(*syscall.Stat_t)
	if !ok || v.Uid != uint32(os.Getuid()) || (!directory && v.Nlink != 1) {
		return FileID{}, false
	}
	return FileID{Device: uint64(v.Dev), Inode: uint64(v.Ino), UID: v.Uid}, true
}

func directoryID(root *os.Root) (FileID, error) {
	if root == nil {
		return FileID{}, Invalid
	}
	f, err := root.Open(".")
	if err != nil {
		return FileID{}, ScopeConflict
	}
	defer f.Close()
	info, err := f.Stat()
	id, ok := fileID(info, true)
	if err != nil || !ok {
		return FileID{}, ScopeConflict
	}
	return id, nil
}

// Bootstrap requires a pre-created empty private root; it never creates the
// root itself. Partial bootstrap files are retained on any failure.
func Bootstrap(ctx context.Context, root *os.Root, config Config) (*Store, Binding, error) {
	s, err := configured(root, config)
	if err != nil {
		return nil, Binding{}, err
	}
	dir, err := directoryID(root)
	if err != nil {
		return nil, Binding{}, err
	}
	f, err := root.Open(".")
	if err != nil {
		return nil, Binding{}, ScopeConflict
	}
	entries, readErr := f.ReadDir(1)
	_ = f.Close()
	if len(entries) != 0 || (readErr != nil && readErr != io.EOF) {
		return nil, Binding{}, ScopeConflict
	}
	if ctx.Err() != nil {
		return nil, Binding{}, Uncertain
	}
	lock, err := root.OpenFile(lockName, os.O_CREATE|os.O_EXCL|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, Binding{}, ScopeConflict
	}
	defer lock.Close()
	if syscall.Flock(int(lock.Fd()), syscall.LOCK_EX) != nil {
		return nil, Binding{}, Uncertain
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	info, err := lock.Stat()
	lockID, ok := fileID(info, false)
	if err != nil || !ok || lock.Sync() != nil {
		return nil, Binding{}, Uncertain
	}
	s.binding = Binding{BootstrapUID: config.BootstrapUID, Directory: dir, Lock: lockID, ConfigSHA256: s.configDigest()}
	raw := encoded(identity{Schema: s.identitySchema(), BootstrapUID: config.BootstrapUID, ConfigSHA256: s.binding.ConfigSHA256, Directory: dir, Lock: lockID})
	s.binding.IdentitySHA256 = rawDigest(raw)
	if s.writeNew(identityName, raw) != nil || s.syncDirectory() != nil {
		return nil, Binding{}, Uncertain
	}
	if s.checkIdentity() != nil {
		return nil, Binding{}, Uncertain
	}
	return s, s.binding, nil
}

// Open never creates/repairs files. Reopen requires a clean coordinator handoff
// and the independent complete Binding, not just a manifest copied elsewhere.
func Open(root *os.Root, binding Binding, config Config) (*Store, error) {
	s, err := configured(root, config)
	if err != nil {
		return nil, err
	}
	s.binding = binding
	if binding.BootstrapUID != config.BootstrapUID || binding.ConfigSHA256 != s.configDigest() || s.checkIdentity() != nil {
		return nil, ScopeConflict
	}
	return s, nil
}

func (s *Store) checkIdentity() error {
	dir, err := directoryID(s.root)
	if err != nil || dir != s.binding.Directory {
		return ScopeConflict
	}
	raw, err := s.readPrivate(identityName, false)
	if err != nil || rawDigest(raw) != s.binding.IdentitySHA256 {
		return ScopeConflict
	}
	var id identity
	if decode(raw, &id) != nil || id.Schema != s.identitySchema() || id.BootstrapUID != s.binding.BootstrapUID || id.ConfigSHA256 != s.binding.ConfigSHA256 || id.Directory != s.binding.Directory || id.Lock != s.binding.Lock {
		return ScopeConflict
	}
	return nil
}

func (s *Store) locked(ctx context.Context, fn func() error) error {
	// Each operation opens its own descriptor. Reusing one flock descriptor
	// across goroutines would permit unintended reentrant access.
	for !s.mu.TryLock() {
		select {
		case <-ctx.Done():
			return Uncertain
		case <-time.After(time.Millisecond):
		}
	}
	defer s.mu.Unlock()
	if s.checkIdentity() != nil {
		return ScopeConflict
	}
	f, err := s.root.OpenFile(lockName, os.O_RDWR|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return ScopeConflict
	}
	defer f.Close()
	info, err := f.Stat()
	id, ok := fileID(info, false)
	if err != nil || !ok || id != s.binding.Lock || info.Size() != 0 {
		return ScopeConflict
	}
	for {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			break
		}
		if err != syscall.EWOULDBLOCK && err != syscall.EAGAIN {
			return Uncertain
		}
		select {
		case <-ctx.Done():
			return Uncertain
		case <-time.After(time.Millisecond):
		}
	}
	defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	pathInfo, pathErr := s.root.Lstat(lockName)
	pathID, pathOK := fileID(pathInfo, false)
	if pathErr != nil || !pathOK || pathID != s.binding.Lock || s.checkIdentity() != nil {
		return ScopeConflict
	}
	if ctx.Err() != nil {
		return Uncertain
	}
	return fn()
}

func decode(raw []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil {
		return Corrupt
	}
	var trailing any
	if d.Decode(&trailing) != io.EOF {
		return Corrupt
	}
	return nil
}

func (s *Store) readPrivate(name string, sync bool) ([]byte, error) {
	info, err := s.root.Lstat(name)
	_, ok := fileID(info, false)
	if err != nil || !ok || info.Mode()&os.ModeSymlink != 0 || info.Size() > int64(s.config.MaximumRecordBytes) {
		return nil, Corrupt
	}
	f, err := s.root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, Corrupt
	}
	defer f.Close()
	opened, err := f.Stat()
	a, aok := fileID(info, false)
	b, bok := fileID(opened, false)
	if err != nil || !aok || !bok || a != b {
		return nil, Corrupt
	}
	raw, err := io.ReadAll(io.LimitReader(f, int64(s.config.MaximumRecordBytes)+1))
	if err != nil || len(raw) > s.config.MaximumRecordBytes {
		return nil, Corrupt
	}
	if sync && f.Sync() != nil {
		return nil, Uncertain
	}
	return raw, nil
}

func (s *Store) writeNew(name string, raw []byte) error {
	if len(raw) > s.config.MaximumRecordBytes {
		return Capacity
	}
	f, err := s.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return Uncertain
	}
	_, writeErr := f.Write(raw)
	syncErr := f.Sync()
	closeErr := f.Close()
	if writeErr != nil || syncErr != nil || closeErr != nil {
		return Uncertain
	}
	actual, err := s.readPrivate(name, true)
	if err != nil || !bytes.Equal(actual, raw) {
		return Uncertain
	}
	return nil
}

func (s *Store) syncDirectory() error {
	f, err := s.root.Open(".")
	if err != nil {
		return Uncertain
	}
	defer f.Close()
	info, err := f.Stat()
	id, ok := fileID(info, true)
	if err != nil || !ok || id != s.binding.Directory || f.Sync() != nil {
		return Uncertain
	}
	return nil
}

func frameName(seq uint64, kind string) string { return fmt.Sprintf("tx-%06d.%s.json", seq, kind) }

var framePattern = regexp.MustCompile(`^tx-([0-9]{6})\.(intent|commit)\.json$`)
