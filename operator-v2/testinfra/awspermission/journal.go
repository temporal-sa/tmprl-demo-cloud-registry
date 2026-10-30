package awspermission

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"syscall"
)

type FileID struct {
	Device, Inode uint64
	UID           uint32
}
type JournalBinding struct {
	BootstrapUID, SpecSHA256, ManifestSHA256 string
	Directory                                FileID
}
type Journal struct {
	root    *os.Root
	binding JournalBinding
	fault   func(string) error
}
type journalManifest struct {
	Contract, BootstrapUID, SpecSHA256 string
	Directory                          FileID
}

func fileID(i os.FileInfo, dir bool) (FileID, bool) {
	if i == nil || i.IsDir() != dir || dir && i.Mode().Perm() != 0700 || !dir && (!i.Mode().IsRegular() || i.Mode().Perm() != 0600) {
		return FileID{}, false
	}
	s, ok := i.Sys().(*syscall.Stat_t)
	if !ok || s.Uid != uint32(os.Getuid()) || !dir && s.Nlink != 1 {
		return FileID{}, false
	}
	return FileID{uint64(s.Dev), uint64(s.Ino), s.Uid}, true
}
func rootID(r *os.Root) (FileID, error) {
	if r == nil {
		return FileID{}, Invalid
	}
	f, e := r.Open(".")
	if e != nil {
		return FileID{}, Denied
	}
	defer f.Close()
	i, e := f.Stat()
	id, ok := fileID(i, true)
	if e != nil || !ok {
		return FileID{}, Denied
	}
	return id, nil
}
func encode(v any) []byte {
	b, e := json.Marshal(v)
	if e != nil {
		return nil
	}
	return append(b, '\n')
}
func syncRoot(r *os.Root) error {
	f, e := r.Open(".")
	if e != nil {
		return Unproven
	}
	defer f.Close()
	if f.Sync() != nil {
		return Unproven
	}
	return nil
}

// BootstrapJournal requires a precreated empty private root. Bind this receipt
// independently. No default path, reopening creation, reset/prune or deletion
// exists. This is cooperative local one-shot evidence, not distributed fencing
// or resistance to backup restoration/manual removal of all step records.
func BootstrapJournal(root *os.Root, uid, specSHA string) (*Journal, JournalBinding, error) {
	if !shaPattern.MatchString(uid) || !shaPattern.MatchString(specSHA) {
		return nil, JournalBinding{}, Invalid
	}
	id, e := rootID(root)
	if e != nil {
		return nil, JournalBinding{}, e
	}
	f, e := root.Open(".")
	if e != nil {
		return nil, JournalBinding{}, Denied
	}
	entries, re := f.ReadDir(1)
	_ = f.Close()
	if len(entries) != 0 || re != nil && re != io.EOF {
		return nil, JournalBinding{}, AlreadyAttempted
	}
	raw := encode(journalManifest{Contract: ReadJournalContract, BootstrapUID: uid, SpecSHA256: specSHA, Directory: id})
	j := &Journal{root: root, binding: JournalBinding{BootstrapUID: uid, SpecSHA256: specSHA, ManifestSHA256: bytesHash(raw), Directory: id}}
	if e = j.write("identity.json", raw); e != nil {
		return nil, j.binding, e
	}
	return j, j.binding, nil
}

const ReadJournalContract = "registry-aws-readproof-local-attempts-v1"

func OpenJournal(root *os.Root, b JournalBinding) (*Journal, error) {
	j := &Journal{root: root, binding: b}
	if e := j.check(); e != nil {
		return nil, e
	}
	return j, nil
}
func (j *Journal) check() error {
	if j == nil || !shaPattern.MatchString(j.binding.BootstrapUID) || !shaPattern.MatchString(j.binding.SpecSHA256) || !shaPattern.MatchString(j.binding.ManifestSHA256) {
		return Invalid
	}
	id, e := rootID(j.root)
	if e != nil || id != j.binding.Directory {
		return Denied
	}
	raw, e := j.read("identity.json")
	if e != nil || bytesHash(raw) != j.binding.ManifestSHA256 || !bytes.Equal(raw, encode(journalManifest{ReadJournalContract, j.binding.BootstrapUID, j.binding.SpecSHA256, id})) {
		return Denied
	}
	return nil
}
func (j *Journal) read(name string) ([]byte, error) {
	i, e := j.root.Lstat(name)
	if e != nil {
		return nil, Unproven
	}
	before, ok := fileID(i, false)
	if !ok || i.Size() > 8192 {
		return nil, Unproven
	}
	f, e := j.root.OpenFile(name, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if e != nil {
		return nil, Unproven
	}
	defer f.Close()
	opened, e := f.Stat()
	after, aok := fileID(opened, false)
	if e != nil || !aok || before != after {
		return nil, Unproven
	}
	b, e := io.ReadAll(io.LimitReader(f, 8193))
	if e != nil || len(b) > 8192 || f.Sync() != nil {
		return nil, Unproven
	}
	return b, nil
}
func (j *Journal) write(name string, raw []byte) error {
	if len(raw) == 0 || len(raw) > 8192 {
		return Invalid
	}
	f, e := j.root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if e != nil {
		if os.IsExist(e) {
			return AlreadyAttempted
		}
		return Unproven
	}
	_, we := f.Write(raw)
	se := f.Sync()
	ce := f.Close()
	if we != nil || se != nil || ce != nil || syncRoot(j.root) != nil {
		return Unproven
	}
	if j.fault != nil && j.fault(name) != nil {
		return Unproven
	}
	read, e := j.read(name)
	if e != nil || !bytes.Equal(read, raw) {
		return Unproven
	}
	return nil
}
func knownStep(s Step) bool {
	switch s {
	case SourceIdentity, BootstrapAllowMetadata, BootstrapDenyMetadata, AllowMetadata, SeedAllowV1, SeedAllowV2, SeedDenyV1, AssumeReader, ReaderIdentity, ReadV1, ReadV2, ReadDeny, GenerateDenyKey, AssumeTrap:
		return true
	}
	return false
}
func (j *Journal) begin(ctx context.Context, r Receipt) error {
	if ctx.Err() != nil {
		return Unproven
	}
	if e := j.check(); e != nil {
		return e
	}
	if !knownStep(r.Step) || r.SpecSHA256 != j.binding.SpecSHA256 || !shaPattern.MatchString(r.RequestSHA256) || !shaPattern.MatchString(r.TargetSHA256) || r.Outcome != "Requested" || r.Code != "" || r.RequestID != "" || r.Persistence != "" || r.At.IsZero() {
		return Invalid
	}
	return j.write(string(r.Step)+".requested.json", encode(r))
}
func (j *Journal) finish(ctx context.Context, r Receipt) error {
	if ctx.Err() != nil {
		return Unproven
	}
	if e := j.check(); e != nil {
		return e
	}
	if !knownStep(r.Step) || r.SpecSHA256 != j.binding.SpecSHA256 || !shaPattern.MatchString(r.RequestSHA256) || !shaPattern.MatchString(r.TargetSHA256) || r.Persistence != "" || r.At.IsZero() || r.RequestID != "" && !requestIDPattern.MatchString(r.RequestID) {
		return Invalid
	}
	switch r.Outcome {
	case "Allowed", "Denied", "Unproven", "UnexpectedAllowed":
	default:
		return Invalid
	}
	if !safeCode(r.Code) {
		return Invalid
	}
	raw, e := j.read(string(r.Step) + ".requested.json")
	if e != nil {
		return e
	}
	var before Receipt
	if json.Unmarshal(raw, &before) != nil || before.SpecSHA256 != r.SpecSHA256 || before.Step != r.Step || before.RequestSHA256 != r.RequestSHA256 || before.TargetSHA256 != r.TargetSHA256 || before.Outcome != "Requested" || r.At.Before(before.At) {
		return Denied
	}
	return j.write(string(r.Step)+".receipt.json", encode(r))
}
