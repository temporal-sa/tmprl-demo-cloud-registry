package awspermission

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// Canonical finite structs deliberately have one wire representation. This also
// rejects nested duplicate/case-aliased keys: decoding and re-encoding cannot
// reproduce an extra key. No arbitrary map/schema/endpoint framework is used.
func decodeCanonical(raw []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(target) != nil {
		return Invalid
	}
	var trailing any
	if d.Decode(&trailing) != io.EOF || !bytes.Equal(raw, encode(target)) {
		return Invalid
	}
	return nil
}
func cleanAbsolute(path string) bool {
	return path != "" && filepath.IsAbs(path) && filepath.Clean(path) == path
}
func pathDirectories(path string) error {
	if !cleanAbsolute(path) {
		return Invalid
	}
	parent := filepath.Dir(path)
	current := string(filepath.Separator)
	for _, part := range strings.Split(strings.TrimPrefix(parent, string(filepath.Separator)), string(filepath.Separator)) {
		if part == "" {
			continue
		}
		current = filepath.Join(current, part)
		i, e := os.Lstat(current)
		if e != nil || !i.IsDir() || i.Mode()&os.ModeSymlink != 0 {
			return Denied
		}
	}
	i, e := os.Lstat(parent)
	if e != nil {
		return Denied
	}
	if _, ok := fileID(i, true); !ok {
		return Denied
	}
	return nil
}
func openMetadata(path string, max int) (*os.File, error) {
	if pathDirectories(path) != nil {
		return nil, Denied
	}
	i, e := os.Lstat(path)
	if e != nil {
		return nil, Denied
	}
	before, ok := fileID(i, false)
	if !ok || i.Size() < 1 || i.Size() > int64(max) {
		return nil, Denied
	}
	f, e := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if e != nil {
		return nil, Denied
	}
	opened, e := f.Stat()
	after, ok := fileID(opened, false)
	if e != nil || !ok || before != after || opened.Size() != i.Size() {
		f.Close()
		return nil, Denied
	}
	return f, nil
}
func readMetadata(path, expected string, max int) ([]byte, error) {
	if !shaPattern.MatchString(expected) {
		return nil, Invalid
	}
	f, e := openMetadata(path, max)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	raw, e := io.ReadAll(io.LimitReader(f, int64(max+1)))
	if e != nil || len(raw) > max || bytesHash(raw) != expected {
		return nil, Denied
	}
	return raw, nil
}
func openJournalRoot(path string) (*os.Root, error) {
	// Validate the directory as a leaf, then check the opened directory identity.
	if pathDirectories(filepath.Join(path, "identity.json")) != nil {
		return nil, Denied
	}
	i, e := os.Lstat(path)
	if e != nil {
		return nil, Denied
	}
	before, ok := fileID(i, true)
	if !ok {
		return nil, Denied
	}
	root, e := os.OpenRoot(path)
	if e != nil {
		return nil, Denied
	}
	after, e := rootID(root)
	if e != nil || before != after {
		root.Close()
		return nil, Denied
	}
	return root, nil
}
func outside(root, path string) bool {
	if !cleanAbsolute(root) || !cleanAbsolute(path) {
		return false
	}
	relative, e := filepath.Rel(root, path)
	return e == nil && (relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)))
}
func freshInvocationJournal(j *Journal) error {
	if e := j.check(); e != nil {
		return e
	}
	f, e := j.root.Open(".")
	if e != nil {
		return Denied
	}
	defer f.Close()
	entries, e := f.ReadDir(2)
	if e != nil && e != io.EOF {
		return Denied
	}
	if len(entries) != 1 || entries[0].Name() != "identity.json" {
		return AlreadyAttempted
	}
	return nil
}
func absentPrivatePin(path string) error {
	if pathDirectories(path) != nil {
		return Denied
	}
	if _, e := os.Lstat(path); !os.IsNotExist(e) {
		return Denied
	}
	return nil
}
func writePin(path string, raw []byte) error {
	if len(raw) == 0 || len(raw) > 8192 || absentPrivatePin(path) != nil {
		return Denied
	}
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if e != nil {
		return Unproven
	}
	_, we := f.Write(raw)
	se := f.Sync()
	ce := f.Close()
	if we != nil || se != nil || ce != nil {
		return Unproven
	}
	parent, e := os.Open(filepath.Dir(path))
	if e != nil {
		return Unproven
	}
	se = parent.Sync()
	ce = parent.Close()
	if se != nil || ce != nil {
		return Unproven
	}
	observed, e := readMetadata(path, bytesHash(raw), 8192)
	if e != nil || !bytes.Equal(observed, raw) {
		return Unproven
	}
	return nil
}
