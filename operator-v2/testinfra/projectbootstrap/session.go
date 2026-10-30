// Package projectbootstrap is a fixture-specific human bootstrap, never an
// operator credential source. Constructing/loading it makes no network call.
package projectbootstrap

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
)

// Error contains only a fixed diagnostic; provider, parser and OS messages are
// deliberately never wrapped because they can contain credentials/config data.
type Error string

func (e Error) Error() string { return string(e) }

type session struct{ private *sessionValue }
type sessionValue struct {
	token  string
	expiry time.Time
}

func newSession(token string, expiry time.Time) *session {
	return &session{private: &sessionValue{token: token, expiry: expiry}}
}

func (session) String() string               { return "OAuthSession{redacted}" }
func (session) GoString() string             { return "OAuthSession{redacted}" }
func (s session) Format(w fmt.State, _ rune) { _, _ = io.WriteString(w, s.String()) }
func (session) MarshalJSON() ([]byte, error) { return nil, Error("OAuthSerializationForbidden") }
func (session) MarshalText() ([]byte, error) { return nil, Error("OAuthSerializationForbidden") }

// openPrivate rejects symlinks in every existing path component and requires
// an owner-only regular file in an owner-only directory. O_NOFOLLOW closes the
// final-component symlink race. A trusted local user must serialize renames of
// ancestor directories; these checks are not a general hostile-filesystem lock.
func openPrivate(path string) (*os.File, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, Error("ExplicitAbsolutePrivatePathRequired")
	}
	for p := path; ; p = filepath.Dir(p) {
		info, err := os.Lstat(p)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return nil, Error("PrivatePathRejected")
		}
		if p == filepath.Dir(p) {
			break
		}
	}
	dir, err := os.Stat(filepath.Dir(path))
	dirOwner, ownedDir := infoOwner(dir)
	if err != nil || !ownedDir || dirOwner != uint32(os.Getuid()) || !dir.IsDir() || dir.Mode().Perm()&0077 != 0 {
		return nil, Error("PrivateDirectoryRequired")
	}
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, Error("PrivateFileReadFailed")
	}
	info, err := f.Stat()
	owner, ok := infoOwner(info)
	if err != nil || !ok || owner != uint32(os.Getuid()) || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 1<<20 || info.Sys().(*syscall.Stat_t).Nlink != 1 {
		_ = f.Close()
		return nil, Error("PrivateFileRejected")
	}
	return f, nil
}

func infoOwner(info os.FileInfo) (uint32, bool) {
	if info == nil {
		return 0, false
	}
	s, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return s.Uid, true
}

func loadSession(path, profile string, now time.Time) (*session, error) {
	if profile == "" || strings.TrimSpace(profile) != profile {
		return nil, Error("ExplicitProfileRequired")
	}
	f, err := openPrivate(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	// Exact cliext 8bb57b77ad55/config.oauth.go:335-355 schema. In particular,
	// API-key/client-secret/refresh-token/endpoint fields are not loaded or used.
	var cfg struct {
		Profile map[string]struct {
			OAuth *struct {
				AccessToken string `toml:"access_token"`
				TokenType   string `toml:"token_type"`
				ExpiresAt   string `toml:"expires_at"`
			} `toml:"oauth"`
		} `toml:"profile"`
	}
	raw, err := boundedRead(f)
	if err != nil {
		return nil, err
	}
	if _, err := toml.NewDecoder(bytes.NewReader(raw)).Decode(&cfg); err != nil {
		return nil, Error("OAuthConfigInvalid")
	}
	p, ok := cfg.Profile[profile]
	if !ok || p.OAuth == nil || p.OAuth.TokenType != "Bearer" || p.OAuth.AccessToken == "" || strings.ContainsAny(p.OAuth.AccessToken, " \t\r\n") {
		return nil, Error("OAuthSessionMissing")
	}
	expiry, err := time.Parse(time.RFC3339, p.OAuth.ExpiresAt)
	if err != nil || !expiry.After(now.Add(2*time.Minute)) {
		return nil, Error("OAuthSessionRefreshRequired")
	}
	// Config expiry is an advisory upper bound, not a signature/identity proof.
	// TLS GetCurrentIdentity supplies the authenticated identity check.
	return newSession(p.OAuth.AccessToken, expiry), nil
}

func boundedRead(f *os.File) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil || len(raw) > 1<<20 {
		return nil, Error("BootstrapFileReadRejected")
	}
	return raw, nil
}

func readPrivateJSON(path string, out any) error {
	f, err := openPrivate(path)
	if err != nil {
		return err
	}
	defer f.Close()
	raw, err := boundedRead(f)
	if err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil {
		return Error("PrivateJSONInvalid")
	}
	var extra any
	if !errors.Is(d.Decode(&extra), io.EOF) {
		return Error("PrivateJSONInvalid")
	}
	return nil
}
