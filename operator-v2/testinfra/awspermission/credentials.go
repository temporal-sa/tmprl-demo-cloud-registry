package awspermission

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// Credentials is an explicit private memory handle. There is no raw getter,
// serialization, profile/config lookup, IMDS, ambient fallback or refresh. Go
// SDK strings cannot be promised cryptographically erased; Close drops owned
// references and all output boundaries remain redacted/nonserializable.
type Credentials struct{ private *credentialState }
type credentialState struct {
	mu                 sync.Mutex
	key, secret, token string
	expires            time.Time
	kind               string
	closed             bool
	now                func() time.Time
	deadline           time.Time
}

func (Credentials) String() string               { return "AWSReadProofCredentials{private}" }
func (c Credentials) GoString() string           { return c.String() }
func (c Credentials) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, c.String()) }
func (Credentials) MarshalJSON() ([]byte, error) { return nil, PrivacyDenied }
func (Credentials) MarshalText() ([]byte, error) { return nil, PrivacyDenied }
func (c Credentials) Close() {
	if c.private != nil {
		c.private.mu.Lock()
		defer c.private.mu.Unlock()
		c.private.closed = true
		c.private.key = ""
		c.private.secret = ""
		c.private.token = ""
	}
}

// containsBeforeLock is used only while the private mutex is already held.
func (c Credentials) containsBeforeLock(raw []byte) bool {
	for _, part := range []string{c.private.key, c.private.secret, c.private.token} {
		if part != "" && bytes.Contains(raw, []byte(part)) {
			return true
		}
	}
	return false
}
func (c Credentials) contains(s string) bool {
	if c.private == nil {
		return false
	}
	c.private.mu.Lock()
	defer c.private.mu.Unlock()
	for _, part := range []string{c.private.key, c.private.secret, c.private.token} {
		if part != "" && strings.Contains(s, part) {
			return true
		}
	}
	return false
}
func boundedCredential(s string, min, max int) bool {
	if len(s) < min || len(s) > max {
		return false
	}
	for _, b := range []byte(s) {
		if b < 33 || b > 126 {
			return false
		}
	}
	return true
}
func newCredentials(key, secret, token string, expires time.Time, kind string, now func() time.Time, deadline time.Time) (Credentials, error) {
	if now == nil || !strings.HasPrefix(key, "ASIA") || !boundedCredential(key, 20, 128) || !boundedCredential(secret, 40, 256) || !boundedCredential(token, 32, 65536) || !expires.After(now()) || !deadline.After(now()) || kind != "bootstrap" && kind != "reader" {
		return Credentials{}, Invalid
	}
	return Credentials{&credentialState{key: key, secret: secret, token: token, expires: expires, kind: kind, now: now, deadline: deadline}}, nil
}

// DecodeBootstrapCredentials reads ONLY an explicitly supplied bounded stream.
// It does not run a CLI or read any file/profile/token. The live parent/pipe
// delivery adapter and its exact invocation remain a separate review gate.
func DecodeBootstrapCredentials(input io.Reader, now func() time.Time, deadline time.Time) (Credentials, error) {
	if input == nil {
		return Credentials{}, Invalid
	}
	raw, e := io.ReadAll(io.LimitReader(input, 1<<20+1))
	if e != nil || len(raw) > 1<<20 {
		return Credentials{}, Invalid
	}
	defer clear(raw)
	// Reject duplicate/case-aliased keys rather than silently choosing a value.
	keys := json.NewDecoder(bytes.NewReader(raw))
	if token, e := keys.Token(); e != nil || token != json.Delim('{') {
		return Credentials{}, Invalid
	}
	seen := map[string]bool{}
	for keys.More() {
		token, e := keys.Token()
		key, ok := token.(string)
		if e != nil || !ok || seen[key] {
			return Credentials{}, Invalid
		}
		switch key {
		case "Version", "AccessKeyId", "SecretAccessKey", "SessionToken", "Expiration":
		default:
			return Credentials{}, Invalid
		}
		seen[key] = true
		var value json.RawMessage
		if keys.Decode(&value) != nil {
			return Credentials{}, Invalid
		}
		clear(value)
	}
	if len(seen) != 5 {
		return Credentials{}, Invalid
	}
	if token, e := keys.Token(); e != nil || token != json.Delim('}') {
		return Credentials{}, Invalid
	}
	var v struct {
		Version                                    int
		AccessKeyId, SecretAccessKey, SessionToken string
		Expiration                                 time.Time
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&v) != nil {
		return Credentials{}, Invalid
	}
	var extra any
	if d.Decode(&extra) != io.EOF || v.Version != 1 {
		return Credentials{}, Invalid
	}
	return newCredentials(v.AccessKeyId, v.SecretAccessKey, v.SessionToken, v.Expiration, "bootstrap", now, deadline)
}

// SDK credential access requires a live private operation context. This is
// execution memory only; it is not a reusable public permit/context factory.
func (c Credentials) Retrieve(ctx context.Context) (aws.Credentials, error) {
	if c.private == nil {
		return aws.Credentials{}, Denied
	}
	c.private.mu.Lock()
	defer c.private.mu.Unlock()
	p, ok := ctx.Value(operationKey{}).(*operation)
	if !ok || p == nil || p.material != c.private || !p.live.Load() || ctx.Err() != nil || c.private.closed || !c.private.expires.After(c.private.now()) || !c.private.deadline.After(c.private.now()) {
		return aws.Credentials{}, Denied
	}
	expires := c.private.expires
	if c.private.deadline.Before(expires) {
		expires = c.private.deadline
	}
	return aws.Credentials{AccessKeyID: c.private.key, SecretAccessKey: c.private.secret, SessionToken: c.private.token, CanExpire: true, Expires: expires, Source: "ExplicitOwnedReadProof"}, nil
}
