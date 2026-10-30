// Package sourceresolver performs explicitly scoped activity-only acquisition.
// It has no worker registration, ambient credential/config lookup or tool exec.
package sourceresolver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/buildplan"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
)

const ArchiveFormat = "registry-full-tree-tar-v1"
const WatchProfile = "registry-watch-subset-v1"

type Limits struct {
	WireBytes, PackBytes, ObjectBytes, InflatedBytes, ArchiveBytes int64
	Objects, Files, Depth, DeltaDepth                              int
}

func BoundedLimits() Limits {
	return Limits{WireBytes: 64 << 20, PackBytes: 32 << 20, ObjectBytes: 8 << 20, InflatedBytes: 128 << 20, ArchiveBytes: 64 << 20, Objects: 50000, Files: 20000, Depth: 32, DeltaDepth: 32}
}
func (l Limits) valid() bool {
	max := BoundedLimits()
	return l.WireBytes > 0 && l.WireBytes <= max.WireBytes && l.PackBytes > 0 && l.PackBytes <= max.PackBytes && l.ObjectBytes > 0 && l.ObjectBytes <= max.ObjectBytes && l.InflatedBytes > 0 && l.InflatedBytes <= max.InflatedBytes && l.ArchiveBytes > 0 && l.ArchiveBytes <= max.ArchiveBytes && l.Objects > 0 && l.Objects <= max.Objects && l.Files > 0 && l.Files <= max.Files && l.Depth > 0 && l.Depth <= max.Depth && l.DeltaDepth > 0 && l.DeltaDepth <= max.DeltaDepth
}
func failure(code, message string, category diagnostics.Category) error {
	return diagnostics.NewError(diagnostics.DomainFailure{ID: "source-" + code, Code: code, Category: category, Message: message, Step: "resolve-source", NextAction: "Inspect approved source policy and authorized acquisition evidence; do not retry with ambient credentials or another repository."})
}
func invalid(code, message string) error { return failure(code, message, diagnostics.Configuration) }

// Facts contain independently computed fetched-content identities. They are
// still producer assertions at an API boundary; only trusted acquisition and
// storage adapters may create/commit them. No arbitrary provider error text.
type Facts struct {
	Owner                                                                     ownership.Owner
	CheckoutURL, RequestedRef, PolicyRevision, BindingHash                    string
	Commit, ArtifactDigest, WatchSelectionDigest, ArchiveFormat, WatchProfile string
	Files                                                                     int
	ArchiveBytes                                                              int64
}
type Reservation struct {
	SourceUID   string
	Version     uint64
	BindingHash string
}
type Receipt struct {
	SourceUID string
	Version   uint64
	Facts     Facts
}

// SourceStore must verify actual pinned owned identity in Reserve before auth
// acquisition, CAS the reserved version, persist private artifact before record,
// and read back both bytes/record before acknowledgment. Plain receipt equality
// cannot authenticate an arbitrary external adapter implementation.
type SourceStore interface {
	Reserve(context.Context, buildplan.SourceSelection) (Reservation, error)
	Commit(context.Context, Reservation, Facts, []byte) (Receipt, error)
}
type Summary struct {
	Commit, ArtifactDigest, WatchSelectionDigest, SourceUID, SourceVersion, ArchiveFormat, WatchProfile string
	Files                                                                                               int
	ArchiveBytes                                                                                        int64
}

// Artifact exposes only bounded safe references via JSON/fmt. PrivateArchive is
// an explicit execution/staging accessor; never persist its bytes in Temporal
// history, diagnostics, log fields or ordinary catalog discovery.
// The pointer handle is intentional: fmt's %p bypasses Formatter for value
// structs and would otherwise recursively reveal inline private fields.
type Artifact struct{ private *artifactData }
type artifactData struct {
	source  buildplan.ResolvedSource
	summary Summary
	archive []byte
}

func (a Artifact) Source() buildplan.ResolvedSource {
	if a.private == nil {
		return buildplan.ResolvedSource{}
	}
	return a.private.source
}
func (a Artifact) Summary() Summary {
	if a.private == nil {
		return Summary{}
	}
	return a.private.summary
}
func (a Artifact) PrivateArchive() []byte {
	if a.private == nil {
		return nil
	}
	return append([]byte(nil), a.private.archive...)
}
func (a Artifact) String() string                 { b, _ := json.Marshal(a.Summary()); return string(b) }
func (a Artifact) GoString() string               { return a.String() }
func (a Artifact) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, a.String()) }
func (a Artifact) MarshalJSON() ([]byte, error)   { return json.Marshal(a.Summary()) }

// AuthRequest is activity-only reference policy, not an identity credential.
type AuthRequest struct{ private *authRequestData }
type authRequestData struct {
	owner                                            ownership.Owner
	repository, logicalName, versionRef, bindingHash string
}

func (r AuthRequest) value() authRequestData {
	if r.private == nil {
		return authRequestData{}
	}
	return *r.private
}
func (r AuthRequest) Owner() ownership.Owner         { return r.value().owner }
func (r AuthRequest) Repository() string             { return r.value().repository }
func (r AuthRequest) LogicalName() string            { return r.value().logicalName }
func (r AuthRequest) VersionRef() string             { return r.value().versionRef }
func (r AuthRequest) BindingHash() string            { return r.value().bindingHash }
func (r AuthRequest) String() string                 { return "activity-only source credential request" }
func (r AuthRequest) GoString() string               { return r.String() }
func (r AuthRequest) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, r.String()) }
func (r AuthRequest) MarshalJSON() ([]byte, error)   { return []byte(`{"activityOnly":true}`), nil }

type Credential struct{ private *credentialData }
type credentialData struct {
	request            authRequestData
	username, password string
	expiresAt          time.Time
}

// NewCredential wraps an already authorized injected provider result; it does
// not verify a secret store, authenticate a caller or grant source authority.
func NewCredential(request AuthRequest, username, password string, expiresAt time.Time) (Credential, error) {
	if request.private == nil || username == "" || password == "" || len(username) > 1024 || len(password) > 16384 || expiresAt.IsZero() {
		return Credential{}, failure("SourceCredentialInvalid", "The scoped source credential provider returned an invalid result.", diagnostics.Authentication)
	}
	return Credential{private: &credentialData{request: request.value(), username: username, password: password, expiresAt: expiresAt}}, nil
}
func (c Credential) String() string                 { return "private activity-only source credential" }
func (c Credential) GoString() string               { return c.String() }
func (c Credential) Format(state fmt.State, _ rune) { _, _ = io.WriteString(state, c.String()) }
func (c Credential) MarshalJSON() ([]byte, error)   { return []byte(`{"activityOnly":true}`), nil }

type PrivateAuthProvider interface {
	Credential(context.Context, AuthRequest) (Credential, error)
}

var _ fmt.Stringer = Artifact{}
