package temporalcloud

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// ProviderVersionDigest is a safe reference to exact native version bytes.
// It is never a replacement version to send to a provider's conditional API.
// Invalid/unbounded native versions have no usable durable representation.
func ProviderVersionDigest(native string) string {
	if !text(native) {
		return ""
	}
	h := sha256.Sum256([]byte(native))
	return hex.EncodeToString(h[:])
}

func credentialVersionsValid(state CredentialState) bool {
	sa, key := state.ServiceAccount, state.Key
	if sa.VersionDigest != "" && !digestPattern.MatchString(sa.VersionDigest) {
		return false
	}
	if key.VersionDigest != "" && !digestPattern.MatchString(key.VersionDigest) {
		return false
	}
	if sa.ID != "" && !sa.CreatedAt.IsZero() && !digestPattern.MatchString(sa.VersionDigest) {
		return false
	}
	if key.Verified && !digestPattern.MatchString(key.VersionDigest) {
		return false
	}
	return true
}

// Legacy raw-version wire objects are rejected even when the old version was
// empty/unobserved. Ignoring unknown JSON fields would silently reinterpret
// old histories as new digest-only references.
func (r *ServiceAccountRef) UnmarshalJSON(raw []byte) error {
	type wire ServiceAccountRef
	var v wire
	if strictCredentialReference(raw, &v) != nil {
		return errors.New("service account reference requires digest-only version schema")
	}
	*r = ServiceAccountRef(v)
	return nil
}
func (r *KeyRef) UnmarshalJSON(raw []byte) error {
	type wire KeyRef
	var v wire
	if strictCredentialReference(raw, &v) != nil {
		return errors.New("key reference requires digest-only version schema")
	}
	*r = KeyRef(v)
	return nil
}
func strictCredentialReference(raw []byte, out any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(out) != nil {
		return errors.New("credential reference schema rejected")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return errors.New("credential reference schema rejected")
	}
	return nil
}

// keyObservation is an execution-only capability. Its private pointer also
// protects unsupported fmt verbs such as %p on a value from exposing fields.
type keyObservation struct{ private *keyObservationData }
type keyObservationData struct {
	reference     KeyRef
	nativeVersion string
}

func (keyObservation) String() string               { return "CredentialKeyObservation{execution-only}" }
func (keyObservation) GoString() string             { return "CredentialKeyObservation{execution-only}" }
func (o keyObservation) Format(w fmt.State, _ rune) { _, _ = io.WriteString(w, o.String()) }
func (keyObservation) MarshalJSON() ([]byte, error) {
	return nil, errors.New("native credential observation must not be serialized")
}
func (keyObservation) MarshalText() ([]byte, error) {
	return nil, errors.New("native credential observation must not be serialized")
}

// Ensure the explicit rejection also applies if embedded by a caller.
var _ json.Marshaler = keyObservation{}
