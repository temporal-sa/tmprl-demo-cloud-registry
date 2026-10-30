// Package runtimeinputs retains original rendered Deployment, Service and
// explicitly enabled ServiceAccount bodies privately.
// A journal reference selects an immutable artifact; neither an orphan artifact
// nor its name authorizes provider work. This cooperative local filesystem
// capability is not encrypted storage, remote storage or a distributed fence.
package runtimeinputs

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

var (
	ErrNotConfigured = errors.New("private runtime input store is not configured")
	ErrInvalid       = errors.New("invalid private render input contract")
	ErrUnproven      = errors.New("private render input missing, changed or unproven")
	ErrUncertain     = errors.New("private render input persistence uncertain; retain partial artifact")
	ErrCapacity      = errors.New("private render input capacity reached")
)

type Config struct {
	BootstrapUID   string
	Owner          ownership.Owner
	CellUID        string
	MaximumEntries int
	// ServiceAccounts opts a fresh store into the third finite input family.
	// Omitted false preserves the exact historical manifest/config bytes.
	ServiceAccounts bool `json:",omitempty"`
}
type FileID struct {
	Device, Inode uint64
	UID           uint32
}

// Pin is retained independently of the private directory, never discovered
// from that directory to admit a copied/replaced root.
type Pin struct {
	BootstrapUID, ManifestDigest string
	Directory, Lock              FileID
}
type Store struct {
	root   *os.Root
	config Config
	pin    Pin
	mu     sync.Mutex
	schema string // finite pinned manifest, accessed under the store lock
	// Deterministic private filesystem fault hook; no public constructor accepts it.
	fault func(string) error
}

// Target is safe immutable identity metadata from an accepted journal attempt.
// It must never be reconstructed from current mutable project authoring.
type Target struct {
	CellUID                                               string
	Parent                                                ownership.NamespaceParent
	AttemptID, Revision, DesiredDigest, Name, Fingerprint string
}
type Capture struct {
	Target  Target
	Desired *appsv1.Deployment
}

type ServiceCapture struct {
	Target  Target
	Desired *corev1.Service
}

func (ServiceCapture) String() string               { return "ServiceInputCapture{execution-only}" }
func (c ServiceCapture) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, c.String()) }
func (ServiceCapture) MarshalJSON() ([]byte, error) { return json.Marshal("execution-only") }

type ServicePayload struct{ private *servicePayload }
type servicePayload struct{ object *corev1.Service }

func (ServicePayload) String() string               { return "ServiceInput{private}" }
func (p ServicePayload) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, p.String()) }
func (ServicePayload) MarshalJSON() ([]byte, error) { return json.Marshal("private") }
func (p ServicePayload) ObjectForProvider() *corev1.Service {
	if p.private == nil {
		return nil
	}
	return p.private.object.DeepCopy()
}

func (Capture) String() string               { return "DeploymentInputCapture{execution-only}" }
func (c Capture) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, c.String()) }
func (Capture) MarshalJSON() ([]byte, error) { return json.Marshal("execution-only") }

// Payload is opaque even with numeric/pointer/unsupported fmt verbs. The
// explicit copied accessor is solely for private provider execution.
type Payload struct{ private *payload }
type payload struct{ object *appsv1.Deployment }

func (Payload) String() string               { return "DeploymentInput{private}" }
func (p Payload) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, p.String()) }
func (Payload) MarshalJSON() ([]byte, error) { return json.Marshal("private") }
func (p Payload) ObjectForProvider() *appsv1.Deployment {
	if p.private == nil {
		return nil
	}
	return p.private.object.DeepCopy()
}

type manifest struct {
	Schema          string
	Config          Config
	Directory, Lock FileID
}
type envelope struct {
	Schema, BootstrapUID, CellUID string
	Subject                       rollout.Subject
	Owner                         ownership.Owner
	Target                        Target
	Contract                      string
	Object                        json.RawMessage
}
type receipt struct{ Schema, UID, Digest, Contract string }

const maxEnvelopeBytes = 513 * 1024
