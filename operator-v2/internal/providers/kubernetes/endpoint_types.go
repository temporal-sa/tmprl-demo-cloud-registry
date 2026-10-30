package kubernetes

import (
	"encoding/json"
	"fmt"
	"io"
	"time"
)

const EndpointInvalid Code = "kubernetes_endpoint_request_invalid"

// EndpointRead carries original accepted desired bodies and independently
// persisted receipts, NOT public readiness DTOs or caller-supplied descendants.
// The real actor must resolve/pin those inputs from its authorized journal.
// They are freshly verified against the same namespace binding on every read.
// This capability establishes current ancestry/publication, not Pod payload,
// creator identity, admission equivalence, traffic or promotion authority.
type EndpointRead struct {
	Service    ServiceRead
	Deployment DeploymentRead
	Limits     EndpointLimits
}

func (EndpointRead) String() string               { return "EndpointRead{execution-only}" }
func (r EndpointRead) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, r.String()) }
func (EndpointRead) MarshalJSON() ([]byte, error) { return json.Marshal("execution-only") }

// These finite bounds are checked against the trusted pinned typed v1.37 API.
// Byte accounting is post-decode; this is not adversarial wire-allocation proof.
type EndpointLimits struct {
	PageSize                                                             int64
	MaxPages, MaxSlices, MaxEndpoints, MaxPods, MaxReplicaSets, MaxBytes int
	Timeout                                                              time.Duration
}

func endpointLimits(l EndpointLimits) (EndpointLimits, bool) {
	if l == (EndpointLimits{}) {
		return EndpointLimits{PageSize: 16, MaxPages: 8, MaxSlices: 64, MaxEndpoints: 512, MaxPods: 128, MaxReplicaSets: 16, MaxBytes: 2 * 1024 * 1024, Timeout: 10 * time.Second}, true
	}
	return l, l.PageSize > 0 && l.PageSize <= 64 && l.MaxPages > 0 && l.MaxPages <= 16 && l.MaxSlices > 0 && l.MaxSlices <= 128 && l.MaxEndpoints > 0 && l.MaxEndpoints <= 1024 && l.MaxPods > 0 && l.MaxPods <= 256 && l.MaxReplicaSets > 0 && l.MaxReplicaSets <= 32 && l.MaxBytes > 0 && l.MaxBytes <= 4*1024*1024 && l.Timeout > 0 && l.Timeout <= 15*time.Second
}

type EndpointCounts struct {
	Slices                 int `json:"slices"`
	EndpointEntries        int `json:"endpointEntries"`
	DuplicateEntries       int `json:"duplicateEntries"`
	VerifiedPods           int `json:"verifiedPods"`
	ReadyPods              int `json:"readyPods"`
	ServingPods            int `json:"servingPods"`
	TerminatingPods        int `json:"terminatingPods"`
	DefaultedConditionPods int `json:"defaultedConditionPods"`
	ConditionConflicts     int `json:"conditionConflicts"`
	UnverifiedEntries      int `json:"unverifiedEntries"`
}
type EndpointView struct {
	Version              string         `json:"version"`
	Claim                string         `json:"claim"`
	State                string         `json:"state"`
	Code                 string         `json:"code"`
	Complete             bool           `json:"complete"`
	PublicationPresent   bool           `json:"publicationPresent"`
	PayloadVerified      bool           `json:"payloadVerified"`
	PromotionAuthorized  bool           `json:"promotionAuthorized"`
	StartedAt            time.Time      `json:"startedAt"`
	CompletedAt          time.Time      `json:"completedAt"`
	NamespaceUID         string         `json:"namespaceUID"`
	ServiceUID           string         `json:"serviceUID"`
	DeploymentUID        string         `json:"deploymentUID"`
	DeploymentGeneration int64          `json:"deploymentGeneration"`
	SampleDigest         string         `json:"sampleDigest,omitempty"`
	Counts               EndpointCounts `json:"counts"`
}

// Complete describes enumeration/rechecks over this OBSERVED INTERVAL. It is
// not an atomic cross-resource snapshot, a future guarantee or a write fence.
// No raw addresses, selectors, conditions, PodSpec or provider error is kept.
type EndpointSnapshot struct{ private *EndpointView }

func (s EndpointSnapshot) View() EndpointView {
	if s.private == nil {
		return EndpointView{Version: "endpoint-view-v1", Claim: "current_ancestry_publication", State: "Unverified", Code: "EndpointUnverified"}
	}
	return *s.private
}
func (s EndpointSnapshot) MarshalJSON() ([]byte, error) { return json.Marshal(s.View()) }
func (s EndpointSnapshot) String() string               { data, _ := s.MarshalJSON(); return string(data) }
func (s EndpointSnapshot) Format(f fmt.State, _ rune)   { _, _ = io.WriteString(f, s.String()) }
