package kubernetes

import (
	"encoding/json"
	"fmt"
	"io"
)

// NodeBinding is an explicit current Node pin supplied by the trusted local
// supervisor. It is not proof of scheduling origin, node history or runtime.
type NodeBinding struct{ Name, UID string }

// LiteralWorkloadRead must come from independently retained accepted inputs and
// acknowledged canonical receipts. Parsing its JSON cannot grant authority.
type LiteralWorkloadRead struct {
	EndpointRead
	ServiceAccount ServiceAccountRead
	Node           NodeBinding
}

func (LiteralWorkloadRead) String() string               { return "LiteralWorkloadRead{execution-only}" }
func (r LiteralWorkloadRead) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, r.String()) }
func (LiteralWorkloadRead) MarshalJSON() ([]byte, error) { return json.Marshal("execution-only") }

// Equality describes current typed v1.37 API declarations over an observation
// interval. It does not authenticate creators, running process values, installed
// admission configuration or placement. Endpoint publication is the SAME private
// Pod UID sample, never a conjunction inferred from public counts alone.
type LiteralWorkloadView struct {
	Version               string       `json:"version"`
	Claim                 string       `json:"claim"`
	State                 string       `json:"state"`
	Code                  string       `json:"code"`
	Complete              bool         `json:"complete"`
	DeclarationsEqual     bool         `json:"declarationsEqual"`
	ReplicaSets           int          `json:"replicaSets"`
	Pods                  int          `json:"pods"`
	Publication           EndpointView `json:"publication"`
	CreatorOriginVerified bool         `json:"creatorOriginVerified"`
	ProcessValuesVerified bool         `json:"processValuesVerified"`
	PlacementAttested     bool         `json:"placementAttested"`
	PromotionAuthorized   bool         `json:"promotionAuthorized"`
}
type LiteralWorkloadSnapshot struct{ private *literalSnapshotData }
type literalSnapshotData struct {
	view LiteralWorkloadView
	pods map[string]bool
}

func (s LiteralWorkloadSnapshot) View() LiteralWorkloadView {
	if s.private == nil {
		return LiteralWorkloadView{Version: "literal-workload-v1", Claim: "current_workload_api_declaration_v1", State: "Unverified", Code: "LiteralWorkloadUnverified"}
	}
	return s.private.view
}

// SamePodIdentities correlates two complete positive interval samples without
// publishing addresses/names or pretending counts prove identity. It is not a
// continuous lifetime guarantee or authorization for traffic/promotion.
func (s LiteralWorkloadSnapshot) SamePodIdentities(other LiteralWorkloadSnapshot) bool {
	if s.private == nil || other.private == nil || !s.View().DeclarationsEqual || !other.View().DeclarationsEqual || s.View().Publication.NamespaceUID != other.View().Publication.NamespaceUID || s.View().Publication.DeploymentUID != other.View().Publication.DeploymentUID || s.View().Publication.ServiceUID != other.View().Publication.ServiceUID || len(s.private.pods) != len(other.private.pods) {
		return false
	}
	for uid := range s.private.pods {
		if !other.private.pods[uid] {
			return false
		}
	}
	return true
}
func (s LiteralWorkloadSnapshot) MarshalJSON() ([]byte, error) { return json.Marshal(s.View()) }
func (s LiteralWorkloadSnapshot) String() string               { data, _ := s.MarshalJSON(); return string(data) }
func (s LiteralWorkloadSnapshot) Format(f fmt.State, _ rune)   { _, _ = io.WriteString(f, s.String()) }
