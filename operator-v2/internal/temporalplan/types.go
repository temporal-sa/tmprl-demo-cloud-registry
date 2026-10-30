// Package temporalplan turns validated authoring into non-authorizing Temporal
// Cloud namespace decisions. It has no SDK, transport, secret or ambient inputs.
package temporalplan

import (
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
)

type AuthMode string

const (
	APIKey AuthMode = "apiKey"
	MTLS   AuthMode = "mtls"
)

type AttributeType string

const (
	Text        AttributeType = "text"
	Keyword     AttributeType = "keyword"
	Int         AttributeType = "int"
	Double      AttributeType = "double"
	Bool        AttributeType = "bool"
	Datetime    AttributeType = "datetime"
	KeywordList AttributeType = "keyword_list"
)

// Incarnation is fixture/ledger-attested creation provenance, not a name-based
// discovery result. Once present, absence never authorizes re-creation.
type Incarnation struct {
	NamespaceID  string    `json:"namespaceID"`
	CreatedAt    time.Time `json:"createdAt"`
	ProvenanceID string    `json:"provenanceID"`
}
type TargetBinding struct {
	Label       string
	Name        string
	RegionID    string
	Owner       ownership.Owner
	Incarnation *Incarnation
}
type Policy struct {
	Ownership            ownership.Context
	ProjectName          string
	DefaultRegion        string
	ServerlessRegion     string
	AllowedRegions       []string
	DefaultRetentionDays int32
	MaximumRetentionDays int32
	AllowedAuthModes     []AuthMode
	Targets              []TargetBinding
}

// Observation must come from an independent scoped adapter, using its bound
// incarnation. Owner/ProvenanceID are attested ledger metadata, not Cloud tags.
// Matching these DTO fields is not authentication or permission to mutate.
type NamespaceState string

const (
	Active           NamespaceState = "Active"
	Activating       NamespaceState = "Activating"
	Deleting         NamespaceState = "Deleting"
	ActivationFailed NamespaceState = "ActivationFailed"
)

type Observation struct {
	Label            string
	Exists           bool
	State            NamespaceState
	NamespaceID      string
	Name             string
	CreatedAt        time.Time
	ProvenanceID     string
	Owner            ownership.Owner
	ResourceVersion  string
	RegionID         string
	RetentionDays    int32
	FairnessEnabled  bool
	AuthMode         AuthMode
	SearchAttributes map[string]AttributeType
}
type ManagedAttributes struct {
	Label        string
	NamespaceID  string
	CreatedAt    time.Time
	Owner        ownership.Owner
	ProvenanceID string
	Attributes   map[string]AttributeType
}

type Action string

const (
	Observe      Action = "ObservationRequired"
	CreateIntent Action = "CreationIntentRequired"
	Verify       Action = "Verify"
	UpdateIntent Action = "UpdateIntentRequired"
	Blocked      Action = "Blocked"
)

type AttributeAction string

const (
	ObserveAttribute      AttributeAction = "ObservationRequired"
	Add                   AttributeAction = "Add"
	VerifyAttribute       AttributeAction = "Verify"
	AttributeTypeConflict AttributeAction = "TypeConflict"
	RetainedUnsupported   AttributeAction = "RetainedRemovalUnsupported"
	RetainedUnmanaged     AttributeAction = "RetainedUnmanaged"
)

type AttributeDecision struct {
	Name         string          `json:"name"`
	DesiredType  AttributeType   `json:"desiredType,omitempty"`
	ObservedType AttributeType   `json:"observedType,omitempty"`
	Action       AttributeAction `json:"action"`
}
type Namespace struct {
	Label                string              `json:"label"`
	Name                 string              `json:"name"`
	NamespaceID          string              `json:"namespaceID"`
	Incarnation          *Incarnation        `json:"incarnation,omitempty"`
	ExpectedVersion      string              `json:"expectedVersion,omitempty"`
	RegionID             string              `json:"regionID"`
	RetentionDays        int32               `json:"retentionDays"`
	FairnessEnabled      bool                `json:"fairnessEnabled"`
	AuthMode             AuthMode            `json:"authMode"`
	Components           []string            `json:"components,omitempty"`
	Action               Action              `json:"action"`
	Attributes           []AttributeDecision `json:"attributes,omitempty"`
	NeedsRetentionUpdate bool                `json:"needsRetentionUpdate,omitempty"`
	NeedsFairnessUpdate  bool                `json:"needsFairnessUpdate,omitempty"`
}
type Issue struct {
	Code     string `json:"code"`
	Label    string `json:"label"`
	Field    string `json:"field"`
	Message  string `json:"message"`
	Blocking bool   `json:"blocking"`
}
type RetainedNamespace struct {
	Label            string                   `json:"label"`
	Name             string                   `json:"name"`
	NamespaceID      string                   `json:"namespaceID"`
	Reason           string                   `json:"reason"`
	BoundIncarnation Incarnation              `json:"boundIncarnation"`
	ObservationState RetainedObservationState `json:"observationState"`
	ObservedState    NamespaceState           `json:"observedState,omitempty"`
	ObservedVersion  string                   `json:"observedVersion,omitempty"`
	Attributes       []AttributeDecision      `json:"attributes,omitempty"`
}
type RetainedObservationState string

const (
	RetainedUnknown             RetainedObservationState = "ObservationRequired"
	RetainedMissing             RetainedObservationState = "Missing"
	RetainedIncarnationMismatch RetainedObservationState = "IncarnationMismatch"
	RetainedNotActive           RetainedObservationState = "NotActive"
	RetainedObserved            RetainedObservationState = "ObservedActive"
)

type Plan struct {
	Namespaces         []Namespace         `json:"namespaces,omitempty"`
	RetainedNamespaces []RetainedNamespace `json:"retainedNamespaces,omitempty"`
	Issues             []Issue             `json:"issues,omitempty"`
	// This is deliberately always false. Adapters separately verify live
	// provenance, writer guards, provider versions and effective permissions.
	MutationAuthorized bool `json:"mutationAuthorized"`
}

type Error struct{ Code string }

func (e *Error) Error() string { return e.Code }
