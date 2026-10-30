// Package temporalcloud isolates the experimental official Temporal Cloud SDK.
// It requires explicit targets, credentials, capability grants and an external
// mutation guard. It never loads bootstrap secrets or environment credentials.
package temporalcloud

import (
	"context"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	"google.golang.org/grpc"
)

// Transport is deliberately narrower than CloudServiceClient: there are no
// account listing, users, groups, service-account or Nexus operations here.
type Transport interface {
	GetNamespace(context.Context, *cloudservice.GetNamespaceRequest, ...grpc.CallOption) (*cloudservice.GetNamespaceResponse, error)
	CreateNamespace(context.Context, *cloudservice.CreateNamespaceRequest, ...grpc.CallOption) (*cloudservice.CreateNamespaceResponse, error)
	UpdateNamespace(context.Context, *cloudservice.UpdateNamespaceRequest, ...grpc.CallOption) (*cloudservice.UpdateNamespaceResponse, error)
	DeleteNamespace(context.Context, *cloudservice.DeleteNamespaceRequest, ...grpc.CallOption) (*cloudservice.DeleteNamespaceResponse, error)
	UpdateNamespaceTags(context.Context, *cloudservice.UpdateNamespaceTagsRequest, ...grpc.CallOption) (*cloudservice.UpdateNamespaceTagsResponse, error)
	GetAsyncOperation(context.Context, *cloudservice.GetAsyncOperationRequest, ...grpc.CallOption) (*cloudservice.GetAsyncOperationResponse, error)
}

type Capabilities struct {
	CreateNamespaces        bool
	UpdateNamespaces        bool
	DeleteNamespaces        bool
	DisableDeleteProtection bool
	// GlobalTagWrites is separate because the current API/proto documents
	// global admin/account owner authority for namespace tags.
	GlobalTagWrites bool
}

// MutationGuard must establish serialized execution and drain stale writers.
// Run must invoke fn only while the supplied writer still has authority.
// ResourceVersion supplies CAS for namespace updates/deletes, but tags have no
// provider CAS and namespace APIs do not enforce our local writer epoch.
// This interface is an external guarantee, not a lock implemented by this SDK.
type MutationGuard interface {
	Run(ctx context.Context, namespaceID string, writer ownership.Writer, fn func(context.Context) error) error
}

// Binding is trusted ledger/fixture evidence, never a project-supplied claim.
// Cloud namespace identifiers are reusable names; CreatedAt pins the incarnation.
// Ref.ProviderID must equal IncarnationID(NamespaceID, CreatedAt).
type Binding struct {
	NamespaceID  string
	CreatedAt    time.Time
	ProvenanceID string
	Record       ownership.Record
}

type Config struct {
	Ownership ownership.Context
	Bindings  []Binding
	// PlannedCreates contains persisted, trusted creation intents. A target
	// cannot simultaneously have a live binding and a new creation intent.
	PlannedCreates       []ownership.Intent
	CreationProjectID    string
	AllowedRegions       []string
	Capabilities         Capabilities
	MaximumRetentionDays int32
	CallTimeout          time.Duration
	Guard                MutationGuard
	Now                  func() time.Time
}

type SearchAttributeType string

const (
	Text        SearchAttributeType = "text"
	Keyword     SearchAttributeType = "keyword"
	Int         SearchAttributeType = "int"
	Double      SearchAttributeType = "double"
	Bool        SearchAttributeType = "bool"
	Datetime    SearchAttributeType = "datetime"
	KeywordList SearchAttributeType = "keyword_list"
)

type AuthMode string

const (
	APIKeyAuth AuthMode = "apiKey"
	MTLSAuth   AuthMode = "mtls"
)

type DesiredNamespace struct {
	RegionID      string
	RetentionDays int32
	AuthMode      AuthMode
	// Certificate authority certificates only; private keys are rejected.
	MTLSCAPEM        []byte
	FairnessEnabled  bool
	SearchAttributes map[string]SearchAttributeType
	Description      string
	Tags             map[string]string
}

// Patch updates only explicitly supplied fields. Provider fields unknown to
// this adapter are preserved by cloning the observed protobuf spec.
type Patch struct {
	RetentionDays    *int32
	FairnessEnabled  *bool
	DeleteProtection *bool
	Description      *string
	SearchAttributes map[string]SearchAttributeType
	// Current official protobuf documentation says removal is unsupported.
	// Removal is rejected explicitly, never silently treated as successful.
	RemoveSearchAttributes []string
}

type Snapshot struct {
	NamespaceID      string
	IncarnationID    string
	Name             string
	CreatedAt        time.Time
	ResourceVersion  string
	State            string
	Exists           bool
	APIKeyEnabled    bool
	MTLSEnabled      bool
	DeleteProtected  bool
	FairnessEnabled  bool
	RetentionDays    int32
	SearchAttributes map[string]SearchAttributeType
	Tags             map[string]string
	GRPCAddress      string
}

type WriteRequest struct {
	NamespaceID           string
	ExpectedIncarnationID string
	ExpectedVersion       string
	ExpectedRevision      string
	OperationKey          string
	Writer                ownership.Writer
}

// Receipt must be durably saved before polling/retrying another operation. A
// failed transport call can still have been accepted by the Cloud API.
type Receipt struct {
	ID               string
	NamespaceID      string
	Kind             string
	OperationKey     string
	InputFingerprint string
}

type Operation struct {
	Receipt            Receipt
	State              string
	Done               bool
	Failed             bool
	PollAfter          time.Duration
	MutationProtection string
}

type CreatedNamespace struct {
	Snapshot  Snapshot
	Binding   Binding
	Operation Operation
}

func IncarnationID(namespaceID string, createdAt time.Time) string {
	return namespaceID + "#" + createdAt.UTC().Format(time.RFC3339Nano)
}
