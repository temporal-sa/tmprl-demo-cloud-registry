package credentialobservation

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
	workflow "go.temporal.io/api/workflowservice/v1"
	cloud "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type cloudReads interface {
	GetCurrentIdentity(context.Context, *cloud.GetCurrentIdentityRequest, ...grpc.CallOption) (*cloud.GetCurrentIdentityResponse, error)
	GetNamespace(context.Context, *cloud.GetNamespaceRequest, ...grpc.CallOption) (*cloud.GetNamespaceResponse, error)
}
type dataReads interface {
	DescribeNamespace(context.Context, *workflow.DescribeNamespaceRequest, ...grpc.CallOption) (*workflow.DescribeNamespaceResponse, error)
}
type probes struct {
	cloud cloudReads
	data  dataReads
	close func()
}
type probeFactory func(Config, *material, time.Time) (*probes, error)

type material struct{ private *materialState }
type materialState struct {
	mu     sync.Mutex
	bytes  []byte
	closed bool
}

func newMaterial(b []byte) material {
	return material{private: &materialState{bytes: append([]byte(nil), b...)}}
}
func (material) String() string               { return "CandidateMaterial{redacted}" }
func (material) GoString() string             { return "CandidateMaterial{redacted}" }
func (m material) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, m.String()) }
func (material) MarshalJSON() ([]byte, error) { return nil, failure(PrivacyDenied, Secret) }
func (material) MarshalText() ([]byte, error) { return nil, failure(PrivacyDenied, Secret) }
func (m material) close() {
	if m.private != nil {
		m.private.mu.Lock()
		defer m.private.mu.Unlock()
		clear(m.private.bytes)
		m.private.bytes = nil
		m.private.closed = true
	}
}
func (m material) contains(b []byte) bool {
	if m.private == nil {
		return false
	}
	m.private.mu.Lock()
	defer m.private.mu.Unlock()
	return len(m.private.bytes) > 0 && bytes.Contains(b, m.private.bytes)
}

func forbiddenContext(ctx context.Context) bool {
	md, _ := metadata.FromOutgoingContext(ctx)
	for key, values := range md {
		if len(values) == 0 {
			continue
		}
		switch strings.ToLower(key) {
		case "authorization", "proxy-authorization", "x-api-key", "x-temporal-api-key", "temporal-namespace", "temporal-cloud-api-version":
			return true
		}
	}
	return false
}
func inheritedAuthorization(ctx context.Context) bool {
	md, _ := metadata.FromOutgoingContext(ctx)
	for key, values := range md {
		if len(values) == 0 {
			continue
		}
		switch strings.ToLower(key) {
		case "authorization", "proxy-authorization", "x-api-key", "x-temporal-api-key":
			return true
		}
	}
	return false
}

type authBudget struct {
	mu       sync.Mutex
	used     map[string]int
	key      *material
	deadline time.Time
	now      func() time.Time
}
type candidateCredentials struct{ private *credentialState }
type credentialState struct {
	budget    *authBudget
	data      bool
	namespace string
}

func (candidateCredentials) String() string               { return "CandidateCredentials{redacted}" }
func (candidateCredentials) GoString() string             { return "CandidateCredentials{redacted}" }
func (c candidateCredentials) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, c.String()) }
func (candidateCredentials) MarshalJSON() ([]byte, error) {
	return nil, failure(PrivacyDenied, Identity)
}
func (candidateCredentials) MarshalText() ([]byte, error) {
	return nil, failure(PrivacyDenied, Identity)
}
func (candidateCredentials) RequireTransportSecurity() bool { return true }
func (c candidateCredentials) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	if c.private == nil || c.private.budget == nil {
		return nil, failure(Denied, Identity)
	}
	b := c.private.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	info, ok := credentials.RequestInfoFromContext(ctx)
	if !ok || ctx.Err() != nil || !b.deadline.After(b.now()) || inheritedAuthorization(ctx) || credentials.CheckSecurityLevel(info.AuthInfo, credentials.PrivacyAndIntegrity) != nil {
		return nil, failure(Denied, Identity)
	}
	md, _ := metadata.FromOutgoingContext(ctx)
	routing := "temporal-cloud-api-version"
	value := tc.SupportedAPIVersion
	if c.private.data {
		routing = "temporal-namespace"
		value = c.private.namespace
	}
	values := md.Get(routing)
	if len(md) != 1 || len(values) != 1 || values[0] != value {
		return nil, failure(Denied, Identity)
	}
	maximum := 0
	if c.private.data && info.Method == workflow.WorkflowService_DescribeNamespace_FullMethodName {
		maximum = 1
	}
	if !c.private.data {
		switch info.Method {
		case cloud.CloudService_GetCurrentIdentity_FullMethodName:
			maximum = 1
		case cloud.CloudService_GetNamespace_FullMethodName:
			maximum = 2
		}
	}
	if maximum == 0 || b.used[info.Method] >= maximum {
		return nil, failure(Denied, Identity)
	}
	b.used[info.Method]++ // Includes transparent gRPC attempts before HEADERS.
	k := b.key
	if k == nil || k.private == nil {
		return nil, failure(PrivacyDenied, Identity)
	}
	k.private.mu.Lock()
	defer k.private.mu.Unlock()
	if k.private.closed || len(k.private.bytes) == 0 {
		return nil, failure(PrivacyDenied, Identity)
	}
	return map[string]string{"authorization": "Bearer " + string(k.private.bytes)}, nil
}

func boundary(c Config, data bool) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, conn *grpc.ClientConn, invoke grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if forbiddenContext(ctx) || ctx.Err() != nil {
			return failure(Denied, Identity)
		}
		allowed := false
		if data && method == workflow.WorkflowService_DescribeNamespace_FullMethodName {
			r, ok := req.(*workflow.DescribeNamespaceRequest)
			allowed = ok && proto.Equal(r, &workflow.DescribeNamespaceRequest{Namespace: c.Namespace.NamespaceID})
		}
		if !data {
			switch method {
			case cloud.CloudService_GetCurrentIdentity_FullMethodName:
				r, ok := req.(*cloud.GetCurrentIdentityRequest)
				allowed = ok && proto.Equal(r, &cloud.GetCurrentIdentityRequest{})
			case cloud.CloudService_GetNamespace_FullMethodName:
				r, ok := req.(*cloud.GetNamespaceRequest)
				allowed = ok && proto.Equal(r, &cloud.GetNamespaceRequest{Namespace: c.Namespace.NamespaceID})
			}
		}
		if !allowed {
			return failure(Denied, Identity)
		}
		// Replace every inherited outgoing header. Only our own fixed routing/API
		// version remains; per-attempt credentials supply candidate authorization.
		md := metadata.Pairs("temporal-cloud-api-version", tc.SupportedAPIVersion)
		if data {
			md = metadata.Pairs("temporal-namespace", c.Namespace.NamespaceID)
		}
		return invoke(metadata.NewOutgoingContext(ctx, md), method, req, reply, conn, opts...)
	}
}

func newNetworkProbes(c Config, key *material, deadline time.Time) (*probes, error) {
	cloudTLS, e := endpointTLS(c.Cloud)
	if e != nil {
		return nil, e
	}
	dataTLS, e := endpointTLS(c.Data)
	if e != nil {
		return nil, e
	}
	b := &authBudget{used: map[string]int{}, key: key, deadline: deadline, now: c.Now}
	dial := func(address string, auth candidateCredentials, isData bool) (*grpc.ClientConn, error) {
		tlsConfig := cloudTLS
		if isData {
			tlsConfig = dataTLS
		}
		return grpc.NewClient(address, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)), grpc.WithPerRPCCredentials(auth), grpc.WithUnaryInterceptor(boundary(c, isData)), grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(), grpc.WithNoProxy(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1<<20), grpc.MaxRetryRPCBufferSize(0)))
	}
	cc, e := dial(c.Cloud.Address, candidateCredentials{private: &credentialState{budget: b}}, false)
	if e != nil {
		return nil, failure(Invalid, Identity)
	}
	dc, e := dial(c.Data.Address, candidateCredentials{private: &credentialState{budget: b, data: true, namespace: c.Namespace.NamespaceID}}, true)
	if e != nil {
		_ = cc.Close()
		return nil, failure(Invalid, Identity)
	}
	return &probes{cloud: cloud.NewCloudServiceClient(cc), data: workflow.NewWorkflowServiceClient(dc), close: func() { _ = dc.Close(); _ = cc.Close() }}, nil
}
