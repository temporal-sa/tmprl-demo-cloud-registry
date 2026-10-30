package credentialobservation

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
	workflow "go.temporal.io/api/workflowservice/v1"
	cloud "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
)

func authContext(method string, data bool, id string) context.Context {
	md := metadata.Pairs("temporal-cloud-api-version", tc.SupportedAPIVersion)
	if data {
		md = metadata.Pairs("temporal-namespace", id)
	}
	ctx := metadata.NewOutgoingContext(bg, md)
	return credentials.NewContextWithRequestInfo(ctx, credentials.RequestInfo{Method: method, AuthInfo: credentials.TLSInfo{CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity}}})
}

func TestPerAttemptCredentialsAreFiniteCandidateOnlyTLSAndOpaque(t *testing.T) {
	key := newMaterial([]byte(sentinel))
	defer key.close()
	clock := at
	b := &authBudget{used: map[string]int{}, key: &key, deadline: at.Add(time.Second), now: func() time.Time { return clock }}
	c := candidateCredentials{private: &credentialState{budget: b}}
	d := candidateCredentials{private: &credentialState{budget: b, data: true, namespace: "expected-namespace"}}
	if !c.RequireTransportSecurity() || !d.RequireTransportSecurity() {
		t.Fatal("candidate credentials allow plaintext")
	}
	for _, value := range []any{key, &key, c, &c, struct {
		Key         material
		Credentials candidateCredentials
	}{key, c}} {
		for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%p", "%#p", "%d", "%T", "%t", "%b", "%c", "%o", "%O", "%U", "%e", "%E", "%f", "%F", "%g", "%G", "%w", "%20v", "%-20v", "%020v"} {
			if strings.Contains(fmt.Sprintf(format, value), sentinel) {
				t.Fatal("live candidate holder leaked formatting")
			}
		}
		if _, e := json.Marshal(value); e == nil {
			t.Fatal("live candidate holder serialized")
		}
	}
	if _, e := key.MarshalText(); e == nil {
		t.Fatal("material text serialized")
	}
	if _, e := c.MarshalText(); e == nil {
		t.Fatal("credentials text serialized")
	}
	identity := authContext(cloud.CloudService_GetCurrentIdentity_FullMethodName, false, "")
	headers, e := c.GetRequestMetadata(identity)
	if e != nil || len(headers) != 1 || headers["authorization"] != "Bearer "+sentinel {
		t.Fatal("candidate authentication missing")
	}
	if _, e = c.GetRequestMetadata(identity); e == nil {
		t.Fatal("identity transparent attempt exceeded budget")
	}
	for i := 0; i < 2; i++ {
		if _, e = c.GetRequestMetadata(authContext(cloud.CloudService_GetNamespace_FullMethodName, false, "")); e != nil {
			t.Fatal(e)
		}
	}
	if _, e = c.GetRequestMetadata(authContext(cloud.CloudService_GetNamespace_FullMethodName, false, "")); e == nil {
		t.Fatal("namespace attempt budget exceeded")
	}
	if _, e = d.GetRequestMetadata(authContext(workflow.WorkflowService_DescribeNamespace_FullMethodName, true, "expected-namespace")); e != nil {
		t.Fatal(e)
	}
	if _, e = d.GetRequestMetadata(authContext(workflow.WorkflowService_DescribeNamespace_FullMethodName, true, "expected-namespace")); e == nil {
		t.Fatal("data attempt budget exceeded")
	}
	// Independent authority gates each get an unused budget. A prior valid
	// emission/exhaustion must not make plaintext/routing/deadline tests pass.
	fresh := func(data bool) candidateCredentials {
		return candidateCredentials{private: &credentialState{budget: &authBudget{used: map[string]int{}, key: &key, deadline: at.Add(time.Second), now: func() time.Time { return clock }}, data: data, namespace: "expected-namespace"}}
	}
	plain := credentials.NewContextWithRequestInfo(metadata.NewOutgoingContext(bg, metadata.Pairs("temporal-cloud-api-version", tc.SupportedAPIVersion)), credentials.RequestInfo{Method: cloud.CloudService_GetCurrentIdentity_FullMethodName, AuthInfo: credentials.TLSInfo{CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.NoSecurity}}})
	admin := credentials.NewContextWithRequestInfo(metadata.NewOutgoingContext(bg, metadata.Pairs("temporal-cloud-api-version", tc.SupportedAPIVersion, "authorization", "unrelated-admin")), credentials.RequestInfo{Method: cloud.CloudService_GetCurrentIdentity_FullMethodName, AuthInfo: credentials.TLSInfo{CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity}}})
	canceled, cancel := context.WithCancel(identity)
	cancel()
	for _, test := range []struct {
		name string
		data bool
		ctx  context.Context
	}{
		{"method", false, authContext(cloud.CloudService_CreateApiKey_FullMethodName, false, "")},
		{"namespace-route", true, authContext(workflow.WorkflowService_DescribeNamespace_FullMethodName, true, "wrong")},
		{"inherited-auth", false, admin}, {"plaintext", false, plain}, {"canceled", false, canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			credential := fresh(test.data)
			if len(credential.private.budget.used) != 0 {
				t.Fatal("negative fixture budget already consumed")
			}
			if _, e := credential.GetRequestMetadata(test.ctx); e == nil {
				t.Fatal("independent authority gate admitted request")
			}
		})
	}
	clock = at.Add(2 * time.Second)
	expired := fresh(true)
	if len(expired.private.budget.used) != 0 {
		t.Fatal("expiry fixture exhausted")
	}
	if _, e = expired.GetRequestMetadata(authContext(workflow.WorkflowService_DescribeNamespace_FullMethodName, true, "expected-namespace")); e == nil {
		t.Fatal("late attempt emitted credentials")
	}
	clock = at
	closed := fresh(false)
	key.close()
	if _, e = closed.GetRequestMetadata(identity); e == nil {
		t.Fatal("closed material reused")
	}

}

func TestMethodBoundaryRejectsBorrowedAuthorityAndReplacesOtherMetadata(t *testing.T) {
	f := setup(t)
	called := 0
	invoke := func(ctx context.Context, method string, req, reply any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		called++
		md, _ := metadata.FromOutgoingContext(ctx)
		if len(md) != 1 || len(md.Get("authorization")) != 0 || len(md.Get("trace-secret")) != 0 {
			t.Fatal("inherited metadata forwarded")
		}
		if method == workflow.WorkflowService_DescribeNamespace_FullMethodName {
			if got := md.Get("temporal-namespace"); len(got) != 1 || got[0] != f.config.Namespace.NamespaceID {
				t.Fatal("wrong namespace route")
			}
		} else {
			if got := md.Get("temporal-cloud-api-version"); len(got) != 1 || got[0] != tc.SupportedAPIVersion {
				t.Fatal("wrong Cloud API contract")
			}
		}
		return nil
	}
	cloudBoundary := boundary(f.config, false)
	dataBoundary := boundary(f.config, true)
	ctx := metadata.NewOutgoingContext(bg, metadata.Pairs("trace-secret", sentinel))
	if e := cloudBoundary(ctx, cloud.CloudService_GetCurrentIdentity_FullMethodName, &cloud.GetCurrentIdentityRequest{}, nil, nil, invoke); e != nil {
		t.Fatal(e)
	}
	if e := dataBoundary(ctx, workflow.WorkflowService_DescribeNamespace_FullMethodName, &workflow.DescribeNamespaceRequest{Namespace: f.config.Namespace.NamespaceID}, nil, nil, invoke); e != nil {
		t.Fatal(e)
	}
	for _, test := range []struct {
		method  string
		request any
		data    bool
		ctx     context.Context
	}{
		{cloud.CloudService_CreateApiKey_FullMethodName, &cloud.CreateApiKeyRequest{}, false, bg},
		{cloud.CloudService_GetNamespace_FullMethodName, &cloud.GetNamespaceRequest{Namespace: "foreign"}, false, bg},
		{workflow.WorkflowService_DescribeNamespace_FullMethodName, &workflow.DescribeNamespaceRequest{Namespace: f.config.Namespace.NamespaceID, WeakConsistency: true}, true, bg},
		{cloud.CloudService_GetCurrentIdentity_FullMethodName, &cloud.GetCurrentIdentityRequest{}, true, bg},
		{cloud.CloudService_GetCurrentIdentity_FullMethodName, &cloud.GetCurrentIdentityRequest{}, false, metadata.NewOutgoingContext(bg, metadata.Pairs("authorization", "borrowed-admin"))},
	} {
		if e := boundary(f.config, test.data)(test.ctx, test.method, test.request, nil, nil, invoke); e == nil {
			t.Fatal("unreviewed method/input/auth admitted")
		}
	}
	if called != 2 {
		t.Fatal("rejected calls reached transport")
	}
}

func TestConstructionHasExplicitPublicTrustAndNoAmbientFallback(t *testing.T) {
	f := setup(t)
	for _, change := range []func(*Config){
		func(c *Config) { c.Cloud.RootCAPEM = nil }, func(c *Config) { c.Data.Address = "" }, func(c *Config) { c.Cloud.ServerName = "foreign" }, func(c *Config) { c.Cloud.RootCAPEM = []byte("PRIVATE KEY " + sentinel) }, func(c *Config) { c.Timeout = 11 * time.Second }, func(c *Config) {
			c.KMSARN = "arn:aws:kms:us-east-1:123456789012:key/11111111-1111-4111-8111-111111111111"
		},
	} {
		c := f.config
		change(&c)
		if _, e := New(c, f.records, outcomeReader{f.records}, f.secrets, f.guard); e == nil {
			t.Fatal("unsafe constructor admitted")
		}
	}
	observer, e := New(f.config, f.records, outcomeReader{f.records}, f.secrets, f.guard)
	if e != nil || observer == nil {
		t.Fatal("safe lazy constructor rejected")
	}
	if f.secrets.reads != 0 || f.probes.identityCalls != 0 {
		t.Fatal("constructor read credentials or provider")
	}
	config := must(endpointTLS(f.config.Cloud))
	if config.MinVersion < tls.VersionTLS12 || config.InsecureSkipVerify || config.RootCAs == nil || config.ServerName != f.config.Cloud.ServerName {
		t.Fatal("TLS boundary weakened")
	}
	if _, e := observer.MarshalText(); e == nil {
		t.Fatal("observer serialized as text")
	}
}
