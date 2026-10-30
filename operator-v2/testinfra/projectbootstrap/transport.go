package projectbootstrap

import (
	"context"
	"crypto/tls"
	"sync/atomic"
	"time"

	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	"go.temporal.io/cloud-sdk/cloudclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const endpoint = "saas-api.tmprl.cloud:443"

// SDK DisableRetry and gRPC WithDisableRetry alone do not disable transparent
// retries. Pinned grpc v1.83.2 http2_client.go:createHeaderFields calls per-RPC
// credentials before enqueueing every attempt's HEADERS, including transparent
// attempts. The credential budget below is independent of retry buffering: a
// second authenticated CreateProject attempt is rejected before its HEADERS.
// This can consume the only budget on an unsent/REFUSED_STREAM attempt. That is
// intentionally Unknown, never an invitation to replay.
type attemptCredentials struct {
	session *session
	armed   atomic.Bool
	used    atomic.Bool
	denied  atomic.Int32
	now     func() time.Time
	// Set before the first RPC, then immutable. Do not rely solely on Go's
	// context timer firing before a late transparent attempt reaches metadata.
	deadline       time.Time
	mutationMethod string
	projectRead    bool
}

func (c *attemptCredentials) GetAPIKey(ctx context.Context) (string, error) {
	info, ok := credentials.RequestInfoFromContext(ctx)
	if !ok || ctx.Err() != nil || c.session == nil || c.session.private == nil || c.deadline.IsZero() || !c.deadline.After(c.now()) || !c.session.private.expiry.After(c.now().Add(30*time.Second)) {
		return "", Error("OAuthDeadlineExpired")
	}
	method := c.mutationMethod
	if method == "" {
		method = cloudservice.CloudService_CreateProject_FullMethodName
	}
	if info.Method == method {
		if !c.armed.Load() || !c.used.CompareAndSwap(false, true) {
			c.denied.Add(1)
			return "", Error("CreateAttemptBudgetExhausted")
		}
		return c.session.private.token, nil
	}
	switch info.Method {
	case cloudservice.CloudService_GetCurrentIdentity_FullMethodName,
		cloudservice.CloudService_GetNamespace_FullMethodName,
		cloudservice.CloudService_GetAsyncOperation_FullMethodName:
		if c.armed.Load() {
			return "", Error("BootstrapReadPhaseClosed")
		}
	case cloudservice.CloudService_GetProject_FullMethodName:
		if !c.projectRead || c.armed.Load() {
			return "", Error("BootstrapMethodForbidden")
		}
	default:
		return "", Error("BootstrapMethodForbidden")
	}
	return c.session.private.token, nil
}

func sdkClient(s *session, host string, tlsConfig *tls.Config, zeroBuffer bool) (*cloudclient.Client, *attemptCredentials, error) {
	return scopedSDKClient(s, host, tlsConfig, zeroBuffer, cloudservice.CloudService_CreateProject_FullMethodName, nil)
}

func scopedSDKClient(s *session, host string, tlsConfig *tls.Config, zeroBuffer bool, method string, validate grpc.UnaryClientInterceptor) (*cloudclient.Client, *attemptCredentials, error) {
	c := &attemptCredentials{session: s, now: time.Now, deadline: time.Now().Add(20 * time.Second)}
	c.mutationMethod = method
	c.projectRead = method == cloudservice.CloudService_CreateNamespace_FullMethodName
	dial := []grpc.DialOption{grpc.WithDisableRetry(), grpc.WithDisableServiceConfig(), grpc.WithNoProxy()}
	if zeroBuffer {
		dial = append(dial, grpc.WithDefaultCallOptions(grpc.MaxRetryRPCBufferSize(0)))
	}
	if validate != nil {
		dial = append(dial, grpc.WithChainUnaryInterceptor(validate))
	}
	client, err := cloudclient.New(cloudclient.Options{HostPort: host, APIVersion: "v0.24.0", APIKeyReader: c, DisableRetry: true, TLSConfig: tlsConfig, GRPCDialOptions: dial, UserAgent: "registry-v2-fixture-project-bootstrap"})
	if err != nil {
		return nil, nil, Error("BootstrapTransportInvalid")
	}
	return client, c, nil
}
