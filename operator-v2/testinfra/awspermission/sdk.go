package awspermission

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go/logging"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/testinfra/fixtureplan"
)

// No proxy/default dialer/client, HTTP redirect, endpoint override, clock-skew
// replay or SDK retry configuration can enter from callers/environment.
func makeSDKClients(material Credentials, c Config) (clients, error) {
	if material.private == nil {
		return clients{}, Invalid
	}
	transport := &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: -1}).DialContext, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}, TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{}, DisableKeepAlives: true, ForceAttemptHTTP2: false, MaxConnsPerHost: 1, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 10 * time.Second}
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return Denied }}
	guard := &sdkHTTP{material: material, config: c, client: client}
	result := sdkClientsWithGuard(material, guard)
	result.close = func() { guard.closed.Store(true); transport.CloseIdleConnections() }
	return result, nil
}

// Private fake-HTTP seam exercises the actual generated SDK without sockets.
// The public constructor always supplies its own fixed TLS transport above.
func sdkClientsWithGuard(material Credentials, guard *sdkHTTP) clients {
	secret := secretsmanager.New(secretsmanager.Options{Region: fixtureplan.Region, BaseEndpoint: aws.String("https://secretsmanager." + fixtureplan.Region + ".amazonaws.com"), Credentials: material, Retryer: aws.NopRetryer{}, HTTPClient: guard, Logger: logging.Nop{}, DisableClockSkewCorrection: true})
	stsClient := sts.New(sts.Options{Region: fixtureplan.Region, BaseEndpoint: aws.String("https://sts." + fixtureplan.Region + ".amazonaws.com"), Credentials: material, Retryer: aws.NopRetryer{}, HTTPClient: guard, Logger: logging.Nop{}, DisableClockSkewCorrection: true})
	keyClient := kms.New(kms.Options{Region: fixtureplan.Region, BaseEndpoint: aws.String("https://kms." + fixtureplan.Region + ".amazonaws.com"), Credentials: material, Retryer: aws.NopRetryer{}, HTTPClient: guard, Logger: logging.Nop{}, DisableClockSkewCorrection: true})
	return clients{secret: secret, sts: stsClient, kms: keyClient}
}

type sdkHTTP struct {
	material Credentials
	config   Config
	client   *http.Client
	closed   atomic.Bool
}

func allowedOperation(kind string, phase Phase, step Step) (service, method string) {
	if kind == "bootstrap" {
		if step == SourceIdentity {
			return "sts", "GetCallerIdentity"
		}
		if phase == Probe && step == AssumeReader {
			return "sts", "AssumeRole"
		}
		if phase == Probe && (step == BootstrapAllowMetadata || step == BootstrapDenyMetadata) {
			return "secretsmanager", "DescribeSecret"
		}
		if phase == Seed {
			switch step {
			case BootstrapAllowMetadata, BootstrapDenyMetadata:
				return "secretsmanager", "DescribeSecret"
			case SeedAllowV1, SeedAllowV2, SeedDenyV1:
				return "secretsmanager", "PutSecretValue"
			}
		}
	}
	if kind == "reader" && phase == Probe {
		switch step {
		case ReaderIdentity:
			return "sts", "GetCallerIdentity"
		case AllowMetadata:
			return "secretsmanager", "DescribeSecret"
		case ReadV1, ReadV2, ReadDeny:
			return "secretsmanager", "GetSecretValue"
		case GenerateDenyKey:
			return "kms", "GenerateDataKey"
		case AssumeTrap:
			return "sts", "AssumeRole"
		}
	}
	return "", ""
}
func (h *sdkHTTP) Do(r *http.Request) (*http.Response, error) {
	if h == nil || h.closed.Load() || r == nil || r.URL == nil || r.Context().Err() != nil || h.material.private == nil {
		return nil, Denied
	}
	p, ok := r.Context().Value(operationKey{}).(*operation)
	if !ok || p == nil || p.material != h.material.private || !p.live.Load() || !p.httpUsed.CompareAndSwap(false, true) {
		return nil, Denied
	}
	h.material.private.mu.Lock()
	kind, key, token, closed := h.material.private.kind, h.material.private.key, h.material.private.token, h.material.private.closed
	h.material.private.mu.Unlock()
	service, method := allowedOperation(kind, h.config.Phase, p.step)
	host := service + "." + fixtureplan.Region + ".amazonaws.com"
	if service == "" || closed || !h.config.Now().Before(h.config.ApprovedUntil) || r.Method != "POST" || r.URL.Scheme != "https" || r.URL.Host != host || r.Host != "" && r.Host != host || r.URL.Fragment != "" || r.URL.Opaque != "" || r.URL.Path != "/" && r.URL.Path != "" || r.URL.RawQuery != "" || r.URL.User != nil || r.Header.Get("X-Amz-Security-Token") != token || !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 Credential="+key+"/") {
		return nil, Denied
	}
	if service == "sts" {
		if r.Body == nil {
			return nil, Denied
		}
		body, e := io.ReadAll(io.LimitReader(r.Body, 1<<20+1))
		_ = r.Body.Close()
		if e != nil || len(body) > 1<<20 {
			return nil, Denied
		}
		values, e := url.ParseQuery(string(body))
		if e != nil || len(values["Action"]) != 1 || values.Get("Action") != method {
			clear(body)
			return nil, Denied
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		// This is signed request body data, not a durable/log payload. It is
		// consumed synchronously and dropped after the one HTTP call below.
		// STS bodies here contain only fixed operation/role/session metadata,
		// never credentials. Do not clear bytes while net/http may still finish
		// an early-response body write; drop the reference after request close.
	} else {
		prefix := "secretsmanager."
		if service == "kms" {
			prefix = "TrentService."
		}
		if r.Header.Get("X-Amz-Target") != prefix+method {
			return nil, Denied
		}
	}
	return h.client.Do(r)
}

func operationContext(parent context.Context, m Credentials, step Step) (context.Context, *operation) {
	p := &operation{material: m.private, step: step}
	p.live.Store(true)
	return context.WithValue(parent, operationKey{}, p), p
}
