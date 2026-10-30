package sourceresolver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
)

var errTransport = errors.New("scoped source transport rejected request")
var errRemoteDenied = errors.New("approved source is not accessible")

// TransportPolicy is trusted installation configuration, not project authority.
// Exact addresses avoid ambient DNS/proxy resolution and DNS rebinding. RootCAs
// must be supplied explicitly; neither system nor user trust files are loaded.
type TransportPolicy struct {
	Owner         ownership.Owner
	Repository    string
	RootCAs       *x509.CertPool
	AllowedIPs    []netip.Addr
	Timeout       time.Duration
	AllowLoopback bool
}
type scopedTransport struct {
	transport  http.RoundTripper
	repository *url.URL
	remaining  int64
	expiresAt  time.Time
	now        func() time.Time
}

func newHTTPPolicy(policy TransportPolicy, limits Limits, expiresAt time.Time, now func() time.Time) (*http.Client, func(), error) {
	u, err := url.Parse(policy.Repository)
	if err != nil || !validRepository(policy.Repository) || u.RawPath != "" || u.ForceQuery || strings.ContainsAny(policy.Repository, "\r\n\t ") || path.Clean(strings.TrimSuffix(u.Path, "/")) != strings.TrimSuffix(u.Path, "/") || strings.Contains(u.Path, "\\") || len(policy.AllowedIPs) == 0 || len(policy.AllowedIPs) > 32 || policy.RootCAs == nil || len(policy.RootCAs.Subjects()) == 0 || policy.Timeout < time.Millisecond || policy.Timeout > time.Minute || !limits.valid() || now == nil {
		return nil, nil, invalid("SourceTransportPolicyRequired", "Source acquisition requires an exact HTTPS endpoint, explicit TLS roots and bounded address policy.")
	}
	host := u.Hostname()
	if host == "" {
		return nil, nil, errTransport
	}
	port := u.Port()
	if port == "" {
		port = "443"
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return nil, nil, errTransport
	}
	addresses := append([]netip.Addr(nil), policy.AllowedIPs...)
	for index, address := range addresses {
		address = address.Unmap()
		addresses[index] = address
		if !address.IsValid() || address.Zone() != "" || address.IsUnspecified() || address.IsMulticast() || address.IsLinkLocalUnicast() || address.IsLinkLocalMulticast() || address.IsLoopback() && (!policy.AllowLoopback || policy.Owner.Lifetime != ownership.Disposable || policy.Owner.RunID == "") {
			return nil, nil, invalid("SourceAddressDenied", "The source transport address is not approved for this installation lifetime.")
		}
	}
	expectedAddress := net.JoinHostPort(host, port)
	transport := &http.Transport{Proxy: nil, DisableCompression: true, DisableKeepAlives: true, MaxConnsPerHost: 1, MaxResponseHeaderBytes: 64 * 1024, ResponseHeaderTimeout: policy.Timeout, TLSHandshakeTimeout: policy.Timeout, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: policy.RootCAs.Clone(), ServerName: host}, TLSNextProto: map[string]func(string, *tls.Conn) http.RoundTripper{}}
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != expectedAddress || network != "tcp" {
			return nil, errTransport
		}
		dialer := net.Dialer{Timeout: policy.Timeout}
		for _, ip := range addresses {
			connection, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(ip.String(), port))
			if err == nil {
				return connection, nil
			}
			if ctx.Err() != nil {
				return nil, errTransport
			}
		}
		return nil, errTransport
	}
	scoped := &scopedTransport{transport: transport, repository: u, remaining: limits.WireBytes, expiresAt: expiresAt, now: now}
	client := &http.Client{Transport: scoped, Timeout: policy.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return errTransport }}
	return client, transport.CloseIdleConnections, nil
}
func (t *scopedTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	base := strings.TrimSuffix(t.repository.Path, "/")
	if request.URL.Scheme != "https" || request.URL.Host != t.repository.Host || request.URL.User != nil || request.URL.RawPath != "" || request.URL.Fragment != "" || request.Host != "" && request.Host != t.repository.Host || !t.expiresAt.IsZero() && !t.now().Before(t.expiresAt) {
		return nil, errTransport
	}
	limit := int64(1 << 20)
	switch {
	case request.Method == http.MethodGet && request.URL.Path == base+"/info/refs" && request.URL.RawQuery == "service=git-upload-pack":
	case request.Method == http.MethodPost && request.URL.Path == base+"/git-upload-pack" && request.URL.RawQuery == "":
		limit = atomic.LoadInt64(&t.remaining)
	default:
		return nil, errTransport
	}
	response, err := t.transport.RoundTrip(request)
	if err != nil {
		return nil, errTransport
	}
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Encoding") != "" && response.Header.Get("Content-Encoding") != "identity" {
		_ = response.Body.Close()
		if response.StatusCode == 401 || response.StatusCode == 403 || response.StatusCode == 404 {
			return nil, errRemoteDenied
		}
		return nil, errTransport
	}
	if response.ContentLength > limit {
		_ = response.Body.Close()
		return nil, errBudget
	}
	response.Body = &budgetBody{body: response.Body, remaining: limit, total: &t.remaining}
	return response, nil
}

type budgetBody struct {
	body      io.ReadCloser
	remaining int64
	total     *int64
}

func (b *budgetBody) Read(p []byte) (int, error) {
	available := b.remaining
	if global := atomic.LoadInt64(b.total); global < available {
		available = global
	}
	if available < 0 {
		return 0, errBudget
	}
	if int64(len(p)) > available+1 {
		p = p[:int(available+1)]
	}
	n, err := b.body.Read(p)
	b.remaining -= int64(n)
	atomic.AddInt64(b.total, -int64(n))
	if int64(n) > available {
		return 0, errBudget
	}
	return n, err
}
func (b *budgetBody) Close() error { return b.body.Close() }
