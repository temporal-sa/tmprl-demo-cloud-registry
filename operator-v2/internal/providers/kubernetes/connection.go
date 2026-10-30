package kubernetes

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	appsclient "k8s.io/client-go/kubernetes/typed/apps/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	discoveryclient "k8s.io/client-go/kubernetes/typed/discovery/v1"
	"k8s.io/client-go/rest"
)

type Mode string

const LocalSerialized Mode = "local_single_process_serialized"

type Credentials struct{ private *credentialValues }
type credentialValues struct{ ca, certificate, key []byte }

func NewCredentials(ca, certificate, key []byte) (Credentials, error) {
	pool := x509.NewCertPool()
	if len(ca) == 0 || !pool.AppendCertsFromPEM(ca) {
		return Credentials{}, failure(InvalidConfiguration, nil)
	}
	if _, err := tls.X509KeyPair(certificate, key); err != nil {
		return Credentials{}, failure(InvalidConfiguration, nil)
	}
	return Credentials{private: &credentialValues{append([]byte(nil), ca...), append([]byte(nil), certificate...), append([]byte(nil), key...)}}, nil
}
func (Credentials) String() string               { return "KubernetesCredentials{redacted}" }
func (Credentials) GoString() string             { return "KubernetesCredentials{redacted}" }
func (c Credentials) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, c.String()) }
func (Credentials) MarshalJSON() ([]byte, error) { return json.Marshal("redacted") }

type ClusterIdentity struct {
	ID            string
	KubeSystemUID string
}
type Connection struct {
	Mode        Mode
	Endpoint    string
	Identity    ClusterIdentity
	Credentials Credentials
	CallTimeout time.Duration
}

// Client has no exported REST config/client: code cannot accidentally fall back
// to kubeconfig, exec authentication, ambient proxy settings or another scope.
type Client struct {
	core      coreclient.CoreV1Interface
	apps      appsclient.AppsV1Interface
	discovery discoveryclient.DiscoveryV1Interface
	identity  ClusterIdentity
	timeout   time.Duration
	lifecycle localLifecycle
}

func New(ctx context.Context, options Connection) (*Client, error) {
	if options.Mode != LocalSerialized {
		return nil, failure(UnsupportedMode, nil)
	}
	endpoint, err := url.Parse(options.Endpoint)
	if err != nil {
		return nil, failure(InvalidConfiguration, nil)
	}
	port, portError := strconv.Atoi(endpoint.Port())
	if portError != nil || port < 1 || port > 65535 || endpoint.Scheme != "https" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || endpoint.Path != "" && endpoint.Path != "/" || net.ParseIP(endpoint.Hostname()) == nil || strings.TrimSpace(options.Identity.ID) == "" || strings.TrimSpace(options.Identity.ID) != options.Identity.ID || options.Identity.KubeSystemUID == "" || strings.TrimSpace(options.Identity.KubeSystemUID) != options.Identity.KubeSystemUID || options.CallTimeout <= 0 || options.CallTimeout > 30*time.Second || options.Credentials.private == nil || len(options.Credentials.private.ca) == 0 || len(options.Credentials.private.certificate) == 0 || len(options.Credentials.private.key) == 0 {
		return nil, failure(InvalidConfiguration, nil)
	}
	config := &rest.Config{Host: options.Endpoint, Timeout: options.CallTimeout, UserAgent: "registry-operator-v2-local", TLSClientConfig: rest.TLSClientConfig{CAData: append([]byte(nil), options.Credentials.private.ca...), CertData: append([]byte(nil), options.Credentials.private.certificate...), KeyData: append([]byte(nil), options.Credentials.private.key...)}, Proxy: func(*http.Request) (*url.URL, error) { return nil, nil }}
	core, err := coreclient.NewForConfig(config)
	if err != nil {
		return nil, failure(InvalidConfiguration, nil)
	}
	apps, err := appsclient.NewForConfig(config)
	if err != nil {
		return nil, failure(InvalidConfiguration, nil)
	}
	discovery, err := discoveryclient.NewForConfig(config)
	if err != nil {
		return nil, failure(InvalidConfiguration, nil)
	}
	client := &Client{core: core, apps: apps, discovery: discovery, identity: options.Identity, timeout: options.CallTimeout}
	if err := client.verifyCluster(ctx); err != nil {
		return nil, err
	}
	return client, nil
}

func (c *Client) verifyCluster(ctx context.Context) error {
	if c == nil || c.core == nil || c.timeout <= 0 {
		return failure(InvalidConfiguration, nil)
	}
	request, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	namespace, err := c.core.Namespaces().Get(request, "kube-system", metav1.GetOptions{})
	if err != nil {
		return failure(ClusterMismatch, err)
	}
	if string(namespace.UID) != c.identity.KubeSystemUID || namespace.DeletionTimestamp != nil {
		return failure(ClusterMismatch, nil)
	}
	return nil
}
