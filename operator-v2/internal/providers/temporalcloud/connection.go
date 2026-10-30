package temporalcloud

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"

	"go.temporal.io/cloud-sdk/cloudclient"
)

const SupportedAPIVersion = "v0.24.0" // cloud-sdk v0.21.0's explicit API contract.

// Credentials never consult environment variables, files or AWS secret stores.
type Credentials struct{ private *credentialValue }
type credentialValue struct{ key string }

func NewCredentials(apiKey string) (Credentials, error) {
	if strings.TrimSpace(apiKey) == "" {
		return Credentials{}, fail(InvalidConfiguration)
	}
	return Credentials{private: &credentialValue{key: apiKey}}, nil
}
func (Credentials) String() string               { return "TemporalCloudCredentials{redacted}" }
func (Credentials) GoString() string             { return "TemporalCloudCredentials{redacted}" }
func (c Credentials) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, c.String()) }
func (Credentials) MarshalJSON() ([]byte, error) { return json.Marshal("redacted") }

type ConnectionOptions struct {
	Endpoint    string
	APIVersion  string
	Credentials Credentials
	TLSConfig   *tls.Config
}

// NewSDKTransport constructs a lazy TLS client. There is no default endpoint,
// API version, credential lookup, insecure option or nested SDK retry policy.
// Tests inject Transport directly; constructing this client makes no RPC.
func NewSDKTransport(options ConnectionOptions) (Transport, func() error, error) {
	host, port, err := net.SplitHostPort(options.Endpoint)
	portNumber, parseErr := strconv.Atoi(port)
	if err != nil || parseErr != nil || host == "" || strings.ContainsAny(host, " /\t\r\n@") || portNumber < 1 || portNumber > 65535 || options.APIVersion != SupportedAPIVersion || options.Credentials.private == nil || options.Credentials.private.key == "" {
		return nil, nil, fail(InvalidConfiguration)
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if options.TLSConfig != nil {
		tlsConfig = options.TLSConfig.Clone()
		if tlsConfig.InsecureSkipVerify {
			return nil, nil, fail(InvalidConfiguration)
		}
		if tlsConfig.MinVersion < tls.VersionTLS12 {
			tlsConfig.MinVersion = tls.VersionTLS12
		}
	}
	client, err := cloudclient.New(cloudclient.Options{HostPort: options.Endpoint, APIVersion: options.APIVersion, APIKey: options.Credentials.private.key, TLSConfig: tlsConfig, DisableRetry: true, UserAgent: "registry-operator-v2"})
	if err != nil {
		return nil, nil, fail(InvalidConfiguration)
	}
	return client.CloudService(), client.Close, nil
}
