package awssecrets

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/smithy-go/logging"
)

// Credentials are explicit activity-only inputs; no ambient chain is loaded.
type Credentials struct{ private *credentialValues }
type credentialValues struct{ access, secret, session string }

func NewCredentials(access, secret, session string) (Credentials, error) {
	if strings.TrimSpace(access) == "" || strings.TrimSpace(secret) == "" {
		return Credentials{}, fail(InvalidConfiguration)
	}
	return Credentials{private: &credentialValues{access, secret, session}}, nil
}
func (Credentials) String() string               { return "AWSCredentials{redacted}" }
func (Credentials) GoString() string             { return "AWSCredentials{redacted}" }
func (c Credentials) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, c.String()) }
func (Credentials) MarshalJSON() ([]byte, error) { return json.Marshal("redacted") }
func (c Credentials) Retrieve(context.Context) (aws.Credentials, error) {
	if c.private == nil || c.private.access == "" || c.private.secret == "" {
		return aws.Credentials{}, fail(InvalidConfiguration)
	}
	return aws.Credentials{AccessKeyID: c.private.access, SecretAccessKey: c.private.secret, SessionToken: c.private.session, Source: "explicit-registry-candidate-store"}, nil
}

type ConnectionOptions struct {
	Endpoint, Region string
	Credentials      Credentials
	TLSConfig        *tls.Config
	Timeout          time.Duration
}

var regionPattern = regexp.MustCompile(`^[a-z]{2}(?:-[a-z]+)+-[0-9]+$`)

// NewSDKClient is lazy: it neither discovers credentials nor makes a request.
// Request/response logging and SDK retries are disabled; workflow/activity
// recovery must reason about ambiguous writes rather than replay them blindly.
func NewSDKClient(options ConnectionOptions) (Client, error) {
	u, err := url.Parse(options.Endpoint)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || !regionPattern.MatchString(options.Region) || options.Credentials.private == nil || options.Credentials.private.access == "" || options.Credentials.private.secret == "" || options.Timeout <= 0 || options.Timeout > time.Minute {
		return nil, fail(InvalidConfiguration)
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if options.TLSConfig != nil {
		tlsConfig = options.TLSConfig.Clone()
		if tlsConfig.InsecureSkipVerify {
			return nil, fail(InvalidConfiguration)
		}
		if tlsConfig.MinVersion < tls.VersionTLS12 {
			tlsConfig.MinVersion = tls.VersionTLS12
		}
	}
	transport := &http.Transport{TLSClientConfig: tlsConfig, ForceAttemptHTTP2: true, TLSHandshakeTimeout: options.Timeout, ResponseHeaderTimeout: options.Timeout}
	// Explicit endpoint/credentials must not be forwarded across redirects.
	httpClient := &http.Client{Transport: transport, Timeout: options.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return secretsmanager.New(secretsmanager.Options{Region: options.Region, BaseEndpoint: aws.String(options.Endpoint), Credentials: options.Credentials, HTTPClient: httpClient, Retryer: aws.NopRetryer{}, ClientLogMode: 0, Logger: logging.Nop{}}), nil
}
