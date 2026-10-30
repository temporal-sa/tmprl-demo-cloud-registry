package lambdawrapper

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/smithy-go/logging"
)

type SecretClient interface {
	GetSecretValue(context.Context, *secretsmanager.GetSecretValueInput, ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
}
type awsSource struct{ private *awsSourceData }
type awsSourceData struct {
	policy Policy
	client SecretClient
}

func (awsSource) String() string               { return "LambdaSecretSource{private}" }
func (a awsSource) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, a.String()) }
func (awsSource) MarshalJSON() ([]byte, error) { return json.Marshal("private") }

// NewSource accepts an explicit client and immutable policy snapshot. It never
// discovers credentials. Production clients must also meet the transport bounds
// established by DecodeBootstrap; test clients are only local evidence.
func NewSource(policy Policy, client SecretClient) (Source, error) {
	if policy.private == nil || client == nil {
		return nil, fail("WrapperInvalidSource")
	}
	return awsSource{&awsSourceData{policy, client}}, nil
}
func (a awsSource) Get(ctx context.Context, ref Ref) (Secret, error) {
	if err := CheckProcessSafety(); err != nil {
		return Secret{}, err
	}
	if ctx.Err() != nil {
		return Secret{}, fail("WrapperCancelled")
	}
	allowed := false
	for _, g := range a.private.policy.private.grants {
		if g.Ref == ref {
			allowed = true
			break
		}
	}
	if !allowed {
		return Secret{}, fail("WrapperSecretDenied")
	}
	out, err := a.private.client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: aws.String(ref.ARN), VersionId: aws.String(ref.VersionID)})
	if err != nil {
		return Secret{}, fail("WrapperSecretReadFailed")
	}
	if out == nil || aws.ToString(out.ARN) != ref.ARN || aws.ToString(out.VersionId) != ref.VersionID {
		return Secret{}, fail("WrapperSecretIdentityMismatch")
	}
	if out.SecretString != nil && len(out.SecretBinary) != 0 || out.SecretString == nil && len(out.SecretBinary) == 0 {
		return Secret{}, fail("WrapperInvalidSecret")
	}
	if out.SecretString != nil {
		return NewSecret(ref, []byte(*out.SecretString))
	}
	return NewSecret(ref, out.SecretBinary)
}

type credentials struct{ private *credentialData }
type credentialData struct{ access, secret, session string }

func (credentials) String() string               { return "LambdaBootstrapCredentials{redacted}" }
func (c credentials) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, c.String()) }
func (credentials) MarshalJSON() ([]byte, error) { return json.Marshal("redacted") }
func (c credentials) Retrieve(context.Context) (aws.Credentials, error) {
	if c.private == nil {
		return aws.Credentials{}, fail("WrapperInvalidBootstrap")
	}
	return aws.Credentials{AccessKeyID: c.private.access, SecretAccessKey: c.private.secret, SessionToken: c.private.session, Source: "explicit-lambda-bootstrap-fd"}, nil
}

type bootstrapWire struct {
	Version     string      `json:"version"`
	Scope       scopeWire   `json:"scope"`
	Grants      []grantWire `json:"grants"`
	Credentials struct {
		Access  string `json:"accessKeyID"`
		Secret  string `json:"secretAccessKey"`
		Session string `json:"sessionToken"`
	} `json:"credentials"`
}
type scopeWire struct {
	Installation string `json:"installation"`
	Project      string `json:"project"`
	Component    string `json:"component"`
	FunctionARN  string `json:"functionARN"`
	Region       string `json:"region"`
	Account      string `json:"account"`
}
type grantWire struct {
	ARN       string `json:"arn"`
	VersionID string `json:"versionID"`
	Property  string `json:"property"`
	Env       string `json:"env,omitempty"`
	Path      string `json:"path,omitempty"`
	PathEnv   string `json:"pathEnv,omitempty"`
}

// DecodeBootstrap is an explicit TRUSTED process bootstrap boundary, NOT an
// authentication or sealing protocol. FD delivery/provenance, scope ownership,
// credential lifetime and least-privilege IAM must be independently established
// by the future runtime producer. It reads no environment, files or AWS config.
// Network construction is lazy, HTTPS-only, standard aws partition only, fixed
// region host, normal certificate/hostname verification, no proxy/redirects,
// SDK retries/logging disabled, finite timeout and bounded response before SDK
// JSON/error decoding. No AWS request is made here.
func DecodeBootstrap(r io.Reader) (Policy, Source, error) {
	if err := CheckProcessSafety(); err != nil {
		return Policy{}, nil, err
	}
	if r == nil {
		return Policy{}, nil, fail("WrapperInvalidBootstrap")
	}
	data, err := io.ReadAll(io.LimitReader(r, 98305))
	if err != nil {
		return Policy{}, nil, fail("WrapperBootstrapReadFailed")
	}
	if len(data) > 98304 {
		return Policy{}, nil, fail("WrapperBootstrapLimit")
	}
	var wire bootstrapWire
	if err = decodeJSON(data, 98304, &wire); err != nil {
		return Policy{}, nil, fail("WrapperInvalidBootstrap")
	}
	if wire.Version != Version || wire.Credentials.Access == "" || wire.Credentials.Secret == "" || len(wire.Credentials.Access) > 256 || len(wire.Credentials.Secret) > 256 || len(wire.Credentials.Session) > 8192 || strings.ContainsAny(wire.Credentials.Access+wire.Credentials.Secret+wire.Credentials.Session, "\x00\r\n") {
		return Policy{}, nil, fail("WrapperInvalidBootstrap")
	}
	s := wire.Scope
	c := Config{Scope: Scope{s.Installation, s.Project, s.Component, s.FunctionARN, s.Region, s.Account}}
	for _, g := range wire.Grants {
		c.Grants = append(c.Grants, Grant{Ref: Ref{g.ARN, g.VersionID}, Property: g.Property, Env: g.Env, Path: g.Path, PathEnv: g.PathEnv})
	}
	policy, err := NewPolicy(c)
	if err != nil {
		return Policy{}, nil, err
	}
	creds := credentials{&credentialData{wire.Credentials.Access, wire.Credentials.Secret, wire.Credentials.Session}}
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 5 * time.Second, MaxResponseHeaderBytes: 16384, ForceAttemptHTTP2: true}
	client := &http.Client{Transport: boundedTransport{transport}, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	sdk := secretsmanager.New(secretsmanager.Options{Region: s.Region, BaseEndpoint: aws.String("https://secretsmanager." + s.Region + ".amazonaws.com"), Credentials: creds, HTTPClient: client, Retryer: aws.NopRetryer{}, ClientLogMode: 0, Logger: logging.Nop{}})
	source, _ := NewSource(policy, sdk)
	return policy, source, nil
}

type boundedTransport struct{ base http.RoundTripper }

func (t boundedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	out, err := t.base.RoundTrip(r)
	if err != nil {
		return nil, fail("WrapperSecretReadFailed")
	}
	if out.Body == nil {
		return nil, fail("WrapperSecretReadFailed")
	}
	// Disable SDK decoding until the COMPLETE response fits. Base64 SecretBinary
	// and worst-case escaped SecretString can exceed 64KiB; this bound is explicit.
	data, err := io.ReadAll(io.LimitReader(out.Body, 512*1024+1))
	closeErr := out.Body.Close()
	if err != nil || closeErr != nil || len(data) > 512*1024 {
		return nil, fail("WrapperSecretResponseLimit")
	}
	if inspectJSON(data, 512*1024) != nil {
		return nil, fail("WrapperInvalidSecretResponse")
	}
	out.Body = io.NopCloser(strings.NewReader(string(data)))
	return out, nil
}
