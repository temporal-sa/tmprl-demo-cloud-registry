package lambdawrapper

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
)

type clientFunc func(context.Context, *secretsmanager.GetSecretValueInput, ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)

func (f clientFunc) GetSecretValue(c context.Context, in *secretsmanager.GetSecretValueInput, opts ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	return f(c, in, opts...)
}
func TestAWSAdapterExactScopeIdentityAndSafeErrors(t *testing.T) {
	policy := testPolicy(t, Grant{Ref: testRef, Property: "key", Env: "APP_TOKEN"})
	calls := 0
	client := clientFunc(func(_ context.Context, in *secretsmanager.GetSecretValueInput, _ ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
		calls++
		if aws.ToString(in.SecretId) != testRef.ARN || aws.ToString(in.VersionId) != testRef.VersionID || in.VersionStage != nil {
			t.Fatal("unpinned secret read")
		}
		return &secretsmanager.GetSecretValueOutput{ARN: aws.String(testRef.ARN), VersionId: aws.String(testRef.VersionID), SecretString: aws.String(`{"key":"synthetic"}`)}, nil
	})
	source, e := NewSource(policy, client)
	if e != nil {
		t.Fatal(e)
	}
	ref := testRef
	ref.VersionID = strings.Repeat("0", 32)
	if _, err := source.Get(context.Background(), ref); err == nil || calls != 0 {
		t.Fatal("foreign version fetched")
	}
	if _, err := source.Get(context.Background(), testRef); err != nil || calls != 1 {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		out  *secretsmanager.GetSecretValueOutput
		err  error
	}{
		{"raw-error", nil, errors.New("PRIVATE_PROVIDER_RESPONSE")},
		{"nil", nil, nil},
		{"wrong-version", &secretsmanager.GetSecretValueOutput{ARN: aws.String(testRef.ARN), VersionId: aws.String(ref.VersionID), SecretString: aws.String("x")}, nil},
		{"wrong-arn", &secretsmanager.GetSecretValueOutput{ARN: aws.String(testRef.ARN + "foreign"), VersionId: aws.String(testRef.VersionID), SecretString: aws.String("x")}, nil},
		{"both", &secretsmanager.GetSecretValueOutput{ARN: aws.String(testRef.ARN), VersionId: aws.String(testRef.VersionID), SecretString: aws.String("x"), SecretBinary: []byte("y")}, nil},
		{"oversize", &secretsmanager.GetSecretValueOutput{ARN: aws.String(testRef.ARN), VersionId: aws.String(testRef.VersionID), SecretString: aws.String(strings.Repeat("s", 65537))}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := NewSource(policy, clientFunc(func(context.Context, *secretsmanager.GetSecretValueInput, ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
				return tc.out, tc.err
			}))
			_, err := s.Get(context.Background(), testRef)
			if err == nil || strings.Contains(err.Error(), "PRIVATE_") {
				t.Fatal("unsafe response accepted")
			}
		})
	}
}

func bootstrapJSON() string {
	return `{"version":"` + Version + `","scope":{"installation":"installation","project":"project","component":"component","functionARN":"arn:aws:lambda:us-west-2:123456789012:function:test","region":"us-west-2","account":"123456789012"},"grants":[{"arn":"` + testRef.ARN + `","versionID":"` + testRef.VersionID + `","property":"key","env":"APP_TOKEN"}],"credentials":{"accessKeyID":"SYNTHETIC_ACCESS","secretAccessKey":"SYNTHETIC_PRIVATE","sessionToken":"SYNTHETIC_SESSION"}}`
}

func TestExplicitBootstrapConstructsOnlyBoundedClient(t *testing.T) {
	policy, source, err := DecodeBootstrap(strings.NewReader(bootstrapJSON()))
	if err != nil || policy.private == nil || source == nil {
		t.Fatal(err)
	}
	// Inspect the lazy SDK/client without issuing a request. No environment chain,
	// user endpoint, logger, proxy, redirects or automatic retry is introduced.
	opts := source.(awsSource).private.client.(*secretsmanager.Client).Options()
	if opts.Region != "us-west-2" || aws.ToString(opts.BaseEndpoint) != "https://secretsmanager.us-west-2.amazonaws.com" || opts.ClientLogMode != 0 || opts.Retryer.MaxAttempts() != 1 {
		t.Fatal("unsafe SDK configuration")
	}
	client := opts.HTTPClient.(*http.Client)
	transport := client.Transport.(boundedTransport).base.(*http.Transport)
	if transport.Proxy != nil || transport.TLSClientConfig.InsecureSkipVerify || client.Timeout == 0 || transport.MaxResponseHeaderBytes != 16384 {
		t.Fatal("unsafe transport")
	}
	if client.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
		t.Fatal("redirect permitted")
	}
	credential, err := opts.Credentials.Retrieve(context.Background())
	if err != nil || credential.Source != "explicit-lambda-bootstrap-fd" {
		t.Fatal("ambient credential provider")
	}
	for _, data := range []string{"null", `{}`, strings.Replace(bootstrapJSON(), `"version":`, `"Version":`, 1), strings.Replace(bootstrapJSON(), `"version":`, `"version":"duplicate","version":`, 1), strings.Replace(bootstrapJSON(), `"region":"us-west-2"`, `"region":"https://attacker"`, 1), strings.Repeat(" ", 98305)} {
		_, _, err := DecodeBootstrap(strings.NewReader(data))
		if err == nil || strings.Contains(err.Error(), "SYNTHETIC_") {
			t.Fatal("invalid bootstrap accepted or leaked")
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type countedBody struct {
	io.Reader
	read   int
	closed bool
}

func (b *countedBody) Read(p []byte) (int, error) { n, e := b.Reader.Read(p); b.read += n; return n, e }
func (b *countedBody) Close() error               { b.closed = true; return nil }
func TestResponseBoundsPrecedeSDKDecode(t *testing.T) {
	for _, data := range []string{strings.Repeat("x", 1024*1024), `{"SecretString":"one","SecretString":"two"}`, `{"SecretString":{`, "null trailing"} {
		body := &countedBody{Reader: strings.NewReader(data)}
		transport := boundedTransport{roundTripFunc(func(*http.Request) (*http.Response, error) { return &http.Response{StatusCode: 200, Body: body}, nil })}
		_, err := transport.RoundTrip(&http.Request{})
		if err == nil || !body.closed || body.read > 512*1024+1 {
			t.Fatal("response not bounded before decoder")
		}
	}
	body := &countedBody{Reader: strings.NewReader(`{"SecretString":"synthetic"}`)}
	transport := boundedTransport{roundTripFunc(func(*http.Request) (*http.Response, error) { return &http.Response{StatusCode: 200, Body: body}, nil })}
	out, err := transport.RoundTrip(&http.Request{})
	if err != nil || !body.closed {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(out.Body)
	if string(data) != `{"SecretString":"synthetic"}` {
		t.Fatal("response changed")
	}
}

type readerFunc func([]byte) (int, error)

func (f readerFunc) Read(p []byte) (int, error) { return f(p) }
func TestProcessTracingDeniedBeforeBootstrapOrSource(t *testing.T) {
	policy := testPolicy(t, Grant{Ref: testRef, Property: "key", Env: "APP_TOKEN"})
	calls := 0
	source, _ := NewSource(policy, clientFunc(func(context.Context, *secretsmanager.GetSecretValueInput, ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
		calls++
		return nil, errors.New("must not reach provider")
	}))
	for _, key := range []string{"GODEBUG", "GOTRACEBACK", "SSLKEYLOGFILE", "GRPC_BINARY_LOG_FILTER", "AWS_SDK_GO_LOG_MODE"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, "http2debug=2")
			read := false
			_, _, err := DecodeBootstrap(readerFunc(func([]byte) (int, error) { read = true; return 0, io.EOF }))
			if err == nil || err.Error() != "WrapperUnsafeProcessSettings" || read {
				t.Fatal("tracing blocked after bootstrap read")
			}
			if _, err := source.Get(context.Background(), testRef); err == nil || calls != 0 {
				t.Fatal("tracing reached secret source")
			}
			if _, err := NewInput([]string{key + "=trace"}, []string{"/entry"}); err == nil {
				t.Fatal("child tracing setting accepted")
			}
		})
	}
}
