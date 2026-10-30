package ownership

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func config() Config {
	return Config{Owner: Owner{EnvironmentID: "development", InstallationID: "installation-a", ProjectUID: "project-uid-a", Lifetime: Stable}, ResourceRoot: "lab",
		Scopes: []Scope{
			{Provider: AWS, Partition: "aws", AccountID: "123456789012", Region: "us-west-1"},
			{Provider: Kubernetes, ClusterID: "kind-registry-lab", Namespace: "lab-control"},
			{Provider: Temporal, AccountID: "test-account", Namespace: "lab-operator"},
		}}
}

func contextFor(t *testing.T, cfg Config) Context {
	t.Helper()
	c, err := NewContext(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func requireCode(t *testing.T, err error, expected ErrorCode) {
	t.Helper()
	var typed *Error
	if !errors.As(err, &typed) || typed.Code != expected {
		t.Fatalf("got %v; want ownership code %s", err, expected)
	}
}

func TestContextRejectsIncompleteAndAmbiguousAuthority(t *testing.T) {
	cases := map[string]func(*Config){
		"no environment":              func(c *Config) { c.Owner.EnvironmentID = "" },
		"no installation":             func(c *Config) { c.Owner.InstallationID = "" },
		"no project UID":              func(c *Config) { c.Owner.ProjectUID = "" },
		"no lifetime":                 func(c *Config) { c.Owner.Lifetime = "" },
		"stable run":                  func(c *Config) { c.Owner.RunID = "ephemeral" },
		"disposable missing run":      func(c *Config) { c.Owner.Lifetime = Disposable },
		"production root":             func(c *Config) { c.ResourceRoot = "tmprl-dem-cld" },
		"nested production root":      func(c *Config) { c.ResourceRoot = "tmprl-dem-cld-dev" },
		"production ancestor root":    func(c *Config) { c.ResourceRoot = "tmprl-dem" },
		"short production ancestor":   func(c *Config) { c.ResourceRoot = "tmprl" },
		"root path traversal":         func(c *Config) { c.ResourceRoot = "lab/../prod" },
		"no scopes":                   func(c *Config) { c.Scopes = nil },
		"duplicate scope":             func(c *Config) { c.Scopes = append(c.Scopes, c.Scopes[0]) },
		"unknown provider":            func(c *Config) { c.Scopes[0].Provider = "unknown" },
		"implicit AWS partition":      func(c *Config) { c.Scopes[0].Partition = "" },
		"wrong account format":        func(c *Config) { c.Scopes[0].AccountID = "production" },
		"implicit AWS region":         func(c *Config) { c.Scopes[0].Region = "" },
		"mixed AWS scope":             func(c *Config) { c.Scopes[0].Namespace = "prod" },
		"implicit Kubernetes cluster": func(c *Config) { c.Scopes[1].ClusterID = "" },
		"implicit Temporal namespace": func(c *Config) { c.Scopes[2].Namespace = "" },
		"control character":           func(c *Config) { c.Owner.ProjectUID = "uid\n" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := config()
			mutate(&cfg)
			_, err := NewContext(cfg)
			requireCode(t, err, InvalidContext)
		})
	}
}

func TestContextCopiesCallerConfiguration(t *testing.T) {
	cfg := config()
	c := contextFor(t, cfg)
	name, _ := c.Name(AWS, "runtime-secret", "demo", 63)
	cfg.Scopes[0].AccountID = "999999999999"
	cfg.Owner.ProjectUID = "foreign"
	after, _ := c.Name(AWS, "runtime-secret", "demo", 63)
	if name != after {
		t.Fatal("caller mutation changed immutable context")
	}
	scope, _ := c.Scope(AWS)
	scope.AccountID = "999999999999"
	actual, _ := c.Scope(AWS)
	if actual.AccountID != "123456789012" {
		t.Fatal("returned scope changed context")
	}
}

func TestNamesSeparateAllAuthorityDimensionsIncludingTruncation(t *testing.T) {
	logical := strings.Repeat("same-project-", 16)
	base := config()
	root := contextFor(t, base)
	name, err := root.Name(Temporal, "namespace", logical, 39)
	if err != nil {
		t.Fatal(err)
	}
	if len(name) > 39 || !strings.HasPrefix(name, "lab-") || !slugPattern.MatchString(name) {
		t.Fatalf("invalid bounded name %q", name)
	}
	changes := map[string]func(*Config){
		"environment":        func(c *Config) { c.Owner.EnvironmentID = "another-env" },
		"installation":       func(c *Config) { c.Owner.InstallationID = "another-install" },
		"project UID":        func(c *Config) { c.Owner.ProjectUID = "same-name-new-uid" },
		"Temporal account":   func(c *Config) { c.Scopes[2].AccountID = "another-account" },
		"Temporal namespace": func(c *Config) { c.Scopes[2].Namespace = "another-control-namespace" },
		"root":               func(c *Config) { c.ResourceRoot = "dev" },
		"disposable run":     func(c *Config) { c.Owner.Lifetime = Disposable; c.Owner.RunID = "run-one" },
	}
	for description, change := range changes {
		t.Run(description, func(t *testing.T) {
			cfg := config()
			change(&cfg)
			different, err := contextFor(t, cfg).Name(Temporal, "namespace", logical, 39)
			if err != nil {
				t.Fatal(err)
			}
			if different == name {
				t.Fatalf("%s collision after truncation", description)
			}
		})
	}
	for _, args := range [][2]string{{"another-purpose", logical}, {"namespace", logical + "different-tail"}} {
		different, err := root.Name(Temporal, args[0], args[1], 39)
		if err != nil {
			t.Fatal(err)
		}
		if different == name {
			t.Fatal("purpose or truncated logical tail collided")
		}
	}
	otherProvider, err := root.Name(Kubernetes, "namespace", logical, 39)
	if err != nil {
		t.Fatal(err)
	}
	if otherProvider == name {
		t.Fatal("provider identity omitted from seed")
	}
	_, err = root.Name(Temporal, "namespace", logical, 20)
	requireCode(t, err, InvalidContext)
}

func TestDisposableRunsSeparateButStableReconcilesDoNotRename(t *testing.T) {
	c := contextFor(t, config())
	a, err := c.Intent(AWS, "repository", "demo", "revision-1", "operation-1", "spec-1", 63)
	if err != nil {
		t.Fatal(err)
	}
	retry, _ := c.Intent(AWS, "repository", "demo", "revision-1", "operation-1", "spec-1", 63)
	next, _ := c.Intent(AWS, "repository", "demo", "revision-2", "operation-2", "spec-2", 63)
	if a != retry {
		t.Fatal("retry changed creation intent or idempotency key")
	}
	if a.Name != next.Name || a.IdempotencyKey == next.IdempotencyKey {
		t.Fatal("stable resource name and operation identity are conflated")
	}
	cfg := config()
	cfg.Owner.Lifetime = Disposable
	cfg.Owner.RunID = "run-1"
	one, _ := contextFor(t, cfg).Name(AWS, "repository", "demo", 63)
	cfg.Owner.RunID = "run-2"
	two, _ := contextFor(t, cfg).Name(AWS, "repository", "demo", 63)
	if one == two {
		t.Fatal("different disposable runs share a resource")
	}
}

func TestPathIsBoundedAndDoesNotInterpretOpaqueIDsAsTraversal(t *testing.T) {
	c := contextFor(t, config())
	path, err := c.Path(AWS, "runtime-secret", "../../production")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(path, "..") || !strings.HasPrefix(path, "lab/") || len(path) > 256 {
		t.Fatalf("unsafe derived path %q", path)
	}
	intent, err := c.PathIntent(AWS, "runtime-secret", "../../production", "r1", "op1", "fp1")
	if err != nil || intent.Name != path {
		t.Fatalf("path intent %v: %v", intent, err)
	}
}

func fixture(t *testing.T) (Context, Intent, Record, Observation, Writer, time.Time) {
	t.Helper()
	c := contextFor(t, config())
	intent, err := c.Intent(AWS, "repository", "demo", "revision-1", "operation-create", "spec-sha256", 63)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	writer := Writer{ID: "writer-1", Owner: c.Owner(), Epoch: 7, ExpiresAt: now.Add(time.Minute)}
	observed := Observation{Ref: ResourceRef{Scope: intent.Scope, Purpose: intent.Purpose, Name: intent.Name, ProviderID: "arn:aws:ecr:us-west-1:123456789012:repository/" + intent.Name},
		Owner: c.Owner(), CreationIdempotencyKey: intent.IdempotencyKey, Revision: intent.Revision, SpecFingerprint: intent.SpecFingerprint, WriterEpoch: writer.Epoch, Version: "provider-version-7"}
	record, err := c.BindCreated(intent, observed, writer, now)
	if err != nil {
		t.Fatal(err)
	}
	return c, intent, record, observed, writer, now
}

func TestCreationRecoveryRequiresIndependentProvenance(t *testing.T) {
	c, intent, record, observed, writer, now := fixture(t)
	create, err := c.AuthorizeCreate(intent, writer, now)
	if err != nil || create.Intent != intent || create.WriterEpoch != writer.Epoch {
		t.Fatalf("invalid create preconditions: %+v %v", create, err)
	}
	_, err = c.AuthorizeCreate(intent, writer, writer.ExpiresAt)
	requireCode(t, err, StaleWriter)
	recovered, err := c.BindCreated(intent, observed, writer, now)
	if err != nil || recovered != record {
		t.Fatalf("stable create recovery failed: %v", err)
	}
	cases := map[string]struct {
		change func(*Intent, *Observation)
		code   ErrorCode
	}{
		"name-only adoption":      {func(_ *Intent, o *Observation) { o.CreationIdempotencyKey = "" }, UnprovenCreation},
		"wrong operation token":   {func(_ *Intent, o *Observation) { o.CreationIdempotencyKey = "another-run" }, UnprovenCreation},
		"foreign observed owner":  {func(_ *Intent, o *Observation) { o.Owner.ProjectUID = "foreign" }, ForeignOwner},
		"foreign intent":          {func(i *Intent, _ *Observation) { i.Owner.EnvironmentID = "foreign" }, ForeignOwner},
		"changed intent spec":     {func(i *Intent, _ *Observation) { i.SpecFingerprint = "different-spec" }, UnprovenCreation},
		"wrong observed revision": {func(_ *Intent, o *Observation) { o.Revision = "revision-2" }, StaleRevision},
		"wrong writer epoch":      {func(_ *Intent, o *Observation) { o.WriterEpoch = 8 }, StaleWriter},
		"missing canonical ID":    {func(_ *Intent, o *Observation) { o.Ref.ProviderID = "" }, IdentityMismatch},
	}
	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			i, o := intent, observed
			test.change(&i, &o)
			_, err := c.BindCreated(i, o, writer, now)
			requireCode(t, err, test.code)
		})
	}
}

func requestFor(record Record, observed Observation, writer Writer, now time.Time) MutationRequest {
	return MutationRequest{Action: Mutate, Record: record, Observed: observed, ExpectedRevision: record.Revision, Writer: writer, Now: now, Fencing: ProviderConditional}
}

func TestMutationReturnsRequiredProviderPreconditions(t *testing.T) {
	c, _, record, observed, writer, now := fixture(t)
	for _, action := range []Action{Mutate, Adopt, Promote, Cleanup} {
		request := requestFor(record, observed, writer, now)
		request.Action = action
		preconditions, err := c.Authorize(request)
		if err != nil {
			t.Fatalf("%s: %v", action, err)
		}
		if preconditions.Ref != record.Ref || preconditions.ProviderVersion != observed.Version || preconditions.WriterEpoch != writer.Epoch || preconditions.Revision != record.Revision {
			t.Fatalf("missing adapter preconditions: %+v", preconditions)
		}
	}
}

func TestMutationDeniesForeignIdentityStaleCleanupAndUnfencedWrites(t *testing.T) {
	c, _, record, observed, writer, now := fixture(t)
	cases := map[string]struct {
		change func(*MutationRequest)
		code   ErrorCode
	}{
		"raw ID changed": {func(r *MutationRequest) { r.Observed.Ref.ProviderID += "-recreated" }, IdentityMismatch},
		"name reused": {func(r *MutationRequest) {
			r.Observed.Ref.ProviderID = "arn:aws:ecr:us-west-1:123456789012:repository/new-object"
		}, IdentityMismatch},
		"production ARN laundered by test display name": {func(r *MutationRequest) {
			r.Record.Ref.ProviderID = "arn:aws:ecr:us-west-1:123456789012:repository/tmprl-dem-cld/production"
			r.Observed.Ref.ProviderID = r.Record.Ref.ProviderID
		}, IdentityMismatch},
		"foreign account ARN": {func(r *MutationRequest) {
			r.Observed.Ref.ProviderID = strings.Replace(r.Observed.Ref.ProviderID, "123456789012", "999999999999", 1)
		}, ForeignScope},
		"foreign region ARN": {func(r *MutationRequest) {
			r.Observed.Ref.ProviderID = strings.Replace(r.Observed.Ref.ProviderID, "us-west-1", "us-east-1", 1)
		}, ForeignScope},
		"foreign partition ARN": {func(r *MutationRequest) {
			r.Observed.Ref.ProviderID = strings.Replace(r.Observed.Ref.ProviderID, "arn:aws:", "arn:aws-cn:", 1)
		}, ForeignScope},
		"malformed canonical ARN": {func(r *MutationRequest) { r.Observed.Ref.ProviderID = r.Observed.Ref.Name }, ForeignScope},
		"global account omitted":  {func(r *MutationRequest) { r.Observed.Ref.ProviderID = "arn:aws:iam:::role/" + r.Observed.Ref.Name }, ForeignScope},
		"foreign scope": {func(r *MutationRequest) {
			r.Record.Ref.Scope.AccountID = "999999999999"
			r.Observed.Ref.Scope = r.Record.Ref.Scope
		}, ForeignScope},
		"production name alias": {func(r *MutationRequest) {
			r.Record.Ref.Name = "tmprl-dem-cld-prod"
			r.Observed.Ref.Name = r.Record.Ref.Name
		}, IdentityMismatch},
		"traversal alias": {func(r *MutationRequest) {
			r.Record.Ref.Name = "lab/../production"
			r.Observed.Ref.Name = r.Record.Ref.Name
		}, IdentityMismatch},
		"foreign observed owner":         {func(r *MutationRequest) { r.Observed.Owner.ProjectUID = "another-project" }, ForeignOwner},
		"foreign ledger owner":           {func(r *MutationRequest) { r.Record.Owner.EnvironmentID = "production" }, ForeignOwner},
		"foreign writer":                 {func(r *MutationRequest) { r.Writer.Owner.InstallationID = "another-install" }, ForeignOwner},
		"missing creation provenance":    {func(r *MutationRequest) { r.Record.CreationOperationID = "" }, UnprovenCreation},
		"changed creation provenance":    {func(r *MutationRequest) { r.Observed.CreationIdempotencyKey = "different" }, UnprovenCreation},
		"stale expected revision":        {func(r *MutationRequest) { r.ExpectedRevision = "revision-0" }, StaleRevision},
		"new observed candidate":         {func(r *MutationRequest) { r.Observed.Revision = "revision-2" }, StaleRevision},
		"changed observed spec":          {func(r *MutationRequest) { r.Observed.SpecFingerprint = "different" }, StaleRevision},
		"stale cleanup epoch":            {func(r *MutationRequest) { r.Action = Cleanup; r.Writer.Epoch = 6 }, StaleWriter},
		"changed provider epoch":         {func(r *MutationRequest) { r.Observed.WriterEpoch = 8 }, StaleWriter},
		"expired lease":                  {func(r *MutationRequest) { r.Now = r.Writer.ExpiresAt }, StaleWriter},
		"missing trusted clock":          {func(r *MutationRequest) { r.Now = time.Time{} }, StaleWriter},
		"zero epoch":                     {func(r *MutationRequest) { r.Writer.Epoch = 0 }, StaleWriter},
		"no fencing mode":                {func(r *MutationRequest) { r.Fencing = "" }, FencingRequired},
		"unsupported conditional writes": {func(r *MutationRequest) { r.Observed.Version = "" }, FencingRequired},
		"undrained external serialization": {func(r *MutationRequest) {
			r.Fencing = ExternalSerialization
			r.Serialization = SerializationAssertion{CoordinatorID: "coordinator", LeaseID: "lease", WriterEpoch: r.Writer.Epoch}
		}, FencingRequired},
	}
	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			request := requestFor(record, observed, writer, now)
			test.change(&request)
			_, err := c.Authorize(request)
			requireCode(t, err, test.code)
		})
	}
	// Even a perfectly matched record/observation from another disposable run
	// cannot authorize cleanup under this context.
	foreign := config()
	foreign.Owner.Lifetime = Disposable
	foreign.Owner.RunID = "old-run"
	other := contextFor(t, foreign)
	_, err := other.Authorize(requestFor(record, observed, writer, now))
	requireCode(t, err, ForeignOwner)
}

func TestExternalSerializationIsExplicitlyAnAssertion(t *testing.T) {
	c, _, record, observed, writer, now := fixture(t)
	request := requestFor(record, observed, writer, now)
	request.Observed.Version = ""
	request.Fencing = ExternalSerialization
	request.Serialization = SerializationAssertion{CoordinatorID: "external-coordinator", LeaseID: "externally-enforced-lease", WriterEpoch: writer.Epoch, PreviousWriterDrained: true}
	preconditions, err := c.Authorize(request)
	if err != nil {
		t.Fatal(err)
	}
	if preconditions.Fencing != ExternalSerialization || preconditions.ProviderVersion != "" || preconditions.Serialization != request.Serialization {
		t.Fatalf("serialization assertion misrepresented as provider enforcement: %+v", preconditions)
	}
}

func TestAWSCanonicalARNMustAgreeWithObservedOwnedName(t *testing.T) {
	c, intent, _, observed, writer, now := fixture(t)
	name := intent.Name
	for _, arn := range []string{
		"arn:aws:ecr:us-west-1:123456789012:repository/" + name,
		"arn:aws:ecr-public::123456789012:repository/" + name,
		"arn:aws:lambda:us-west-1:123456789012:function:" + name + ":1",
		"arn:aws:iam::123456789012:role/" + name,
		"arn:aws:secretsmanager:us-west-1:123456789012:secret:" + name + "-Abc123",
		"arn:aws:s3:::" + name,
		"arn:aws:codebuild:us-west-1:123456789012:project:" + name,
		"arn:aws:logs:us-west-1:123456789012:log-group:" + name + ":*",
		"arn:aws:kms:us-west-1:123456789012:alias/" + name,
	} {
		observed.Ref.ProviderID = arn
		if _, err := c.BindCreated(intent, observed, writer, now); err != nil {
			t.Fatalf("supported ARN %s: %v", arn, err)
		}
	}
	for _, arn := range []string{
		"arn:aws:ecr:us-west-1:123456789012:repository/tmprl-dem-cld/production",
		"arn:aws:iam::123456789012:role/tmprl-dem-cld-production",
		"arn:aws:secretsmanager:us-west-1:123456789012:secret:tmprl-dem-cld/production-Abc123",
		"arn:aws:kms:us-west-1:123456789012:key/opaque-id",
		"arn:aws:route53::123456789012:hostedzone/opaque-id",
	} {
		observed.Ref.ProviderID = arn
		_, err := c.BindCreated(intent, observed, writer, now)
		requireCode(t, err, IdentityMismatch)
	}
}

func TestScopeRejectsForeignKubernetesAndTemporalAuthority(t *testing.T) {
	for _, provider := range []Provider{Kubernetes, Temporal} {
		t.Run(string(provider), func(t *testing.T) {
			c := contextFor(t, config())
			intent, err := c.Intent(provider, "runtime", "demo", "r1", "op1", "fp1", 63)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now()
			writer := Writer{ID: "writer", Owner: c.Owner(), Epoch: 1, ExpiresAt: now.Add(time.Minute)}
			observed := Observation{Ref: ResourceRef{Scope: intent.Scope, Name: intent.Name, Purpose: intent.Purpose, ProviderID: "provider-generated-id"}, Owner: c.Owner(), CreationIdempotencyKey: intent.IdempotencyKey, Revision: intent.Revision, SpecFingerprint: intent.SpecFingerprint, WriterEpoch: 1}
			if _, err := c.BindCreated(intent, observed, writer, now); err != nil {
				t.Fatal(err)
			}
			observed.Ref.Scope.Namespace = "production"
			_, err = c.BindCreated(intent, observed, writer, now)
			requireCode(t, err, ForeignScope)
		})
	}
}
