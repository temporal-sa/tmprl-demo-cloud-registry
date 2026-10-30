package localmetadata

import (
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	awsstore "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/awssecrets"
	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
)

func retainManager(t *testing.T, c *composition, change func(*tc.CredentialConfig)) (*tc.CredentialManager, error) {
	t.Helper()
	cfg := c.backend.config
	o := cfg.Ownership
	i, e := o.Intent(ownership.Temporal, "namespace", "metadata-target", "fixture-v1", "namespace-create", "fixture-fingerprint", 39)
	if e != nil {
		t.Fatal(e)
	}
	w := c.request.Namespace.Writer
	r, e := o.BindCreated(i, ownership.Observation{Ref: ownership.ResourceRef{Scope: i.Scope, Purpose: i.Purpose, Name: i.Name, ProviderID: cfg.NamespaceIncarnationID}, Owner: o.Owner(), CreationIdempotencyKey: i.IdempotencyKey, Revision: i.Revision, SpecFingerprint: i.SpecFingerprint, WriterEpoch: 1, Version: "namespace-v1"}, w, testNow)
	if e != nil {
		t.Fatal(e)
	}
	a, e := tc.New(tc.Config{Ownership: o, Bindings: []tc.Binding{{NamespaceID: cfg.NamespaceID, CreatedAt: cfg.NamespaceCreatedAt, ProvenanceID: "fixture-create-provenance", Record: r}}, CallTimeout: time.Second, Guard: compositionNamespaceGuard{cfg}, Now: func() time.Time { return testNow }}, c.cloud)
	if e != nil {
		t.Fatal(e)
	}
	s, e := awsstore.New(awsstore.Config{Ownership: o, StoreID: cfg.StoreID, KMS: awsstore.KMSBinding{ARN: c.aws.kms, ProvenanceID: "fixture-kms-create", Owner: o.Owner()}, Namespaces: []awsstore.NamespaceBinding{{ID: cfg.NamespaceID, IncarnationID: cfg.NamespaceIncarnationID, Owner: o.Owner()}}, AllowCandidateCreate: true, CallTimeout: time.Second}, c.aws, c.backend.CredentialJournal(), c.backend.OutcomeLedger(), c.guard)
	if e != nil {
		t.Fatal(e)
	}
	config := tc.CredentialConfig{FailurePolicy: tc.DisposableRetainOnFailure, Capabilities: tc.CredentialCapabilities{StageAPIKeys: true}, MaximumKeyTTL: time.Hour, MaximumRecoveryPages: 1, StoreID: cfg.StoreID, PreprovisionedServiceAccounts: []tc.ScopedServiceAccountBinding{{NamespaceID: cfg.NamespaceID, NamespaceIncarnationID: cfg.NamespaceIncarnationID, Owner: o.Owner(), Reference: cfg.PreprovisionedServiceAccount}}}
	if change != nil {
		change(&config)
	}
	return tc.NewCredentialManager(config, a, c.cloud, c.backend.CredentialJournal(), s, c.canary)
}

func TestRetainPolicyRejectsUnsupportedOrContradictoryConfiguration(t *testing.T) {
	for name, change := range map[string]func(*tc.CredentialConfig){
		"unknown":                func(c *tc.CredentialConfig) { c.FailurePolicy = "future" },
		"overlong":               func(c *tc.CredentialConfig) { c.MaximumKeyTTL = time.Hour + time.Nanosecond },
		"create-sa":              func(c *tc.CredentialConfig) { c.Capabilities.CreateScopedServiceAccounts = true },
		"revoke":                 func(c *tc.CredentialConfig) { c.Capabilities.RevokeOwnedCandidates = true },
		"verify":                 func(c *tc.CredentialConfig) { c.Capabilities.VerifyCandidates = true },
		"missing-sa":             func(c *tc.CredentialConfig) { c.PreprovisionedServiceAccounts = nil },
		"default-without-revoke": func(c *tc.CredentialConfig) { c.FailurePolicy = "" },
	} {
		t.Run(name, func(t *testing.T) {
			c := composed(t)
			if _, e := retainManager(t, c, change); e == nil {
				t.Fatal("unsupported policy admitted")
			}
			if c.cloud.creates != 0 || c.aws.creates != 0 || c.cloud.deletes != 0 {
				t.Fatal("constructor touched provider")
			}
		})
	}
}

func TestRetainPolicyStagesButNeverVerifiesOrReinterprets(t *testing.T) {
	c := composed(t)
	m, e := retainManager(t, c, nil)
	if e != nil {
		t.Fatal(e)
	}
	c.manager = m
	c.advance(t)
	c.advance(t)
	staged := c.advance(t)
	if staged.Phase != tc.CredentialStaged || staged.Intent.FailurePolicy != tc.DisposableRetainOnFailure || c.cloud.creates != 1 || c.aws.creates != 1 {
		t.Fatal("retained mode did not stage")
	}
	c.advance(t)
	if c.canary.calls != 0 || c.cloud.deletes != 0 {
		t.Fatal("staged mode verified or revoked")
	}
	c.restart(t) // Reopens with the default compensation manager.
	_, e = c.manager.Advance(ctx, c.request)
	compositionCode(t, e, tc.CredentialAttemptConflict)
	if c.cloud.creates != 1 || c.aws.creates != 1 || c.cloud.deletes != 0 {
		t.Fatal("default mode reinterpreted retained stage")
	}
	c.privacy(t)
}

func TestRetainPolicyUnknownCreateStoreAndMetadataNeverReplayOrDelete(t *testing.T) {
	for _, mode := range []string{"key-reply", "store-reply", "key-metadata", "material-empty"} {
		t.Run(mode, func(t *testing.T) {
			c := composed(t)
			m, e := retainManager(t, c, nil)
			if e != nil {
				t.Fatal(e)
			}
			c.manager = m
			if mode == "key-reply" {
				c.cloud.lostCreate = true
			}
			if mode == "store-reply" {
				c.aws.lostCreate = true
			}
			if mode == "key-metadata" {
				c.cloud.badKeyObservation = true
			}
			if mode == "material-empty" {
				c.cloud.emptyToken = true
			}
			c.advance(t)
			c.advance(t)
			_, e = c.manager.Advance(ctx, c.request)
			if e == nil {
				t.Fatal("fault did not retain stage")
			}
			beforeKey, beforeStore := c.cloud.creates, c.aws.creates
			for j := 0; j < 2; j++ {
				_, e = c.manager.Advance(ctx, c.request)
				compositionCode(t, e, tc.CredentialRetained)
			}
			next, e := Open(c.root, c.binding, c.backend.config)
			if e != nil {
				t.Fatal(e)
			}
			c.backend = next
			c.manager, e = retainManager(t, c, nil)
			if e != nil {
				t.Fatal(e)
			}
			_, e = c.manager.Advance(ctx, c.request)
			compositionCode(t, e, tc.CredentialRetained)
			other := c.request
			other.AttemptID = "different-retained-attempt"
			_, e = c.manager.Advance(ctx, other)
			compositionCode(t, e, tc.CredentialAttemptConflict)
			if c.cloud.creates != beforeKey || c.aws.creates != beforeStore || c.cloud.deletes != 0 || c.canary.calls != 0 {
				t.Fatal("retained phase replayed or cleaned up")
			}
			c.privacy(t)
		})
	}
}
