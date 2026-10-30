package credentialobservation

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	awsstore "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/awssecrets"
	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
)

var digestPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var identifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,255}$`)
var suffixPattern = regexp.MustCompile(`^[A-Za-z0-9]{6}$`)
var kmsPattern = regexp.MustCompile(`^key/([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}|mrk-[a-f0-9]{32})$`)

func hash(v any) string {
	b, e := json.Marshal(v)
	if e != nil {
		return ""
	}
	d := sha256.Sum256(b)
	return hex.EncodeToString(d[:])
}
func same(a, b any) bool { return hash(a) == hash(b) }
func text(s string) bool {
	return len(s) > 0 && len(s) <= 256 && strings.TrimSpace(s) == s && !strings.ContainsAny(s, "\r\n\x00")
}

func endpointTLS(e Endpoint) (*tls.Config, error) {
	host, port, err := net.SplitHostPort(e.Address)
	n, pe := strconv.Atoi(port)
	if err != nil || pe != nil || n < 1 || n > 65535 || host == "" || host != e.ServerName || strings.ContainsAny(host, " /\\\t\r\n@") || net.ParseIP(host) != nil || len(e.RootCAPEM) == 0 || len(e.RootCAPEM) > 262144 {
		return nil, failure(Invalid, Metadata)
	}
	pool := x509.NewCertPool()
	rest := e.RootCAPEM
	count := 0
	for len(strings.TrimSpace(string(rest))) > 0 {
		if !strings.HasPrefix(strings.TrimSpace(string(rest)), "-----BEGIN CERTIFICATE-----") {
			return nil, failure(Invalid, Metadata)
		}
		block, left := pem.Decode(rest)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return nil, failure(Invalid, Metadata)
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !cert.IsCA {
			return nil, failure(Invalid, Metadata)
		}
		pool.AddCert(cert)
		rest = left
		count++
	}
	if count == 0 {
		return nil, failure(Invalid, Metadata)
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, ServerName: e.ServerName, RootCAs: pool}, nil
}

func configured(c Config, s StateReader, o OutcomeReader, a SecretClient, g Guard, p probeFactory) (*Observer, error) {
	if s == nil || o == nil || a == nil || g == nil || p == nil || c.Ownership.Owner().Lifetime != ownership.Disposable || c.Ownership.Owner().RunID == "" || !identifier.MatchString(c.ProjectID) || !identifier.MatchString(c.StoreID) || !identifier.MatchString(c.TrustBindingID) || c.Timeout <= 0 || c.Timeout > 10*time.Second || c.ExpectedDataPlaneID != "" && !identifier.MatchString(c.ExpectedDataPlaneID) {
		return nil, failure(Invalid, Metadata)
	}
	if c.Now == nil {
		c.Now = time.Now
	}
	t, err := c.Ownership.Scope(ownership.Temporal)
	if err != nil {
		return nil, failure(Invalid, Metadata)
	}
	aScope, err := c.Ownership.Scope(ownership.AWS)
	if err != nil || !arnService(c.KMSARN, aScope, "kms") || !kmsPattern.MatchString(strings.SplitN(c.KMSARN, ":", 6)[5]) {
		return nil, failure(Invalid, Metadata)
	}
	b := c.Namespace
	r := b.Record
	if r.Owner != c.Ownership.Owner() || r.Ref.Scope != t || r.Ref.Purpose != "namespace" || b.NamespaceID != r.Ref.Name+"."+t.AccountID || b.CreatedAt.IsZero() || r.Ref.ProviderID != tc.IncarnationID(b.NamespaceID, b.CreatedAt) || !identifier.MatchString(b.ProvenanceID) || r.CreationOperationID == "" || r.CreationIdempotencyKey == "" || r.Revision == "" || r.WriterEpoch == 0 {
		return nil, failure(Invalid, Metadata)
	}
	observed := ownership.Observation{Ref: r.Ref, Owner: r.Owner, CreationIdempotencyKey: r.CreationIdempotencyKey, Revision: r.Revision, SpecFingerprint: r.SpecFingerprint, WriterEpoch: r.WriterEpoch, Version: "configuration-validation"}
	if _, err = c.Ownership.Authorize(ownership.MutationRequest{Action: ownership.Adopt, Record: r, Observed: observed, ExpectedRevision: r.Revision, Writer: ownership.Writer{ID: "configuration-validation", Owner: r.Owner, Epoch: r.WriterEpoch, ExpiresAt: c.Now().Add(time.Minute)}, Now: c.Now(), Fencing: ownership.ProviderConditional}); err != nil {
		return nil, failure(Invalid, Metadata)
	}
	if _, err = endpointTLS(c.Cloud); err != nil {
		return nil, err
	}
	if _, err = endpointTLS(c.Data); err != nil {
		return nil, err
	}
	c.Cloud.RootCAPEM = append([]byte(nil), c.Cloud.RootCAPEM...)
	c.Data.RootCAPEM = append([]byte(nil), c.Data.RootCAPEM...)
	return &Observer{private: &observerState{config: c, states: s, outcomes: o, secrets: a, guard: g, probes: p}}, nil
}

func arnService(v string, s ownership.Scope, service string) bool {
	p := strings.SplitN(v, ":", 6)
	return len(p) == 6 && p[0] == "arn" && p[1] == s.Partition && p[2] == service && p[3] == s.Region && p[4] == s.AccountID && p[5] != ""
}
func (s *observerState) validARN(arn, name string) bool {
	scope, _ := s.config.Ownership.Scope(ownership.AWS)
	if !arnService(arn, scope, "secretsmanager") {
		return false
	}
	resource := strings.SplitN(arn, ":", 6)[5]
	prefix := "secret:" + name + "-"
	return strings.HasPrefix(resource, prefix) && suffixPattern.MatchString(strings.TrimPrefix(resource, prefix))
}

func (s *observerState) validate(v tc.CredentialState, o awsstore.Outcome, ref tc.SecretVersionRef) error {
	c := s.config
	i := v.Intent
	k := v.Key
	sa := v.ServiceAccount
	if !digestPattern.MatchString(v.Version) || !digestPattern.MatchString(o.Version) || v.Phase != tc.CredentialStaged || i.FailurePolicy != tc.DisposableRetainOnFailure || i.Owner != c.Ownership.Owner() || i.NamespaceID != c.Namespace.NamespaceID || i.NamespaceIncarnationID != c.Namespace.Record.Ref.ProviderID || !identifier.MatchString(i.OperationKey) || !identifier.MatchString(i.AttemptID) || !same(v.Candidate, ref) || ref.StoreID != c.StoreID || !ref.Encrypted || !ref.Candidate || ref.Active || v.DeleteReceipt != (tc.Receipt{}) || v.FailureCode != "" || v.CleanupFailureCode != "" {
		return failure(Denied, Metadata)
	}
	if !i.ExpiresAt.After(c.Now().Add(5*time.Second)) || i.ExpiresAt.Sub(c.Now()) > time.Hour {
		return failure(Expired, Metadata)
	}
	slot := hash(struct {
		Owner       any
		Incarnation string
	}{i.Owner, i.NamespaceIncarnationID})
	stage := hash(struct {
		Slot, Operation, Attempt string
		Expiry                   time.Time
		FailurePolicy            tc.CredentialFailurePolicy `json:",omitempty"`
	}{slot, i.OperationKey, i.AttemptID, i.ExpiresAt.UTC(), i.FailurePolicy})
	saName, e1 := c.Ownership.Name(ownership.Temporal, "service-account", slot, 63)
	keyName, e2 := c.Ownership.Name(ownership.Temporal, "api-key", stage, 63)
	candidate, e3 := c.Ownership.Path(ownership.Temporal, "api-key-candidate", stage)
	write := hash(struct{ Stage, Key string }{stage, k.ID})
	if e1 != nil || e2 != nil || e3 != nil || i.SlotID != slot || i.StageID != stage || i.ServiceAccountName != saName || i.ServiceAccountMarker != "registry-sa:"+slot || i.KeyDisplayName != keyName || i.KeyMarker != "registry-key:"+stage || i.CandidateID != candidate || !identifier.MatchString(sa.ID) || sa.Name != saName || sa.CreatedAt.IsZero() || sa.CreatedAt.After(c.Now()) || !identifier.MatchString(sa.ProvenanceID) || !digestPattern.MatchString(sa.VersionDigest) || !identifier.MatchString(k.ID) || k.ServiceAccountID != sa.ID || !k.Verified || k.CreatedAt.IsZero() || k.CreatedAt.After(c.Now()) || !digestPattern.MatchString(k.VersionDigest) || k.CreationReceiptID == "" || v.KeyReceipt.ID != k.CreationReceiptID || v.KeyReceipt.NamespaceID != i.NamespaceID || v.KeyReceipt.Kind != "api-key-create" || v.KeyReceipt.OperationKey != stage || !digestPattern.MatchString(v.KeyReceipt.InputFingerprint) || v.CandidateWriteID != write || ref.WriteID != write || ref.VersionID != write || ref.Owner != i.Owner || ref.NamespaceIncarnationID != i.NamespaceIncarnationID || ref.CandidateID != candidate || ref.KeyID != k.ID || ref.ServiceAccountID != sa.ID {
		return failure(Denied, Metadata)
	}
	key := k
	key.VersionDigest = ""
	key.State = ""
	if o.Phase != awsstore.MetadataVerified || o.CreatedAt.IsZero() || !s.validARN(o.ARN, candidate) || !same(o.Write.Intent, i) || o.Write.WriteID != write || !same(o.Write.Key, key) {
		return failure(SecretUnproven, Metadata)
	}
	return nil
}

func (s *observerState) validSecret(o *secretsmanager.DescribeSecretOutput, v tc.CredentialState, receipt awsstore.Outcome) bool {
	if o == nil || aws.ToString(o.ARN) != receipt.ARN || aws.ToString(o.Name) != v.Intent.CandidateID || aws.ToString(o.KmsKeyId) != s.config.KMSARN || o.CreatedDate == nil || !o.CreatedDate.Equal(receipt.CreatedAt) || o.DeletedDate != nil || aws.ToBool(o.RotationEnabled) || o.RotationLambdaARN != nil || o.RotationRules != nil || len(o.ExternalSecretRotationMetadata) != 0 || o.ExternalSecretRotationRoleArn != nil || o.OwningService != nil || o.Type != nil || len(o.ReplicationStatus) != 0 || o.PrimaryRegion != nil {
		return false
	}
	expected := map[string]string{"registry:owner": hash(v.Intent.Owner), "registry:stage": v.Intent.StageID, "registry:namespace": hash(v.Intent.NamespaceIncarnationID), "registry:key": hash(struct{ SA, Key string }{v.ServiceAccount.ID, v.Key.ID}), "registry:write": v.CandidateWriteID}
	if len(o.Tags) != len(expected) {
		return false
	}
	seen := map[string]bool{}
	for _, t := range o.Tags {
		key := aws.ToString(t.Key)
		if seen[key] || expected[key] != aws.ToString(t.Value) {
			return false
		}
		seen[key] = true
	}
	stages := o.VersionIdsToStages[v.CandidateWriteID]
	return len(o.VersionIdsToStages) == 1 && len(stages) == 1 && stages[0] == "AWSCURRENT"
}
