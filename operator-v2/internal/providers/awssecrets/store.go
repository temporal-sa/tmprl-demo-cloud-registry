package awssecrets

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
)

type Store struct {
	config     Config
	scope      ownership.Scope
	namespaces map[string]NamespaceBinding
	client     Client
	journal    tc.CredentialJournal
	ledger     OutcomeLedger
	guard      CandidateGuard
}

var digestPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)
var kmsResourcePattern = regexp.MustCompile(`^key/(?:[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}|mrk-[a-f0-9]{32})$`)
var arnSuffixPattern = regexp.MustCompile(`^[a-zA-Z0-9]{6}$`)

func New(config Config, client Client, journal tc.CredentialJournal, ledger OutcomeLedger, guard CandidateGuard) (*Store, error) {
	scope, err := config.Ownership.Scope(ownership.AWS)
	if err != nil || client == nil || journal == nil || ledger == nil || guard == nil {
		return nil, fail(InvalidConfiguration)
	}
	if config.StoreID == "" || config.CallTimeout <= 0 || config.CallTimeout > time.Minute {
		return nil, fail(InvalidConfiguration)
	}
	if config.KMS.Owner != config.Ownership.Owner() || config.KMS.ProvenanceID == "" {
		return nil, fail(InvalidConfiguration)
	}
	if !scopedARN(config.KMS.ARN, scope, "kms") || !kmsResourcePattern.MatchString(strings.SplitN(config.KMS.ARN, ":", 6)[5]) {
		return nil, fail(InvalidConfiguration)
	}
	if _, err = config.Ownership.Scope(ownership.Temporal); err != nil {
		return nil, fail(InvalidConfiguration)
	}
	s := &Store{config: config, scope: scope, namespaces: map[string]NamespaceBinding{}, client: client, journal: journal, ledger: ledger, guard: guard}
	s.config.Namespaces = nil
	for _, binding := range config.Namespaces {
		if binding.Owner != config.Ownership.Owner() || binding.ID == "" || binding.IncarnationID == "" {
			return nil, fail(InvalidConfiguration)
		}
		if _, exists := s.namespaces[binding.ID]; exists {
			return nil, fail(InvalidConfiguration)
		}
		s.namespaces[binding.ID] = binding
	}
	if len(s.namespaces) == 0 {
		return nil, fail(InvalidConfiguration)
	}
	return s, nil
}

func scopedARN(value string, scope ownership.Scope, service string) bool {
	p := strings.SplitN(value, ":", 6)
	return len(p) == 6 && p[0] == "arn" && p[1] == scope.Partition && p[2] == service && p[3] == scope.Region && p[4] == scope.AccountID && p[5] != ""
}
func (s *Store) validARN(value, name string) bool {
	if !scopedARN(value, s.scope, "secretsmanager") {
		return false
	}
	resource := strings.SplitN(value, ":", 6)[5]
	prefix := "secret:" + name + "-"
	return strings.HasPrefix(resource, prefix) && arnSuffixPattern.MatchString(strings.TrimPrefix(resource, prefix))
}
func hash(value any) string {
	b, _ := json.Marshal(value)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func equal(a, b any) bool {
	aBytes, _ := json.Marshal(a)
	bBytes, _ := json.Marshal(b)
	return string(aBytes) == string(bBytes)
}

// validate obtains independent safe journal evidence inside the guard. Caller
// names, token IDs, tags or project annotations cannot grant write authority.
func (s *Store) validate(ctx context.Context, write tc.CandidateWrite, creating bool) error {
	intent := write.Intent
	binding, ok := s.namespaces[intent.NamespaceID]
	name, err := s.config.Ownership.Path(ownership.Temporal, "api-key-candidate", intent.StageID)
	if err != nil || !ok || intent.Owner != s.config.Ownership.Owner() || intent.NamespaceIncarnationID != binding.IncarnationID {
		return fail(InvalidWrite)
	}
	if !digestPattern.MatchString(intent.StageID) || !digestPattern.MatchString(intent.SlotID) || intent.CandidateID != name {
		return fail(InvalidWrite)
	}
	if write.WriteID != hash(struct{ Stage, Key string }{intent.StageID, write.Key.ID}) || !write.Key.Verified {
		return fail(InvalidWrite)
	}
	if write.Key.ID == "" || write.Key.ServiceAccountID == "" || write.Key.CreatedAt.IsZero() || write.Key.CreationReceiptID == "" {
		return fail(InvalidWrite)
	}
	state, found, err := s.journal.Load(ctx, intent.SlotID)
	if err != nil {
		return fail(JournalUnknown)
	}
	if !found || state.Version == "" || !equal(state.Intent, intent) || state.CandidateWriteID != write.WriteID {
		return fail(AuthorizationDenied)
	}
	if state.ServiceAccount.ID != write.Key.ServiceAccountID || state.ServiceAccount.CreatedAt.IsZero() || state.ServiceAccount.ProvenanceID == "" {
		return fail(AuthorizationDenied)
	}
	if !state.Key.Verified || state.Key.ID != write.Key.ID || state.Key.ServiceAccountID != write.Key.ServiceAccountID {
		return fail(AuthorizationDenied)
	}
	if !state.Key.CreatedAt.Equal(write.Key.CreatedAt) || state.Key.CreationReceiptID != write.Key.CreationReceiptID || state.KeyReceipt.ID != write.Key.CreationReceiptID {
		return fail(AuthorizationDenied)
	}
	if creating {
		if !s.config.AllowCandidateCreate {
			return fail(CapabilityDisabled)
		}
		if state.Phase != tc.CredentialStoreRequested {
			return fail(AuthorizationDenied)
		}
	} else {
		switch state.Phase {
		case tc.CredentialStoreRequested, tc.CredentialStaged, tc.CredentialVerified, tc.CredentialCompensating, tc.CredentialRevocationPending, tc.CredentialRevoked:
		default:
			return fail(AuthorizationDenied)
		}
	}
	return nil
}

func (s *Store) guarded(ctx context.Context, write tc.CandidateWrite, creating bool, fn func(context.Context) error) error {
	calls := 0
	var callbackErr error
	err := s.guard.Run(ctx, write, func(call context.Context) error {
		calls++
		if calls != 1 {
			return fail(AuthorizationDenied)
		}
		bounded, cancel := context.WithTimeout(call, s.config.CallTimeout)
		defer cancel()
		callbackErr = s.validate(bounded, write, creating)
		if callbackErr == nil {
			callbackErr = fn(bounded)
		}
		return callbackErr
	})
	// Only our callback can supply a typed store diagnostic. A guard's raw or
	// fabricated typed errors must not copy arbitrary strings into safe results.
	if calls != 1 {
		return fail(AuthorizationDenied)
	}
	if callbackErr != nil {
		return callbackErr
	}
	if err != nil {
		return fail(AuthorizationDenied)
	}
	return nil
}

func (s *Store) load(ctx context.Context, write tc.CandidateWrite) (Outcome, bool, error) {
	entry, found, err := s.ledger.Load(ctx, write.WriteID)
	if err != nil {
		return Outcome{}, false, fail(JournalUnknown)
	}
	if !found {
		return entry, false, nil
	}
	if entry.Version == "" || !sameWrite(entry.Write, write) {
		return Outcome{}, false, fail(OwnershipUnproven)
	}
	switch entry.Phase {
	case CreateRequested:
		if entry.ARN != "" || !entry.CreatedAt.IsZero() {
			return Outcome{}, false, fail(OwnershipUnproven)
		}
	case CreateReceipted, MetadataVerified:
		if !s.validARN(entry.ARN, write.Intent.CandidateID) {
			return Outcome{}, false, fail(OwnershipUnproven)
		}
		if entry.Phase == MetadataVerified && entry.CreatedAt.IsZero() {
			return Outcome{}, false, fail(OwnershipUnproven)
		}
	default:
		return Outcome{}, false, fail(OwnershipUnproven)
	}
	return entry, found, nil
}

// Key state/version digest can legitimately evolve after staging. The birth,
// identity and create receipt remain immutable and are always checked instead.
func sameWrite(a, b tc.CandidateWrite) bool {
	a.Key.VersionDigest = ""
	b.Key.VersionDigest = ""
	a.Key.State = ""
	b.Key.State = ""
	return equal(a, b)
}
func (s *Store) save(ctx context.Context, previous, next Outcome) (Outcome, error) {
	next.Version = ""
	// These mutable caller echoes grant no authority and are not durable store
	// provenance. Retain only the verified immutable key birth/create identity.
	next.Write.Key.VersionDigest = ""
	next.Write.Key.State = ""
	saved, err := s.ledger.CompareAndSwap(ctx, next.Write.WriteID, previous.Version, next)
	copy := saved
	copy.Version = ""
	if err != nil || saved.Version == "" || !equal(copy, next) {
		return Outcome{}, fail(JournalUnknown)
	}
	return saved, nil
}

func (s *Store) PutCandidate(ctx context.Context, write tc.CandidateWrite, material tc.KeyMaterial) (tc.SecretVersionRef, error) {
	return s.put(ctx, write, material.Use)
}

// put accepts a synchronous material consumer so the SDK payload lives only
// within the secure-store call. It is unexported; callers use KeyMaterial.
func (s *Store) put(ctx context.Context, write tc.CandidateWrite, consume func(func([]byte) error) error) (tc.SecretVersionRef, error) {
	var ref tc.SecretVersionRef
	err := s.guarded(ctx, write, true, func(call context.Context) error {
		entry, found, err := s.load(call, write)
		if err != nil {
			return err
		}
		if found {
			ref, err = s.resolve(call, write, entry)
			return err
		}
		// A successful lookup, even with perfectly matching metadata, is a
		// collision without independently persisted create provenance.
		_, err = s.client.DescribeSecret(call, &secretsmanager.DescribeSecretInput{SecretId: aws.String(write.Intent.CandidateID)})
		var missing *types.ResourceNotFoundException
		if !errors.As(err, &missing) {
			if err == nil {
				return fail(OwnershipUnproven)
			}
			return fail(OutcomeUnknown)
		}
		entry, err = s.save(call, Outcome{}, Outcome{Write: write, Phase: CreateRequested})
		if err != nil {
			return err
		}
		var response *secretsmanager.CreateSecretOutput
		err = consume(func(material []byte) error {
			if len(material) == 0 || len(material) > 65536 {
				return fail(InvalidWrite)
			}
			// SecretBinary avoids another immutable Go string copy. SDK owns a
			// temporary byte copy only for the synchronous call; it is cleared.
			payload := append([]byte(nil), material...)
			defer clear(payload)
			input := &secretsmanager.CreateSecretInput{Name: aws.String(write.Intent.CandidateID), ClientRequestToken: aws.String(write.WriteID), KmsKeyId: aws.String(s.config.KMS.ARN), Description: aws.String("Registry isolated credential candidate"), Tags: s.tags(write), SecretBinary: payload}
			defer func() { input.SecretBinary = nil }()
			var createErr error
			response, createErr = s.client.CreateSecret(call, input)
			if createErr != nil {
				return fail(OutcomeUnknown)
			}
			return nil
		})
		if err != nil {
			return fail(OutcomeUnknown)
		}
		if response == nil || aws.ToString(response.Name) != write.Intent.CandidateID || aws.ToString(response.VersionId) != write.WriteID || !s.validARN(aws.ToString(response.ARN), write.Intent.CandidateID) {
			return fail(OwnershipUnproven)
		}
		next := entry
		next.Phase = CreateReceipted
		next.ARN = aws.ToString(response.ARN)
		entry, err = s.save(call, entry, next)
		if err != nil {
			return err
		}
		ref, err = s.resolve(call, write, entry)
		return err
	})
	if err != nil {
		return tc.SecretVersionRef{}, err
	}
	return ref, nil
}

func (s *Store) ResolveWrite(ctx context.Context, write tc.CandidateWrite) (tc.CandidateResolution, error) {
	result := tc.CandidateResolution{Outcome: tc.WriteUnknown}
	err := s.guarded(ctx, write, false, func(call context.Context) error {
		entry, found, err := s.load(call, write)
		if err != nil {
			return err
		}
		if !found || entry.Phase == CreateRequested {
			return nil
		}
		ref, err := s.resolve(call, write, entry)
		if err != nil {
			return err
		}
		result = tc.CandidateResolution{Outcome: tc.WriteCommitted, Reference: ref}
		return nil
	})
	return result, err
}

func (s *Store) resolve(ctx context.Context, write tc.CandidateWrite, entry Outcome) (tc.SecretVersionRef, error) {
	if entry.Phase == CreateRequested {
		return tc.SecretVersionRef{}, fail(OutcomeUnknown)
	}
	actual, err := s.client.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretId: aws.String(entry.ARN)})
	if err != nil {
		return tc.SecretVersionRef{}, fail(OutcomeUnknown)
	}
	if !s.validMetadata(actual, write, entry) {
		return tc.SecretVersionRef{}, fail(MetadataConflict)
	}

	// AWSCURRENT here is the initial version of a distinct candidate identity;
	// it is NOT the runtime active reference, which the guard must exclude.
	if entry.Phase != MetadataVerified {
		next := entry
		next.Phase = MetadataVerified
		next.CreatedAt = *actual.CreatedDate
		if _, err = s.save(ctx, entry, next); err != nil {
			return tc.SecretVersionRef{}, err
		}
	}
	return tc.SecretVersionRef{StoreID: s.config.StoreID, CandidateID: write.Intent.CandidateID, VersionID: write.WriteID, WriteID: write.WriteID, Owner: write.Intent.Owner, NamespaceIncarnationID: write.Intent.NamespaceIncarnationID, ServiceAccountID: write.Key.ServiceAccountID, KeyID: write.Key.ID, Encrypted: true, Candidate: true, Active: false}, nil
}

func (s *Store) validMetadata(actual *secretsmanager.DescribeSecretOutput, write tc.CandidateWrite, entry Outcome) bool {
	if actual == nil || aws.ToString(actual.ARN) != entry.ARN || aws.ToString(actual.Name) != write.Intent.CandidateID {
		return false
	}
	if aws.ToString(actual.KmsKeyId) != s.config.KMS.ARN || actual.CreatedDate == nil || actual.CreatedDate.IsZero() {
		return false
	}
	if !entry.CreatedAt.IsZero() && !entry.CreatedAt.Equal(*actual.CreatedDate) {
		return false
	}
	if actual.DeletedDate != nil || aws.ToBool(actual.RotationEnabled) || actual.RotationLambdaARN != nil || actual.RotationRules != nil {
		return false
	}
	if len(actual.ExternalSecretRotationMetadata) != 0 || actual.ExternalSecretRotationRoleArn != nil || actual.OwningService != nil || actual.Type != nil {
		return false
	}
	if len(actual.ReplicationStatus) != 0 || actual.PrimaryRegion != nil || !sameTags(actual.Tags, s.tags(write)) {
		return false
	}
	// Describe lists labelled versions, not every historical unlabelled version.
	// The immutable expected version must be the sole labelled current version.
	stages := actual.VersionIdsToStages[write.WriteID]
	return len(actual.VersionIdsToStages) == 1 && len(stages) == 1 && stages[0] == "AWSCURRENT"
}

func (s *Store) tags(write tc.CandidateWrite) []types.Tag {
	return []types.Tag{
		{Key: aws.String("registry:owner"), Value: aws.String(hash(write.Intent.Owner))},
		{Key: aws.String("registry:stage"), Value: aws.String(write.Intent.StageID)},
		{Key: aws.String("registry:namespace"), Value: aws.String(hash(write.Intent.NamespaceIncarnationID))},
		{Key: aws.String("registry:key"), Value: aws.String(hash(struct{ SA, Key string }{write.Key.ServiceAccountID, write.Key.ID}))},
		{Key: aws.String("registry:write"), Value: aws.String(write.WriteID)},
	}
}
func sameTags(a, b []types.Tag) bool {
	if len(a) != len(b) {
		return false
	}
	values := map[string]string{}
	for _, t := range a {
		key := aws.ToString(t.Key)
		if _, dup := values[key]; dup {
			return false
		}
		values[key] = aws.ToString(t.Value)
	}
	for _, t := range b {
		if value, ok := values[aws.ToString(t.Key)]; !ok || value != aws.ToString(t.Value) {
			return false
		}
	}
	return true
}

// DeleteCandidate is deliberately unavailable until active-ref CAS and delayed
// provider-write exclusion can be enforced atomically. No AWS delete is sent.
func (*Store) DeleteCandidate(context.Context, tc.SecretVersionRef) error {
	return fail(DeletionUnsupported)
}

var _ tc.SecureCandidateStore = (*Store)(nil)
