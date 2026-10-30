package awspermission

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/testinfra/fixtureplan"
)

type Helper struct{ private *helperState }
type helperState struct {
	mu        sync.Mutex
	config    Config
	bootstrap Credentials
	log       operationLog
	factory   factory
	used      bool
	nextCall  time.Time
}

func (Helper) String() string               { return "AWSReadProofHelper{private}" }
func (h Helper) GoString() string           { return h.String() }
func (h Helper) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, h.String()) }
func (Helper) MarshalJSON() ([]byte, error) { return nil, PrivacyDenied }
func (Helper) MarshalText() ([]byte, error) { return nil, PrivacyDenied }

// New constructs explicit SDK clients only when Run begins. There is no live
// CLI/credential-acquisition adapter here: its private approval/pipe invocation
// is a separate review gate. Supplying credentials is NOT an authorization
// inferred from source files or a rendering's applyAuthorized=false flags.
func New(c Config, bootstrap Credentials, log *Journal) (*Helper, error) {
	if log == nil || log.binding.SpecSHA256 != ConfigurationDigest(c) || log.check() != nil {
		return nil, Invalid
	}
	return configured(c, bootstrap, log, makeSDKClients)
}
func configured(c Config, bootstrap Credentials, log operationLog, f factory) (*Helper, error) {
	if validateConfig(c) != nil || log == nil || f == nil || bootstrap.private == nil {
		return nil, Invalid
	}
	bootstrap.private.mu.Lock()
	defer bootstrap.private.mu.Unlock()
	if bootstrap.containsBeforeLock(encode(struct {
		Digest   string
		Bindings fixtureplan.ReadProofBindings
	}{ConfigurationDigest(c), c.Bindings})) {
		return nil, PrivacyDenied
	}
	if bootstrap.private.kind != "bootstrap" || bootstrap.private.closed || !bootstrap.private.expires.After(c.Now()) {
		return nil, Invalid
	}
	bootstrap.private.deadline = c.ApprovedUntil
	bootstrap.private.now = c.Now
	return &Helper{&helperState{config: c, bootstrap: bootstrap, log: log, factory: f}}, nil
}

type callResult struct{ outcome, code, requestID string }

func providerResult(err error, meta middleware.Metadata, negative bool, material Credentials) (callResult, error) {
	id, _ := awsmiddleware.GetRequestIDMetadata(meta)
	if id == "" && err != nil {
		var r interface{ ServiceRequestID() string }
		if errors.As(err, &r) {
			id = r.ServiceRequestID()
		}
	}
	if !requestIDPattern.MatchString(id) || material.contains(id) {
		id = ""
	}
	if err == nil {
		if negative {
			return callResult{"UnexpectedAllowed", string(UnexpectedAllowed), id}, UnexpectedAllowed
		}
		return callResult{"Allowed", "", id}, nil
	}
	code := ""
	var api smithy.APIError
	if errors.As(err, &api) {
		switch api.ErrorCode() {
		case "AccessDenied", "AccessDeniedException":
			code = api.ErrorCode()
		}
	}
	if negative && code != "" {
		return callResult{"Denied", code, id}, nil
	}
	if code == "" {
		code = string(ProviderFailure)
	}
	return callResult{"Unproven", code, id}, ProviderFailure
}
func (s *helperState) call(ctx context.Context, m Credentials, step Step, target string, detail any, fn func(context.Context) (callResult, error)) (Receipt, error) {
	r := Receipt{Step: step, SpecSHA256: s.config.ApprovalSHA256, RequestSHA256: hash(struct {
		Step   Step
		Target string
		Detail any
	}{step, target, detail}), TargetSHA256: hash(target), Outcome: "Requested", At: s.config.Now().UTC()}
	if ctx.Err() != nil || !s.config.Now().Before(s.config.ApprovedUntil) {
		r.Outcome = "Unproven"
		r.Code = string(Denied)
		r.Persistence = "NotAttempted"
		return r, Denied
	}
	if delay := s.nextCall.Sub(s.config.Now()); delay > 0 {
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			r.Outcome = "Unproven"
			r.Code = string(Denied)
			r.Persistence = "NotAttempted"
			return r, Denied
		case <-timer.C:
		}
	}
	s.nextCall = s.config.Now().Add(time.Second)
	if m.contains(string(encode(r))) {
		return Receipt{}, PrivacyDenied
	}
	if e := s.log.begin(ctx, r); e != nil {
		r.Outcome = "Unproven"
		r.Code = string(Unproven)
		r.Persistence = "Unknown"
		return r, Unproven
	}
	call, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	call, permit := operationContext(call, m, step)
	observed, e := fn(call)
	permit.live.Store(false)
	r.Outcome, r.Code, r.RequestID, r.At = observed.outcome, observed.code, observed.requestID, s.config.Now().UTC()
	if r.Outcome == "" {
		r.Outcome = "Unproven"
		r.Code = string(Unproven)
	}
	if ctx.Err() != nil || !s.config.Now().Before(s.config.ApprovedUntil) {
		r.Persistence = "NotAttempted"
		return r, Unproven
	}
	if m.contains(string(encode(r))) {
		return Receipt{}, PrivacyDenied
	}
	if pe := s.log.finish(ctx, r); pe != nil {
		r.Persistence = "Unknown"
		return r, Unproven
	}
	r.Persistence = "Acknowledged"
	return r, e
}
func (h *Helper) Run(ctx context.Context) (Result, error) {
	result := Result{Contract: fixtureplan.ReadProofContract, Outcome: "Unproven"}
	if h == nil || h.private == nil {
		return result, Invalid
	}
	s := h.private
	s.mu.Lock()
	defer s.mu.Unlock()
	result.SpecSHA256 = s.config.ApprovalSHA256
	if s.used {
		return result, AlreadyAttempted
	}
	s.used = true
	defer s.bootstrap.Close()
	if ctx.Err() != nil || validateConfig(s.config) != nil {
		return result, Denied
	}
	duration := s.config.ApprovedUntil.Sub(s.config.Now())
	if duration > 5*time.Minute {
		duration = 5 * time.Minute
	}
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	source, e := s.factory(s.bootstrap, s.config)
	if e != nil {
		return result, Invalid
	}
	closeSource := sync.OnceFunc(func() {
		if source.close != nil {
			source.close()
		}
	})
	defer closeSource()
	appendCall := func(m Credentials, step Step, target string, detail any, fn func(context.Context) (callResult, error)) error {
		r, e := s.call(ctx, m, step, target, detail, fn)
		result.Steps = append(result.Steps, r)
		return e
	}
	adminPrefix := "arn:aws:sts::" + fixtureplan.Account + ":assumed-role/" + fixtureplan.ProvisionerRole[strings.LastIndex(fixtureplan.ProvisionerRole, "/")+1:] + "/"
	if e = appendCall(s.bootstrap, SourceIdentity, fixtureplan.ProvisionerRole, nil, func(c context.Context) (callResult, error) {
		return identity(c, source.sts, s.bootstrap, s.config.SourceAdminRoleID, adminPrefix, false)
	}); e != nil {
		return result, e
	}
	if s.config.Phase == Seed {
		for _, target := range []struct {
			step Step
			arn  string
			deny bool
		}{{BootstrapAllowMetadata, s.config.Bindings.AllowSecretARN, false}, {BootstrapDenyMetadata, fixtureplan.DenySecretARN, true}} {
			t := target
			if e = appendCall(s.bootstrap, t.step, t.arn, nil, func(c context.Context) (callResult, error) {
				return s.describe(c, source.secret, s.bootstrap, t.deny, true)
			}); e != nil {
				return result, e
			}
		}
		for i, seed := range fixtureplan.ReadProofSeeds() {
			seed := seed
			step := []Step{SeedAllowV1, SeedAllowV2, SeedDenyV1}[i]
			target := s.config.Bindings.AllowSecretARN
			if seed.Target == "deny" {
				target = fixtureplan.DenySecretARN
			}
			if e = appendCall(s.bootstrap, step, target, seed, func(c context.Context) (callResult, error) {
				body, be := fixtureplan.ReadProofSeedBody(seed.Label)
				if be != nil {
					return callResult{}, Invalid
				}
				defer clear(body)
				r, e := source.secret.PutSecretValue(c, &secretsmanager.PutSecretValueInput{SecretId: aws.String(target), ClientRequestToken: aws.String(seed.VersionID), VersionStages: []string{seed.Stage}, SecretBinary: body}, noSecretRetry)
				var meta middleware.Metadata
				if r != nil {
					meta = r.ResultMetadata
				}
				out, pe := providerResult(e, meta, false, s.bootstrap)
				if pe != nil {
					return out, pe
				}
				if r == nil || aws.ToString(r.ARN) != target || aws.ToString(r.VersionId) != seed.VersionID || len(r.VersionStages) != 1 || r.VersionStages[0] != seed.Stage {
					return callResult{"Unproven", string(MetadataChanged), out.requestID}, MetadataChanged
				}
				return out, nil
			}); e != nil {
				return result, e
			}
		}
		result.Outcome = "SeededSyntheticVersions"
		return result, nil
	}
	// Verify native existing version identities before assuming the test role.
	// A structural SHA receipt field alone is not live version/existence proof.
	for _, target := range []struct {
		step Step
		arn  string
		deny bool
	}{{BootstrapAllowMetadata, s.config.Bindings.AllowSecretARN, false}, {BootstrapDenyMetadata, fixtureplan.DenySecretARN, true}} {
		t := target
		if e = appendCall(s.bootstrap, t.step, t.arn, nil, func(c context.Context) (callResult, error) {
			return s.describe(c, source.secret, s.bootstrap, t.deny, false)
		}); e != nil {
			return result, e
		}
	}
	var reader Credentials
	names, _ := fixtureplan.ReadProofResourceNames()
	roleARN := "arn:aws:iam::" + fixtureplan.Account + ":role/" + names.ReaderRole
	if e = appendCall(s.bootstrap, AssumeReader, roleARN, ReaderSessionName, func(c context.Context) (callResult, error) {
		r, e := source.sts.AssumeRole(c, &sts.AssumeRoleInput{RoleArn: aws.String(roleARN), RoleSessionName: aws.String(ReaderSessionName), ExternalId: aws.String(fixtureplan.ReadProofID), DurationSeconds: aws.Int32(900)}, noSTSRetry)
		if r != nil && r.Credentials != nil {
			defer func() {
				r.Credentials.AccessKeyId = nil
				r.Credentials.SecretAccessKey = nil
				r.Credentials.SessionToken = nil
			}()
		}
		var meta middleware.Metadata
		if r != nil {
			meta = r.ResultMetadata
		}
		out, pe := providerResult(e, meta, false, s.bootstrap)
		if pe != nil {
			return out, pe
		}
		expectedARN := "arn:aws:sts::" + fixtureplan.Account + ":assumed-role/" + names.ReaderRole + "/" + ReaderSessionName
		if r == nil || r.AssumedRoleUser == nil || aws.ToString(r.AssumedRoleUser.Arn) != expectedARN || aws.ToString(r.AssumedRoleUser.AssumedRoleId) != s.config.Bindings.ReaderRoleID+":"+ReaderSessionName || r.Credentials == nil || r.Credentials.Expiration == nil || !r.Credentials.Expiration.After(s.config.Now().Add(5*time.Minute)) {
			return callResult{"Unproven", string(WrongIdentity), out.requestID}, WrongIdentity
		}
		reader, e = newCredentials(aws.ToString(r.Credentials.AccessKeyId), aws.ToString(r.Credentials.SecretAccessKey), aws.ToString(r.Credentials.SessionToken), *r.Credentials.Expiration, "reader", s.config.Now, s.config.ApprovedUntil)
		if e != nil {
			return callResult{"Unproven", string(Invalid), out.requestID}, Invalid
		}
		if reader.contains(string(encode(struct {
			Digest   string
			Bindings fixtureplan.ReadProofBindings
		}{ConfigurationDigest(s.config), s.config.Bindings}))) {
			reader.Close()
			return callResult{"Unproven", string(PrivacyDenied), out.requestID}, PrivacyDenied
		}
		return out, nil
	}); e != nil {
		return result, e
	}
	defer reader.Close()
	// The source provider/transport loses all authority before scoped proof.
	s.bootstrap.Close()
	closeSource()
	scoped, e := s.factory(reader, s.config)
	if e != nil {
		return result, Invalid
	}
	if scoped.close != nil {
		defer scoped.close()
	}
	readerARN := "arn:aws:sts::" + fixtureplan.Account + ":assumed-role/" + names.ReaderRole + "/" + ReaderSessionName
	if e = appendCall(reader, ReaderIdentity, roleARN, nil, func(c context.Context) (callResult, error) {
		return identity(c, scoped.sts, reader, s.config.Bindings.ReaderRoleID, readerARN, true)
	}); e != nil {
		return result, e
	}
	if e = appendCall(reader, AllowMetadata, s.config.Bindings.AllowSecretARN, nil, func(c context.Context) (callResult, error) { return s.describe(c, scoped.secret, reader, false, false) }); e != nil {
		return result, e
	}
	seeds := fixtureplan.ReadProofSeeds()
	for _, target := range []struct {
		step Step
		arn  string
		seed fixtureplan.ReadProofSeed
		deny bool
	}{{ReadV1, s.config.Bindings.AllowSecretARN, seeds[0], false}, {ReadV2, s.config.Bindings.AllowSecretARN, seeds[1], true}, {ReadDeny, fixtureplan.DenySecretARN, seeds[2], true}} {
		t := target
		if e = appendCall(reader, t.step, t.arn, t.seed.VersionID, func(c context.Context) (callResult, error) {
			r, e := scoped.secret.GetSecretValue(c, &secretsmanager.GetSecretValueInput{SecretId: aws.String(t.arn), VersionId: aws.String(t.seed.VersionID)}, noSecretRetry)
			if r != nil {
				defer func() { clear(r.SecretBinary); r.SecretBinary = nil; r.SecretString = nil }()
			}
			var meta middleware.Metadata
			if r != nil {
				meta = r.ResultMetadata
			}
			out, pe := providerResult(e, meta, t.deny, reader)
			if pe != nil {
				return out, pe
			}
			if t.deny {
				return out, nil
			}
			if r == nil || aws.ToString(r.ARN) != t.arn || aws.ToString(r.VersionId) != t.seed.VersionID || r.SecretString != nil || len(r.SecretBinary) != t.seed.Bytes || bytesHash(r.SecretBinary) != t.seed.BodySHA256 {
				return callResult{"Unproven", string(MetadataChanged), out.requestID}, MetadataChanged
			}
			return out, nil
		}); e != nil {
			return result, e
		}
	}
	if e = appendCall(reader, GenerateDenyKey, fixtureplan.DenyKeyARN, nil, func(c context.Context) (callResult, error) {
		r, e := scoped.kms.GenerateDataKey(c, &kms.GenerateDataKeyInput{KeyId: aws.String(fixtureplan.DenyKeyARN), KeySpec: kmstypes.DataKeySpecAes256, EncryptionContext: map[string]string{"SecretARN": fixtureplan.DenySecretARN, "RegistryProof": fixtureplan.ReadProofID}}, noKMSRetry)
		if r != nil {
			defer func() { clear(r.Plaintext); clear(r.CiphertextBlob); r.Plaintext = nil; r.CiphertextBlob = nil }()
		}
		var meta middleware.Metadata
		if r != nil {
			meta = r.ResultMetadata
		}
		return providerResult(e, meta, true, reader)
	}); e != nil {
		return result, e
	}
	trapARN := "arn:aws:iam::" + fixtureplan.Account + ":role/" + names.TrapRole
	if e = appendCall(reader, AssumeTrap, trapARN, TrapSessionName, func(c context.Context) (callResult, error) {
		r, e := scoped.sts.AssumeRole(c, &sts.AssumeRoleInput{RoleArn: aws.String(trapARN), RoleSessionName: aws.String(TrapSessionName), DurationSeconds: aws.Int32(900)}, noSTSRetry)
		if r != nil && r.Credentials != nil {
			defer func() {
				r.Credentials.AccessKeyId = nil
				r.Credentials.SecretAccessKey = nil
				r.Credentials.SessionToken = nil
			}()
		}
		var meta middleware.Metadata
		if r != nil {
			meta = r.ResultMetadata
		}
		return providerResult(e, meta, true, reader)
	}); e != nil {
		return result, e
	}
	result.Outcome = "ScopedEffectiveCallsMatched"
	return result, nil
}

func identity(ctx context.Context, client stsAPI, m Credentials, roleID, arn string, exact bool) (callResult, error) {
	r, e := client.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{}, noSTSRetry)
	var meta middleware.Metadata
	if r != nil {
		meta = r.ResultMetadata
	}
	out, pe := providerResult(e, meta, false, m)
	if pe != nil {
		return out, pe
	}
	if r == nil || aws.ToString(r.Account) != fixtureplan.Account || aws.ToString(r.UserId) != roleID+":"+aws.ToString(r.Arn)[strings.LastIndex(aws.ToString(r.Arn), "/")+1:] || exact && aws.ToString(r.Arn) != arn || !exact && !strings.HasPrefix(aws.ToString(r.Arn), arn) {
		return callResult{"Unproven", string(WrongIdentity), out.requestID}, WrongIdentity
	}
	return out, nil
}
func (s *helperState) describe(ctx context.Context, client secretAPI, m Credentials, deny, empty bool) (callResult, error) {
	arn, key, born, expiry := s.config.Bindings.AllowSecretARN, fixtureplan.BaseCandidateKeyARN, s.config.Bindings.AllowSecretCreatedAt, s.config.FixtureExpiresAt
	if deny {
		arn = fixtureplan.DenySecretARN
		key = fixtureplan.DenyKeyARN
		born, _ = time.Parse(time.RFC3339Nano, "2026-10-30T02:18:00.749Z")
		expiry = s.config.DenyFixtureExpiresAt
	}
	r, e := client.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretId: aws.String(arn)}, noSecretRetry)
	var meta middleware.Metadata
	if r != nil {
		meta = r.ResultMetadata
	}
	out, pe := providerResult(e, meta, false, m)
	if pe != nil {
		return out, pe
	}
	if r == nil || aws.ToString(r.ARN) != arn || aws.ToString(r.KmsKeyId) != key || r.CreatedDate == nil || !r.CreatedDate.Equal(born) || r.DeletedDate != nil || aws.ToBool(r.RotationEnabled) || len(r.ReplicationStatus) != 0 || r.PrimaryRegion != nil || r.OwningService != nil || empty && len(r.VersionIdsToStages) != 0 {
		return callResult{"Unproven", string(MetadataChanged), out.requestID}, MetadataChanged
	}
	name := strings.SplitN(arn, ":secret:", 2)[1]
	name = name[:len(name)-7]
	if aws.ToString(r.Name) != name || r.RotationRules != nil || r.RotationLambdaARN != nil || len(r.ExternalSecretRotationMetadata) != 0 || r.ExternalSecretRotationRoleArn != nil || r.Type != nil {
		return callResult{"Unproven", string(MetadataChanged), out.requestID}, MetadataChanged
	}
	if !empty {
		seeds := fixtureplan.ReadProofSeeds()
		if deny {
			if len(r.VersionIdsToStages) != 1 || len(r.VersionIdsToStages[seeds[2].VersionID]) != 1 || r.VersionIdsToStages[seeds[2].VersionID][0] != seeds[2].Stage {
				return callResult{"Unproven", string(MetadataChanged), out.requestID}, MetadataChanged
			}
		} else {
			if len(r.VersionIdsToStages) != 2 || len(r.VersionIdsToStages[seeds[0].VersionID]) != 1 || r.VersionIdsToStages[seeds[0].VersionID][0] != seeds[0].Stage || len(r.VersionIdsToStages[seeds[1].VersionID]) != 1 || r.VersionIdsToStages[seeds[1].VersionID][0] != seeds[1].Stage {
				return callResult{"Unproven", string(MetadataChanged), out.requestID}, MetadataChanged
			}
		}
	}
	owner, _ := fixtureplan.Context()
	tags := map[string]string{"RegistryFixture": "registry-operator-v2", "RegistryOwner": hash(owner.Owner()), "RegistryRun": fixtureplan.RunID, "RegistryExpiresAt": expiry.UTC().Format(time.RFC3339Nano)}
	if deny {
		tags["RegistrySentinel"] = "deny"
	} else {
		tags["RegistryProof"] = fixtureplan.ReadProofID
	}
	if len(r.Tags) != len(tags) {
		return callResult{"Unproven", string(MetadataChanged), out.requestID}, MetadataChanged
	}
	seen := map[string]bool{}
	for _, tag := range r.Tags {
		k, v := aws.ToString(tag.Key), aws.ToString(tag.Value)
		expected, member := tags[k]
		if seen[k] || !member || expected != v {
			return callResult{"Unproven", string(MetadataChanged), out.requestID}, MetadataChanged
		}
		seen[k] = true
	}
	return out, nil
}
