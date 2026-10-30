package credentialobservation

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/smithy-go"
	awsstore "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/awssecrets"
	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
	enums "go.temporal.io/api/enums/v1"
	workflow "go.temporal.io/api/workflowservice/v1"
	cloud "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	identity "go.temporal.io/cloud-sdk/api/identity/v1"
	namespace "go.temporal.io/cloud-sdk/api/namespace/v1"
	resource "go.temporal.io/cloud-sdk/api/resource/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func noRetry(o *secretsmanager.Options) { o.Retryer = aws.NopRetryer{} }

// Observe returns evidence for this finite authenticated read only. It performs
// no writes to journals/providers and cannot satisfy manager CredentialVerified.
// Unknown/failed observations retain the stage; callers must not compensate.
func (o *Observer) Observe(ctx context.Context, ref tc.SecretVersionRef) (Observation, error) {
	if o == nil || o.private == nil {
		return Observation{}, failure(Invalid, Metadata)
	}
	s := o.private
	if ctx.Err() != nil {
		return Observation{}, readFailure(ctx, ctx.Err(), Metadata)
	}
	if forbiddenContext(ctx) || !digestPattern.MatchString(ref.WriteID) {
		return Observation{}, failure(Denied, Metadata)
	}
	started := s.config.Now()
	call, cancel := context.WithTimeout(ctx, s.config.Timeout)
	defer cancel()
	var calls atomic.Int32
	var finished atomic.Bool
	var result Observation
	var callbackError error
	err := s.guard.Run(call, ref, func(guarded context.Context) error {
		if calls.Add(1) != 1 || call.Err() != nil || guarded.Err() != nil || forbiddenContext(guarded) {
			return failure(Denied, Metadata)
		}
		defer finished.Store(true)
		// Preserve cancellation of the caller even if a trusted guard supplies a
		// derived context. No outgoing application metadata is forwarded.
		inner, closeInner := context.WithCancel(guarded)
		defer closeInner()
		stop := context.AfterFunc(call, closeInner)
		defer stop()
		result, callbackError = s.observe(inner, ref, started)
		return callbackError
	})
	if call.Err() != nil {
		return Observation{}, readFailure(call, call.Err(), Completion)
	}
	if calls.Load() != 1 || !finished.Load() {
		return Observation{}, failure(Denied, Metadata)
	}
	if callbackError != nil {
		return Observation{}, callbackError
	}
	if err != nil {
		return Observation{}, failure(Denied, Completion)
	}
	return result, nil
}

func (s *observerState) load(ctx context.Context, ref tc.SecretVersionRef) (tc.CredentialState, awsstore.Outcome, error) {
	slot := hash(struct {
		Owner       any
		Incarnation string
	}{s.config.Ownership.Owner(), s.config.Namespace.Record.Ref.ProviderID})
	v, found, e := s.states.Load(ctx, slot)
	if e != nil || !found {
		return tc.CredentialState{}, awsstore.Outcome{}, failure(MetadataChanged, Metadata)
	}
	o, found, e := s.outcomes.Load(ctx, ref.WriteID)
	if e != nil || !found {
		return tc.CredentialState{}, awsstore.Outcome{}, failure(MetadataChanged, Metadata)
	}
	if e = s.validate(v, o, ref); e != nil {
		return tc.CredentialState{}, awsstore.Outcome{}, e
	}
	return v, o, nil
}

func (s *observerState) observe(ctx context.Context, ref tc.SecretVersionRef, started time.Time) (Observation, error) {
	v, receipt, e := s.load(ctx, ref)
	if e != nil {
		return Observation{}, e
	}
	deadline := started.Add(s.config.Timeout)
	if limit := v.Intent.ExpiresAt.Add(-5 * time.Second); limit.Before(deadline) {
		deadline = limit
	}
	remaining := deadline.Sub(s.config.Now())
	if remaining <= 0 {
		return Observation{}, failure(Expired, Metadata)
	}
	ctx, cancel := context.WithTimeout(ctx, remaining)
	defer cancel()
	metadata, e := s.secrets.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{SecretId: aws.String(receipt.ARN)}, noRetry)
	if e != nil {
		return Observation{}, readFailure(ctx, e, Secret)
	}
	if !s.validSecret(metadata, v, receipt) {
		return Observation{}, failure(SecretUnproven, Secret)
	}
	value, e := s.secrets.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: aws.String(receipt.ARN), VersionId: aws.String(ref.WriteID)}, noRetry)
	if value != nil {
		defer func() { clear(value.SecretBinary); value.SecretBinary = nil; value.SecretString = nil }()
	}
	if e != nil {
		return Observation{}, readFailure(ctx, e, Secret)
	}
	if value == nil || aws.ToString(value.ARN) != receipt.ARN || aws.ToString(value.Name) != ref.CandidateID || aws.ToString(value.VersionId) != ref.WriteID || value.SecretString != nil || len(value.SecretBinary) == 0 || len(value.SecretBinary) > 65536 {
		return Observation{}, failure(SecretUnproven, Secret)
	}
	// GetSecretValue.CreatedDate is descriptive VERSION metadata, deliberately
	// ignored and never returned. Secret birth is independently checked against
	// the create receipt via DescribeSecret. No version-date/clock claim is made.
	for _, b := range value.SecretBinary {
		if b < 33 || b > 126 {
			return Observation{}, failure(SecretUnproven, Secret)
		}
	}
	key := newMaterial(value.SecretBinary)
	defer key.close()
	clear(value.SecretBinary)
	value.SecretBinary = nil
	p, e := s.probes(s.config, &key, deadline)
	if e != nil || p == nil || p.cloud == nil || p.data == nil || p.close == nil {
		return Observation{}, readFailure(ctx, e, Identity)
	}
	defer p.close()
	identityResponse, e := p.cloud.GetCurrentIdentity(ctx, &cloud.GetCurrentIdentityRequest{})
	if e != nil {
		return Observation{}, readFailure(ctx, e, Identity)
	}
	keyVersion, saVersion, e := s.identity(v, identityResponse)
	if e != nil {
		return Observation{}, e
	}
	before, e := p.cloud.GetNamespace(ctx, &cloud.GetNamespaceRequest{Namespace: s.config.Namespace.NamespaceID})
	if e != nil {
		return Observation{}, readFailure(ctx, e, Namespace)
	}
	if before == nil || !s.namespace(before.Namespace) {
		return Observation{}, failure(NamespaceUnproven, Namespace)
	}
	data, e := p.data.DescribeNamespace(ctx, &workflow.DescribeNamespaceRequest{Namespace: s.config.Namespace.NamespaceID})
	if e != nil {
		return Observation{}, readFailure(ctx, e, DataPlane)
	}
	if data == nil || data.NamespaceInfo == nil || data.NamespaceInfo.Name != s.config.Namespace.NamespaceID || data.NamespaceInfo.State != enums.NAMESPACE_STATE_REGISTERED || !text(data.NamespaceInfo.Id) || s.config.ExpectedDataPlaneID != "" && data.NamespaceInfo.Id != s.config.ExpectedDataPlaneID {
		return Observation{}, failure(NamespaceUnproven, DataPlane)
	}
	after, e := p.cloud.GetNamespace(ctx, &cloud.GetNamespaceRequest{Namespace: s.config.Namespace.NamespaceID})
	if e != nil {
		return Observation{}, readFailure(ctx, e, Namespace)
	}
	if after == nil || !s.namespace(after.Namespace) {
		return Observation{}, failure(NamespaceUnproven, Namespace)
	}
	current, currentReceipt, e := s.load(ctx, ref)
	if e != nil {
		return Observation{}, e
	}
	if !same(current, v) || !same(currentReceipt, receipt) {
		return Observation{}, failure(MetadataChanged, Completion)
	}
	completed := s.config.Now()
	if ctx.Err() != nil {
		return Observation{}, readFailure(ctx, ctx.Err(), Completion)
	}
	if completed.Before(started) || !v.Intent.ExpiresAt.After(completed) {
		return Observation{}, failure(Expired, Completion)
	}
	if !deadline.After(completed) {
		return Observation{}, failure(Deadline, Completion)
	}
	result := Observation{Schema: 1, Contract: Contract, Owner: v.Intent.Owner, SlotID: v.Intent.SlotID, StageID: v.Intent.StageID, AttemptID: v.Intent.AttemptID, NamespaceID: v.Intent.NamespaceID, NamespaceIncarnationID: v.Intent.NamespaceIncarnationID, StoreID: ref.StoreID, CandidateID: ref.CandidateID, WriteID: ref.WriteID, VersionID: ref.VersionID, ServiceAccountID: v.ServiceAccount.ID, KeyID: v.Key.ID, CredentialJournalVersion: v.Version, OutcomeLedgerVersion: receipt.Version, EndpointBindingDigest: endpointBindingDigest(EndpointBindingInput{Cloud: s.config.Cloud, Data: s.config.Data, TrustBindingID: s.config.TrustBindingID, ProjectID: s.config.ProjectID}), DataPlaneIDHash: hash(data.NamespaceInfo.Id), KeyVersionDigest: keyVersion, ServiceAccountVersionDigest: saVersion, NamespaceVersionDigest: tc.ProviderVersionDigest(after.Namespace.ResourceVersion), StartedAt: started, ObservedAt: completed, ExpiresAt: v.Intent.ExpiresAt}
	raw, e := json.Marshal(result)
	if e != nil || key.contains(raw) {
		return Observation{}, failure(PrivacyDenied, Completion)
	}
	return result, nil
}

func (s *observerState) identity(v tc.CredentialState, r *cloud.GetCurrentIdentityResponse) (string, string, error) {
	if r == nil || r.GetUser() != nil {
		return "", "", failure(IdentityUnproven, Identity)
	}
	sa, key := r.GetServiceAccount(), r.PrincipalApiKey
	if sa == nil || sa.Spec == nil || sa.Id != v.ServiceAccount.ID || sa.CreatedTime == nil || sa.CreatedTime.CheckValid() != nil || !sa.CreatedTime.AsTime().Equal(v.ServiceAccount.CreatedAt) || sa.State != resource.ResourceState_RESOURCE_STATE_ACTIVE || sa.Spec.Name != v.ServiceAccount.Name || sa.Spec.Description != v.Intent.ServiceAccountMarker || sa.Spec.Access != nil || sa.Spec.ProjectScopedAccess != nil || sa.Spec.NamespaceScopedAccess == nil || sa.Spec.NamespaceScopedAccess.Namespace != v.Intent.NamespaceID || sa.Spec.NamespaceScopedAccess.Access == nil || sa.Spec.NamespaceScopedAccess.Access.Permission != identity.NamespaceAccess_PERMISSION_WRITE || len(sa.Spec.ProtoReflect().GetUnknown()) != 0 || len(sa.Spec.NamespaceScopedAccess.ProtoReflect().GetUnknown()) != 0 || len(sa.Spec.NamespaceScopedAccess.Access.ProtoReflect().GetUnknown()) != 0 || !text(sa.ResourceVersion) {
		return "", "", failure(IdentityUnproven, Identity)
	}
	if key == nil || key.Spec == nil || key.Id != v.Key.ID || key.CreatedTime == nil || key.CreatedTime.CheckValid() != nil || !key.CreatedTime.AsTime().Equal(v.Key.CreatedAt) || key.State != resource.ResourceState_RESOURCE_STATE_ACTIVE || key.Spec.OwnerId != sa.Id || key.Spec.OwnerType != identity.OwnerType_OWNER_TYPE_SERVICE_ACCOUNT || key.Spec.DisplayName != v.Intent.KeyDisplayName || key.Spec.Description != v.Intent.KeyMarker || key.Spec.ExpiryTime == nil || key.Spec.ExpiryTime.CheckValid() != nil || !key.Spec.ExpiryTime.AsTime().Equal(v.Intent.ExpiresAt) || key.Spec.Disabled || len(key.Spec.ProtoReflect().GetUnknown()) != 0 || !text(key.ResourceVersion) {
		return "", "", failure(IdentityUnproven, Identity)
	}
	return tc.ProviderVersionDigest(key.ResourceVersion), tc.ProviderVersionDigest(sa.ResourceVersion), nil
}
func (s *observerState) namespace(n *namespace.Namespace) bool {
	return n != nil && n.Namespace == s.config.Namespace.NamespaceID && n.Spec != nil && n.Spec.Name == s.config.Namespace.Record.Ref.Name && n.CreatedTime != nil && n.CreatedTime.CheckValid() == nil && n.CreatedTime.AsTime().Equal(s.config.Namespace.CreatedAt) && n.ProjectId == s.config.ProjectID && n.State == resource.ResourceState_RESOURCE_STATE_ACTIVE && n.Spec.GetApiKeyAuth().GetEnabled() && text(n.ResourceVersion)
}

func readFailure(ctx context.Context, err error, step Step) error {
	if errors.Is(ctx.Err(), context.Canceled) || errors.Is(err, context.Canceled) {
		return failure(Canceled, step)
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return failure(Deadline, step)
	}
	switch status.Code(err) {
	case codes.Unauthenticated:
		return failure(AuthenticationRejected, step)
	case codes.PermissionDenied:
		return failure(AuthorizationRejected, step)
	case codes.Unimplemented:
		return failure(EvidenceUnsupported, step)
	}
	var api smithy.APIError
	if errors.As(err, &api) {
		switch api.ErrorCode() {
		case "AccessDeniedException", "AccessDenied":
			return failure(AuthorizationRejected, step)
		case "UnrecognizedClientException", "InvalidClientTokenId":
			return failure(AuthenticationRejected, step)
		}
	}
	return failure(ReadUnknown, step)
}
