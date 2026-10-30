package localmetadata

import (
	"crypto/sha1" // UUIDv5 compatibility with the namespace adapter; not authentication.
	"encoding/hex"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	awsstore "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/awssecrets"
	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
	cloudidentity "go.temporal.io/cloud-sdk/api/identity/v1"
	cloudresource "go.temporal.io/cloud-sdk/api/resource/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var hex64 = regexp.MustCompile(`^[0-9a-f]{64}$`)
var identifier = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._:/@+-]{0,255}$`)
var uidPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{16,128}$`)

func configured(root *os.Root, c Config) (*Store, error) {
	temporal, err := c.Ownership.Scope(ownership.Temporal)
	if err != nil {
		return nil, Invalid
	}
	if _, err = c.Ownership.Scope(ownership.AWS); err != nil {
		return nil, Invalid
	}
	if root == nil || !uidPattern.MatchString(c.BootstrapUID) || !identifier.MatchString(c.StoreID) || !validTime(c.NamespaceCreatedAt, false) || c.NamespaceIncarnationID != tc.IncarnationID(c.NamespaceID, c.NamespaceCreatedAt) || !strings.HasSuffix(c.NamespaceID, "."+temporal.AccountID) || !identifier.MatchString(c.NamespaceID) || c.MaximumRevisions < 1 || c.MaximumRevisions > 512 || c.MaximumRecordBytes < 1024 || c.MaximumRecordBytes > 65536 {
		return nil, Invalid
	}
	c.NamespaceCreatedAt = c.NamespaceCreatedAt.UTC()
	c.PreprovisionedServiceAccount.CreatedAt = c.PreprovisionedServiceAccount.CreatedAt.UTC()
	slot := digest(struct {
		Owner       any
		Incarnation string
	}{c.Ownership.Owner(), c.NamespaceIncarnationID})
	if sa := c.PreprovisionedServiceAccount; sa.ID != "" {
		name, err := c.Ownership.Name(ownership.Temporal, "service-account", slot, 63)
		if err != nil || !identifier.MatchString(sa.ID) || sa.Name != name || !validTime(sa.CreatedAt, false) || !identifier.MatchString(sa.ProvenanceID) || !hex64.MatchString(sa.VersionDigest) {
			return nil, Invalid
		}
	} else if c.PreprovisionedServiceAccount != (tc.ServiceAccountRef{}) {
		return nil, Invalid
	}
	if c.AuthObservationPolicy != nil {
		policy := *c.AuthObservationPolicy
		if !validAuthPolicy(policy) || c.Ownership.Owner().Lifetime != ownership.Disposable || c.PreprovisionedServiceAccount.ID == "" {
			return nil, Invalid
		}
		c.AuthObservationPolicy = &policy
	}
	return &Store{root: root, config: c, slot: slot}, nil
}

func validTime(v time.Time, zero bool) bool {
	return (zero && v.IsZero()) || (!v.IsZero() && v.Year() >= 1 && v.Year() <= 9999)
}

func (s *Store) configDigest() string {
	t, _ := s.config.Ownership.Scope(ownership.Temporal)
	a, _ := s.config.Ownership.Scope(ownership.AWS)
	seal, _ := s.config.Ownership.Name(ownership.Temporal, "metadata-scope", s.config.BootstrapUID, 63)
	base := digest(struct {
		UID, Store, Namespace, Incarnation, Slot, ScopeSeal string
		Owner                                               ownership.Owner
		Temporal, AWS                                       ownership.Scope
		Born                                                time.Time
		SA                                                  tc.ServiceAccountRef
		MaxRevisions, MaxBytes                              int
	}{s.config.BootstrapUID, s.config.StoreID, s.config.NamespaceID, s.config.NamespaceIncarnationID, s.slot, seal, s.config.Ownership.Owner(), t, a, s.config.NamespaceCreatedAt, s.config.PreprovisionedServiceAccount, s.config.MaximumRevisions, s.config.MaximumRecordBytes})
	if s.config.AuthObservationPolicy == nil {
		return base
	}
	return digest(struct {
		Schema, Base string
		Policy       AuthObservationPolicy
	}{s.identitySchema(), base, *s.config.AuthObservationPolicy})
}

func normalizeState(v tc.CredentialState) tc.CredentialState {
	v.Intent.ExpiresAt = v.Intent.ExpiresAt.UTC()
	v.ServiceAccount.CreatedAt = v.ServiceAccount.CreatedAt.UTC()
	v.Key.CreatedAt = v.Key.CreatedAt.UTC()
	return v
}
func normalizeOutcome(v awsstore.Outcome) awsstore.Outcome {
	v.Write.Intent.ExpiresAt = v.Write.Intent.ExpiresAt.UTC()
	v.Write.Key.CreatedAt = v.Write.Key.CreatedAt.UTC()
	v.CreatedAt = v.CreatedAt.UTC()
	return v
}

func (s *Store) validIntent(i tc.CredentialIntent) bool {
	if i.FailurePolicy != "" && (i.FailurePolicy != tc.DisposableRetainOnFailure || i.Owner.Lifetime != ownership.Disposable || s.config.PreprovisionedServiceAccount.ID == "") {
		return false
	}
	if i.Owner != s.config.Ownership.Owner() || i.NamespaceID != s.config.NamespaceID || i.NamespaceIncarnationID != s.config.NamespaceIncarnationID || i.SlotID != s.slot || !identifier.MatchString(i.OperationKey) || !identifier.MatchString(i.AttemptID) || !validTime(i.ExpiresAt, false) {
		return false
	}
	stage := digest(struct {
		Slot, Operation, Attempt string
		Expiry                   time.Time
		FailurePolicy            tc.CredentialFailurePolicy `json:",omitempty"`
	}{s.slot, i.OperationKey, i.AttemptID, i.ExpiresAt.UTC(), i.FailurePolicy})
	sa, e1 := s.config.Ownership.Name(ownership.Temporal, "service-account", s.slot, 63)
	key, e2 := s.config.Ownership.Name(ownership.Temporal, "api-key", stage, 63)
	candidate, e3 := s.config.Ownership.Path(ownership.Temporal, "api-key-candidate", stage)
	return e1 == nil && e2 == nil && e3 == nil && i.StageID == stage && i.ServiceAccountName == sa && i.ServiceAccountMarker == "registry-sa:"+s.slot && i.KeyDisplayName == key && i.KeyMarker == "registry-key:"+stage && i.CandidateID == candidate
}

func (s *Store) expectedReceipt(kind, operation, input string) tc.Receipt {
	seed := digest(struct {
		Owner                                             ownership.Owner
		NamespaceID, Kind, OperationKey, InputFingerprint string
	}{s.config.Ownership.Owner(), s.config.NamespaceID, kind, operation, input})
	ns := []byte{0x6b, 0xa7, 0xb8, 0x10, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0x00, 0xc0, 0x4f, 0xd4, 0x30, 0xc8}
	h := sha1.Sum(append(ns, []byte("registry-operator-v2/"+seed)...))
	b := append([]byte(nil), h[:16]...)
	b[6] = (b[6] & 0xf) | 0x50
	b[8] = (b[8] & 0x3f) | 0x80
	x := hex.EncodeToString(b)
	return tc.Receipt{ID: fmt.Sprintf("%s-%s-%s-%s-%s", x[:8], x[8:12], x[12:16], x[16:20], x[20:]), NamespaceID: s.config.NamespaceID, Kind: kind, OperationKey: operation, InputFingerprint: input}
}
func (s *Store) validReceipt(r tc.Receipt, kind string) bool {
	return hex64.MatchString(r.OperationKey) && hex64.MatchString(r.InputFingerprint) && r == s.expectedReceipt(kind, r.OperationKey, r.InputFingerprint)
}
func saFingerprint(i tc.CredentialIntent) string {
	return digest(&cloudidentity.ServiceAccountSpec{Name: i.ServiceAccountName, Description: i.ServiceAccountMarker, NamespaceScopedAccess: &cloudidentity.NamespaceScopedAccess{Namespace: i.NamespaceID, Access: &cloudidentity.NamespaceAccess{Permission: cloudidentity.NamespaceAccess_PERMISSION_WRITE}}})
}
func keyFingerprint(v tc.CredentialState) string {
	return digest(&cloudidentity.ApiKeySpec{OwnerId: v.ServiceAccount.ID, OwnerType: cloudidentity.OwnerType_OWNER_TYPE_SERVICE_ACCOUNT, DisplayName: v.Intent.KeyDisplayName, Description: v.Intent.KeyMarker, ExpiryTime: timestamppb.New(v.Intent.ExpiresAt)})
}
func writeID(v tc.CredentialState) string {
	return digest(struct{ Stage, Key string }{v.Intent.StageID, v.Key.ID})
}
func immutableKey(a, b tc.KeyRef) bool {
	a.VersionDigest = ""
	b.VersionDigest = ""
	a.State = ""
	b.State = ""
	return equal(a, b)
}
func saIdentity(a, b tc.ServiceAccountRef) bool {
	a.VersionDigest = ""
	b.VersionDigest = ""
	return equal(a, b)
}

func validCode(c tc.CredentialCode) bool {
	switch c {
	case "", tc.CredentialInvalid, tc.CredentialDisabled, tc.CredentialOwnership, tc.CredentialAttemptConflict, tc.CredentialJournalUnknown, tc.CredentialProviderFailure, tc.CredentialSAUnproven, tc.CredentialKeyUnproven, tc.CredentialMaterialLost, tc.CredentialStoreUnknown, tc.CredentialStoreRejected, tc.CredentialCanaryFailed, tc.CredentialCleanupBlocked, tc.CredentialVersionChanged, tc.CredentialRetained:
		return true
	}
	return false
}

func (s *Store) validState(v *view, next tc.CredentialState) bool {
	i := next.Intent
	if s.config.AuthObservationPolicy != nil && i.FailurePolicy != tc.DisposableRetainOnFailure {
		return false
	}
	if i.FailurePolicy == tc.DisposableRetainOnFailure {
		switch next.Phase {
		case tc.CredentialRecorded, tc.CredentialSAReady, tc.CredentialKeyRequested, tc.CredentialStoreRequested, tc.CredentialStaged:
		default:
			return false
		}
	}
	if next.Version != "" || !s.validIntent(i) || !validCode(next.FailureCode) || !validCode(next.CleanupFailureCode) {
		return false
	}
	sa := next.ServiceAccount
	if !validTime(sa.CreatedAt, true) || !validTime(next.Key.CreatedAt, true) || (sa.VersionDigest != "" && !hex64.MatchString(sa.VersionDigest)) {
		return false
	}
	if sa.ID != "" && (!identifier.MatchString(sa.ID) || sa.Name != i.ServiceAccountName || !identifier.MatchString(sa.ProvenanceID)) {
		return false
	}
	ready := sa.ID != "" && !sa.CreatedAt.IsZero() && hex64.MatchString(sa.VersionDigest)
	if next.SAReceipt != (tc.Receipt{}) {
		if !s.validReceipt(next.SAReceipt, "service-account-create") || next.SAReceipt.InputFingerprint != saFingerprint(i) || (next.SAReceipt.OperationKey != i.StageID && !v.usedStages[next.SAReceipt.OperationKey]) {
			return false
		}
	}
	if next.KeyReceipt != (tc.Receipt{}) && (next.KeyReceipt.OperationKey != i.StageID || next.KeyReceipt != s.expectedReceipt("api-key-create", i.StageID, keyFingerprint(next))) {
		return false
	}
	key := next.Key
	if key.ID != "" {
		if !identifier.MatchString(key.ID) || key.ServiceAccountID != sa.ID || key.CreationReceiptID != next.KeyReceipt.ID || next.KeyReceipt.ID == "" || (key.VersionDigest != "" && !hex64.MatchString(key.VersionDigest)) || (key.Verified && key.CreatedAt.IsZero()) {
			return false
		}
		// An independently observed key may still be ACTIVATING while its
		// one-time material must be staged. The manager separately gates canary
		// and revocation; this field cannot grant either capability.
		if key.State != "" {
			if _, known := cloudresource.ResourceState_value[key.State]; !known {
				return false
			}
		}
	}
	if next.CandidateWriteID != "" && (key.ID == "" || !key.Verified || next.CandidateWriteID != writeID(next)) {
		return false
	}
	if next.DeleteReceipt != (tc.Receipt{}) && (!s.validReceipt(next.DeleteReceipt, "api-key-delete") || next.DeleteReceipt.OperationKey != i.StageID) {
		return false
	}
	candidate := next.Candidate
	if candidate != (tc.SecretVersionRef{}) && (candidate.StoreID != s.config.StoreID || candidate.CandidateID != i.CandidateID || candidate.WriteID != next.CandidateWriteID || candidate.VersionID != next.CandidateWriteID || !hex64.MatchString(candidate.WriteID) || candidate.Owner != i.Owner || candidate.NamespaceIncarnationID != i.NamespaceIncarnationID || candidate.ServiceAccountID != sa.ID || candidate.KeyID != key.ID || !candidate.Candidate || !candidate.Encrypted || candidate.Active) {
		return false
	}
	noKey := key == (tc.KeyRef{}) && next.KeyReceipt == (tc.Receipt{}) && next.CandidateWriteID == "" && candidate == (tc.SecretVersionRef{}) && next.DeleteReceipt == (tc.Receipt{})
	switch next.Phase {
	case tc.CredentialRecorded:
		return sa == (tc.ServiceAccountRef{}) && next.SAReceipt == (tc.Receipt{}) && noKey && next.FailureCode == "" && next.CleanupFailureCode == ""
	case tc.CredentialSARequested:
		return sa == (tc.ServiceAccountRef{}) && next.SAReceipt.ID != "" && noKey
	case tc.CredentialSAKnown:
		return sa.ID != "" && noKey
	case tc.CredentialSAReady:
		return ready && noKey
	case tc.CredentialKeyRequested:
		return ready && key == (tc.KeyRef{}) && next.KeyReceipt.ID != "" && next.CandidateWriteID == "" && candidate == (tc.SecretVersionRef{}) && next.DeleteReceipt == (tc.Receipt{})
	case tc.CredentialStoreRequested:
		return ready && key.Verified && hex64.MatchString(key.VersionDigest) && next.CandidateWriteID != "" && candidate == (tc.SecretVersionRef{}) && next.DeleteReceipt == (tc.Receipt{})
	case tc.CredentialStaged, tc.CredentialVerified:
		if !ready || !key.Verified || candidate == (tc.SecretVersionRef{}) || next.DeleteReceipt != (tc.Receipt{}) || (next.Phase == tc.CredentialVerified && next.FailureCode != "") {
			return false
		}
		out, ok := v.outcomes[next.CandidateWriteID]
		return ok && out.Phase == awsstore.MetadataVerified && equal(out.Write.Intent, i) && immutableKey(out.Write.Key, key)
	case tc.CredentialCompensating:
		return ready && key.ID != "" && next.FailureCode != ""
	case tc.CredentialRevocationPending:
		return ready && key.Verified && next.DeleteReceipt.ID != "" && next.FailureCode != ""
	case tc.CredentialRevoked, tc.CredentialAborted:
		return ready && key.Verified && next.FailureCode != ""
	}
	return false
}

func (s *Store) validateFrame(v *view, f frame) error {
	if f.Kind == "auth-observation" {
		return s.validateAuthFrame(v, f)
	}
	if f.AuthObservation != nil {
		return IllegalTransition
	}
	if f.Kind == "journal" {
		if f.Journal == nil || f.Outcome != nil || f.Key != s.slot || !s.validState(v, *f.Journal) {
			return IllegalTransition
		}
		next := *f.Journal
		if v.journal == nil {
			if f.ExpectedVersion != "" || next.Phase != tc.CredentialRecorded {
				return IllegalTransition
			}
			return nil
		}
		previous := *v.journal
		if f.ExpectedVersion != previous.Version {
			return CASConflict
		}
		previous.Version = ""
		if !equal(previous.Intent, next.Intent) {
			if previous.Phase != tc.CredentialAborted || next.Phase != tc.CredentialSAKnown || v.usedStages[next.Intent.StageID] {
				return IllegalTransition
			}
			expected := tc.CredentialState{Intent: next.Intent, Phase: tc.CredentialSAKnown, ServiceAccount: previous.ServiceAccount, SAReceipt: previous.SAReceipt}
			if !equal(expected, next) {
				return IllegalTransition
			}
			return nil
		}
		if previous.ServiceAccount.ID != "" {
			if previous.ServiceAccount.ID != next.ServiceAccount.ID || previous.ServiceAccount.Name != next.ServiceAccount.Name || previous.ServiceAccount.ProvenanceID != next.ServiceAccount.ProvenanceID || (!previous.ServiceAccount.CreatedAt.IsZero() && !previous.ServiceAccount.CreatedAt.Equal(next.ServiceAccount.CreatedAt)) {
				return IllegalTransition
			}
		}
		if previous.Key.ID != "" && (previous.Key.ID != next.Key.ID || previous.Key.ServiceAccountID != next.Key.ServiceAccountID || previous.Key.CreationReceiptID != next.Key.CreationReceiptID || (!previous.Key.CreatedAt.IsZero() && !previous.Key.CreatedAt.Equal(next.Key.CreatedAt)) || (previous.Key.Verified && !next.Key.Verified)) {
			return IllegalTransition
		}
		verifiedObservation := previous.Phase == tc.CredentialStaged && (next.Phase == tc.CredentialVerified || next.Phase == tc.CredentialCompensating && next.FailureCode == tc.CredentialCanaryFailed) && next.Key.State == cloudresource.ResourceState_RESOURCE_STATE_ACTIVE.String()
		if previous.Key.VersionDigest != "" && next.Key.VersionDigest != previous.Key.VersionDigest && !verifiedObservation {
			return IllegalTransition
		}
		masked := next
		masked.Phase = previous.Phase
		if verifiedObservation {
			masked.Key.State = previous.Key.State
			masked.Key.VersionDigest = previous.Key.VersionDigest
		}
		switch {
		case previous.Phase == next.Phase:
			if previous.Phase == tc.CredentialCompensating || previous.Phase == tc.CredentialRevocationPending || previous.Phase == tc.CredentialRevoked {
				if previous.CleanupFailureCode != "" && next.CleanupFailureCode == "" {
					return IllegalTransition
				}
				masked.CleanupFailureCode = previous.CleanupFailureCode
			}
		case previous.Phase == tc.CredentialRecorded && next.Phase == tc.CredentialSARequested:
			masked.SAReceipt = previous.SAReceipt
		case previous.Phase == tc.CredentialRecorded && next.Phase == tc.CredentialSAReady:
			if s.config.PreprovisionedServiceAccount.ID == "" || !saIdentity(next.ServiceAccount, s.config.PreprovisionedServiceAccount) {
				return IllegalTransition
			}
			masked.ServiceAccount = previous.ServiceAccount
		case previous.Phase == tc.CredentialSARequested && next.Phase == tc.CredentialSAKnown:
			if next.ServiceAccount.ProvenanceID != next.SAReceipt.ID {
				return IllegalTransition
			}
			masked.ServiceAccount = previous.ServiceAccount
		case previous.Phase == tc.CredentialSAKnown && next.Phase == tc.CredentialSAReady:
			masked.ServiceAccount = previous.ServiceAccount
		case previous.Phase == tc.CredentialSAReady && next.Phase == tc.CredentialKeyRequested:
			masked.KeyReceipt = previous.KeyReceipt
		case previous.Phase == tc.CredentialKeyRequested && next.Phase == tc.CredentialStoreRequested:
			masked.Key = previous.Key
			masked.CandidateWriteID = previous.CandidateWriteID
		case previous.Phase == tc.CredentialKeyRequested && next.Phase == tc.CredentialCompensating:
			masked.Key = previous.Key
			masked.FailureCode = previous.FailureCode
		case previous.Phase == tc.CredentialStoreRequested && next.Phase == tc.CredentialStaged:
			masked.Candidate = previous.Candidate
		case previous.Phase == tc.CredentialStaged && next.Phase == tc.CredentialVerified:
		case previous.Phase == tc.CredentialStaged && next.Phase == tc.CredentialCompensating:
			masked.FailureCode = previous.FailureCode
		case previous.Phase == tc.CredentialCompensating && next.Phase == tc.CredentialRevocationPending:
			// Native opaque versions are deliberately unavailable here. The
			// trusted provider activity hashes its exact native request; this
			// backend verifies receipt structure/owner and immutable digest.
			if next.DeleteReceipt == (tc.Receipt{}) || next.Key.VersionDigest == "" {
				return IllegalTransition
			}
			masked.Key = previous.Key
			masked.DeleteReceipt = previous.DeleteReceipt
		case previous.Phase == tc.CredentialCompensating && next.Phase == tc.CredentialRevoked:
			masked.Key = previous.Key
		case previous.Phase == tc.CredentialRevocationPending && (next.Phase == tc.CredentialCompensating || next.Phase == tc.CredentialRevoked):
			if (next.Phase == tc.CredentialCompensating && next.CleanupFailureCode != tc.CredentialProviderFailure) || (next.Phase == tc.CredentialRevoked && next.CleanupFailureCode != "") {
				return IllegalTransition
			}
			masked.CleanupFailureCode = previous.CleanupFailureCode
		case previous.Phase == tc.CredentialRevoked && next.Phase == tc.CredentialAborted:
			if next.CleanupFailureCode != "" {
				return IllegalTransition
			}
			masked.CleanupFailureCode = previous.CleanupFailureCode
		default:
			return IllegalTransition
		}
		if !equal(masked, previous) {
			return IllegalTransition
		}
		return nil
	}
	if f.Kind != "outcome" || f.Outcome == nil || f.Journal != nil || f.Key != f.Outcome.Write.WriteID {
		return IllegalTransition
	}
	next := *f.Outcome
	w := next.Write
	j := v.journal
	if next.Version != "" || !hex64.MatchString(f.Key) || !s.validIntent(w.Intent) || w.Key.VersionDigest != "" || w.Key.State != "" || !w.Key.Verified || !validTime(w.Key.CreatedAt, false) || !validTime(next.CreatedAt, true) || !identifier.MatchString(w.Key.ID) || w.WriteID != digest(struct{ Stage, Key string }{w.Intent.StageID, w.Key.ID}) || j == nil || !equal(j.Intent, w.Intent) || j.CandidateWriteID != w.WriteID || !immutableKey(j.Key, w.Key) {
		return IllegalTransition
	}
	previous, found := v.outcomes[f.Key]
	if !found {
		if f.ExpectedVersion != "" || next.Phase != awsstore.CreateRequested || next.ARN != "" || !next.CreatedAt.IsZero() || j.Phase != tc.CredentialStoreRequested {
			return IllegalTransition
		}
		return nil
	}
	if f.ExpectedVersion != previous.Version || !equal(previous.Write, next.Write) {
		return CASConflict
	}
	previous.Version = ""
	if previous.Phase == awsstore.CreateRequested && next.Phase == awsstore.CreateReceipted {
		if previous.ARN != "" || !next.CreatedAt.IsZero() || !s.validARN(next.ARN, w.Intent.CandidateID) {
			return IllegalTransition
		}
		return nil
	}
	if previous.Phase == awsstore.CreateReceipted && next.Phase == awsstore.MetadataVerified && previous.ARN == next.ARN && !next.CreatedAt.IsZero() {
		return nil
	}
	if equal(previous, next) {
		return nil
	}
	return IllegalTransition
}

func (s *Store) validARN(arn, name string) bool {
	a, _ := s.config.Ownership.Scope(ownership.AWS)
	prefix := "arn:" + a.Partition + ":secretsmanager:" + a.Region + ":" + a.AccountID + ":secret:" + name + "-"
	return strings.HasPrefix(arn, prefix) && regexp.MustCompile(`^[A-Za-z0-9]{6}$`).MatchString(strings.TrimPrefix(arn, prefix))
}
