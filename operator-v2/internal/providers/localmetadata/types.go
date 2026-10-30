// Package localmetadata persists typed credential metadata in an explicitly
// supplied private local filesystem. It is a cooperative Unix sandbox backend,
// not a distributed lock, cloud fencing mechanism, secure token store, active
// reference registry, CandidateGuard, canary, or deletion implementation.
// It requires a local filesystem honoring flock/fsync and cooperative exclusive
// lifecycle of these files: no NFS/FUSE, backup rollback, manual tail deletion or
// uncoordinated directory restoration. Hash chaining detects altered/gapped
// history, but complete tail truncation needs an independently pinned external
// high-water witness. This slice does not invent one or claim adversarial
// tamper resistance. Reopen is a clean coordinator handoff, not a reset.
package localmetadata

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	awsstore "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/awssecrets"
	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
)

type Error string

func (e Error) Error() string { return string(e) }

const (
	Invalid           Error = "LocalMetadataInvalidConfiguration"
	ScopeConflict     Error = "LocalMetadataScopeConflict"
	CASConflict       Error = "LocalMetadataCASConflict"
	IllegalTransition Error = "LocalMetadataIllegalTransition"
	Uncertain         Error = "LocalMetadataOutcomeUncertain"
	Corrupt           Error = "LocalMetadataHistoryUnproven"
	Capacity          Error = "LocalMetadataHistoryCapacityReached"
)

type Config struct {
	// BootstrapUID must be independently chosen/pinned by the coordinator.
	// Neither Open nor a missing-directory fallback generates or replaces it.
	BootstrapUID                 string
	Ownership                    ownership.Context
	NamespaceID                  string
	NamespaceCreatedAt           time.Time
	NamespaceIncarnationID       string
	StoreID                      string
	PreprovisionedServiceAccount tc.ServiceAccountRef
	MaximumRevisions             int
	MaximumRecordBytes           int
	// A nonzero policy selects a distinct, fresh-bootstrap-only schema. It
	// records historical read facts, never changes the credential phase.
	AuthObservationPolicy *AuthObservationPolicy `json:",omitempty"`
}

type FileID struct {
	Device, Inode uint64
	UID           uint32
}

// Binding must be retained independently of this directory. A copied manifest
// or same pathname does not authorize a different root/lock inode on reopen.
type Binding struct {
	BootstrapUID   string
	Directory      FileID
	Lock           FileID
	IdentitySHA256 string
	ConfigSHA256   string
}

type Store struct {
	root      *os.Root
	config    Config
	binding   Binding
	slot      string
	mu        sync.Mutex
	uncertain bool
	// Private deterministic fault hook for filesystem tests only. No production
	// constructor accepts callbacks, opaque bytes or arbitrary serialization.
	fault func(string) error
}

type Journal struct{ store *Store }
type Ledger struct{ store *Store }

func (s *Store) CredentialJournal() *Journal { return &Journal{s} }
func (s *Store) OutcomeLedger() *Ledger      { return &Ledger{s} }

var _ tc.CredentialJournal = (*Journal)(nil)
var _ awsstore.OutcomeLedger = (*Ledger)(nil)

type Revision struct {
	Sequence        uint64
	Digest          string
	PreviousDigest  string
	Kind            string
	Key             string
	ExpectedVersion string
	Journal         *tc.CredentialState
	Outcome         *awsstore.Outcome
	AuthObservation *AuthObservationReceipt `json:",omitempty"`
}

type Pending struct {
	Sequence          uint64
	Digest, Kind, Key string
}

func (s *Store) History(ctx context.Context) ([]Revision, error) {
	var result []Revision
	err := s.locked(ctx, func() error {
		if s.uncertain {
			return Uncertain
		}
		view, err := s.scan(ctx, false)
		if err != nil {
			return err
		}
		result = view.history
		return nil
	})
	return result, err
}
