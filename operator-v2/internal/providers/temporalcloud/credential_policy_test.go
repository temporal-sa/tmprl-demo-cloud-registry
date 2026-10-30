package temporalcloud

import (
	"testing"
	"time"
)

func TestRetainPolicyRefusesStableOwnerEvenWithExistingScopedSA(t *testing.T) {
	f := credentialsFixture(t)
	state := f.staged(t)
	config := CredentialConfig{FailurePolicy: DisposableRetainOnFailure, Capabilities: CredentialCapabilities{StageAPIKeys: true}, MaximumKeyTTL: time.Hour, MaximumRecoveryPages: 1, StoreID: f.manager.config.StoreID, PreprovisionedServiceAccounts: []ScopedServiceAccountBinding{{NamespaceID: state.Intent.NamespaceID, NamespaceIncarnationID: state.Intent.NamespaceIncarnationID, Owner: state.Intent.Owner, Reference: state.ServiceAccount}}}
	if _, e := NewCredentialManager(config, f.base.adapter, f.transport, f.journal, f.store, nil); e == nil {
		t.Fatal("stable namespace accepted disposable retain mode")
	}
}
