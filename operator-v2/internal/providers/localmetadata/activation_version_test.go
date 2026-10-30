package localmetadata

import (
	"testing"

	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
	resource "go.temporal.io/cloud-sdk/api/resource/v1"
)

func TestActivationVersionObservationPersistsBeforeVerificationOrCompensation(t *testing.T) {
	for _, mode := range []string{"verified", "canary-failed", "later-version-drift"} {
		t.Run(mode, func(t *testing.T) {
			c := composed(t)
			c.cloud.initialKeyActivating = true
			c.advance(t)
			c.advance(t)
			staged := c.advance(t)
			c.cloud.key.State = resource.ResourceState_RESOURCE_STATE_ACTIVE
			c.cloud.key.ResourceVersion = "activated-native-version"
			c.canary.fail = mode != "verified"
			next, e := c.manager.Advance(ctx, c.request)
			if mode == "verified" {
				if e != nil || next.Phase != tc.CredentialVerified {
					t.Fatal("activation did not verify")
				}
			} else {
				compositionCode(t, e, tc.CredentialCanaryFailed)
			}
			if next.Key.State != resource.ResourceState_RESOURCE_STATE_ACTIVE.String() || next.Key.VersionDigest != tc.ProviderVersionDigest("activated-native-version") || next.Key.VersionDigest == staged.Key.VersionDigest {
				t.Fatal("explicit ACTIVE observation not persisted")
			}
			c.restart(t)
			retained, found, e := c.backend.CredentialJournal().Load(ctx, next.Intent.SlotID)
			if e != nil || !found || retained.Key != next.Key {
				t.Fatal("reopen lost accepted observation")
			}
			if mode == "verified" {
				if c.cloud.deletes != 0 {
					t.Fatal("verification deleted key")
				}
				return
			}
			if mode == "later-version-drift" {
				c.cloud.key.ResourceVersion = "newer-after-compensation"
				_, e = c.manager.Advance(ctx, c.request)
				compositionCode(t, e, tc.CredentialVersionChanged)
				if c.cloud.deletes != 0 {
					t.Fatal("compensation silently refreshed version")
				}
				return
			}
			pending := c.advance(t)
			if pending.Phase != tc.CredentialRevocationPending || c.cloud.deletes != 1 || pending.Key.VersionDigest != retained.Key.VersionDigest {
				t.Fatal("exact persisted observation did not authorize conditional delete")
			}
			c.cloud.key = nil
			c.restart(t)
			if c.advance(t).Phase != tc.CredentialRevoked {
				t.Fatal("fulfilled exact native delete did not recover")
			}
			c.privacy(t)
		})
	}
}
