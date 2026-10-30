package temporalcloud

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	operation "go.temporal.io/cloud-sdk/api/operation/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestNativeKeyObservationIsOpaqueAcrossFormattingAndSerialization(t *testing.T) {
	f := credentialsFixture(t)
	state := f.staged(t)
	f.transport.keys[state.Key.ID].ResourceVersion = sentinelToken
	observation, err := f.manager.observeKeyExecution(context.Background(), state, false)
	if err != nil || observation.private.reference.VersionDigest != ProviderVersionDigest(sentinelToken) {
		t.Fatal("opaque observation missing safe version digest")
	}
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%p", "%#p", "%d", "%T", "%t", "%b", "%c", "%o", "%O", "%U", "%e", "%E", "%f", "%F", "%g", "%G", "%w", "%20v", "%-20v", "%020v"} {
		for _, v := range []any{observation, &observation, struct{ Observation keyObservation }{observation}} {
			if strings.Contains(fmt.Sprintf(format, v), sentinelToken) {
				t.Fatal("native provider version escaped formatting")
			}
		}
	}
	if _, err = json.Marshal(observation); err == nil {
		t.Fatal("native observation serialized")
	}
	if _, err = observation.MarshalText(); err == nil {
		t.Fatal("native observation serialized as text")
	}
}

func TestConditionalRevokeRejectsChangedNativeVersionWithoutSubstitution(t *testing.T) {
	f := credentialsFixture(t)
	state := f.staged(t)
	f.canary.fail = true
	_, _ = f.manager.Advance(context.Background(), f.request)
	f.transport.keys[state.Key.ID].ResourceVersion = "new-native-version"
	next, err := f.manager.Advance(context.Background(), f.request)
	credentialError(t, err, CredentialVersionChanged)
	if len(f.transport.deletes) != 0 || next.Key.VersionDigest != state.Key.VersionDigest || next.Phase != CredentialCompensating || next.FailureCode != CredentialCanaryFailed || next.CleanupFailureCode != CredentialVersionChanged {
		t.Fatal("changed native version was substituted into delete")
	}
}

func TestDeleteRecoveryRequiresExactEchoedNativeVersionAfterAbsence(t *testing.T) {
	for _, mode := range []string{"correct", "missing", "changed-version", "digest-as-native"} {
		t.Run(mode, func(t *testing.T) {
			f := credentialsFixture(t)
			state := f.staged(t)
			f.canary.fail = true
			_, _ = f.manager.Advance(context.Background(), f.request)
			pending := f.advance(t)
			request := f.transport.deletes[0]
			op := f.transport.ops[request.AsyncOperationId]
			op.State = operation.AsyncOperation_STATE_FULFILLED
			switch mode {
			case "missing":
				op.OperationInput = nil
			case "changed-version", "digest-as-native":
				copy := proto.Clone(request).(*cloudservice.DeleteApiKeyRequest)
				copy.ResourceVersion = "changed-native"
				if mode == "digest-as-native" {
					copy.ResourceVersion = pending.Key.VersionDigest
				}
				op.OperationInput, _ = anypb.New(copy)
			}
			delete(f.transport.keys, state.Key.ID) // Native version can no longer be fetched.
			// A reconstructed manager has no execution observation from the delete.
			copy := *f.manager
			f.manager = &copy
			next, err := f.manager.Advance(context.Background(), f.request)
			if mode == "correct" {
				if err != nil || next.Phase != CredentialRevoked {
					t.Fatal("matching fulfilled native delete did not recover")
				}
			} else {
				credentialError(t, err, CredentialKeyUnproven)
				if next.Phase != CredentialRevocationPending {
					t.Fatal("unproven echo acknowledged deletion")
				}
			}
			if len(f.transport.deletes) != 1 {
				t.Fatal("delete was replayed during recovery")
			}
			assertNoMaterial(t, next, err, f.journal.history)
		})
	}
}

func TestRawVersionWireRecordsAndMissingDigestsFailClosed(t *testing.T) {
	for _, raw := range []string{`{"ID":"sa","ResourceVersion":""}`, `{"ID":"sa","ResourceVersion":"raw-native"}`} {
		var sa ServiceAccountRef
		var key KeyRef
		if json.Unmarshal([]byte(raw), &sa) == nil || json.Unmarshal([]byte(raw), &key) == nil {
			t.Fatal("legacy raw native version wire reference silently reinterpreted")
		}
	}
	f := credentialsFixture(t)
	state := f.staged(t)
	state.ServiceAccount.VersionDigest = ""
	f.journal.states[state.Intent.SlotID] = state
	_, err := f.manager.Advance(context.Background(), f.request)
	credentialError(t, err, CredentialOwnership)
	if len(f.transport.deletes) != 0 || len(f.transport.keyRequests) != 1 {
		t.Fatal("missing digest authorized credential mutation")
	}
	if ProviderVersionDigest("") != "" || ProviderVersionDigest("bad\nversion") != "" {
		t.Fatal("invalid native version accepted")
	}
}
