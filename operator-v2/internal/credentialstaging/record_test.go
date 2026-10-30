package credentialstaging

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	observation "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/credentialobservation"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	awsstore "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/awssecrets"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/localmetadata"
	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
	"google.golang.org/grpc/metadata"
)

func freshReceiptBootstrap(t *testing.T, f *fixture, settings ObservationSettings, reader observation.SecretClient, change func(*localmetadata.AuthObservationPolicy)) {
	t.Helper()
	readDigest := must(ObservationSettingsFingerprint(settings, f.config.KMS))
	endpoints := must(observation.ExpectedEndpointBinding(observation.EndpointBindingInput{Cloud: settings.Cloud, Data: settings.Data, TrustBindingID: settings.TrustBindingID, ProjectID: settings.ProjectID}))
	policy := &localmetadata.AuthObservationPolicy{Contract: localmetadata.AuthObservationReceiptContract, ObservationContract: observation.Contract, ObservationBindingDigest: readDigest, EndpointBindingDigest: endpoints.Digest, MaximumPerStage: 2, MaximumDuration: settings.Timeout + 2*time.Second}
	if change != nil {
		change(policy)
	}
	metadataRoot, _ := root(t)
	mc := f.config.MetadataConfig
	mc.BootstrapUID = "fresh-auth-receipt-metadata"
	mc.AuthObservationPolicy = policy
	_, binding, e := localmetadata.Bootstrap(background, metadataRoot, mc)
	if e != nil {
		t.Fatal(e)
	}
	f.config.Mode = LocalDisposableStagingAuthReceipts
	f.config.MetadataRoot, f.config.MetadataConfig, f.config.MetadataBinding = metadataRoot, mc, binding
	registryRoot, path := root(t)
	rc := f.config.RegistryConfig
	rc.BootstrapUID = "fresh-auth-receipt-registry"
	rc.Metadata, rc.MetadataConfigDigest = binding, binding.ConfigSHA256
	rc.ObservationBindingDigest = readDigest
	rc.AuthObservationPolicyDigest = must(AuthObservationPolicyFingerprint(*policy, binding.ConfigSHA256, readDigest))
	f.config.RegistryRoot, f.config.RegistryConfig = registryRoot, rc
	f.config.RegistryBinding = must(BootstrapRegistry(background, registryRoot, rc))
	f.registryPath = path
	f.config.Observation = &ObservationRuntime{Settings: settings, SecretReader: reader}
}

func setupReceipt(t *testing.T) (*fixture, *denyReadClient, *time.Time) {
	t.Helper()
	f := setup(t, nil)
	reader := &denyReadClient{}
	freshReceiptBootstrap(t, f, publicReadSettings(t), reader, nil)
	clock := now
	f.config.Now = func() time.Time { return clock }
	writer := f.config.Writer
	f.config.Writer = func() ownership.Writer { w := writer(); w.ExpiresAt = clock.Add(time.Hour); return w }
	return f, reader, &clock
}

func recordRequest(state tc.CredentialState, attempt string) RecordObservationRequest {
	return RecordObservationRequest{ObserveRequest: readRequest(state), AttemptID: sum(attempt)}
}

func TestAuthReceiptConstructorExplicitFreshPolicyAndBudget(t *testing.T) {
	for _, mode := range []string{"old-mode", "no-policy", "no-registry-pin", "policy-drift", "endpoint-pin", "timeout-reserve", "reader-drift", "implicit-enable"} {
		t.Run(mode, func(t *testing.T) {
			f := setup(t, nil)
			settings := publicReadSettings(t)
			if mode == "implicit-enable" {
				freshReadBootstrap(t, f, settings, &denyReadClient{})
				f.config.Mode = LocalDisposableStagingAuthReceipts
			} else {
				var change func(*localmetadata.AuthObservationPolicy)
				if mode == "endpoint-pin" {
					change = func(p *localmetadata.AuthObservationPolicy) { p.EndpointBindingDigest = sum("foreign-endpoints") }
				}
				if mode == "timeout-reserve" {
					change = func(p *localmetadata.AuthObservationPolicy) {
						p.MaximumDuration = settings.Timeout + AuthReceiptPersistenceReserve - time.Nanosecond
					}
				}
				freshReceiptBootstrap(t, f, settings, &denyReadClient{}, change)
				switch mode {
				case "old-mode":
					f.config.Mode = LocalDisposableStaging
				case "no-policy":
					f.config.MetadataConfig.AuthObservationPolicy = nil
				case "no-registry-pin":
					f.config.RegistryConfig.AuthObservationPolicyDigest = ""
				case "policy-drift":
					f.config.MetadataConfig.AuthObservationPolicy.MaximumPerStage++
				case "reader-drift":
					f.config.Observation.Settings.ReaderBindingID = "other-reader"
				}
			}
			if _, e := OpenInitial(background, f.config); e == nil {
				t.Fatal("receipt actor accepted implicit/drifting bootstrap")
			}
			if f.control.version != 1 || f.cloud.reads != 0 || f.cloud.creates != 0 || f.aws.creates != 0 {
				t.Fatal("invalid receipt configuration claimed admission/provider")
			}
		})
	}
}

func TestAuthReceiptExpiredFailureHistoricalHandoffWithoutSDK(t *testing.T) {
	f, reader, clock := setupReceipt(t)
	a := must(OpenInitial(background, f.config))
	state := stageForRead(t, a, f)
	beforeJournal := mustJournal(t, a, state.Intent.SlotID)
	beforeOutcome := mustOutcome(t, a, state.CandidateWriteID)
	reads := f.cloud.reads
	*clock = now.Add(2 * time.Hour)
	request := recordRequest(state, "known-expired")
	result, e := a.RecordAuthObservation(background, request)
	failure := finiteObservationFailure(e)
	if failure == nil || failure.Code != observation.Expired || result.private.Acknowledgement != ReceiptRecorded || result.private.Receipt == nil || result.private.Receipt.Kind != localmetadata.AuthNotEstablished || reader.calls != 0 || f.cloud.reads != reads {
		t.Fatal("known expiry did not record failure without SDK calls")
	}
	if !equalReceiptMetadata(t, a, beforeJournal, beforeOutcome) {
		t.Fatal("receipt changed staged metadata/version")
	}
	originalSession := result.private.Receipt.Authority.SessionID
	handoff := must(a.Quiesce(background))
	if handoff.value.Schema != 2 || handoff.value.AuthObservationPolicyDigest != f.config.RegistryConfig.AuthObservationPolicyDigest {
		t.Fatal("fresh receipt checkpoint discrimination missing")
	}
	b := must(OpenClean(background, f.config, must(ParseCheckpoint(must(handoff.JSON())))))
	if b.snapshot.Envelope.Admission.SessionID == originalSession {
		t.Fatal("fixture did not change actual admission session")
	}
	historical, e := b.RecordAuthObservation(background, request)
	if e != nil || historical.private.Acknowledgement != ReceiptHistorical || historical.private.Receipt.Authority.SessionID != originalSession || !historical.private.Receipt.RecordedAt.Equal(result.private.Receipt.RecordedAt) || reader.calls != 0 || f.cloud.reads != reads {
		t.Fatal("expired historical receipt became a fresh call or changed provenance")
	}
	// Independent stateless API retains its original expiry gate and no writes.
	before := sum(must(b.metadata.History(background)))
	if _, e := b.ObserveStaged(background, request.ObserveRequest); !errors.Is(e, NotAuthorized) || sum(must(b.metadata.History(background))) != before {
		t.Fatal("receipt lane weakened original stateless expiry/no-write semantics")
	}
}

func mustJournal(t *testing.T, a *Actor, slot string) tc.CredentialState {
	t.Helper()
	s, found, e := a.metadata.CredentialJournal().Load(background, slot)
	if e != nil || !found {
		t.Fatal("missing journal")
	}
	return s
}
func mustOutcome(t *testing.T, a *Actor, write string) awsstore.Outcome {
	t.Helper()
	o, found, e := a.metadata.OutcomeLedger().Load(background, write)
	if e != nil || !found {
		t.Fatal("missing outcome")
	}
	return o
}

func equalReceiptMetadata(t *testing.T, a *Actor, j tc.CredentialState, o awsstore.Outcome) bool {
	t.Helper()
	return sum(mustJournal(t, a, j.Intent.SlotID)) == sum(j) && sum(mustOutcome(t, a, o.Write.WriteID)) == sum(o)
}

func TestAuthReceiptDisabledDeniedAndCapacityNeverRead(t *testing.T) {
	f, reader, clock := setupReceipt(t)
	a := must(OpenInitial(background, f.config))
	state := stageForRead(t, a, f)
	request := recordRequest(state, "denials")
	for _, change := range []func(*RecordObservationRequest){
		func(r *RecordObservationRequest) { r.AttemptID = "bad" },
		func(r *RecordObservationRequest) { r.StageID = sum("foreign") },
		func(r *RecordObservationRequest) { r.CredentialJournalVersion = sum("foreign") },
	} {
		r := request
		change(&r)
		if _, e := a.RecordAuthObservation(background, r); e == nil {
			t.Fatal("foreign record request accepted")
		}
	}
	ctx, cancel := context.WithCancel(background)
	cancel()
	if _, e := a.RecordAuthObservation(ctx, request); e == nil {
		t.Fatal("canceled record accepted")
	}
	header := metadata.NewOutgoingContext(background, metadata.Pairs("authorization", "ambient"))
	if _, e := a.RecordAuthObservation(header, request); !errors.Is(e, NotAuthorized) {
		t.Fatal("inherited auth accepted")
	}
	*clock = now.Add(2 * time.Hour)
	for _, attempt := range []string{"capacity-one", "capacity-two"} {
		result, _ := a.RecordAuthObservation(background, recordRequest(state, attempt))
		if result.private.Acknowledgement != ReceiptRecorded {
			t.Fatal("finite expiry receipt did not fill capacity")
		}
	}
	result, e := a.RecordAuthObservation(background, recordRequest(state, "capacity-three"))
	if !errors.Is(e, AuthReceiptCapacity) || result.private.Acknowledgement != ReceiptRejected || reader.calls != 0 {
		t.Fatal("capacity did not stop before secret read")
	}
	legacy, _ := setupRead(t)
	b := must(OpenInitial(background, legacy.config))
	if _, e := b.RecordAuthObservation(background, request); !errors.Is(e, AuthReceiptDisabled) {
		t.Fatal("legacy read actor gained recording authority")
	}
}

func TestAuthReceiptFiniteOutcomeBoundaryPreservesOriginalFailure(t *testing.T) {
	original := &localmetadata.AuthObservationFailure{Code: observation.IdentityUnproven, Step: observation.Identity}
	for _, input := range []error{localmetadata.Uncertain, localmetadata.Corrupt, errors.New(material)} {
		result := recordObservationState{ObservationFailure: original, Receipt: &localmetadata.AuthObservationReceipt{}}
		e := recordReceiptOutcome(&result, localmetadata.AuthObservationReceipt{}, input)
		if !errors.Is(e, AuthReceiptOutcomeUnknown) || result.Acknowledgement != ReceiptUnknown || result.Receipt != nil || result.ObservationFailure != original || bytes.Contains(must(json.Marshal(result)), []byte(material)) {
			t.Fatal("unknown receipt ACK erased original cause or disclosed data")
		}
	}
	result := recordObservationState{ObservationFailure: original}
	if e := recordReceiptOutcome(&result, localmetadata.AuthObservationReceipt{}, localmetadata.Capacity); !errors.Is(e, AuthReceiptCapacity) || result.Acknowledgement != ReceiptRejected || result.ObservationFailure != original {
		t.Fatal("proven capacity refusal lost separate cause")
	}
	for _, e := range []error{(*observation.Error)(nil), &observation.Error{Code: observation.Code(material), Step: observation.Identity}, &observation.Error{Code: observation.Denied, Step: observation.Step(material)}, errors.New(material)} {
		if finiteObservationFailure(e) != nil {
			t.Fatal("unbounded error entered finite receipt")
		}
	}
}

func TestAuthReceiptZeroCheckpointBytesAndPolicyCopies(t *testing.T) {
	f, _ := setupRead(t)
	a := must(OpenInitial(background, f.config))
	h := must(a.Quiesce(background))
	type legacyCheckpoint struct {
		Schema        int
		Registry      RegistryBinding
		Metadata      localmetadata.Binding
		HistoryCount  int
		HistoryDigest string
		Handoff       []byte
	}
	legacy := legacyCheckpoint{h.value.Schema, h.value.Registry, h.value.Metadata, h.value.HistoryCount, h.value.HistoryDigest, h.value.Handoff}
	if !bytes.Equal(must(h.JSON()), must(json.Marshal(legacy))) {
		t.Fatal("zero receipt changed legacy checkpoint bytes")
	}
	bad := h
	bad.value.Schema = 2
	if _, e := bad.JSON(); e == nil {
		t.Fatal("schema2 without policy accepted")
	}
	bad = h
	bad.value.AuthObservationPolicyDigest = sum("foreign-policy")
	if _, e := bad.JSON(); e == nil {
		t.Fatal("schema1 with receipt policy accepted")
	}
	f, _, _ = setupReceipt(t)
	a = must(OpenInitial(background, f.config))
	digest := a.authReceiptPolicy.EndpointBindingDigest
	f.config.MetadataConfig.AuthObservationPolicy.EndpointBindingDigest = sum("caller-mutated")
	if a.authReceiptPolicy.EndpointBindingDigest != digest || a.config.MetadataConfig.AuthObservationPolicy.EndpointBindingDigest != digest {
		t.Fatal("caller can mutate retained policy")
	}
}

func TestAuthReceiptControlResultOpaqueAndSafeCopiedView(t *testing.T) {
	privateARN := "arn:aws:secretsmanager:us-west-2:123456789012:secret:PRIVATE-SECRET-LOCATOR-AbC123"
	privateSession := strings.Repeat("b", 64)
	privateCell := "private-control-cell-provenance-712"
	receipt := &localmetadata.AuthObservationReceipt{ID: sum("receipt-id"), Version: sum("receipt-version"), Contract: localmetadata.AuthObservationReceiptContract, Kind: localmetadata.AuthNotEstablished, RecordedAt: now, Request: localmetadata.AuthObservationRequest{CandidateARN: privateARN}, Authority: localmetadata.AuthObservationAuthority{SessionID: privateSession, ControlCellUID: privateCell, WriterID: "private-writer-provenance"}}
	r := RecordObservationResult{private: &recordObservationState{AttemptID: sum("attempt"), Acknowledgement: ReceiptHistorical, Receipt: receipt, ObservationFailure: &localmetadata.AuthObservationFailure{Code: observation.IdentityUnproven, Step: observation.Identity}}}
	for _, value := range []any{r, &r, struct{ Result RecordObservationResult }{r}, map[string]any{"result": r}} {
		for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d", "%p", "%f", "%e", "%g", "%t", "%T", "%b", "%c", "%U", "%o", "%O"} {
			printed := fmt.Sprintf(verb, value)
			for _, private := range []string{privateARN, privateSession, privateCell, "private-writer-provenance"} {
				if strings.Contains(printed, private) {
					t.Fatal("control result formatting disclosed private receipt/provenance")
				}
			}
		}
		if _, e := json.Marshal(value); e == nil || !errors.Is(e, AuthReceiptPrivacyDenied) {
			t.Fatal("control result serialized into JSON")
		}
	}
	if _, e := r.MarshalText(); !errors.Is(e, AuthReceiptPrivacyDenied) {
		t.Fatal("control result text serialization allowed")
	}
	v := r.View()
	raw := must(json.Marshal(v))
	if v.Contract != AuthReceiptViewContract || v.Acknowledgement != ReceiptHistorical || v.ReceiptID != receipt.ID || v.ReceiptVersion != receipt.Version || v.Kind != localmetadata.AuthNotEstablished || !v.RecordedAt.Equal(now) || v.ObservationFailure.Code != observation.IdentityUnproven || len(raw) > 2048 {
		t.Fatal("safe view lost useful finite receipt facts")
	}
	for _, private := range []string{privateARN, privateSession, privateCell, "private-writer-provenance"} {
		if bytes.Contains(raw, []byte(private)) || strings.Contains(fmt.Sprintf("%+v", v), private) {
			t.Fatal("safe view disclosed private receipt/provenance")
		}
	}
	v.ObservationFailure.Code = observation.Code("caller-mutated")
	if r.View().ObservationFailure.Code != observation.IdentityUnproven {
		t.Fatal("safe view aliases local control state")
	}
}
