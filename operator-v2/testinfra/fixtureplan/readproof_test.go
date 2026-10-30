package fixtureplan

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestReadProofInitialIsDeterministicMetadataAndDenyAllOnly(t *testing.T) {
	before := mustJSON(t, mustRender(t, nil).Terraform)
	r, e := RenderReadProofInitial()
	if e != nil {
		t.Fatal(e)
	}
	resources := r.Initial["resource"].(Object)
	count := 0
	for kind, group := range resources {
		count += len(group.(Object))
		if strings.HasPrefix(kind, "aws_kms") || kind == "aws_secretsmanager_secret_version" || kind == "aws_codebuild_project" {
			t.Fatal("read-proof initial creates crypto/build/value resources")
		}
	}
	if count != 7 || r.Review["awsCreates"] != 6 || r.Review["applyAuthorized"] != false || r.Review["baseResourcesModified"] != false {
		t.Fatal("initial review inventory/authorization incorrect")
	}
	deny := resources["aws_iam_policy"].(Object)["reader_boundary"].(Object)["policy"].(string)
	for _, label := range []string{"reader", "trap"} {
		if resources["aws_iam_role_policy"].(Object)[label].(Object)["policy"] != deny {
			t.Fatal("initial principal has non-deny inline permission")
		}
	}
	if strings.Contains(deny, `"Allow"`) || !strings.Contains(deny, `"Deny"`) {
		t.Fatal("initial boundary grants authority")
	}
	if strings.Contains(resources["aws_iam_role"].(Object)["reader"].(Object)["assume_role_policy"].(string), `"Allow"`) {
		t.Fatal("initial reader trust enabled")
	}
	n, e := ReadProofResourceNames()
	if e != nil {
		t.Fatal(e)
	}
	if resources["aws_iam_role"].(Object)["trap"].(Object)["permissions_boundary"] != DenyBoundaryARN || !strings.Contains(resources["aws_iam_role"].(Object)["trap"].(Object)["assume_role_policy"].(string), roleARN(n.ReaderRole)) {
		t.Fatal("trap does not have exact reader trust+existing deny boundary")
	}
	if resources["aws_secretsmanager_secret"].(Object)["allow"].(Object)["kms_key_id"] != BaseCandidateKeyARN {
		t.Fatal("initial secret uses unowned/wrong key")
	}
	if _, exists := r.Initial["variable"].(Object)["fixture_expires_at"].(Object)["default"]; exists {
		t.Fatal("initial fixture silently chooses live expiry")
	}
	if r.Initial["terraform"].(Object)["backend"].(Object)["local"].(Object)["path"] != ".state/"+ReadProofID+".tfstate" {
		t.Fatal("readproof shares state")
	}
	again, e := RenderReadProofInitial()
	if e != nil || !bytes.Equal(mustJSON(t, r), mustJSON(t, again)) {
		t.Fatal("offline rendering nondeterministic")
	}
	if !bytes.Equal(before, mustJSON(t, mustRender(t, nil).Terraform)) {
		t.Fatal("offline reader rendering modified base")
	}
}

func TestReadProofSeedsUseFixedTokensAndSmallInertBodiesOutsideState(t *testing.T) {
	r, e := RenderReadProofInitial()
	if e != nil {
		t.Fatal(e)
	}
	seen := map[string]bool{}
	for _, seed := range r.Seeds {
		body, e := ReadProofSeedBody(seed.Label)
		if e != nil || len(body) > 64 || len(body) != seed.Bytes || rawSHA(body) != seed.BodySHA256 || len(seed.VersionID) != 64 || seen[seed.VersionID] {
			t.Fatal("invalid fixed seed intent")
		}
		seen[seed.VersionID] = true
		if bytes.Contains(mustJSON(t, r.Initial), body) || bytes.Contains(mustJSON(t, r.Review), body) {
			t.Fatal("seed bytes entered IaC/review")
		}
		if seed.Label == "allow-v2" && seed.Stage != "RV2-PERMISSION-DENY-V2" {
			t.Fatal("wrong version seed would move AWSCURRENT")
		}
		clear(body)
	}
	if len(seen) != 3 {
		t.Fatal("seed inventory changed")
	}
	if _, e := ReadProofSeedBody("arbitrary-payload"); e == nil {
		t.Fatal("arbitrary payload input accepted")
	}
}

func readProofBindingFixture(t *testing.T) (ReadProofBindings, time.Time) {
	t.Helper()
	n, e := ReadProofResourceNames()
	if e != nil {
		t.Fatal(e)
	}
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	return ReadProofBindings{AllowSecretARN: arn("secretsmanager", "secret:"+n.Secret+"-AbC123"), AllowSecretCreatedAt: now.Add(-time.Minute), AllowSecretCreationReceiptSHA256: digest("owned-created-secret"), ReaderRoleID: "AROA0123456789ABCDEFG", ReaderRoleCreatedAt: now.Add(-time.Minute), ReaderCreationReceiptSHA256: digest("owned-created-reader"), TrapRoleID: "AROA1234567890ABCDEFG", TrapRoleCreatedAt: now.Add(-time.Minute), TrapCreationReceiptSHA256: digest("owned-created-trap"), SeedReceiptSHA256: digest("all-three-native-seed-receipts")}, now
}

func TestReadProofEnableNeedsAttestedBindingsAndShortExpiry(t *testing.T) {
	b, now := readProofBindingFixture(t)
	for label, change := range map[string]func(*ReadProofBindings){
		"wrong-name":      func(b *ReadProofBindings) { b.AllowSecretARN = arn("secretsmanager", "secret:unrelated-AbC123") },
		"partial-arn":     func(b *ReadProofBindings) { b.AllowSecretARN = strings.TrimSuffix(b.AllowSecretARN, "-AbC123") },
		"unproven-secret": func(b *ReadProofBindings) { b.AllowSecretCreationReceiptSHA256 = "" },
		"unproven-reader": func(b *ReadProofBindings) { b.ReaderCreationReceiptSHA256 = "" },
		"unproven-trap":   func(b *ReadProofBindings) { b.TrapCreationReceiptSHA256 = "" },
		"no-seed":         func(b *ReadProofBindings) { b.SeedReceiptSHA256 = "" },
		"future-birth":    func(b *ReadProofBindings) { b.ReaderRoleCreatedAt = now.Add(time.Second) },
		"same-role":       func(b *ReadProofBindings) { b.TrapRoleID = b.ReaderRoleID },
	} {
		t.Run(label, func(t *testing.T) {
			bad := b
			change(&bad)
			if _, e := RenderReadProofEnabled(bad, now.Add(10*time.Minute), now); e == nil {
				t.Fatal("unattested enable render accepted")
			}
		})
	}
	for _, deadline := range []time.Time{now, now.Add(-time.Second), now.Add(15*time.Minute + time.Nanosecond)} {
		if _, e := RenderReadProofEnabled(b, deadline, now); e == nil {
			t.Fatal("unbounded/expired policy accepted")
		}
	}
}

func TestReadProofExactVersionPolicyAndSingleBaseKeyOwner(t *testing.T) {
	b, now := readProofBindingFixture(t)
	r, e := RenderReadProofEnabled(b, now.Add(10*time.Minute), now)
	if e != nil {
		t.Fatal(e)
	}
	res := r.Companion["resource"].(Object)
	reader := res["aws_iam_policy"].(Object)["reader_boundary"].(Object)["policy"].(string)
	if reader != res["aws_iam_role_policy"].(Object)["reader"].(Object)["policy"] {
		t.Fatal("boundary/inline proof scopes differ")
	}
	for _, document := range []string{reader, res["aws_iam_role_policy"].(Object)["reader"].(Object)["policy"].(string)} {
		var wire Object
		if json.Unmarshal([]byte(document), &wire) != nil {
			t.Fatal("invalid reader permission document")
		}
		expired := wire["Statement"].([]any)[6].(map[string]any)
		if expired["Effect"] != "Deny" || expired["Action"] != "*" || expired["Condition"].(map[string]any)["DateGreaterThanEquals"].(map[string]any)["aws:CurrentTime"] != now.Add(10*time.Minute).Format(time.RFC3339Nano) {
			t.Fatal("boundary or inline lacks exact unconditional expiry denial")
		}
	}
	if len(res["aws_iam_policy"].(Object)) != 1 || res["aws_iam_role"].(Object)["trap"].(Object)["permissions_boundary"] != DenyBoundaryARN {
		t.Fatal("companion manages shared deny boundary")
	}
	for kind := range res {
		if strings.HasPrefix(kind, "aws_kms_") {
			t.Fatal("companion introduces second base key state owner")
		}
	}
	var doc Object
	if json.Unmarshal([]byte(reader), &doc) != nil {
		t.Fatal("bad policy")
	}
	statements := doc["Statement"].([]any)
	if len(statements) != 7 {
		t.Fatal("unexpected effective policy inventory")
	}
	read := statements[1].(map[string]any)
	if read["Resource"].([]any)[0] != b.AllowSecretARN || read["Condition"].(map[string]any)["StringEquals"].(map[string]any)["secretsmanager:VersionId"] != ReadProofSeeds()[0].VersionID {
		t.Fatal("reader not pinned to exact native allowed version")
	}
	if strings.Contains(reader, "kms:EncryptionContext:SecretVersionId") || strings.Contains(string(mustJSON(t, r.BaseKeyPolicyDelta)), "kms:EncryptionContext:SecretVersionId") {
		t.Fatal("KMS version condition confounds wrong-version proof")
	}
	for _, key := range []string{"kms:ViaService", "kms:CallerAccount", "kms:EncryptionContext:SecretARN", "NoOtherActions", "NoOtherSecrets", "NoOtherKeys", "ProofExpired"} {
		if !strings.Contains(reader, key) {
			t.Fatal("reader policy missing explicit scope/expiry")
		}
	}
	if r.BaseKeyPolicyDelta["resourceAddress"] != "aws_kms_key.candidate" || r.BaseKeyPolicyDelta["standaloneTFResourceForbidden"] != true || r.BaseKeyPolicyDelta["onlyAddedSid"] != ReadProofKeySid {
		t.Fatal("base key delta has second state owner")
	}
	after := r.BaseKeyPolicyDelta["afterDocument"].(Object)["Statement"].([]any)
	if len(after) != 3 || after[2].(Object)["Sid"] != ReadProofKeySid {
		t.Fatal("key delta changes more than one statement")
	}
	base := mustRender(t, nil)
	original := base.Terraform["resource"].(Object)["aws_kms_key"].(Object)["candidate"].(Object)["policy"].(string)
	if r.BaseKeyPolicyDelta["expectedBeforeSHA256"] != rawSHA([]byte(original)) || bytes.Contains([]byte(original), []byte(ReadProofKeySid)) {
		t.Fatal("base policy changed during readproof prep")
	}
	if r.Review["applyAuthorized"] != false || r.Review["probeAuthorized"] != false {
		t.Fatal("render self-authorized live execution")
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, e := json.Marshal(v)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func mustRender(t *testing.T, c *Candidate) Rendered {
	t.Helper()
	r, e := Render(c)
	if e != nil {
		t.Fatal(e)
	}
	return r
}
