package fixtureplan

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"time"
)

const ReadProofID = RunID + "-aws-readproof"
const ReadProofContract = "registry-aws-exact-secret-permission-proof-v1"
const ReadProofKeySid = "SyntheticReaderOnlyViaExactSecretsManagerSecret"
const BaseCandidateKeyARN = "arn:aws:kms:us-west-2:429214323166:key/b4f03dbd-fe40-431f-b5ea-3e08d4871c67"
const DenyKeyARN = "arn:aws:kms:us-west-2:429214323166:key/3157ff01-3ed9-4771-8a5a-73c60019d81e"
const DenySecretARN = "arn:aws:secretsmanager:us-west-2:429214323166:secret:rv2/9026c9a8c2a2e28594174cd400392f80/rv2-deny-secret-primary-6d4c3e8f5a00c3cd44a96a46e65087fb-L57UtM"
const DenyBoundaryARN = "arn:aws:iam::429214323166:policy/rv2-deny-build-boundary-primar-b3f63acfc2421992cf39852c9f09f23f"

func rawSHA(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

type ReadProofNames struct{ Secret, ReaderRole, ReaderBoundary, TrapRole string }
type ReadProofSeed struct {
	Label, Target, VersionID, Stage, BodySHA256 string
	Bytes                                       int
}
type ReadProofPrepared struct {
	Initial        Object
	Review         Object
	Seeds          []ReadProofSeed
	EnableTemplate Object
}

func ReadProofResourceNames() (ReadProofNames, error) {
	c, e := Context()
	if e != nil {
		return ReadProofNames{}, e
	}
	var n ReadProofNames
	n.Secret, e = c.Path("aws", "permission-allow-secret", "permission-read-v1")
	if e != nil {
		return n, e
	}
	n.ReaderRole, e = c.Name("aws", "permission-reader-role", "permission-read-v1", 63)
	if e != nil {
		return n, e
	}
	n.ReaderBoundary, e = c.Name("aws", "permission-reader-boundary", "permission-read-v1", 63)
	if e != nil {
		return n, e
	}
	n.TrapRole, e = c.Name("aws", "permission-trap-role", "permission-read-v1", 63)
	return n, e
}

// ReadProofSeedBody is fixed INERT non-credential data. There is no arbitrary
// payload input and no real token generation. SDK helpers still keep it out of
// state/logs/output and clear temporary copies, like real secret material.
func ReadProofSeedBody(label string) ([]byte, error) {
	if label != "allow-v1" && label != "allow-v2" && label != "deny-v1" {
		return nil, errors.New("unknown synthetic seed label")
	}
	return []byte("rv2-inert-" + digest(struct{ Contract, Proof, Label string }{ReadProofContract, ReadProofID, label})[:32]), nil
}
func ReadProofSeeds() []ReadProofSeed {
	var result []ReadProofSeed
	for _, label := range []string{"allow-v1", "allow-v2", "deny-v1"} {
		body, _ := ReadProofSeedBody(label)
		seed := ReadProofSeed{Label: label, Target: "allow", VersionID: digest(struct{ Contract, Proof, Version string }{ReadProofContract, ReadProofID, label}), Stage: "AWSCURRENT", BodySHA256: rawSHA(body), Bytes: len(body)}
		if label == "allow-v2" {
			seed.Stage = "RV2-PERMISSION-DENY-V2"
		}
		if label == "deny-v1" {
			seed.Target = "deny"
			seed.Stage = "RV2-PERMISSION-DENY-V1"
		}
		clear(body)
		result = append(result, seed)
	}
	return result
}

// RenderReadProofInitial emits only initial deny-all principals and metadata.
// It never changes the base key/candidate roles or writes a secret version.
func RenderReadProofInitial() (ReadProofPrepared, error) {
	c, e := Context()
	if e != nil {
		return ReadProofPrepared{}, e
	}
	n, e := ReadProofResourceNames()
	if e != nil {
		return ReadProofPrepared{}, e
	}
	base, e := Render(nil)
	if e != nil {
		return ReadProofPrepared{}, e
	}
	resources := Object{}
	guard := base.Terraform["resource"].(Object)["terraform_data"].(Object)["scope_guard"].(Object)
	guard["input"] = ReadProofID
	guard["lifecycle"].(Object)["precondition"].([]Object)[1]["error_message"] = "Review a future fixture expiration no more than 24 hours from provisioning."
	guard["lifecycle"].(Object)["precondition"].([]Object)[1]["condition"] = ref("timecmp(timestamp(), var.fixture_expires_at) < 0 && timecmp(var.fixture_expires_at, timeadd(timestamp(), \"24h\")) <= 0")
	resourcesFor(resources, "terraform_data", "scope_guard", guard)
	tags := Object{"RegistryFixture": "registry-operator-v2", "RegistryOwner": digest(c.Owner()), "RegistryRun": RunID, "RegistryProof": ReadProofID, "RegistryExpiresAt": ref("var.fixture_expires_at")}
	deny := encoded(policy(statement("SyntheticReaderInitiallyDisabled", "Deny", []string{"*"}, []any{"*"})))
	noAssume := encoded(policy(Object{"Effect": "Deny", "Principal": "*", "Action": "sts:AssumeRole"}))
	resourcesFor(resources, "aws_iam_policy", "reader_boundary", Object{"name": n.ReaderBoundary, "description": "Owned synthetic read proof only; initially deny all", "policy": deny, "tags": tags, "depends_on": []string{"terraform_data.scope_guard"}})
	resourcesFor(resources, "aws_iam_role", "reader", Object{"name": n.ReaderRole, "assume_role_policy": noAssume, "permissions_boundary": ref("aws_iam_policy.reader_boundary.arn"), "max_session_duration": 3600, "tags": tags, "depends_on": []string{"terraform_data.scope_guard"}})
	resourcesFor(resources, "aws_iam_role_policy", "reader", Object{"name": "synthetic-read-proof-only", "role": ref("aws_iam_role.reader.id"), "policy": deny})
	trapTrust := encoded(policy(Object{"Effect": "Allow", "Principal": Object{"AWS": roleARN(n.ReaderRole)}, "Action": "sts:AssumeRole"}))
	resourcesFor(resources, "aws_iam_role", "trap", Object{"name": n.TrapRole, "assume_role_policy": trapTrust, "permissions_boundary": DenyBoundaryARN, "max_session_duration": 3600, "tags": tags, "depends_on": []string{"aws_iam_role.reader"}})
	resourcesFor(resources, "aws_iam_role_policy", "trap", Object{"name": "synthetic-trap-deny-all", "role": ref("aws_iam_role.trap.id"), "policy": deny})
	resourcesFor(resources, "aws_secretsmanager_secret", "allow", Object{"name": n.Secret, "description": "Owned inert AWS permission proof; synthetic versions seeded outside IaC only", "kms_key_id": BaseCandidateKeyARN, "recovery_window_in_days": 7, "tags": tags, "depends_on": []string{"terraform_data.scope_guard"}})
	initial := base.Terraform
	initial["resource"] = resources
	initial["terraform"].(Object)["backend"] = Object{"local": Object{"path": ".state/" + ReadProofID + ".tfstate"}}
	// Deliberately no default: a root-reviewed fresh expiry is required later.
	initial["variable"].(Object)["fixture_expires_at"].(Object)["description"] = "Review a future <=24h fixture expiry; no automatic TTL cleanup"
	delete(initial["variable"].(Object)["fixture_expires_at"].(Object), "default")
	initial["output"] = Object{"read_proof_bindings": Object{"value": Object{"proof_id": ReadProofID, "account": Account, "region": Region, "owner": c.Owner(), "reader_role_arn": ref("aws_iam_role.reader.arn"), "reader_role_id": ref("aws_iam_role.reader.unique_id"), "trap_role_arn": ref("aws_iam_role.trap.arn"), "trap_role_id": ref("aws_iam_role.trap.unique_id"), "allow_secret_arn": ref("aws_secretsmanager_secret.allow.arn"), "base_key_arn": BaseCandidateKeyARN}}}
	seeds := ReadProofSeeds()
	review := Object{"schemaVersion": ReadProofContract, "stage": "InitialDenyAllOnly", "applyAuthorized": false, "seedAuthorized": false, "assumeAuthorized": false, "probeAuthorized": false, "account": Account, "region": Region, "owner": c.Owner(), "proofID": ReadProofID, "names": n, "terraformCreates": 7, "awsCreates": 6, "additionalKeys": 0, "additionalBuilds": 0, "additionalNamespaces": 0, "baseResourcesModified": false, "candidateCanaryStillDenyAll": true, "initialTrust": "Reader deny-all; trap trusts exact reader but boundary+inline deny all", "state": "Separate private ignored .state/" + ReadProofID + ".tfstate", "existingBindingsNeedFreshAttestation": Object{"baseKeyARN": BaseCandidateKeyARN, "baseKeyBorn": "2026-10-30T01:56:02.904Z", "denyKeyARN": DenyKeyARN, "denyKeyBorn": "2026-10-30T02:17:42.931Z", "denySecretARN": DenySecretARN, "denySecretBorn": "2026-10-30T02:18:00.749Z", "sharedDenyBoundaryARN": DenyBoundaryARN}, "seedCount": 3, "seedBytes": 126, "readerSessionSeconds": 900, "maximumProbeCalls": 7, "maximumBootstrapProofCalls": 4, "maximumProofMinutes": 5, "authorizationMinutes": 15, "maximumCadencePerSecond": 1, "softUSDStop": 5, "hardDollarFence": false, "requiredBeforeLive": []string{"root exact names/source/helper/initial saved-plan review", "fresh exact identities/state/absence/account and approved counts/quota reserve review", "initial new-only apply+canonical RoleID/birth/secret provenance attestation", "root review of three fixed seed requests/body hashes; no value in state/logs", "native valid V1/V2/denyV1 seed receipts before enable/assume", "separate root-reviewed exact scoped reader policy and SAME base-owner single CMK Sid delta", "fresh signed short expiry + opaque bootstrap-to-scoped delivery review; no ambient fallback"}, "proofLimits": []string{"positive own read must pass before any negative", "V2 SecretVersionId is NOT also restricted at KMS: avoid confounding", "wrong-key GenerateDataKey denial is combined/no-direct authority, not isolated key-condition proof", "no live PassRole/build/override proof in this lane", "GetCallerIdentity proves signing identity, not permissions", "synthetic names/values are NOT temporalcloud CredentialIntent or production credentials"}, "cleanup": []string{"stop/drain helper and discard temporary creds; unknowns retain state/evidence", "reader inline+boundary deny-all and restore exact base key policy via ORIGINAL state owner before deleting principal", "delete only new roles/inline/boundary after RoleID/attachments check; NEVER shared deny-boundary", "new allow-secret delete recovery7d only after exact identity check", "existing deny-secret seed retained until parent fixture cleanup; versions have no standalone delete API", "no automatic destroy/recreate/import or original CMK deletion"}}
	template := Object{"schemaVersion": ReadProofContract, "executable": false, "reason": "Await new canonical ARN/RoleID/birth/provenance and successful native seed receipts; NO name/prefix adoption", "requiredBindingFields": []string{"AllowSecretARN(full provider suffix)", "AllowSecretCreatedAt", "AllowSecretCreationReceiptSHA256", "ReaderRoleID", "ReaderRoleCreatedAt", "ReaderCreationReceiptSHA256", "TrapRoleID", "TrapRoleCreatedAt", "TrapCreationReceiptSHA256", "SeedReceiptSHA256(all3)", "ProofExpiresAt<=15m"}, "singleExistingStateOwner": Object{"address": "aws_kms_key.candidate", "state": "fixture-20261030-01.tfstate", "sid": ReadProofKeySid, "onlyExpectedBaseChange": "key policy append1statement; preserve candidate/canary denyall/admin/recovery and existing expiry/tags", "rollback": "remove exactSid via same base owner, no second key-policy resource/import"}, "seedReview": seeds}
	return ReadProofPrepared{Initial: initial, Review: review, Seeds: seeds, EnableTemplate: template}, nil
}

type ReadProofBindings struct {
	AllowSecretARN                            string
	AllowSecretCreatedAt                      time.Time
	AllowSecretCreationReceiptSHA256          string
	ReaderRoleID, ReaderCreationReceiptSHA256 string
	ReaderRoleCreatedAt                       time.Time
	TrapRoleID, TrapCreationReceiptSHA256     string
	TrapRoleCreatedAt                         time.Time
	SeedReceiptSHA256                         string
}
type ReadProofEnabled struct {
	Companion          Object
	BaseKeyPolicyDelta Object
	Review             Object
}

// ValidateReadProofMetadata checks trusted bootstrap reference structure only;
// SHA receipt fields are NOT independent live attestation. Seed preparation
// needs native metadata before a seed receipt exists, so it cannot pretend the
// enable gate has already been met. Live preflight remains a separate action.
func ValidateReadProofMetadata(b ReadProofBindings, now time.Time) error {
	return validateReadProofBindings(b, now, false)
}

func validateReadProofBindings(b ReadProofBindings, now time.Time, seeded bool) error {
	n, e := ReadProofResourceNames()
	if e != nil {
		return e
	}
	secretPrefix := arn("secretsmanager", "secret:"+n.Secret+"-")
	sha := regexp.MustCompile(`^[a-f0-9]{64}$`)
	roleID := regexp.MustCompile(`^AROA[A-Z0-9]{17}$`)
	if now.IsZero() || !strings.HasPrefix(b.AllowSecretARN, secretPrefix) || !regexp.MustCompile(`^[A-Za-z0-9]{6}$`).MatchString(strings.TrimPrefix(b.AllowSecretARN, secretPrefix)) || !roleID.MatchString(b.ReaderRoleID) || !roleID.MatchString(b.TrapRoleID) || b.ReaderRoleID == b.TrapRoleID || b.AllowSecretCreatedAt.IsZero() || b.ReaderRoleCreatedAt.IsZero() || b.TrapRoleCreatedAt.IsZero() || b.AllowSecretCreatedAt.After(now) || b.ReaderRoleCreatedAt.After(now) || b.TrapRoleCreatedAt.After(now) || !sha.MatchString(b.AllowSecretCreationReceiptSHA256) || !sha.MatchString(b.ReaderCreationReceiptSHA256) || !sha.MatchString(b.TrapCreationReceiptSHA256) || seeded && !sha.MatchString(b.SeedReceiptSHA256) {
		return errors.New("unproven synthetic reader metadata references")
	}
	return nil
}

// RenderReadProofEnabled is inert private control preparation. It requires
// independently attested new bindings; names/tags alone cannot enable it.
// Delta is NOT a standalone TF key-policy manager: use the original base owner.
func RenderReadProofEnabled(b ReadProofBindings, expiresAt, now time.Time) (ReadProofEnabled, error) {
	n, e := ReadProofResourceNames()
	if e != nil {
		return ReadProofEnabled{}, e
	}
	if validateReadProofBindings(b, now, true) != nil || !expiresAt.After(now) || expiresAt.After(now.Add(15*time.Minute)) {
		return ReadProofEnabled{}, errors.New("unproven synthetic reader bindings or expiry")
	}
	prepared, e := RenderReadProofInitial()
	if e != nil {
		return ReadProofEnabled{}, e
	}
	deadline := expiresAt.UTC().Format(time.RFC3339Nano)
	cryptoConditions := Object{"StringEquals": Object{"kms:ViaService": "secretsmanager." + Region + ".amazonaws.com", "kms:CallerAccount": Account, "kms:EncryptionContext:SecretARN": b.AllowSecretARN}}
	describe := statement("DescribeOnlyOwnedSyntheticSecret", "Allow", []string{"secretsmanager:DescribeSecret"}, []any{b.AllowSecretARN})
	read := statement("ReadOnlyExactSyntheticVersion", "Allow", []string{"secretsmanager:GetSecretValue"}, []any{b.AllowSecretARN})
	read["Condition"] = Object{"StringEquals": Object{"secretsmanager:VersionId": prepared.Seeds[0].VersionID}}
	crypto := statement("DecryptOnlyViaExactSyntheticSecret", "Allow", []string{"kms:Decrypt"}, []any{BaseCandidateKeyARN})
	crypto["Condition"] = cryptoConditions
	statements := []Object{describe, read, crypto, {"Sid": "NoOtherActions", "Effect": "Deny", "NotAction": []string{"secretsmanager:DescribeSecret", "secretsmanager:GetSecretValue", "kms:Decrypt", "sts:GetCallerIdentity"}, "Resource": "*"}, {"Sid": "NoOtherSecrets", "Effect": "Deny", "Action": []string{"secretsmanager:DescribeSecret", "secretsmanager:GetSecretValue"}, "NotResource": b.AllowSecretARN}, {"Sid": "NoOtherKeys", "Effect": "Deny", "Action": "kms:Decrypt", "NotResource": BaseCandidateKeyARN}, {"Sid": "ProofExpired", "Effect": "Deny", "Action": "*", "Resource": "*", "Condition": Object{"DateGreaterThanEquals": Object{"aws:CurrentTime": deadline}}}}
	readerPolicy := encoded(policy(statements...))
	res := prepared.Initial["resource"].(Object)
	res["aws_iam_policy"].(Object)["reader_boundary"].(Object)["policy"] = readerPolicy
	res["aws_iam_role_policy"].(Object)["reader"].(Object)["policy"] = readerPolicy
	res["aws_iam_role"].(Object)["reader"].(Object)["assume_role_policy"] = encoded(policy(Object{"Effect": "Allow", "Principal": Object{"AWS": ProvisionerRole}, "Action": "sts:AssumeRole", "Condition": Object{"StringEquals": Object{"sts:ExternalId": ReadProofID}, "DateLessThan": Object{"aws:CurrentTime": deadline}}}))
	base, e := Render(nil)
	if e != nil {
		return ReadProofEnabled{}, e
	}
	oldDoc := base.Terraform["resource"].(Object)["aws_kms_key"].(Object)["candidate"].(Object)["policy"].(string)
	var document Object
	if json.Unmarshal([]byte(oldDoc), &document) != nil {
		return ReadProofEnabled{}, errors.New("invalid pinned base key policy")
	}
	grant := Object{"Sid": ReadProofKeySid, "Effect": "Allow", "Principal": Object{"AWS": roleARN(n.ReaderRole)}, "Action": "kms:Decrypt", "Resource": "*", "Condition": Object{"StringEquals": cryptoConditions["StringEquals"], "DateLessThan": Object{"aws:CurrentTime": deadline}}}
	document["Statement"] = append(document["Statement"].([]any), grant)
	delta := Object{"schemaVersion": ReadProofContract, "applyAuthorized": false, "standaloneTFResourceForbidden": true, "stateOwner": "fixture-20261030-01.tfstate", "resourceAddress": "aws_kms_key.candidate", "expectedCanonicalKeyARN": BaseCandidateKeyARN, "expectedBeforeSHA256": rawSHA([]byte(oldDoc)), "afterDocument": document, "afterSHA256": rawSHA([]byte(encoded(document))), "onlyAddedSid": ReadProofKeySid, "restoreExactDocument": json.RawMessage(oldDoc), "noKMSVersionIdCondition": true}
	review := Object{"schemaVersion": ReadProofContract, "executable": false, "applyAuthorized": false, "assumeAuthorized": false, "seedAuthorized": false, "probeAuthorized": false, "proofID": ReadProofID, "expiresAt": deadline, "bindingSHA256": digest(b), "bindings": b, "seeds": prepared.Seeds, "readerPolicySHA256": rawSHA([]byte(readerPolicy)), "readerTrustPrincipal": ProvisionerRole, "externalId": ReadProofID, "sessionSeconds": 900, "expectedCompanionUpdates": []string{"aws_iam_policy.reader_boundary.policy", "aws_iam_role_policy.reader.policy", "aws_iam_role.reader.assume_role_policy"}, "expectedBaseUpdates": []string{"aws_kms_key.candidate.policy"}, "productionAndCandidateLanesUntouched": true}
	return ReadProofEnabled{Companion: prepared.Initial, BaseKeyPolicyDelta: delta, Review: review}, nil
}
