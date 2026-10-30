// Package fixtureplan renders the exact, staged shared-account sandbox plan.
// Rendering and validation perform no credential lookup or provider operation.
package fixtureplan

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
)

const Account = "429214323166"
const Region = "us-west-2"
const TemporalAccount = "a2dd6"

// Explicit selection after exact account catalog/GetRegion preflight. The AWS
// fixture region stays us-west-2; this is never an automatic region fallback.
const TemporalNamespaceRegion = "aws-us-east-2"
const RunID = "fixture-20261030-01"
const AWSProviderVersion = "6.71.0"
const ProvisionerRole = "arn:aws:iam::429214323166:role/aws-reserved/sso.amazonaws.com/us-west-2/AWSReservedSSO_AWSAdministratorAccess_ce84ce92ec884c3c"
const Profile = "SolutionsArchitecture/AWSAdministratorAccess"

type Object = map[string]any
type Candidate struct {
	// Independently persisted namespace creation provenance. This is required
	// before even rendering the second policy, never inferred by name.
	Binding tc.Binding
	Intent  tc.CredentialIntent
}
type Rendered struct {
	Terraform Object
	Review    Object
	Temporal  Object
}
type names struct {
	build, repository, logs, keyAlias string
	roles, boundaries                 map[string]string
	control, target, project          string
}

func Context() (ownership.Context, error) {
	return ownership.NewContext(ownership.Config{Owner: ownership.Owner{EnvironmentID: "registry-v2-lab", InstallationID: "registry-v2-fixtures", ProjectUID: "sentinel-20261030-01", Lifetime: ownership.Disposable, RunID: RunID}, ResourceRoot: "rv2", Scopes: []ownership.Scope{{Provider: ownership.AWS, Partition: "aws", AccountID: Account, Region: Region}, {Provider: ownership.Temporal, AccountID: TemporalAccount, Namespace: "fixture-authority"}}})
}
func derive(c ownership.Context) (names, error) {
	n := names{roles: map[string]string{}, boundaries: map[string]string{}}
	var err error
	for purpose, out := range map[string]*string{"build-project": &n.build, "kms-alias": &n.keyAlias} {
		*out, err = c.Name(ownership.AWS, purpose, "primary", 63)
		if err != nil {
			return n, err
		}
	}
	n.repository, err = c.Path(ownership.AWS, "repository", "primary")
	if err != nil {
		return n, err
	}
	n.logs = "/aws/codebuild/" + n.build
	for _, role := range []string{"build", "invoker", "candidate-writer", "canary"} {
		n.roles[role], err = c.Name(ownership.AWS, role+"-role", "primary", 63)
		if err != nil {
			return n, err
		}
		n.boundaries[role], err = c.Name(ownership.AWS, role+"-boundary", "primary", 63)
		if err != nil {
			return n, err
		}
	}
	n.control, err = namespaceName(c, "control")
	if err != nil {
		return n, err
	}
	n.target, err = namespaceName(c, "target")
	if err != nil {
		return n, err
	}
	n.project, err = c.Name(ownership.Temporal, "project", "sandbox", 63)
	return n, err
}
func roleARN(name string) string { return "arn:aws:iam::" + Account + ":role/" + name }
func namespaceName(c ownership.Context, label string) (string, error) {
	return c.Name(ownership.Temporal, "namespace", label+"-"+TemporalNamespaceRegion, 39)
}
func arn(service, resource string) string {
	return "arn:aws:" + service + ":" + Region + ":" + Account + ":" + resource
}
func policy(statements ...Object) Object {
	return Object{"Version": "2012-10-17", "Statement": statements}
}
func statement(sid, effect string, actions []string, resources []any) Object {
	return Object{"Sid": sid, "Effect": effect, "Action": actions, "Resource": resources}
}
func digest(value any) string {
	raw, _ := json.Marshal(value)
	hash := sha256.Sum256(raw)
	return hex.EncodeToString(hash[:])
}
func encoded(value any) string { raw, _ := json.Marshal(value); return string(raw) }
func ref(path string) string   { return "${" + path + "}" }

// Render produces a complete standalone OpenTofu configuration. The base has
// deny-all candidate identities. Candidate updates require real immutable Cloud
// namespace binding and a fixed safe CredentialIntent; no prefix fallback exists.
func Render(candidate *Candidate) (Rendered, error) {
	c, err := Context()
	if err != nil {
		return Rendered{}, err
	}
	n, err := derive(c)
	if err != nil {
		return Rendered{}, err
	}
	ownerHash := digest(c.Owner())
	tags := Object{"RegistryFixture": "registry-operator-v2", "RegistryOwner": ownerHash, "RegistryRun": RunID, "RegistryExpiresAt": ref("var.fixture_expires_at")}
	projectARN := arn("codebuild", "project/"+n.build)
	repositoryARN := arn("ecr", "repository/"+n.repository)
	logARN := arn("logs", "log-group:"+n.logs+":log-stream:*")
	buildAllows := []Object{statement("RepositoryToken", "Allow", []string{"ecr:GetAuthorizationToken"}, []any{"*"}), statement("ExactFixtureRepository", "Allow", []string{"ecr:BatchCheckLayerAvailability", "ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer", "ecr:InitiateLayerUpload", "ecr:UploadLayerPart", "ecr:CompleteLayerUpload", "ecr:PutImage"}, []any{repositoryARN}), statement("ExactFixtureLogStream", "Allow", []string{"logs:CreateLogStream", "logs:PutLogEvents"}, []any{logARN})}
	invokerAllows := []Object{statement("InspectAndStopExactProject", "Allow", []string{"codebuild:BatchGetProjects", "codebuild:BatchGetBuilds", "codebuild:StopBuild"}, []any{projectARN})}
	start := statement("StartFixedSentinelOnly", "Allow", []string{"codebuild:StartBuild"}, []any{projectARN})
	// Every key is documented for StartBuild. Null checks field presence, never
	// pretend to inspect a buildspec's content. Undocumented timeout overrides
	// remain a separate adapter/budget restriction recorded in the review file.
	absent := Object{}
	for _, key := range []string{"serviceRole", "source", "source.buildspec", "source.auth.resource", "source.auth.type", "source.location", "source.insecureSsl", "source.buildStatusConfig.context", "source.buildStatusConfig.targetUrl", "environment", "environment.image", "environment.computeType", "environment.type", "environment.privilegedMode", "environment.imagePullCredentialsType", "environment.registryCredential", "environment.certificate", "environment.environmentVariables", "environment.fleet.fleetArn", "artifacts", "secondaryArtifacts", "secondarySources", "cache", "logsConfig", "encryptionKey", "autoRetryLimit"} {
		absent["codebuild:"+key] = "true"
	}
	start["Condition"] = Object{"Null": absent}
	invokerAllows = append(invokerAllows, start)
	permissions := map[string][]Object{"build": buildAllows, "invoker": invokerAllows, "candidate-writer": nil, "canary": nil}
	keyStatements := []Object{{"Sid": "ExactHumanProvisioner", "Effect": "Allow", "Principal": Object{"AWS": ProvisionerRole}, "Action": []string{"kms:*"}, "Resource": "*"}, {"Sid": "AccountRecoveryAdministration", "Effect": "Allow", "Principal": Object{"AWS": "arn:aws:iam::" + Account + ":root"}, "Action": []string{"kms:DescribeKey", "kms:GetKeyPolicy", "kms:PutKeyPolicy", "kms:CancelKeyDeletion"}, "Resource": "*"}}
	stage := Object{"state": "BlockedUntilIndependentTemporalBirth", "candidateCreateAuthorized": false, "genericPrefixFallback": false, "reason": "CandidateID depends on the provider-observed namespace incarnation. Base writer/canary policies deny all actions."}
	if candidate != nil {
		writer, canary, keyUse, info, err := candidatePolicies(c, n, *candidate)
		if err != nil {
			return Rendered{}, err
		}
		permissions["candidate-writer"], permissions["canary"] = writer, canary
		keyStatements = append(keyStatements, keyUse...)
		stage = info
	}
	resources := Object{}
	for _, role := range []string{"build", "invoker", "candidate-writer", "canary"} {
		key := strings.ReplaceAll(role, "-", "_")
		allow := permissions[role]
		if len(allow) == 0 {
			allow = []Object{statement("CandidateLaneDisabled", "Deny", []string{"*"}, []any{"*"})}
		}
		boundary := boundary(permissions[role])
		if role == "build" {
			boundary = boundaryWithToken(permissions[role])
		}
		resourcesFor(resources, "aws_iam_policy", key+"_boundary", Object{"name": n.boundaries[role], "description": "Exact disposable registry-v2 fixture boundary; not production policy", "policy": encoded(boundary), "tags": tags, "depends_on": []string{"terraform_data.scope_guard"}})
		trust := Object{"Version": "2012-10-17", "Statement": []Object{{"Effect": "Allow", "Principal": Object{"AWS": ProvisionerRole}, "Action": "sts:AssumeRole", "Condition": Object{"StringEquals": Object{"sts:ExternalId": RunID}}}}}
		if role == "build" {
			trust = Object{"Version": "2012-10-17", "Statement": []Object{{"Effect": "Allow", "Principal": Object{"Service": "codebuild.amazonaws.com"}, "Action": "sts:AssumeRole", "Condition": Object{"StringEquals": Object{"aws:SourceAccount": Account, "aws:SourceArn": projectARN}}}}}
		}
		resourcesFor(resources, "aws_iam_role", key, Object{"name": n.roles[role], "assume_role_policy": encoded(trust), "permissions_boundary": ref("aws_iam_policy." + key + "_boundary.arn"), "max_session_duration": 3600, "tags": tags, "depends_on": []string{"terraform_data.scope_guard"}})
		resourcesFor(resources, "aws_iam_role_policy", key, Object{"name": "fixture-only", "role": ref("aws_iam_role." + key + ".id"), "policy": encoded(policy(allow...))})
	}
	resourcesFor(resources, "terraform_data", "scope_guard", Object{"input": RunID, "lifecycle": Object{"precondition": []Object{{"condition": ref("startswith(data.aws_caller_identity.provisioner.arn, \"arn:aws:sts::" + Account + ":assumed-role/AWSReservedSSO_AWSAdministratorAccess_ce84ce92ec884c3c/\")"), "error_message": "Only the explicitly reviewed human provisioning role may provision this isolated fixture."}, {"condition": ref("timecmp(timestamp(), var.fixture_expires_at) < 0 && timecmp(var.fixture_expires_at, timeadd(timestamp(), \"48h\")) <= 0"), "error_message": "Review a future fixture expiration no more than 48 hours from provisioning."}}}})
	resourcesFor(resources, "aws_kms_key", "candidate", Object{"description": "Disposable registry-v2 candidate encryption key " + RunID, "deletion_window_in_days": 7, "enable_key_rotation": true, "policy": encoded(policy(keyStatements...)), "tags": tags, "depends_on": []string{"terraform_data.scope_guard"}})
	resourcesFor(resources, "aws_kms_alias", "candidate", Object{"name": "alias/" + n.keyAlias, "target_key_id": ref("aws_kms_key.candidate.key_id")})
	resourcesFor(resources, "aws_ecr_repository", "sentinel", Object{"name": n.repository, "image_tag_mutability": "IMMUTABLE", "force_delete": false, "encryption_configuration": []Object{{"encryption_type": "AES256"}}, "image_scanning_configuration": []Object{{"scan_on_push": false}}, "tags": tags, "depends_on": []string{"terraform_data.scope_guard"}})
	resourcesFor(resources, "aws_ecr_lifecycle_policy", "sentinel", Object{"repository": ref("aws_ecr_repository.sentinel.name"), "policy": encoded(Object{"rules": []Object{{"rulePriority": 1, "description": "Keep at most three fixture images; this is not a hard byte/cost limit", "selection": Object{"tagStatus": "any", "countType": "imageCountMoreThan", "countNumber": 3}, "action": Object{"type": "expire"}}}})})
	resourcesFor(resources, "aws_cloudwatch_log_group", "build", Object{"name": n.logs, "retention_in_days": 1, "tags": tags, "depends_on": []string{"terraform_data.scope_guard"}})
	resourcesFor(resources, "aws_codebuild_project", "sentinel", Object{"name": n.build, "description": "Disposable fixed no-source sentinel; no production roles or credentials", "service_role": ref("aws_iam_role.build.arn"), "build_timeout": 5, "queued_timeout": 5, "concurrent_build_limit": 1, "auto_retry_limit": 0, "project_visibility": "PRIVATE", "artifacts": []Object{{"type": "NO_ARTIFACTS"}}, "source": []Object{{"type": "NO_SOURCE", "buildspec": "version: 0.2\nphases:\n  build:\n    commands:\n      - printf '%s\\n' registry-v2-fixture-sentinel\n"}}, "environment": []Object{{"compute_type": "BUILD_GENERAL1_SMALL", "image": "aws/codebuild/standard:7.0", "type": "LINUX_CONTAINER", "privileged_mode": false, "image_pull_credentials_type": "CODEBUILD"}}, "logs_config": []Object{{"cloudwatch_logs": []Object{{"status": "ENABLED", "group_name": ref("aws_cloudwatch_log_group.build.name"), "stream_name": "sentinel"}}}}, "tags": tags, "depends_on": []string{"aws_iam_role_policy.build"}})
	tf := Object{"terraform": Object{"required_version": ">= 1.10.0, < 2.0.0", "required_providers": Object{"aws": Object{"source": "hashicorp/aws", "version": "= " + AWSProviderVersion}}, "backend": Object{"local": Object{"path": ".state/" + RunID + ".tfstate"}}}, "provider": Object{"aws": Object{"region": Region, "profile": Profile, "allowed_account_ids": []string{Account}, "max_retries": 0}}, "variable": Object{"fixture_expires_at": Object{"type": "string", "description": "Human-reviewed expiry, at most48 hours after provisioning; TTL tags are not automatic deletion", "default": "2026-10-31T23:59:59Z"}}, "data": Object{"aws_caller_identity": Object{"provisioner": Object{}}}, "resource": resources, "output": Object{"fixture_bindings": Object{"value": Object{"account": Account, "region": Region, "run_id": RunID, "owner": c.Owner(), "build_project_arn": projectARN, "kms_key_arn": ref("aws_kms_key.candidate.arn"), "repository_arn": repositoryARN, "build_role_arn": roleARN(n.roles["build"]), "invoker_role_arn": roleARN(n.roles["invoker"]), "candidate_writer_role_arn": roleARN(n.roles["candidate-writer"]), "canary_role_arn": roleARN(n.roles["canary"])}}}}
	review := reviewPlan(c, n, stage)
	return Rendered{tf, review, temporalPlan(c, n)}, nil
}

func resourcesFor(resources Object, kind, name string, value Object) {
	group, _ := resources[kind].(Object)
	if group == nil {
		group = Object{}
		resources[kind] = group
	}
	group[name] = value
}
func boundary(allows []Object) Object {
	if len(allows) == 0 {
		return policy(statement("DisabledCandidateLane", "Deny", []string{"*"}, []any{"*"}))
	}
	statements := append([]Object(nil), allows...)
	resources := []any{}
	for _, allow := range allows {
		for _, resource := range allow["Resource"].([]any) {
			resources = append(resources, resource)
		}
	}
	statements = append(statements, Object{"Sid": "DenyOutsideExactFixtureResources", "Effect": "Deny", "Action": "*", "NotResource": resources}, noEscalation())
	return policy(statements...)
}
func boundaryWithToken(allows []Object) Object {
	bounded := boundary(allows[1:])
	statements := bounded["Statement"].([]Object)
	for _, item := range statements {
		if item["Sid"] == "DenyOutsideExactFixtureResources" {
			delete(item, "Action")
			item["NotAction"] = []string{"ecr:GetAuthorizationToken"}
		}
	}
	return policy(append([]Object{allows[0]}, statements...)...)
}
func noEscalation() Object {
	return statement("NoPrivilegeOrResourcePolicyChanges", "Deny", []string{"iam:*", "sts:AssumeRole*", "codebuild:CreateProject", "codebuild:UpdateProject", "codebuild:DeleteProject", "codebuild:StartBuildBatch", "ecr:SetRepositoryPolicy", "ecr:DeleteRepositoryPolicy", "secretsmanager:PutResourcePolicy", "secretsmanager:DeleteResourcePolicy", "secretsmanager:PutSecretValue", "secretsmanager:UpdateSecretVersionStage", "secretsmanager:DeleteSecret", "secretsmanager:ReplicateSecretToRegions", "kms:CreateGrant", "kms:PutKeyPolicy", "kms:ScheduleKeyDeletion"}, []any{"*"})
}

var hexDigest = regexp.MustCompile(`^[a-f0-9]{64}$`)

func candidatePolicies(c ownership.Context, n names, value Candidate) ([]Object, []Object, []Object, Object, error) {
	binding, intent := value.Binding, value.Intent
	scope, _ := c.Scope(ownership.Temporal)
	name, err := c.Path(ownership.Temporal, "api-key-candidate", intent.StageID)
	if err != nil || binding.NamespaceID != n.target+"."+TemporalAccount || binding.CreatedAt.IsZero() || binding.ProvenanceID == "" || binding.Record.Ref.Scope != scope || binding.Record.Ref.Name != n.target || binding.Record.Ref.Purpose != "namespace" || binding.Record.Owner != c.Owner() || binding.Record.Ref.ProviderID != tc.IncarnationID(binding.NamespaceID, binding.CreatedAt) || intent.Owner != c.Owner() || intent.NamespaceID != binding.NamespaceID || intent.NamespaceIncarnationID != binding.Record.Ref.ProviderID || !hexDigest.MatchString(intent.StageID) || !hexDigest.MatchString(intent.SlotID) || intent.CandidateID != name || intent.OperationKey == "" || intent.AttemptID == "" || !intent.ExpiresAt.After(binding.CreatedAt) || intent.ExpiresAt.Sub(binding.CreatedAt) > 48*time.Hour {
		return nil, nil, nil, nil, errors.New("exact bound Temporal incarnation and fixed safe stage intent required")
	}
	if binding.Record.CreationOperationID == "" || binding.Record.CreationIdempotencyKey == "" || binding.Record.Revision == "" || binding.Record.SpecFingerprint == "" || binding.Record.WriterEpoch == 0 {
		return nil, nil, nil, nil, errors.New("independently recorded namespace creation ledger required")
	}
	slot := digest(struct {
		Owner       any
		Incarnation string
	}{c.Owner(), binding.Record.Ref.ProviderID})
	stageID := digest(struct {
		Slot, Operation, Attempt string
		Expiry                   time.Time
	}{slot, intent.OperationKey, intent.AttemptID, intent.ExpiresAt.UTC()})
	saName, _ := c.Name(ownership.Temporal, "service-account", slot, 63)
	keyName, _ := c.Name(ownership.Temporal, "api-key", stageID, 63)
	if intent.SlotID != slot || intent.StageID != stageID || intent.ServiceAccountName != saName || intent.ServiceAccountMarker != "registry-sa:"+slot || intent.KeyDisplayName != keyName || intent.KeyMarker != "registry-key:"+stageID {
		return nil, nil, nil, nil, errors.New("candidate must match exact durable CredentialManager Prepare contract")
	}
	// The intent is a trusted Prepare result; this renderer does not grant
	// provider mutation authority or reconstruct/adopt its creation provenance.
	secretARN := arn("secretsmanager", "secret:"+name+"-??????")
	keyARN := ref("aws_kms_key.candidate.arn")
	request := Object{"aws:RequestTag/registry:owner": digest(c.Owner()), "aws:RequestTag/registry:stage": intent.StageID, "aws:RequestTag/registry:namespace": digest(intent.NamespaceIncarnationID)}
	create := statement("CreateOnlyExactCandidate", "Allow", []string{"secretsmanager:CreateSecret"}, []any{secretARN})
	create["Condition"] = Object{"StringEquals": merge(request, Object{"secretsmanager:Name": name}), "ArnEquals": Object{"secretsmanager:KmsKeyArn": keyARN}, "ForAllValues:StringEquals": Object{"aws:TagKeys": []string{"registry:owner", "registry:stage", "registry:namespace", "registry:key", "registry:write"}}, "Null": Object{"aws:RequestTag/registry:key": "false", "aws:RequestTag/registry:write": "false", "secretsmanager:AddReplicaRegions": "true"}, "StringLike": Object{"aws:RequestTag/registry:key": strings.Repeat("?", 64), "aws:RequestTag/registry:write": strings.Repeat("?", 64)}}
	tag := statement("TagOnlyExactCandidateDuringCreate", "Allow", []string{"secretsmanager:TagResource"}, []any{secretARN})
	tag["Condition"] = Object{"StringEquals": request, "ForAllValues:StringEquals": Object{"aws:TagKeys": []string{"registry:owner", "registry:stage", "registry:namespace", "registry:key", "registry:write"}}}
	describe := statement("DescribeExactCandidate", "Allow", []string{"secretsmanager:DescribeSecret"}, []any{secretARN})
	crypto := statement("EncryptExactCandidateOnly", "Allow", []string{"kms:GenerateDataKey", "kms:Decrypt"}, []any{keyARN})
	crypto["Condition"] = Object{"StringEquals": Object{"kms:ViaService": "secretsmanager." + Region + ".amazonaws.com"}, "StringLike": Object{"kms:EncryptionContext:SecretARN": secretARN}}
	read := statement("ReadOnlyExactCandidateVersion", "Allow", []string{"secretsmanager:GetSecretValue", "secretsmanager:DescribeSecret"}, []any{secretARN})
	read["Condition"] = Object{"StringEquals": Object{"secretsmanager:ResourceTag/registry:owner": digest(c.Owner()), "secretsmanager:ResourceTag/registry:stage": intent.StageID, "secretsmanager:ResourceTag/registry:namespace": digest(intent.NamespaceIncarnationID)}}
	decrypt := statement("DecryptExactCandidateOnly", "Allow", []string{"kms:Decrypt"}, []any{keyARN})
	decrypt["Condition"] = crypto["Condition"]
	keyUse := Object{"Sid": "CandidateUseOnlyViaSecretsManager", "Effect": "Allow", "Principal": Object{"AWS": "arn:aws:iam::" + Account + ":root"}, "Action": []string{"kms:GenerateDataKey", "kms:Decrypt"}, "Resource": "*", "Condition": Object{"ArnEquals": Object{"aws:PrincipalArn": []string{roleARN(n.roles["candidate-writer"]), roleARN(n.roles["canary"])}}, "StringEquals": Object{"kms:ViaService": "secretsmanager." + Region + ".amazonaws.com"}, "StringLike": Object{"kms:EncryptionContext:SecretARN": secretARN}}}
	info := Object{"state": "ExactCandidatePolicyPreparedForSecondReview", "candidateCreateAuthorized": false, "requiresReviewBeforeApply": true, "candidateID": name, "candidateARNPattern": secretARN, "intent": intent, "namespaceBirth": binding.CreatedAt, "namespaceCreationProvenance": binding.ProvenanceID, "unknownKeyAndWriteTagValues": "Future provider key facts; backend exact journal checks remain mandatory, IAM only bounds shape/presence."}
	return []Object{create, tag, describe, crypto}, []Object{read, decrypt}, []Object{keyUse}, info, nil
}
func merge(a, b Object) Object {
	result := Object{}
	for k, v := range a {
		result[k] = v
	}
	for k, v := range b {
		result[k] = v
	}
	return result
}

func WriteJSON(value any) ([]byte, error) { return json.MarshalIndent(value, "", "  ") }
func (r Rendered) String() string {
	return fmt.Sprintf("Exact registry-v2 fixture plan for %s/%s, review required", Account, Region)
}
