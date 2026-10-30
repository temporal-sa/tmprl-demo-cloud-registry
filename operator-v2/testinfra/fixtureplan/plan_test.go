package fixtureplan

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
	tc "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/providers/temporalcloud"
	cloudservice "go.temporal.io/cloud-sdk/api/cloudservice/v1"
	"google.golang.org/protobuf/encoding/protojson"
)

func group(tf Object, kind string) Object { return tf["resource"].(Object)[kind].(Object) }
func document(t *testing.T, raw any) Object {
	t.Helper()
	var result Object
	if err := json.Unmarshal([]byte(raw.(string)), &result); err != nil {
		t.Fatal(err)
	}
	return result
}
func statements(t *testing.T, raw any) []Object {
	t.Helper()
	p := document(t, raw)
	var result []Object
	for _, s := range p["Statement"].([]any) {
		result = append(result, s.(map[string]any))
	}
	return result
}
func stringValues(value any) []string {
	switch v := value.(type) {
	case string:
		return []string{v}
	case []any:
		var values []string
		for _, x := range v {
			values = append(values, x.(string))
		}
		return values
	case []string:
		return v
	}
	return nil
}

// ARN wildcard structure only, not IAM authorization evaluation. It deliberately
// lets * span / as documented by AWS; conditions/effective IAM need live proof.
func matches(pattern, value string) bool {
	p := regexp.QuoteMeta(pattern)
	p = strings.ReplaceAll(p, `\*`, ".*")
	p = strings.ReplaceAll(p, `\?`, ".")
	return regexp.MustCompile("^" + p + "$").MatchString(value)
}

func TestExactBasePlanSeparatesRolesAndDisablesCandidateAuthority(t *testing.T) {
	r, err := Render(nil)
	if err != nil {
		t.Fatal(err)
	}
	provider := r.Terraform["provider"].(Object)["aws"].(Object)
	if provider["region"] != Region || provider["profile"] != Profile || provider["allowed_account_ids"].([]string)[0] != Account {
		t.Fatal("wrong provisioning identity/scope")
	}
	if r.Review["applyAuthorized"] != false || r.Review["cloudAuthorizationProven"] != false {
		t.Fatal("rendering claimed authorization")
	}
	for _, role := range []string{"candidate_writer", "canary"} {
		for _, s := range statements(t, group(r.Terraform, "aws_iam_role_policy")[role].(Object)["policy"]) {
			if s["Effect"] != "Deny" {
				t.Fatal("base candidate lane enabled")
			}
		}
		for _, s := range statements(t, group(r.Terraform, "aws_iam_policy")[role+"_boundary"].(Object)["policy"]) {
			if s["Effect"] != "Deny" {
				t.Fatal("base candidate boundary grants authority")
			}
		}
	}
	for name, role := range group(r.Terraform, "aws_iam_role") {
		object := role.(Object)
		if object["permissions_boundary"] == nil {
			t.Fatal("role lacks boundary")
		}
		trust := statements(t, object["assume_role_policy"])[0]
		if name == "build" {
			condition := trust["Condition"].(map[string]any)["StringEquals"].(map[string]any)
			if condition["aws:SourceAccount"] != Account || !strings.HasPrefix(condition["aws:SourceArn"].(string), "arn:aws:codebuild:us-west-2:"+Account+":project/rv2-") {
				t.Fatal("build trust not exact account/project")
			}
		} else if trust["Principal"].(map[string]any)["AWS"] != ProvisionerRole {
			t.Fatal("runtime role trusts broad principal")
		}
	}
	project := group(r.Terraform, "aws_codebuild_project")["sentinel"].(Object)
	if project["concurrent_build_limit"] != 1 || project["build_timeout"] != 5 || project["queued_timeout"] != 5 || project["auto_retry_limit"] != 0 {
		t.Fatal("unbounded initial build fixture")
	}
	env := project["environment"].([]Object)[0]
	if env["privileged_mode"] != false {
		t.Fatal("sentinel unexpectedly privileged")
	}
	if len(group(r.Terraform, "aws_iam_role")) != 4 {
		t.Fatal("unexpected identity closure")
	}
}

func TestBaseAllowResourcesNeverIncludeProductionOrPrivilegePaths(t *testing.T) {
	r, err := Render(nil)
	if err != nil {
		t.Fatal(err)
	}
	production := []string{"arn:aws:codebuild:us-west-1:" + Account + ":project/tmprl-dem-cld-build", "arn:aws:ecr:us-west-1:" + Account + ":repository/tmprl-dem-cld/operator", "arn:aws:secretsmanager:us-west-1:" + Account + ":secret:tmprl-dem-cld/platform/bootstrap-Ab1234", "arn:aws:iam::" + Account + ":role/tmprl-dem-cld-codebuild"}
	for _, role := range group(r.Terraform, "aws_iam_role_policy") {
		for _, s := range statements(t, role.(Object)["policy"]) {
			if s["Effect"] != "Allow" {
				continue
			}
			for _, action := range stringValues(s["Action"]) {
				if strings.HasPrefix(action, "iam:") || strings.HasPrefix(action, "sts:") || strings.HasPrefix(action, "secretsmanager:") || strings.HasPrefix(action, "ecr-public:") {
					t.Fatalf("unexpected privilege path: %s", action)
				}
			}
			for _, resource := range stringValues(s["Resource"]) {
				if resource == "*" {
					if strings.Join(stringValues(s["Action"]), ",") != "ecr:GetAuthorizationToken" {
						t.Fatal("unexpected global resource allow")
					}
					continue
				}
				for _, prod := range production {
					if matches(resource, prod) {
						t.Fatal("production ARN matched fixture allow")
					}
				}
			}
		}
	}
	invoker := statements(t, group(r.Terraform, "aws_iam_role_policy")["invoker"].(Object)["policy"])
	var start Object
	for _, s := range invoker {
		if s["Sid"] == "StartFixedSentinelOnly" {
			start = s
		}
	}
	absent := start["Condition"].(map[string]any)["Null"].(map[string]any)
	for _, key := range []string{"codebuild:serviceRole", "codebuild:source.buildspec", "codebuild:environment.image", "codebuild:environment.environmentVariables", "codebuild:artifacts", "codebuild:logsConfig"} {
		if absent[key] != "true" {
			t.Fatalf("override %s is not refused", key)
		}
	}
}

// Prepare is pure and never invokes these embedded I/O methods. A nil embedded
// method would panic if this fixture accidentally attempted any provider call.
type noIO struct {
	tc.Transport
	tc.CredentialTransport
	tc.CredentialJournal
	tc.SecureCandidateStore
}

func realPreparedCandidate(t *testing.T) Candidate {
	t.Helper()
	c, err := Context()
	if err != nil {
		t.Fatal(err)
	}
	n, err := derive(c)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	desired := tc.DesiredNamespace{RegionID: TemporalNamespaceRegion, RetentionDays: 1, AuthMode: tc.APIKeyAuth}
	fingerprint, err := tc.NamespaceFingerprint(desired)
	if err != nil {
		t.Fatal(err)
	}
	i, err := c.Intent(ownership.Temporal, "namespace", "target-"+TemporalNamespaceRegion, "fixture-v1", "bootstrap-target", fingerprint, 39)
	if err != nil {
		t.Fatal(err)
	}
	writer := ownership.Writer{ID: "fixture-writer", Owner: c.Owner(), Epoch: 1, ExpiresAt: now.Add(time.Hour)}
	namespaceID := n.target + "." + TemporalAccount
	observation := ownership.Observation{Ref: ownership.ResourceRef{Scope: i.Scope, Purpose: i.Purpose, Name: i.Name, ProviderID: tc.IncarnationID(namespaceID, now)}, Owner: c.Owner(), CreationIdempotencyKey: i.IdempotencyKey, Revision: i.Revision, SpecFingerprint: i.SpecFingerprint, WriterEpoch: 1, Version: "v1"}
	record, err := c.BindCreated(i, observation, writer, now)
	if err != nil {
		t.Fatal(err)
	}
	binding := tc.Binding{NamespaceID: namespaceID, CreatedAt: now, ProvenanceID: "independent-provider-create-operation", Record: record}
	io := &noIO{}
	adapter, err := tc.New(tc.Config{Ownership: c, Bindings: []tc.Binding{binding}, AllowedRegions: []string{TemporalNamespaceRegion}, MaximumRetentionDays: 7, CallTimeout: time.Second, Now: func() time.Time { return now }}, io)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := tc.NewCredentialManager(tc.CredentialConfig{MaximumKeyTTL: 24 * time.Hour, MaximumRecoveryPages: 1, StoreID: "fixture-store"}, adapter, io, io, io, nil)
	if err != nil {
		t.Fatal(err)
	}
	intent, err := manager.Prepare(tc.StageRequest{Namespace: tc.WriteRequest{NamespaceID: namespaceID, ExpectedIncarnationID: record.Ref.ProviderID, ExpectedRevision: record.Revision, OperationKey: "fixed-credential-stage", Writer: writer}, AttemptID: "attempt-one", ExpiresAt: now.Add(24 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	return Candidate{binding, intent}
}

func TestSecondStageUsesActualPrepareContractAndBackendTagSchema(t *testing.T) {
	candidate := realPreparedCandidate(t)
	r, err := Render(&candidate)
	if err != nil {
		t.Fatal(err)
	}
	exact := arn("secretsmanager", "secret:"+candidate.Intent.CandidateID+"-??????")
	writer := statements(t, group(r.Terraform, "aws_iam_role_policy")["candidate_writer"].(Object)["policy"])
	var create Object
	for _, s := range writer {
		if s["Sid"] == "CreateOnlyExactCandidate" {
			create = s
		}
		for _, resource := range stringValues(s["Resource"]) {
			if strings.HasPrefix(resource, "arn:aws:secretsmanager:") && resource != exact {
				t.Fatal("generic prefix candidate fallback")
			}
		}
	}
	if create == nil {
		t.Fatal("missing exact candidate create")
	}
	condition := create["Condition"].(map[string]any)
	stringsEqual := condition["StringEquals"].(map[string]any)
	if stringsEqual["aws:RequestTag/registry:stage"] != candidate.Intent.StageID || stringsEqual["aws:RequestTag/registry:namespace"] != digest(candidate.Intent.NamespaceIncarnationID) || stringsEqual["secretsmanager:Name"] != candidate.Intent.CandidateID {
		t.Fatal("backend intent/tag mismatch")
	}
	if len(condition["ForAllValues:StringEquals"].(map[string]any)["aws:TagKeys"].([]any)) != 5 {
		t.Fatal("backend five-tag schema not enforced")
	}
	if !matches(exact, arn("secretsmanager", "secret:"+candidate.Intent.CandidateID+"-Ab1234")) || matches(exact, arn("secretsmanager", "secret:tmprl-dem-cld/platform/bootstrap-Ab1234")) {
		t.Fatal("wrong AWS random-suffix pattern")
	}
	if r.Review["candidateStage"].(Object)["candidateCreateAuthorized"] != false {
		t.Fatal("rendering update granted creation authority")
	}
}

func TestSecondStageRejectsUnprovenBirthAndFabricatedIntent(t *testing.T) {
	for name, change := range map[string]func(*Candidate){"missingBirth": func(c *Candidate) { c.Binding.CreatedAt = time.Time{} }, "foreignOwner": func(c *Candidate) { c.Binding.Record.Owner.ProjectUID = "foreign" }, "reusedBirth": func(c *Candidate) { c.Binding.CreatedAt = c.Binding.CreatedAt.Add(time.Second) }, "unproven": func(c *Candidate) { c.Binding.Record.CreationIdempotencyKey = "" }, "genericName": func(c *Candidate) { c.Intent.CandidateID = "rv2/generic-candidate" }, "fakeStage": func(c *Candidate) { c.Intent.StageID = digest("unrelated") }, "attemptChanged": func(c *Candidate) { c.Intent.AttemptID = "another-attempt" }} {
		t.Run(name, func(t *testing.T) {
			candidate := realPreparedCandidate(t)
			change(&candidate)
			if _, err := Render(&candidate); err == nil {
				t.Fatal("unsafe candidate policy rendered")
			}
		})
	}
}

func TestCheckedInBaseArtifactsExactlyMatchGenerator(t *testing.T) {
	r, err := Render(nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]any{"main.tf.json": r.Terraform, "review.json": r.Review, "temporal.review.json": r.Temporal} {
		actual, err := os.ReadFile(filepath.Join("..", "fixtures", RunID, name))
		if err != nil {
			t.Fatal(err)
		}
		expected, err := WriteJSON(value)
		if err != nil {
			t.Fatal(err)
		}
		if string(actual) != string(append(expected, '\n')) {
			t.Fatalf("%s is stale; regenerate exact review artifact", name)
		}
	}
}

func TestRenderedPoliciesFitProviderLimitsAndNeverContainKeyMaterial(t *testing.T) {
	for _, candidate := range []*Candidate{nil, func() *Candidate { c := realPreparedCandidate(t); return &c }()} {
		r, err := Render(candidate)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range group(r.Terraform, "aws_iam_policy") {
			policy := item.(Object)["policy"].(string)
			if len(policy) > 6144 {
				t.Fatal("managed permissions boundary exceeds IAM policy limit")
			}
		}
		for _, item := range group(r.Terraform, "aws_iam_role_policy") {
			if len(item.(Object)["policy"].(string)) > 10240 {
				t.Fatal("role inline policy exceeds IAM limit")
			}
		}
		for _, resourceType := range []string{"aws_secretsmanager_secret_version", "aws_iam_access_key", "aws_ssm_parameter"} {
			if _, found := r.Terraform["resource"].(Object)[resourceType]; found {
				t.Fatal("credential-bearing resource entered IaC state")
			}
		}
		base, err := WriteJSON(r.Terraform)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(base), "SecretString") || strings.Contains(string(base), "SecretBinary") || strings.Contains(string(base), "AWS_SECRET_ACCESS_KEY") {
			t.Fatal("credential payload field appeared in deployable plan")
		}
	}
}

func TestDenySentinelsHaveSeparateStateAndNoBaseAuthorityChanges(t *testing.T) {
	baseBefore, err := Render(nil)
	if err != nil {
		t.Fatal(err)
	}
	before, _ := WriteJSON(baseBefore.Terraform)
	r, err := RenderDenySentinels()
	if err != nil {
		t.Fatal(err)
	}
	baseAfter, err := Render(nil)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := WriteJSON(baseAfter.Terraform)
	if string(before) != string(after) {
		t.Fatal("deny rendering mutated allowed base")
	}
	backend := r.Terraform["terraform"].(Object)["backend"].(Object)["local"].(Object)
	if backend["path"] != ".state/"+RunID+"-deny.tfstate" {
		t.Fatal("deny module shares base state")
	}
	count := 0
	for _, items := range r.Terraform["resource"].(Object) {
		count += len(items.(Object))
	}
	if count != 12 {
		t.Fatalf("unexpected deny creates: %d", count)
	}
	if r.Review["applyAuthorized"] != false || r.Review["seedingAuthorized"] != false || r.Review["probesAuthorized"] != false {
		t.Fatal("denial review granted action")
	}
	for _, kind := range []string{"aws_iam_policy", "aws_iam_role_policy"} {
		for _, item := range group(r.Terraform, kind) {
			for _, s := range statements(t, item.(Object)["policy"]) {
				if s["Effect"] != "Deny" || strings.Join(stringValues(s["Action"]), ",") != "*" {
					t.Fatal("deny role has workload authority")
				}
			}
		}
	}
	secret := group(r.Terraform, "aws_secretsmanager_secret")["deny"].(Object)
	if secret["kms_key_id"] != "${aws_kms_key.deny.arn}" || secret["secret_string"] != nil || secret["secret_binary"] != nil {
		t.Fatal("deny secret has value or wrong CMK")
	}
	if _, exists := r.Terraform["resource"].(Object)["aws_secretsmanager_secret_version"]; exists {
		t.Fatal("secret seed enters IaC state")
	}
	project := group(r.Terraform, "aws_codebuild_project")["deny"].(Object)
	if project["service_role"] != "${aws_iam_role.deny_build.arn}" || project["concurrent_build_limit"] != 1 || project["build_timeout"] != 5 || project["environment"].([]Object)[0]["privileged_mode"] != false {
		t.Fatal("unbounded unexpected-start effect")
	}
	if len(group(r.Terraform, "aws_cloudwatch_log_stream")) != 1 {
		t.Fatal("valid log-write deny target missing")
	}
}

func TestCheckedInDenyArtifactsExactlyMatchGenerator(t *testing.T) {
	r, err := RenderDenySentinels()
	if err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]any{"main.tf.json": r.Terraform, "review.json": r.Review, "temporal.review.json": r.Temporal} {
		actual, err := os.ReadFile(filepath.Join("..", "fixtures", RunID+"-deny", name))
		if err != nil {
			t.Fatal(err)
		}
		expected, err := WriteJSON(value)
		if err != nil {
			t.Fatal(err)
		}
		if string(actual) != string(append(expected, '\n')) {
			t.Fatalf("%s deny review artifact is stale", name)
		}
	}
}

func TestProjectCreateRequestIsExactProtectedSDKContractWithoutMutation(t *testing.T) {
	payload, review, err := ProjectCreateReview()
	if err != nil {
		t.Fatal(err)
	}
	var request cloudservice.CreateProjectRequest
	if err := protojson.Unmarshal(payload, &request); err != nil {
		t.Fatal(err)
	}
	if request.Spec == nil || request.Spec.Lifecycle == nil || !request.Spec.Lifecycle.EnableDeleteProtection || !strings.HasPrefix(request.Spec.DisplayName, "rv2-project-") || !strings.Contains(request.Spec.Description, RunID) || request.AsyncOperationId == "" {
		t.Fatal("missing protected disjoint ownership/operation receipt")
	}
	if review["createAuthorized"] != false || review["expectedAccountID"] != "a2dd6" {
		t.Fatal("render authorized or mis-scoped create")
	}
	if strings.Contains(string(payload), "eca773c382124db79bbfab0b429faa5a") || strings.Contains(string(payload), "apiKey") {
		t.Fatal("existing project or credentials entered create body")
	}
	if _, unsafe := review["proposedCLIArgvNotExecuted"]; unsafe {
		t.Fatal("retrying CLI must not be proposed as approved transport")
	}
	for _, arg := range review["rejectedCLIArgvNotExecuted"].([]string) {
		if arg == "--idempotent" || arg == "--api-key" {
			t.Fatal("unsafe name adoption/credential option")
		}
	}
	unknown := review["unknownOutcome"].(Object)
	if unknown["automaticReplay"] != false || unknown["accountWideDiscovery"] != false {
		t.Fatal("unknown project ID triggered discovery/replay")
	}
	again, _, err := ProjectCreateReview()
	if err != nil || string(payload) != string(again) {
		t.Fatal("project request bytes unstable across pure rendering")
	}
}

func TestCheckedInProjectRequestMatchesDeterministicReview(t *testing.T) {
	payload, review, err := ProjectCreateReview()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := WriteJSON(review)
	if err != nil {
		t.Fatal(err)
	}
	for name, expected := range map[string][]byte{"temporal-project-create.request.json": payload, "temporal-project-create.review.json": append(raw, '\n')} {
		actual, err := os.ReadFile(filepath.Join("..", "fixtures", RunID, name))
		if err != nil {
			t.Fatal(err)
		}
		if string(actual) != string(expected) {
			t.Fatalf("%s is stale", name)
		}
	}
}
