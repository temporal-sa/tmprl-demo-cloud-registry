package testinfra

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
)

func fixtureConfig() Config {
	return Config{Owner: ownership.Owner{EnvironmentID: "sandbox", InstallationID: "fixture-installation", ProjectUID: "fixture-project-uid", Lifetime: ownership.Disposable, RunID: "run-one"},
		ResourceRoot: "lab", AWS: ownership.Scope{Provider: ownership.AWS, Partition: "aws", AccountID: "111122223333", Region: "us-west-1"}}
}

func fixtureDraft(t *testing.T) Draft {
	t.Helper()
	raw, err := RenderForReview(fixtureConfig())
	if err != nil {
		t.Fatal(err)
	}
	// This synthetic key binding is only for static JSON-policy examples. It
	// makes no claim about existence, ownership or the key's real key policy.
	raw = bindJSON(raw, map[string]string{"SANDBOX_KMS_KEY_ID": "00000000-0000-0000-0000-000000000001"})
	var draft Draft
	if err := json.Unmarshal(raw, &draft); err != nil {
		t.Fatal(err)
	}
	return draft
}

func TestDraftRetainsPlaceholdersAndCannotAuthorizeApply(t *testing.T) {
	raw, err := RenderForReview(fixtureConfig())
	if err != nil {
		t.Fatal(err)
	}
	var draft Draft
	if err := json.Unmarshal(raw, &draft); err != nil {
		t.Fatal(err)
	}
	if draft.ApplyAuthorized || draft.AuthorizationProven || len(draft.RequiredBindings) == 0 {
		t.Fatal("review artifact pretends to authorize cloud work")
	}
	for _, binding := range draft.RequiredBindings {
		if !strings.Contains(string(raw), "${"+binding+"}") {
			t.Fatalf("unreviewed binding %s was silently resolved", binding)
		}
	}
	if draft.Roles["externalSecrets"].Enabled || draft.Roles["runtime"].Enabled || draft.Roles["temporalInvoker"].Enabled || draft.Budgets["lambdaEnabled"] != false {
		t.Fatal("unproven runtime/controller capabilities activated")
	}
	if draft.Temporal["authorizationProven"] != false || draft.Temporal["namespaceCreationEnabled"] != false || draft.Temporal["globalReadersGroupEnabled"] != false || draft.Temporal["nexusEnabled"] != false {
		t.Fatal("account-wide Temporal capability enabled")
	}
	if draft.Budgets["activeRuns"] != float64(1) || draft.Budgets["concurrentBuilds"] != float64(1) || draft.Budgets["reservedLambdaConcurrencyChangesAllowed"] != false {
		t.Fatal("unbounded shared-account budget")
	}
	if len(draft.PrimaryReferences) < 6 {
		t.Fatal("API-specific references missing")
	}
}

func TestDraftUsesDisjointRunDerivedIdentitiesAndSeparateState(t *testing.T) {
	cfg := fixtureConfig()
	first, err := RenderForReview(cfg)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := RenderForReview(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if string(first) != string(retry) {
		t.Fatal("retry changed fixture identities")
	}
	cfg.Owner.RunID = "run-two"
	second, err := RenderForReview(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var a, b Draft
	if err := json.Unmarshal(first, &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(second, &b); err != nil {
		t.Fatal(err)
	}
	for key, value := range a.Resources {
		if key == "kmsKeyArn" {
			continue
		}
		if value == b.Resources[key] {
			t.Fatalf("resource %s shared across disposable runs", key)
		}
	}
	if a.Resources["stateBucket"] == a.Resources["artifactBucket"] {
		t.Fatal("state and artifacts share a bucket")
	}
	for _, role := range a.Roles {
		raw, _ := json.Marshal(role)
		if strings.Contains(string(raw), a.Resources["stateBucket"]) {
			t.Fatalf("role %s can reach fixture state", role.Name)
		}
		if strings.Contains(string(raw), "tmprl-dem-cld") {
			t.Fatal("draft references production resource roots")
		}
	}
	backend, err := RenderBackendForReview(fixtureConfig())
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(backend), a.Resources["stateBucket"]) || !strings.Contains(string(backend), a.Resources["stateKey"]) || !strings.Contains(string(backend), "use_lockfile = true") || !strings.Contains(string(backend), "REVIEW-ONLY") {
		t.Fatalf("missing separate inert backend: %s", backend)
	}
	for _, change := range []func(*Config){func(c *Config) { c.Owner.Lifetime = ownership.Stable; c.Owner.RunID = "" }, func(c *Config) { c.ResourceRoot = "tmprl-dem-cld" }, func(c *Config) { c.AWS.Partition = "aws-cn" }} {
		invalid := fixtureConfig()
		change(&invalid)
		if _, err := RenderForReview(invalid); err == nil {
			t.Fatal("unsafe or unreviewed draft authority accepted")
		}
	}
}

func TestOfflinePolicyFixturesCoverTransitiveAndForeignAuthority(t *testing.T) {
	draft := fixtureDraft(t)
	repo, project := draft.Resources["repositoryArn"], draft.Resources["buildProjectArn"]
	runtimeSecret := strings.ReplaceAll(draft.Resources["runtimeSecretArnPattern"], "??????", "Abc123")
	sourceSecret := strings.ReplaceAll(draft.Resources["sourceSecretArnPattern"], "??????", "Def456")
	ownerToken := strings.Split(draft.Resources["stateKey"], "/")[1]
	ownedTags := map[string]string{"aws:RequestTag/RegistryOwner": ownerToken, "secretsmanager:ResourceTag/RegistryOwner": ownerToken}
	key := draft.Resources["kmsKeyArn"]
	for _, test := range []struct {
		name, role, action, resource string
		context                      map[string]string
		allow                        bool
	}{
		{"own repository read", "operator", "ecr:DescribeRepositories", repo, nil, true},
		{"owned repository creation", "operator", "ecr:CreateRepository", repo, ownedTags, true},
		{"unowned repository creation", "operator", "ecr:CreateRepository", repo, nil, false},
		{"production repository cleanup", "operator", "ecr:DeleteRepository", "arn:aws:ecr:us-west-1:111122223333:repository/tmprl-dem-cld/production", nil, false},
		{"own build only", "operator", "codebuild:StartBuild", project, nil, true},
		{"own build status uses project resource", "operator", "codebuild:BatchGetBuilds", project, nil, true},
		{"production shared build", "operator", "codebuild:StartBuild", "arn:aws:codebuild:us-west-1:111122223333:project/tmprl-dem-cld-build", nil, false},
		{"role override", "operator", "codebuild:StartBuild", project, map[string]string{"codebuild:serviceRole": "arn:aws:iam::111122223333:role/production"}, false},
		{"own secret update", "operator", "secretsmanager:PutSecretValue", runtimeSecret, ownedTags, true},
		{"create with owned key only", "operator", "secretsmanager:CreateSecret", runtimeSecret, map[string]string{"aws:RequestTag/RegistryOwner": ownerToken, "secretsmanager:KmsKeyArn": key}, true},
		{"create cannot choose a foreign key", "operator", "secretsmanager:CreateSecret", runtimeSecret, map[string]string{"aws:RequestTag/RegistryOwner": ownerToken, "secretsmanager:KmsKeyArn": "arn:aws:kms:us-west-1:111122223333:key/production"}, false},
		{"create cannot silently use default key", "operator", "secretsmanager:CreateSecret", runtimeSecret, ownedTags, false},
		{"unknown owner secret update", "operator", "secretsmanager:PutSecretValue", runtimeSecret, nil, false},
		{"source is read only for build", "build", "secretsmanager:GetSecretValue", sourceSecret, nil, true},
		{"build cannot read runtime secret", "build", "secretsmanager:GetSecretValue", runtimeSecret, nil, false},
		{"build cannot overwrite source credential", "build", "secretsmanager:PutSecretValue", sourceSecret, nil, false},
		{"build pushes only own image", "build", "ecr:PutImage", repo, nil, true},
		{"build cannot push production", "build", "ecr:PutImage", "arn:aws:ecr:us-west-1:111122223333:repository/tmprl-dem-cld/operator", nil, false},
		{"build cannot change repository authority", "build", "ecr:SetRepositoryPolicy", repo, nil, false},
		{"global token is not repository access", "build", "ecr:GetAuthorizationToken", "*", nil, true},
		{"source decryption context", "build", "kms:Decrypt", key, map[string]string{"kms:ViaService": "secretsmanager.us-west-1.amazonaws.com", "kms:EncryptionContext:SecretARN": sourceSecret}, true},
		{"runtime context cannot be used by build", "build", "kms:Decrypt", key, map[string]string{"kms:ViaService": "secretsmanager.us-west-1.amazonaws.com", "kms:EncryptionContext:SecretARN": runtimeSecret}, false},
		{"direct KMS decrypt denied", "build", "kms:Decrypt", key, nil, false},
		{"production secret via shared store", "externalSecrets", "secretsmanager:GetSecretValue", "arn:aws:secretsmanager:us-west-1:111122223333:secret:tmprl-dem-cld/platform/temporal-cloud-bootstrap-Abc123", nil, false},
		{"candidate controller reads only runtime", "externalSecrets", "secretsmanager:GetSecretValue", runtimeSecret, nil, true},
		{"controller cannot read source", "externalSecrets", "secretsmanager:GetSecretValue", sourceSecret, nil, false},
		{"runtime cannot assume operator", "runtime", "sts:AssumeRole", "arn:aws:iam::111122223333:role/production", nil, false},
		{"test function invoke candidate policy", "temporalInvoker", "lambda:InvokeFunction", draft.Resources["lambdaArn"], nil, true},
		{"production function cannot be invoked", "temporalInvoker", "lambda:InvokeFunction", "arn:aws:lambda:us-west-1:111122223333:function:tmprl-dem-cld-production", nil, false},
		{"state is fixture authority only", "operator", "s3:GetObject", "arn:aws:s3:::" + draft.Resources["stateBucket"] + "/" + draft.Resources["stateKey"], nil, false},
		{"production DNS disabled", "operator", "route53:ChangeResourceRecordSets", "arn:aws:route53:::hostedzone/production", nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			role := draft.Roles[test.role]
			if allowed(role.Permissions, role.Boundary, test.action, test.resource, test.context) != test.allow {
				t.Fatalf("unexpected offline policy match for %s", test.name)
			}
		})
	}
}

func TestExplicitBoundaryDenialsSurviveAnAdditionalBroadIdentityGrant(t *testing.T) {
	draft := fixtureDraft(t)
	maliciousAdditionalGrant := Policy{Version: "2012-10-17", Statements: []Statement{{Effect: "Allow", Action: []string{"*"}, Resource: []string{"*"}}}}
	for roleName, role := range draft.Roles {
		for _, resource := range []string{
			"arn:aws:ecr:us-west-1:111122223333:repository/tmprl-dem-cld/production",
			"arn:aws:secretsmanager:us-west-1:111122223333:secret:tmprl-dem-cld/platform/bootstrap-Abc123",
			"arn:aws:codebuild:us-west-1:111122223333:project/tmprl-dem-cld-build",
			"arn:aws:iam::111122223333:role/production",
		} {
			if evaluate(role.Boundary, "arbitrary:Write", resource, nil) != "Deny" {
				t.Fatalf("%s lacks explicit outside-fixture denial", roleName)
			}
		}
		for _, action := range []string{"iam:PassRole", "iam:CreateRole", "iam:UpdateAssumeRolePolicy", "sts:AssumeRole", "kms:CreateGrant", "kms:PutKeyPolicy", "ecr:SetRepositoryPolicy"} {
			if allowed(maliciousAdditionalGrant, role.Boundary, action, draft.Resources["repositoryArn"], nil) {
				t.Fatalf("%s boundary allowed authority mutation %s", roleName, action)
			}
		}
	}
	operator := draft.Roles["operator"]
	if evaluate(operator.Boundary, "codebuild:StartBuild", draft.Resources["buildProjectArn"], map[string]string{"codebuild:serviceRole": "arbitrary"}) != "Deny" {
		t.Fatal("build service-role override lacks explicit deny")
	}
}

func TestBuildTrustIsBoundToItsOwnAccountAndProject(t *testing.T) {
	draft := fixtureDraft(t)
	trust := draft.Roles["build"].TrustPolicy
	if len(trust.Statements) != 1 {
		t.Fatal("unexpected build trust")
	}
	statement := trust.Statements[0]
	if statement.Principal["Service"] != "codebuild.amazonaws.com" || !conditionsMatch(statement.Condition, map[string]string{"aws:SourceAccount": "111122223333", "aws:SourceArn": draft.Resources["buildProjectArn"]}) {
		t.Fatal("missing exact CodeBuild trust boundary")
	}
	if conditionsMatch(statement.Condition, map[string]string{"aws:SourceAccount": "111122223333", "aws:SourceArn": "arn:aws:codebuild:us-west-1:111122223333:project/tmprl-dem-cld-build"}) {
		t.Fatal("production build can assume test build role")
	}
}

func TestEveryGrantedActionHasAnExplicitPrimaryDocumentReview(t *testing.T) {
	raw, err := templates.ReadFile("action-resource-review.json")
	if err != nil {
		t.Fatal(err)
	}
	var review struct {
		Rows []struct {
			Actions        []string `json:"actions"`
			Reference      string   `json:"reference"`
			RemainingProof string   `json:"remainingProof"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(raw, &review); err != nil {
		t.Fatal(err)
	}
	reviewed := map[string]bool{}
	for _, row := range review.Rows {
		if !strings.HasPrefix(row.Reference, "https://docs.aws.amazon.com/") && !strings.HasPrefix(row.Reference, "https://docs.temporal.io/") {
			t.Fatalf("non-primary permission reference: %s", row.Reference)
		}
		if row.RemainingProof == "" {
			t.Fatal("review silently equates document support with effective account proof")
		}
		for _, action := range row.Actions {
			reviewed[action] = true
		}
	}
	for roleName, role := range fixtureDraft(t).Roles {
		for _, statement := range role.Permissions.Statements {
			for _, action := range statement.Action {
				if !reviewed[action] {
					t.Fatalf("new action %s on %s needs action/resource/condition review", action, roleName)
				}
			}
		}
	}
}

// The helpers below implement ONLY the small JSON-policy subset used by these
// offline fixtures. They deliberately make no claim about AWS policy semantics
// outside that subset, resource policies, session principals or effective IAM.
func allowed(identity, boundary Policy, action, resource string, context map[string]string) bool {
	return evaluate(identity, action, resource, context) == "Allow" && evaluate(boundary, action, resource, context) == "Allow"
}

func evaluate(policy Policy, action, resource string, context map[string]string) string {
	decision := "ImplicitDeny"
	for _, statement := range policy.Statements {
		actionMatch := matchesAny(statement.Action, action, true)
		if len(statement.NotAction) > 0 {
			actionMatch = !matchesAny(statement.NotAction, action, true)
		}
		resourceMatch := matchesAny(statement.Resource, resource, false)
		if len(statement.NotResource) > 0 {
			resourceMatch = !matchesAny(statement.NotResource, resource, false)
		}
		if !actionMatch || !resourceMatch || !conditionsMatch(statement.Condition, context) {
			continue
		}
		if statement.Effect == "Deny" {
			return "Deny"
		}
		if statement.Effect == "Allow" {
			decision = "Allow"
		}
	}
	return decision
}

func conditionsMatch(conditions Condition, context map[string]string) bool {
	for operator, keys := range conditions {
		for key, values := range keys {
			actual, present := context[key]
			switch operator {
			case "Null":
				if len(values) != 1 || (values[0] != "true" && values[0] != "false") || (!present) != (values[0] == "true") {
					return false
				}
			case "StringEquals":
				if !present {
					return false
				}
				matched := false
				for _, value := range values {
					if actual == value {
						matched = true
					}
				}
				if !matched {
					return false
				}
			case "StringLike":
				if !present || !matchesAny(values, actual, false) {
					return false
				}
			default:
				return false
			}
		}
	}
	return true
}

func matchesAny(patterns []string, value string, foldCase bool) bool {
	for _, pattern := range patterns {
		if foldCase {
			pattern = strings.ToLower(pattern)
			value = strings.ToLower(value)
		}
		expression := regexp.QuoteMeta(pattern)
		expression = strings.ReplaceAll(strings.ReplaceAll(expression, `\*`, ".*"), `\?`, ".")
		if regexp.MustCompile("^" + expression + "$").MatchString(value) {
			return true
		}
	}
	return false
}
