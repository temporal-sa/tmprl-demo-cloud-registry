// Package testinfra renders inert sandbox review artifacts. It has no cloud
// clients, command execution, credential lookup, deployment or apply facility.
// Its offline checks are intentionally not an AWS IAM simulator or proof of
// effective cloud permissions, trust policies, quotas or account features.
package testinfra

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/ownership"
)

//go:embed sandbox-plan.template.json backend.hcl.example action-resource-review.json
var templates embed.FS

type Config struct {
	Owner        ownership.Owner
	ResourceRoot string
	AWS          ownership.Scope
}

type Condition map[string]map[string][]string

type Statement struct {
	SID         string            `json:"Sid,omitempty"`
	Effect      string            `json:"Effect"`
	Action      []string          `json:"Action,omitempty"`
	NotAction   []string          `json:"NotAction,omitempty"`
	Resource    []string          `json:"Resource,omitempty"`
	NotResource []string          `json:"NotResource,omitempty"`
	Principal   map[string]string `json:"Principal,omitempty"`
	Condition   Condition         `json:"Condition,omitempty"`
}

type Policy struct {
	Version    string      `json:"Version"`
	Statements []Statement `json:"Statement"`
}

type Role struct {
	Name                string `json:"name"`
	Enabled             bool   `json:"enabled"`
	TrustReviewRequired bool   `json:"trustReviewRequired"`
	TrustPolicy         Policy `json:"trustPolicy"`
	Permissions         Policy `json:"permissions"`
	Boundary            Policy `json:"requiredPermissionBoundary"`
}

type Draft struct {
	SchemaVersion           string              `json:"schemaVersion"`
	ApplyAuthorized         bool                `json:"applyAuthorized"`
	AuthorizationProven     bool                `json:"authorizationProven"`
	Purpose                 string              `json:"purpose"`
	RequiredBindings        []string            `json:"requiredBindings"`
	Resources               map[string]string   `json:"resources"`
	Budgets                 map[string]any      `json:"budgets"`
	BuildProject            map[string]any      `json:"buildProject"`
	Roles                   map[string]Role     `json:"roles"`
	InactiveLambdaExtension map[string]any      `json:"inactiveLambdaExtension"`
	Temporal                map[string]any      `json:"temporal"`
	DisabledCapabilities    []string            `json:"disabledCapabilities"`
	ReviewBeforeApply       []string            `json:"reviewBeforeAnyApply"`
	PrimaryReferences       map[string][]string `json:"primaryReferences"`
}

// RenderForReview derives all resource names from a disposable ownership
// context. Trust/KMS/Temporal bindings intentionally remain placeholders until
// independent fixture review; there is no switch that authorizes application.
func RenderForReview(config Config) ([]byte, error) {
	bindings, err := reviewBindings(config)
	if err != nil {
		return nil, err
	}
	raw, err := templates.ReadFile("sandbox-plan.template.json")
	if err != nil {
		return nil, err
	}
	var draft Draft
	if err := json.Unmarshal(bindJSON(raw, bindings), &draft); err != nil {
		return nil, err
	}
	for key, role := range draft.Roles {
		role.Boundary = boundaryFor(role.Permissions)
		draft.Roles[key] = role
	}
	draft.PrimaryReferences = map[string][]string{
		"codebuild": {
			"https://docs.aws.amazon.com/service-authorization/latest/reference/list_codebuild.html",
			"https://docs.aws.amazon.com/codebuild/latest/APIReference/API_StartBuild.html",
			"https://docs.aws.amazon.com/codebuild/latest/userguide/action-context-keys.html",
			"https://docs.aws.amazon.com/codebuild/latest/userguide/setting-up-service-role.html",
		},
		"ecr": {"https://docs.aws.amazon.com/service-authorization/latest/reference/list_ecr.html"},
		"secretsAndKms": {
			"https://docs.aws.amazon.com/service-authorization/latest/reference/list_secretsmanager.html",
			"https://docs.aws.amazon.com/secretsmanager/latest/userguide/security-encryption.html",
			"https://docs.aws.amazon.com/kms/latest/developerguide/conditions-kms.html",
		},
		"lambdaAndPassRole": {
			"https://docs.aws.amazon.com/service-authorization/latest/reference/list_lambda.html",
			"https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_use_passrole.html",
		},
		"logsAndArtifacts": {
			"https://docs.aws.amazon.com/service-authorization/latest/reference/list_logs.html",
			"https://docs.aws.amazon.com/service-authorization/latest/reference/list_s3.html",
		},
		"temporal": {"https://docs.temporal.io/cloud/manage-access/permissions-reference"},
		"state":    {"https://opentofu.org/docs/language/settings/backends/s3/"},
	}
	return json.MarshalIndent(draft, "", "  ")
}

// RenderBackendForReview emits only an inert .hcl.example body. Operator, build,
// external-secret and runtime policies contain no permission to this state.
func RenderBackendForReview(config Config) ([]byte, error) {
	bindings, err := reviewBindings(config)
	if err != nil {
		return nil, err
	}
	raw, err := templates.ReadFile("backend.hcl.example")
	if err != nil {
		return nil, err
	}
	return bindJSON(raw, bindings), nil
}

func reviewBindings(config Config) (map[string]string, error) {
	if config.Owner.Lifetime != ownership.Disposable || config.AWS.Provider != ownership.AWS {
		return nil, fmt.Errorf("sandbox drafts require explicit disposable ownership and AWS authority")
	}
	if config.AWS.Partition != "aws" {
		return nil, fmt.Errorf("this draft has reviewed only commercial AWS service endpoints; another partition requires a separate review")
	}
	context, err := ownership.NewContext(ownership.Config{Owner: config.Owner, ResourceRoot: config.ResourceRoot, Scopes: []ownership.Scope{config.AWS}})
	if err != nil {
		return nil, err
	}
	bindings := map[string]string{"AWS_PARTITION": config.AWS.Partition, "AWS_ACCOUNT_ID": config.AWS.AccountID, "AWS_REGION": config.AWS.Region}
	ownerJSON, _ := json.Marshal(config.Owner)
	digest := sha256.Sum256(ownerJSON)
	bindings["OWNER_TOKEN"] = hex.EncodeToString(digest[:16])
	for _, item := range []struct {
		key, purpose string
		path         bool
	}{
		{"ECR_REPOSITORY_NAME", "repository", true},
		{"BUILD_PROJECT_NAME", "build-project", false},
		{"BUILD_ROLE_NAME", "build-role", false},
		{"OPERATOR_ROLE_NAME", "operator-role", false},
		{"SECRET_READER_ROLE_NAME", "secret-reader-role", false},
		{"RUNTIME_ROLE_NAME", "runtime-role", false},
		{"INVOKER_ROLE_NAME", "invoker-role", false},
		{"LAMBDA_FUNCTION_NAME", "lambda-function", false},
		{"ARTIFACT_BUCKET_NAME", "artifacts", false},
		{"STATE_BUCKET_NAME", "state", false},
		{"RUNTIME_SECRET_NAME", "runtime-secret", true},
		{"CANDIDATE_SECRET_NAME", "candidate-secret", true},
		{"SOURCE_SECRET_NAME", "source-secret", true},
	} {
		var name string
		if item.path {
			name, err = context.Path(ownership.AWS, item.purpose, "primary")
		} else {
			name, err = context.Name(ownership.AWS, item.purpose, "primary", 63)
		}
		if err != nil {
			return nil, err
		}
		bindings[item.key] = name
	}
	return bindings, nil
}

func bindJSON(raw []byte, bindings map[string]string) []byte {
	keys := make([]string, 0, len(bindings))
	for key := range bindings {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([]string, 0, len(keys)*2)
	for _, key := range keys {
		encoded, _ := json.Marshal(bindings[key])
		pairs = append(pairs, "${"+key+"}", string(encoded[1:len(encoded)-1]))
	}
	return []byte(strings.NewReplacer(pairs...).Replace(string(raw)))
}

// boundaryFor preserves narrow allows and adds explicit denials, including
// outside-resource grants. It must be attached by a separately reviewed fixture
// authority; this renderer cannot establish effective account permissions.
func boundaryFor(permissions Policy) Policy {
	boundary := Policy{Version: "2012-10-17", Statements: append([]Statement(nil), permissions.Statements...)}
	resources, global := map[string]bool{}, map[string]bool{}
	for _, statement := range permissions.Statements {
		if statement.Effect != "Allow" {
			continue
		}
		for _, resource := range statement.Resource {
			if resource == "*" {
				for _, action := range statement.Action {
					global[action] = true
				}
			} else {
				resources[resource] = true
			}
		}
	}
	outside := Statement{SID: "ExplicitlyDenyOutsideFixture", Effect: "Deny", NotResource: sortedKeys(resources)}
	if len(global) > 0 {
		outside.NotAction = sortedKeys(global)
	} else {
		outside.Action = []string{"*"}
	}
	boundary.Statements = append(boundary.Statements, outside, Statement{
		SID: "NeverEscalateOrAlterAuthority", Effect: "Deny", Resource: []string{"*"},
		Action: []string{"iam:*", "sts:AssumeRole*", "codebuild:CreateProject", "codebuild:UpdateProject", "ecr:SetRepositoryPolicy", "ecr:DeleteRepositoryPolicy", "secretsmanager:PutResourcePolicy", "secretsmanager:DeleteResourcePolicy", "s3:PutBucketPolicy", "s3:PutBucketAcl", "s3:PutObjectAcl", "kms:PutKeyPolicy", "kms:CreateGrant"},
	})
	for _, statement := range permissions.Statements {
		for _, action := range statement.Action {
			if action == "codebuild:StartBuild" {
				boundary.Statements = append(boundary.Statements, Statement{
					SID: "NeverOverrideBuildServiceRole", Effect: "Deny", Action: []string{"codebuild:StartBuild"}, Resource: statement.Resource,
					Condition: Condition{"Null": {"codebuild:serviceRole": {"false"}}},
				})
			}
		}
	}
	return boundary
}

func sortedKeys(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
