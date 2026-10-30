package fixtureplan

// RenderDenySentinels describes real owned negative-test targets. It is a
// different module/state from the allowed base, not an extension of its roles.
// It creates metadata only; secret/image seeding and all probes remain disabled.
func RenderDenySentinels() (Rendered, error) {
	c, err := Context()
	if err != nil {
		return Rendered{}, err
	}
	base, err := Render(nil)
	if err != nil {
		return Rendered{}, err
	}
	names := map[string]string{}
	for key, purpose := range map[string]string{"project": "deny-build-project", "role": "deny-build-role", "boundary": "deny-build-boundary", "keyAlias": "deny-kms-alias"} {
		name, err := c.Name("aws", purpose, "primary", 63)
		if err != nil {
			return Rendered{}, err
		}
		names[key] = name
	}
	for key, purpose := range map[string]string{"repository": "deny-repository", "secret": "deny-secret"} {
		name, err := c.Path("aws", purpose, "primary")
		if err != nil {
			return Rendered{}, err
		}
		names[key] = name
	}
	names["logGroup"] = "/aws/codebuild/" + names["project"]
	denyNamespace, err := namespaceName(c, "deny")
	if err != nil {
		return Rendered{}, err
	}
	denySA, err := c.Name("temporal", "service-account", "deny-self-key-owner", 63)
	if err != nil {
		return Rendered{}, err
	}
	resources := Object{}
	guard := base.Terraform["resource"].(Object)["terraform_data"].(Object)["scope_guard"].(Object)
	guard["input"] = RunID + "-deny"
	resourcesFor(resources, "terraform_data", "scope_guard", guard)
	tags := Object{"RegistryFixture": "registry-operator-v2", "RegistryOwner": digest(c.Owner()), "RegistryRun": RunID, "RegistrySentinel": "deny", "RegistryExpiresAt": ref("var.fixture_expires_at")}
	denyAll := encoded(policy(statement("NoDataOrPrivilegeAuthority", "Deny", []string{"*"}, []any{"*"})))
	resourcesFor(resources, "aws_iam_policy", "deny_boundary", Object{"name": names["boundary"], "policy": denyAll, "tags": tags, "depends_on": []string{"terraform_data.scope_guard"}})
	projectARN := arn("codebuild", "project/"+names["project"])
	trust := policy(Object{"Effect": "Allow", "Principal": Object{"Service": "codebuild.amazonaws.com"}, "Action": "sts:AssumeRole", "Condition": Object{"StringEquals": Object{"aws:SourceAccount": Account, "aws:SourceArn": projectARN}}})
	resourcesFor(resources, "aws_iam_role", "deny_build", Object{"name": names["role"], "assume_role_policy": encoded(trust), "permissions_boundary": ref("aws_iam_policy.deny_boundary.arn"), "max_session_duration": 3600, "tags": tags, "depends_on": []string{"terraform_data.scope_guard"}})
	resourcesFor(resources, "aws_iam_role_policy", "deny_build", Object{"name": "fixture-deny-all", "role": ref("aws_iam_role.deny_build.id"), "policy": denyAll})
	adminKeyPolicy := base.Terraform["resource"].(Object)["aws_kms_key"].(Object)["candidate"].(Object)["policy"]
	resourcesFor(resources, "aws_kms_key", "deny", Object{"description": "Owned synthetic deny-sentinel encryption only " + RunID, "deletion_window_in_days": 7, "enable_key_rotation": true, "policy": adminKeyPolicy, "tags": tags, "depends_on": []string{"terraform_data.scope_guard"}})
	resourcesFor(resources, "aws_kms_alias", "deny", Object{"name": "alias/" + names["keyAlias"], "target_key_id": ref("aws_kms_key.deny.key_id")})
	resourcesFor(resources, "aws_secretsmanager_secret", "deny", Object{"name": names["secret"], "description": "Owned synthetic negative-test target; value is NOT seeded by IaC", "kms_key_id": ref("aws_kms_key.deny.arn"), "recovery_window_in_days": 7, "tags": tags, "depends_on": []string{"terraform_data.scope_guard"}})
	resourcesFor(resources, "aws_ecr_repository", "deny", Object{"name": names["repository"], "image_tag_mutability": "IMMUTABLE", "force_delete": false, "encryption_configuration": []Object{{"encryption_type": "AES256"}}, "image_scanning_configuration": []Object{{"scan_on_push": false}}, "tags": tags, "depends_on": []string{"terraform_data.scope_guard"}})
	resourcesFor(resources, "aws_ecr_lifecycle_policy", "deny", Object{"repository": ref("aws_ecr_repository.deny.name"), "policy": encoded(Object{"rules": []Object{{"rulePriority": 1, "description": "Keep at most one harmless deny-sentinel image", "selection": Object{"tagStatus": "any", "countType": "imageCountMoreThan", "countNumber": 1}, "action": Object{"type": "expire"}}}})})
	resourcesFor(resources, "aws_cloudwatch_log_group", "deny", Object{"name": names["logGroup"], "retention_in_days": 1, "tags": tags, "depends_on": []string{"terraform_data.scope_guard"}})
	resourcesFor(resources, "aws_cloudwatch_log_stream", "deny", Object{"name": "deny-sentinel", "log_group_name": ref("aws_cloudwatch_log_group.deny.name")})
	resourcesFor(resources, "aws_codebuild_project", "deny", Object{"name": names["project"], "description": "Owned harmless deny target; unexpected StartBuild must be stopped/reported", "service_role": ref("aws_iam_role.deny_build.arn"), "build_timeout": 5, "queued_timeout": 5, "concurrent_build_limit": 1, "auto_retry_limit": 0, "project_visibility": "PRIVATE", "artifacts": []Object{{"type": "NO_ARTIFACTS"}}, "source": []Object{{"type": "NO_SOURCE", "buildspec": "version: 0.2\nphases:\n  build:\n    commands:\n      - printf '%s\\n' deny-sentinel-unexpected-start\n"}}, "environment": []Object{{"compute_type": "BUILD_GENERAL1_SMALL", "image": "aws/codebuild/standard:7.0", "type": "LINUX_CONTAINER", "privileged_mode": false, "image_pull_credentials_type": "CODEBUILD"}}, "logs_config": []Object{{"cloudwatch_logs": []Object{{"status": "DISABLED"}}}}, "tags": tags, "depends_on": []string{"aws_iam_role_policy.deny_build"}})
	base.Terraform["resource"] = resources
	base.Terraform["terraform"].(Object)["backend"] = Object{"local": Object{"path": ".state/" + RunID + "-deny.tfstate"}}
	base.Terraform["output"] = Object{"deny_bindings": Object{"value": Object{"owner": c.Owner(), "account": Account, "region": Region, "run_id": RunID, "project_arn": projectARN, "role_arn": ref("aws_iam_role.deny_build.arn"), "kms_key_arn": ref("aws_kms_key.deny.arn"), "secret_arn": ref("aws_secretsmanager_secret.deny.arn"), "repository_arn": ref("aws_ecr_repository.deny.arn"), "log_group_arn": ref("aws_cloudwatch_log_group.deny.arn"), "log_stream_arn": ref("aws_cloudwatch_log_stream.deny.arn")}}}
	base.Review = Object{"schemaVersion": "registry-owned-deny-sentinels-v1", "applyAuthorized": false, "seedingAuthorized": false, "probesAuthorized": false, "baseResourcesModified": false, "productionResourcesUsed": false, "account": Account, "region": Region, "owner": c.Owner(), "names": names, "terraformCreates": 12, "awsCreates": 11, "state": "Separate ignored .state/" + RunID + "-deny.tfstate; base module/state untouched", "purpose": "Real controlled resource identities for later effective-permission checks, never missing-name/production stand-ins", "budgets": Object{"additionalCMKs": 1, "additionalBuildProjects": 1, "actualAdditionalBuildStarts": 0, "maximumUnexpectedSuccessBuilds": 1, "maximumBuildMinutes": 5, "maximumQueuedMinutes": 5, "buildRole": "Explicit deny-all boundary and inline; no data/secret/log privileges", "syntheticSecretVersions": 0, "syntheticImages": 0, "logRetentionDays": 1, "totalCompanionNamespaceTargetCount": 3, "softUSDStop": 5, "hardDollarFence": false}, "requiredBeforeProbes": []string{"root reviews this exact source/saved-plan digest before additional creation", "attest each actual canonical ARN/RoleID/key birth/resource creation time and exact tags after any approved apply", "separately authorize secure synthetic non-production secret value seed outside IaC state; bind exact VersionID", "separately authorize one harmless valid synthetic OCI image seed; bind exact tag/digest/config/layers", "use valid harmless API payloads and owner-bound versions/IDs, never arbitrary or nonexistent references", "review expected AccessDenied and unexpected-success handling for every exact call, with stop IDs/receipt/cleanup", "do not reinterpret a resource-not-found or invalid request as a permission denial", "candidate writer/canary remain disabled until separately reviewed actual stage2 policy; base deny-all is not a positive candidate permission proof", "no account/global credentials enter tests; no builds/assume-role/probes authorized by this render"}, "cleanup": "Reviewed exact owned cleanup only; retain unknown outcomes. ECR force_delete=false, secret recovery7d, CMK pending deletion7d. No automatic destroy/retag/recreate/import.", "probes": []Object{{"target": "deny project/role", "requested": "StartBuild or unapproved service-role override", "expected": "AccessDenied through reviewed invoker boundary; no build should start", "unexpectedSuccess": "Record exact owned BuildID, stop/report under separately authorized procedure; do not continue probe batch"}, {"target": "deny repository seeded synthetic image", "requested": "BatchGetImage or harmless PutImage valid manifest", "expected": "AccessDenied outside exact base ECR repository"}, {"target": "deny secret seeded synthetic version", "requested": "GetSecretValue", "expected": "AccessDenied, not missing-value error", "unexpectedSuccess": "Never print/serialize returned bytes; fail/report exact safe IDs"}, {"target": "deny CMK", "requested": "GenerateDataKey with harmless reviewed encryption context", "expected": "AccessDenied outside exact permitted key", "unexpectedSuccess": "Never print/serialize plaintext key; clear/discard in secure boundary and stop"}, {"target": "deny log stream", "requested": "One current-timestamp harmless PutLogEvents event", "expected": "AccessDenied outside exact base log stream pattern"}}}
	base.Temporal = Object{"schemaVersion": "registry-temporal-deny-review-v1", "createAuthorized": false, "requestsEmitted": false, "account": TemporalAccount, "denyNamespace": denyNamespace + "." + TemporalAccount, "denyServiceAccountName": denySA, "principalScopes": "Separate newly owned lab namespace and distinct same-target WRITE SA/key for SELF versus other-owner checks; no production names", "requiredBeforeCreation": []string{"new dedicated owned ProjectID/create provenance and namespace capacity proof", "root-reviewed exact bootstrap plan and short-lived non-printing key delivery", "deny namespace must have no running control/production workloads", "same-target deny SA/key must have real independent ID/birth/provenance; no guessed owner IDs"}, "newProjectNamespacesTotal": 3, "namespaceScopedSAsTotal": 4, "maximumBootstrapKeys": 4, "projectScopedPrincipalLane": false, "crossProjectProofClaimed": false, "noAdminFallback": true}
	return base, nil
}
