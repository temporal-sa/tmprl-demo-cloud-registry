# Isolated Lambda workspaces

Implementation brief for the catalog operator. The product decision is settled: do not reopen it. Build the capability described here.

The first consumer is the rainbow serverless demo (`temporal-demo-rainbow-serverless`). That application change is a separate task. This task makes the operator able to give one authenticated user a private copy of a Lambda-backed demo.

## Decision

An isolated workspace is a time-limited, per-user copy of a demo. It has its own hostname, task queue, and Temporal Worker Deployment. Its order workers are temporary AWS Lambda functions. It reuses the demo's Temporal Cloud namespace, container image, Lambda execution role, and Temporal invoke role.

It is not a crashable workspace. Lambda has no worker process the platform can kill. Disabling a function or setting its reserved concurrency to zero only stops new invocations; invocations already running finish or time out. Do not add crash, scale-to-zero, or scale-to-one controls for this mode, and do not describe it as crashable in the catalog.

The existing crashable workflow remains the right lifecycle to extend: owner identity, TTL, continue-as-new, Kubernetes namespace, ingress, and deletion already exist. Isolation changes which workers are provisioned and which controls are offered.

## Why a new Lambda function is required

Temporal can register another Worker Deployment Version whose compute config points at an existing Lambda ARN and invoke role. That registration does not select the task queue or the deployment identity the process claims.

A published Lambda version freezes its environment. The operator writes `TEMPORAL_DEPLOYMENT_NAME`, `TEMPORAL_WORKER_DEPLOYMENT_NAME`, and `TEMPORAL_WORKER_BUILD_ID` into that environment in `default_worker_deployment_env`. The worker polls whatever task queue it reads at startup. Reusing the always-on qualified ARN therefore starts a worker for the shared deployment and the shared queue.

Create one new Lambda function per serverless component per workspace. Publish a qualified version of that function with the workspace environment. Register that qualified ARN. Reuse the image digest and the IAM roles. Do not publish a new image build.

Do not add the workspace environment as a new version of the always-on function. Publishing updates `$LATEST`, and function-level controls would affect the shared demo.

## Non-goals

- Do not convert an existing crashable demo into this mode.
- Do not remove or relabel the existing "Crashable environment" card.
- Do not crash, throttle, or scale Lambda as a stand-in for killing a pod.
- Do not create a Kubernetes order-worker Deployment for a serverless component.
- Do not create a Temporal namespace, a new image, a new execution role, or a new invoke role per workspace.
- Do not change the always-on Worker Deployment's current version.
- Do not onboard the rainbow demo or edit its repository in this task.

## Schema

Add `spec.registry.isolatedLambda` beside the existing `crashable` object in `schemas/project.schema.json`:

```yaml
registry:
  isolatedLambda:
    enabled: true
    taskQueueEnv: TEMPORAL_TASK_QUEUE
    baseTaskQueue: orders
```

`enabled` defaults to false. `taskQueueEnv` is the one environment variable injected into every workspace client and worker. `baseTaskQueue` is the shared demo's normal queue; the workspace queue is `{baseTaskQueue}-{workspaceId}`, using `derive.crashable_workspace_task_queue`.

A demo is eligible only when `enabled` is true and `taskQueueEnv` is non-empty. This gate must match in validation, catalog JSON, the web API, and the provisioning activity. A visible button must not fail the same way the crashable gate is kept aligned.

`isolatedLambda` and `crashable.enabled` are mutually exclusive. Reject that combination in `scripts/validate_projects.py`.

For a demo whose serverless components share one Worker Deployment and register several build IDs, add an optional boolean `components.<name>.serverless.workerDeployment.initialCurrent`. At most one component per `deploymentName` may set it. Validation fails when two set it. The isolated provisioner uses it to call `SetCurrentVersion` once. The always-on Lambda workflow currently calls `SetCurrentVersion` for every component; do not copy that behavior into the workspace path.

## Workspace lifecycle

Use a separate `IsolatedLambdaWorkspaceWorkflow`. Keep the workflow ID `crashable-workspace/{email}/{demo}` so ownership, TTL, and the current API shape stay singular. The catalog starts that type when `isolatedLambda` is enabled and `CrashableWorkspaceWorkflow` when `crashable` is enabled. Carry mode `isolated-lambda` on the isolated workspace status.

Do not add Lambda activities to `CrashableWorkspaceWorkflow`. That workflow is replayed by a workspace still provisioning during a deploy, and its command sequence has to stay the one its history already recorded. Lambda create, Worker Deployment registration, invoke-policy updates, and Lambda cleanup belong only to the new workflow. `enabled` stays default false: a Lambda demo is eligible only after its application reads `taskQueueEnv` and keeps workflow IDs from colliding in the shared namespace.

Provisioning order:

1. Resolve the always-on images, as today.
2. Render the Kubernetes namespace for the non-serverless components. That includes the UI, the API, and a normal control-plane worker when the demo has one.
3. Inject `taskQueueEnv` into Kubernetes components that already receive Temporal configuration.
4. Do not call `_label_crashable_worker_workloads` in isolated-lambda mode. Do not change the source deployment's routing.
5. Suffix each serverless Worker Deployment name with the workspace ID through `resources.suffixed_dns_name`. Leave the source deployment untouched.
6. For each serverless component, create and register its workspace Lambda function as specified below.
7. Set the current Worker Deployment Version once per deployment name.
8. Wait until the Kubernetes workloads are ready and each workspace Lambda version can be described.

Reject `crash`, `scale_down`, and `scale_up` updates in isolated-lambda mode with a stable application error. Extend and delete still work. Delete and TTL expiry run the Lambda cleanup below, then delete the Kubernetes namespace as they do today. Provisioning failure compensates both.

The workspace still reuses the demo's Temporal namespace and Secrets Manager paths. The ExternalSecret behavior in `crashable_workspace_resources.py` stays as it is.

## Lambda and Temporal resources

Use the activities already called by `DemoProjectServerlessLambdaWorkflow`. Do not duplicate the Temporal RPC payloads.

For each serverless component:

- Function name: a new helper next to `lambda_function_name` that includes the workspace ID and respects Lambda's 64-character limit through the existing `aws_name` truncation. The same user and demo must always produce the same name. `derive.crashable_workspace_id` is the 10-character ID to include.
- Image, architecture, memory, and timeout: copy the always-on component. Use the image already resolved for that build. Do not run CodeBuild.
- Execution role: reuse the always-on role ARN. Do not create a workspace role.
- Invoke role and external ID: reuse `temporal_lambda_invoke_role_name` and `temporal_invoke_external_id` for the project and component. Update that role's invoke policy by unioning the workspace function ARN, `{function ARN}:*`, and the qualified version ARN into the existing resource list. Replacing the list with only the workspace ARN breaks the shared demo. Cleanup removes only the workspace ARNs.
- Environment: start from the same environment the always-on function receives, then override the deployment name with the suffixed name and set `taskQueueEnv` to the workspace queue. Keep the component's literal environment, including a version selector such as `ORDER_VERSION`. Keep the component's configured `buildId` when present; otherwise use the existing default build ID. Build IDs do not need a workspace suffix because they only need to be unique inside the suffixed Worker Deployment.
- Register `CreateWorkerDeployment` for the suffixed name when absent.
- Upsert the Worker Deployment Version with the new qualified ARN, the shared invoke role, and the shared external ID.
- Validate that compute config.
- After every version in a deployment is registered, call `SetCurrentVersion` for the component marked `initialCurrent`. If none is marked, use the lexicographically first component name and record that choice in status. Use the existing allow-no-pollers behavior. Do not call it once per component.

Record the workspace function names, qualified ARNs, deployment names, and build IDs on the workspace status so cleanup does not rediscover them by guessing.

Cleanup, in order:

1. Remove each workspace version's compute registration, or delete the workspace Worker Deployment if the API supports deleting the whole deployment.
2. Delete each workspace Lambda function.
3. Remove the workspace ARNs from the shared invoke policy.
4. Delete the Kubernetes namespace.

Never delete the shared function, either IAM role, the source Worker Deployment, or the Temporal namespace. Cleanup must be idempotent. A missing function or deployment is success.

Tag workspace functions with the existing registry tags plus the workspace ID, so an operator can find orphans after a failed cleanup.

## Catalog

`resources.catalog_entry` currently exposes `crashable.enabled`. Also expose `isolatedLambda.enabled` using the same two-field gate.

In `registry/web/static/app.js`, render a separate card when `isolatedLambda.enabled` is true:

- Title: "Isolated environment".
- Empty-state copy: "Spin up a private, time-limited copy of this demo. Your traffic and deployments stay on their own workers. Auto-deletes after 1 hour."
- Button: "Create isolated environment".
- Running actions: Open, Extend, and Delete only.
- Do not render "Crash workers", "Scale down workers", or "Scale up workers".

Keep the crashable card and the "Crashable" catalog count unchanged. An isolated demo is not counted as crashable.

The web API can keep the existing `/api/crashable-workspace` routes for create, status, extend, and delete. Those handlers must accept a demo when either mode is enabled. The three worker-disruption routes return the same stable error for an isolated demo; the UI must not offer them. Do not add a parallel route family unless the existing handlers cannot carry the mode without breaking a crashable client.

## Application contract

The operator is responsible for injection. The demo application, changed separately, must:

- Read the task queue from `taskQueueEnv` in every worker and every process that starts, signals, queries, or lists work.
- Derive any additional queue from that value, for example `{injected}-control`, so the workspace's clients and workers stay paired.
- Read the Worker Deployment name and build ID from the environment variables the operator already sets.
- Keep queue names out of workflow literals and out of direct environment reads inside workflow code.
- Include the active task queue in workflow IDs and visibility filters. Isolated workspaces share the demo's Temporal namespace.
- Keep its UI and API available while its Lambda workers are absent. This mode does not itself make workers absent, but the application still cannot treat a missing poller as a failed page.

Rainbow's shared deployment has five versions registered at once, with build IDs `v1` through `v5` and one current version. The workspace must preserve that shape on its own deployment name. Pinned versioning overrides are how several versions take traffic together; the operator only has to register all five and select the initial current version.

## Acceptance tests

Add focused tests beside `registry/worker/tests/test_crashable_workspace.py` and `registry/worker/tests/test_serverless_lambda.py`.

- A demo with `isolatedLambda` and no crashable block renders a Kubernetes plan without crashable-worker labels, injects the suffixed task queue, and does not render serverless components as pods.
- Two serverless components sharing a deployment name produce two function names, one suffixed deployment name, two version registrations, and exactly one set-current call.
- The invoke-policy update retains the always-on function ARN.
- Cleanup removes workspace functions and their policy ARNs while leaving the source function and roles present. Repeating cleanup succeeds.
- Crash and scale updates are rejected in isolated-lambda mode.
- A replay fixture for an in-flight crashable provision does not gain Lambda activities.
- Catalog JSON reports `isolatedLambda.enabled` only when both required fields are set, and validation rejects enabling both modes.
- The catalog card for an isolated demo has no crash or scale control.

Run the operator unit tests that cover these files and `python3 scripts/validate_projects.py` from the registry repository.
