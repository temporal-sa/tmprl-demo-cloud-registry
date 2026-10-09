# Monty deployment adapter

The project declaration lives at `projects/demo/monty.yaml` and builds this
directory from the existing registry repository.

The Dockerfile downloads temporal-community/temporal-agent-harness main
at commit `ceb6ef3fdf3f31b32f571c8460d4fd545e117977` and installs only the
genai, code-mode, ui, and s3 extras from the upstream lockfile. The upstream
repository needs no changes. Its committed browser assets need no Node build.

The launcher imports the upstream workflows, activities, and packaged web and
session-manager factories. The web process serves the existing Monty registry.
The worker process hosts all three Monty workflows plus the session manager.
The upstream registry offers the two conversational
agents; the dynamic agent remains available as their subagent.

All processes use the harness's S3 payload-storage helper. This replaces the
upstream launchers' local filesystem default, so offloaded snapshots remain
readable across pods, replacement pods, and worker versions.

Required shared Secrets Manager entry in us-west-1:

- `tmprl-dem-cld/platform/shared/gemini`: JSON property `GEMINI_API_KEY`.

Payloads live in the private bucket
`tmprl-dem-cld-monty-payloads-429214323166-us-west-1`. Both components use the
`tmprl-dem-cld-monty-payloads` IAM role through `awsIamRoleArn`. The role trusts
Monty's web and worker service accounts on blue, including revision namespaces
and the `web` and `worker` service accounts in crashable namespaces, and permits
only GetObject and PutObject in that bucket. `payload-role-trust.json` records
that trust policy; its OIDC provider must match the current blue cluster.
The bucket requires HTTPS, enables server-side encryption, and blocks public
access. Keep objects for at least as long as any
open workflow or retained history may reference them; an arbitrary short TTL
would break replay. IAM user creation is blocked by account policy, so no
long-lived access keys are used. The operator manages Temporal credentials
separately. The bucket and workload role are external prerequisites; the
operator does not create or delete them.

The worker uses platform-injected deployment metadata and defaults workflows
to PINNED. Keep old versions while their workflows remain open. In particular,
the upstream singleton session manager does not Continue-As-New, so an old
worker version can remain necessary after an upstream upgrade.

Flux watches the adapter path in the existing registry repository. To upgrade
Monty, update the pinned archive commit in the Dockerfile and its documentation.
Upstream commits do not automatically deploy. The registry-operator source
filters exclude `deployments/`, avoiding an operator rebuild for adapter edits.

Ingress remains behind the platform's Temporal employee authentication at
`https://monty.tmprl-demo.cloud`. The candidate smoke check uses `/api/agents`,
which queries the session manager and reports agent worker availability.

The catalog can create a private crashable environment with Crash Workers,
Scale Down Workers, and Scale Up Workers controls. The platform injects
`MONTY_AGENT_TASK_QUEUE=monty-dynamic-agent-<workspace-id>` into both components
and gives the worker its own Temporal Worker Deployment. The launcher uses that
queue for every agent and derives a separate session-manager queue and workflow
ID by appending `-session-manager`. With the variable unset, the always-on demo
retains `monty-dynamic-agent` and `session-manager`.

`patch_workspace_routing.py` adjusts the pinned source inside the image: subagents
use their parent workflow's task queue, and session discovery filters by the
registry's task queues. It fails the build if either source location changes.
The upstream checkout is untouched. Existing always-on histories retain the same
subagent queue; new workspace histories use their own queue during replay.

Before enabling crashable environments in the live catalog, apply the payload
role's trust policy so workspace pods can retrieve S3 snapshots after a crash:

```sh
aws --profile SolutionsArchitecture/AWSAdministratorAccess iam update-assume-role-policy \
  --role-name tmprl-dem-cld-monty-payloads \
  --policy-document file://deployments/monty/payload-role-trust.json
```

Publish the adapter and project declaration together, and wait for the new image
to be promoted before creating a workspace. Crash Workers deletes only the
workspace's worker pods; Scale Down Workers keeps them down until Scale Up Workers
is requested. The web pod stays running. Verify a pending turn resumes after
replacement workers start and that sessions from other environments do not
appear in the workspace UI.

The adapter has been validated locally against an isolated Temporal server and
S3 emulator: the UI and agent list return 200, workers poll both queues, a
sandbox script books a flight and updates the trip board, and payloads offloaded
by another process are retrieved from S3.

Before publication, validate projects with `python3 scripts/validate_projects.py`
and run the registry worker tests covering IAM role rendering and validation.
Publish operator support for `awsIamRoleArn` before applying the Monty project,
then verify DemoProject promotion, both task
queues, and a real Gemini conversation with tool approval and a trip-board update.
