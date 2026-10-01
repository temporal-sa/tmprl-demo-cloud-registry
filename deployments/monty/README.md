# Monty deployment adapter

The project declaration lives at `projects/demo/monty.yaml` and builds this
directory from the existing registry repository.

The Dockerfile downloads temporal-community/temporal-agent-harness release
0.5.0 at commit `4cef523a285b0c585f169678e1dbbf5e7644c98f` and installs only the
genai, code-mode, ui, and s3 extras from the upstream lockfile. The upstream
repository needs no changes. Its committed browser assets need no Node build.

The launcher imports the upstream workflows, activities, and packaged web and
session-manager factories. The web process serves the existing Monty registry.
The worker process hosts all three Monty workflows plus the session manager on
their existing task queues. The upstream registry offers the two conversational
agents; the dynamic agent remains available as their subagent.

All processes use the harness's S3 payload-storage helper. This replaces the
upstream launchers' local filesystem default, so offloaded snapshots remain
readable across pods, replacement pods, and worker versions.

Required user-managed Secrets Manager entry in us-west-1:

- `tmprl-dem-cld/monty/llm`: JSON property `GEMINI_API_KEY`.

Payloads live in the private bucket
`tmprl-dem-cld-monty-payloads-429214323166-us-west-1`. Both components use the
`tmprl-dem-cld-monty-payloads` IAM role through `awsIamRoleArn`. The role trusts
only Monty's web and worker service accounts on blue, including revision
namespaces, and permits only GetObject and PutObject in that bucket.
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

The adapter has been validated locally against an isolated Temporal server and
S3 emulator: the UI and agent list return 200, workers poll both queues, a
sandbox script books a flight and updates the trip board, and payloads offloaded
by another process are retrieved from S3.

Before publication, validate projects with `python3 scripts/validate_projects.py`
and run the registry worker tests covering IAM role rendering and validation.
Publish operator support for `awsIamRoleArn` before applying the Monty project,
then verify DemoProject promotion, both task
queues, and a real Gemini conversation with tool approval and a trip-board update.
