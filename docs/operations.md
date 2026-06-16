# Operations Runbook

How to operate the `sa-demo` registry platform. Pairs with
[architecture.md](architecture.md). Assumes `kubectl` is pointed at the
`sa-demo` EKS cluster and the AWS profile is set:

```sh
export AWS_PROFILE="SolutionsArchitecture/AWSAdministratorAccess"   # account 429214323166, region us-west-1
```

Key namespaces: operator and registry app run in
`tmprl-dem-cld-registry-operator`; each demo gets `tmprl-dem-cld-<name>`.
Temporal Cloud namespace for the operator:
`tmprl-dem-cld-registry-operator.a2dd6`, task queue `registry-management`.

## Deploying / upgrading the operator image

The operator worker is a Temporal Worker Controller `WorkerDeployment` in
`tmprl-dem-cld-registry-operator`; the singleton Kubernetes watch loop runs in
the non-versioned `registry-operator-watch` Deployment. The
`registry-operator` `DemoProject` owns the versioned operator worker: source
watch resolves the current source commit, CodeBuild builds the web and worker
images, the operator updates the `WorkerDeployment`, and Worker Controller runs
the canary gate before promotion.

- After pushing source changes, Flux/source watch picks them up on its interval.
  The WorkerDeployment status can still show the old current version while the
  target is building or gated; confirm the running image:
  ```sh
  kubectl -n tmprl-dem-cld-registry-operator get pods \
    -o jsonpath='{range .items[*]}{.spec.containers[0].image}{"\n"}{end}'
  ```

## Restarting the watch loop — use delete pod, NOT rollout restart

`kubectl rollout restart` stamps a `restartedAt` pod-template annotation that
isn't in git, so the `registry-operator` Flux Kustomization reverts it on its
next reconcile → a second rollout (pod churn that can kill in-flight activities).
To restart cleanly under Flux, **delete the pod** (the ReplicaSet recreates it;
the template doesn't drift):

```sh
kubectl -n tmprl-dem-cld-registry-operator delete pod -l app.kubernetes.io/component=watch
```

Why you'd restart: the watch loop keeps an **in-memory `seen` cache** and only
signals a project when its spec hash changes. If you intentionally terminate an
entity workflow so it restarts on the current worker version, the watch will
**not** re-signal until `seen` clears; restarting the pod clears it and
re-signals every project.

## Workflow-code changes and pinned Worker Versioning

The operator worker uses Temporal Worker Controller plus SDK Worker Versioning
with pinned workflow behavior. Existing entity workflows stay on the worker
version they started on; new workflows route to the current WorkerDeployment
version after the canary gate passes. This is the normal path for workflow-code
changes and avoids forcing old histories to replay on new code.

Do **not** manually delete old Worker Controller-managed Deployments while they
may still have pinned executions. Worker Controller drains and sunsets old
versions according to the WorkerDeployment `sunset` policy.

Only terminate entity workflows when you deliberately want them to restart on
the current code rather than finish on their pinned version. After terminating
`demo-project/<name>`, delete the `registry-operator-watch` pod so the in-memory
watch cache clears and `SignalWithStart` recreates the workflow.

## Forcing a Flux reconcile

```sh
kubectl -n flux-system annotate gitrepository cluster-gitops-config \
  reconcile.fluxcd.io/requestedAt="$(date +%s)" --overwrite
# wait for the GitRepository to fetch the new revision, then:
kubectl -n flux-system annotate kustomization registry-operator \
  reconcile.fluxcd.io/requestedAt="$(date +%s)" --overwrite
kubectl -n flux-system get gitrepository cluster-gitops-config \
  -o jsonpath='{.status.artifact.revision}{"\n"}'
```

## Flux wiring

The Flux `Kustomization` CRs in `clusters/sa-demo/flux-system/kustomizations.yaml`
are self-managed by the root `flux-system` Kustomization. After the initial
bootstrap apply, edits to `clusters/sa-demo/flux-system/` reconcile from git like
the rest of the platform wiring.

To force Flux to fetch and reconcile the current branch:

```sh
kubectl -n flux-system annotate gitrepository cluster-gitops-config \
  reconcile.fluxcd.io/requestedAt="$(date +%s)" --overwrite
kubectl -n flux-system annotate kustomization flux-system \
  reconcile.fluxcd.io/requestedAt="$(date +%s)" --overwrite
```

## Testing a managed Instruqt course

After the operator image with Instruqt support is current, force the project
Kustomizations if you do not want to wait for the normal interval:

```sh
kubectl -n flux-system annotate kustomization registry-instruqt-courses \
  reconcile.fluxcd.io/requestedAt="$(date +%s)" --overwrite
kubectl get instruqtcourse durable-ad-campaign-python-workshop \
  -o jsonpath='{.status.phase}{" "}{.status.message}{"\n"}'
```

Useful status paths while the workflow runs:

```sh
kubectl get instruqtcourse durable-ad-campaign-python-workshop \
  -o jsonpath='{.status.buildStatus.phase}{"\n"}'
kubectl get instruqtcourse durable-ad-campaign-python-workshop \
  -o jsonpath='{.status.deployment.phase}{"\n"}'
kubectl get instruqtcourse durable-ad-campaign-python-workshop \
  -o jsonpath='{.status.testing.deployment.phase}{"\n"}'
```

Push or merge a `live-test` branch in the course source repo to create/update
the testing track. Delete that branch to have the operator delete the managed
testing track.

## Installing Temporal Worker Controller

The Flux Kustomizations are self-managed. After adding or changing Worker
Controller wiring in git, force a source/root reconcile if you do not want to
wait for the normal interval:

```sh
kubectl -n flux-system annotate gitrepository cluster-gitops-config \
  reconcile.fluxcd.io/requestedAt="$(date +%s)" --overwrite
kubectl -n flux-system annotate kustomization flux-system \
  reconcile.fluxcd.io/requestedAt="$(date +%s)" --overwrite
```

Expected install checks:

```sh
kubectl -n flux-system get kustomization temporal-worker-controller
kubectl -n tmprl-dem-cld-worker-controller get helmrelease
kubectl get crd connections.temporal.io workerdeployments.temporal.io
```

Both HelmReleases should be ready, and the `Connection` and `WorkerDeployment`
CRDs must exist before `registry-operator` or `registry-projects` reconcile.

## Worker Controller acceptance checks

For a versioned project, use a temporary `DemoProject` whose worker code already
enables SDK Worker Versioning from `TEMPORAL_DEPLOYMENT_NAME` and
`TEMPORAL_WORKER_BUILD_ID`. After Flux applies it:

```sh
kubectl get demoproject <name> -o jsonpath='{.status.workerDeploymentStatus.phase}{"\n"}'
kubectl get demoproject <name> -o jsonpath='{.status.candidateRolloutStatus.workerDeployments}{"\n"}'
kubectl -n tmprl-dem-cld-<name> get connection,workerdeployment,svc
kubectl -n tmprl-dem-cld-<name> get workerdeployment <worker-deployment-name> -o yaml
```

Acceptance evidence is: WorkerDeployment target version registers, any gate test
workflow completes, the current version advances, and older versions move to
draining/drained according to Worker Controller status.

For operator canary acceptance, first verify the happy path:

```sh
kubectl -n tmprl-dem-cld-registry-operator get connection temporal
kubectl -n tmprl-dem-cld-registry-operator get workerdeployment registry-operator -o yaml
kubectl get demoproject -l registry.tmprl-demo.cloud/canary=true
```

To prove the gate blocks promotion, test in a controlled branch or maintenance
window with a candidate that makes `RegistryOperatorCanaryWorkflow` fail. The
previous current WorkerDeployment version should remain current. Restore the
good-image path and verify the canary creates, promotes, finalizes, and deletes
its namespace.

## Watching a rollout

```sh
kubectl get demoproject                                   # PHASE per project
kubectl get demoproject <name> -o yaml                    # full status
kubectl -n tmprl-dem-cld-registry-operator logs deploy/registry-operator-watch -f
kubectl -n tmprl-dem-cld-registry-operator get workerdeployment registry-operator -o yaml
kubectl -n tmprl-dem-cld-<name> get deploy,svc,ingressroute,certificate,externalsecret
aws codebuild list-builds-for-project --project-name tmprl-dem-cld-build --region us-west-1 \
  --query 'ids[0]' --output text
```

Phase progression for ordinary app components: `Reconciling →
WaitingForExternalResources → WaitingForImages → CandidateApplied →
SmokeCheckPending → Promoted`. Versioned workers insert
`WorkerRolloutPending → WorkerRolloutReady` before smoke checks or route
promotion. Failure phases include `WorkerRolloutFailed`, `CandidateNotReady`,
and `SmokeCheckFailed`; the live route is left untouched. Proof the workflow is
actually progressing past its `wait_condition` is a
`Reconciling external resources project=<name>` activity log line.

Temporal runtime API keys are 30-day keys. Each `DemoProject` entity workflow
stores the key expiry returned by the Cloud Operations API and sleeps durably
until 10 days before expiry. On wake-up it runs a normal reconcile, mints a
replacement key, updates AWS Secrets Manager, patches the existing Kubernetes
runtime Secret when the namespace already exists, and renders a new candidate
rollout version from the new `keyId`.

Pods consume Temporal credentials as environment variables, so a Kubernetes
Secret update alone is not enough. Flux does not restart those Pods for us; the
operator rolls the dependent Deployments/WorkerDeployments by changing the
candidate version, waits for readiness and smoke checks, then promotes the
route. The old key remains valid until its original expiry, so failed candidate
validation leaves the live Pods on the old key.

## Prerequisites that must exist (external bootstrap)

- CodeBuild project `tmprl-dem-cld-build` (region us-west-1).
- Secrets Manager: `tmprl-dem-cld/platform/temporal-cloud-bootstrap`
  (`TEMPORAL_CLOUD_API_KEY`, `TEMPORAL_CLOUD_ACCOUNT_ID`, `TEMPORAL_CLOUD_REGION`).
- Secrets Manager: `tmprl-dem-cld/platform/github-temporal-sa-readonly`
  (`username`, `password`, optional `expiresAt`) for temporal-sa HTTPS source
  watches and CodeBuild checkouts.
- Secrets Manager: `tmprl-dem-cld/platform/instruqt-cli`
  (`INSTRUQT_TOKEN`, optional `organization`) for operator-owned Instruqt CLI
  operations.
- IRSA role `tmprl-dem-cld-registry-operator` on the operator ServiceAccount
  (ECR + Secrets Manager + CodeBuild, scoped to `tmprl-dem-cld/*`).
- Operator RBAC: `demoprojects` needs `get/list/watch/update/patch`
  (update/patch for the finalizer), `demoprojects/status` patch, and `namespaces`
  `delete` for teardown. (These were added after the first managed project, whose
  finalizer first exercised the path the control-plane registry skips.)
- Custom-secret **values** are still created manually at
  `tmprl-dem-cld/<name>/<secret>`; the operator only verifies they exist.

## Tearing down a demo

Remove `projects/demo/<name>.yaml` from git. Flux prunes the `DemoProject` CR; the
finalizer holds deletion while the entity workflow's `finalize` runs (delete k8s
namespace + Temporal namespace/SA/key/secret + ECR), then removes the finalizer.
Flux prune is per-Kustomization inventory — it only ever touches `DemoProject`
CRs it applied, never the hand-created demo namespaces in the cluster.
