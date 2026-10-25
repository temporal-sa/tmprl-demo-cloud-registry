# Runbook: the catalog as an infra provisioning demo

This runbook is for presenting the catalog at [catalog.tmprl-demo.cloud](https://catalog.tmprl-demo.cloud) as a Temporal demo. For operating the platform, see [docs/operations.md](docs/operations.md).

## What it shows

A platform team turns one reviewed YAML file into a fully provisioned environment (a Temporal Cloud namespace, scoped keys, images, routes and TLS), and later removes it cleanly. No tickets, and nothing left half-built. The catalog is that system, and it runs on Temporal.

Three problems come up along the way:

1. **Half-provisioned environments.** A step fails partway and leaves orphaned namespaces, keys and image repositories behind. Here, the workflow undoes what it created, and teardown proves each resource is gone.
2. **Long jobs outliving the process that started them.** An image build takes several minutes, and the operator pod can restart during it. Here, the build activity heartbeats its build ID, and a retry reattaches to the running build instead of starting a second one.
3. **Bad rollouts reaching users.** A broken build takes the live route. Here, a candidate gets the route only after it's ready and passes its health check. The operator protects its own upgrades the same way, with a canary gate.

## Before you start

- Sign in at [catalog.tmprl-demo.cloud](https://catalog.tmprl-demo.cloud) with your temporal.io Google account.
- Make your own copy of the [slide deck](https://docs.google.com/presentation/d/1Y7ZK4FFo9z1c4rIM3cRNvlwATOU1AwYYqK2p-TbygIY). Its **Demo time** slide hands off to this walkthrough, and its speaker notes follow the same beats.
- Open the **Demo Cloud Registry** entry and click **Open demo**. It takes you to the control panel at [/control](https://catalog.tmprl-demo.cloud/control).
- Under **Get Temporal UI access**, click **Request Namespace Access**. Do it before the audience arrives, and check that you can open the operator's namespace in the Temporal Cloud UI.
- Each presenter gets their own demo environment, keyed to their sign-in. Other presenters can't see or change yours. One environment per person, and it ends on its own after 2 hours.
- A full provision takes several minutes, mostly the Temporal Cloud namespace and the image build. Rehearse once and note your times. Before the audience arrives, provision one environment so you have a finished one to show if the live run is slow. You can only have one at a time, so if you want both a warm and a live run, use a second presenter's account for the warm one.
- Keep the AWS console and kubeconfigs off the shared screen. The panel leaves out the CodeBuild log link for that reason.

## Start it

1. Open [catalog.tmprl-demo.cloud/control](https://catalog.tmprl-demo.cloud/control).
2. A healthy start shows the three steps: **Get Temporal UI access**, **Provision a demo environment** (with a **Provision a demo environment** button) and **Tear it down**.
3. In a second tab, open the Temporal Cloud UI for the operator's namespace, `tmprl-dem-cld-registry-operator`. This is where the catalog's own workflows run.

## The walkthrough

### 1. One file in, one workflow per project

- **Do:** Show [projects/demo/banking-money-transfer.yaml](projects/demo/banking-money-transfer.yaml) on GitHub. It's the whole input for a real demo.
- **Say:** "A project owner commits this file. Everything after this is a Temporal workflow."
- **Do:** In the Temporal UI, filter for `WorkflowType = "DemoProjectWorkflow"`. There's one running entity workflow per catalog project, with the ID `demo-project/blue/<name>`.
- **Proof:** Open any one. Its history shows the reconcile signals and the provisioning activities.

### 2. Provision an environment live

- **Do:** On the panel, click **Provision a demo environment**.
- **The audience sees:** a progress list: **Temporal Cloud and image repositories**, **Build images**, **Deploy**, **Roll out workers and check health**. The current step shows its live detail.
- **Say:** "This is the same path every catalog project takes. It creates a Temporal Cloud namespace and a scoped API key, image repositories, runs a CodeBuild build, and deploys a candidate."
- **Do:** Click **Watch the provisioning workflow**. It opens the entity workflow for your environment (`demo-project/blue/pv-infra-demo-…`).
- **Proof:** Point at the activities as they run: `reconcile_temporal_cloud_resource`, `reconcile_ecr_repositories`, `start_project_image_build`, `wait_for_project_image_build`.

### 3. The build outlives the process (problem 2)

- **Do:** While **Build images** is the current step, open `wait_for_project_image_build` in the history and show its heartbeat details. They carry the CodeBuild build ID and phase.
- **Say:** "This build runs for minutes. If the operator restarts now, Temporal retries the activity, and the retry reads the build ID from the last heartbeat and reattaches. No duplicate build, no lost progress."
- **Show it in code:** [Reattach to a running build after a restart](#reattach-to-a-running-build-after-a-restart).

### 4. Nothing goes live until it's healthy (problem 3)

- **Do:** When **Roll out workers and check health** finishes, the panel shows **Provisioned and live.** Click the **Open …** link to show the running sample app, type a name and click **Say hello**. The greeting comes back from a workflow running in the namespace that was created a few minutes ago.
- **Say:** "The new version ran beside the old one as a candidate. It got the public route only after its readiness probe and health check passed. A broken build never reaches users."
- **Proof:** In the history, the readiness and smoke-check activities come before route promotion.
- **Show it in code:** [Promote a candidate only after it proves it's healthy](#promote-a-candidate-only-after-it-proves-its-healthy).

### 5. Tear it down and prove it (problem 1)

- **Do:** Under **Tear it down**, click **Tear it down** and confirm.
- **The audience sees:** **Tearing down: Deleting…**, then **Waiting for cleanup…**, then **Your last demo environment ended, and everything it created was deleted.**
- **Say:** "Deleting the project starts finalize. The workflow removes the namespace, the key, the images and the Kubernetes resources, and the panel only reports done once it has checked each one is gone."
- **Proof:** Click **The demo's own Temporal namespace** before teardown, then refresh after. The namespace no longer exists.
- **Show it in code:** [Undo a failed first provision](#undo-a-failed-first-provision) and [Prove cleanup before calling it done](#prove-cleanup-before-calling-it-done).

### 6. The operator upgrades itself safely (optional)

- **Say:** "The operator is itself a Temporal worker, so a bad operator release could break every environment. Its own rollouts use Worker Versioning: a running workflow stays on the version it started on until it continues as new, and a new version only takes traffic after a canary workflow provisions and tears down real test projects."
- **Proof:** In the Temporal UI, filter for `WorkflowType = "RegistryOperatorCanaryWorkflow"` and open the latest run. It fans out one child workflow per canary template (API key, mTLS, Lambda, Nexus and more).
- **Show it in code:** [Gate operator releases on a canary workflow](#gate-operator-releases-on-a-canary-workflow).

## Show it in code

Links point at commit `19751dc` on `main`. After the control panel merges, refresh any link whose file changed (see [Keeping links current](#keeping-links-current)).

### Start or signal one workflow per project

**When to use it:** beat 1, or when someone asks how a YAML file becomes a workflow.

**Where it is:** [`registry/worker/src/demo_registry_operator/watch.py` lines 649–672](https://github.com/temporal-sa/tmprl-demo-cloud-registry/blob/19751dcea5d976fd3df0a1976966bebd648a2f6d/registry/worker/src/demo_registry_operator/watch.py#L649-L672) (`code -g registry/worker/src/demo_registry_operator/watch.py:649`)

```python
# SignalWithStart: signals the existing run if present, otherwise starts the
# workflow and signals it.
await temporal_client.start_workflow(
    DemoProjectWorkflow.run,
    args=[DemoProjectRunRequest(project_name=name)],
    id=demo_project_workflow_id(name, config.cluster),
    task_queue=config.task_queue,
    start_signal=signal_name,
    start_signal_args=[signal_arg],
)
```

**What you need to understand:**

- A small poll loop outside Temporal lists the project files every 15 seconds and sends each one to its workflow. The signal carries the whole desired state, so a missed or duplicate signal is harmless: the next one converges.
- The workflow ID is derived from the project name, so each project has exactly one entity workflow, and Signal-With-Start creates it on first sight.
- The poller runs outside Temporal on purpose, so polling doesn't cost Actions.

**Docs:** [Signal-With-Start](https://docs.temporal.io/sending-messages#signal-with-start), [Entity workflows](https://docs.temporal.io/design-patterns/entity-workflow)

### Reattach to a running build after a restart

**When to use it:** beat 3, or when someone asks what happens to a long job if a worker dies.

**Where it is:** [`activities/builds/codebuild/execution.py` lines 82–92](https://github.com/temporal-sa/tmprl-demo-cloud-registry/blob/19751dcea5d976fd3df0a1976966bebd648a2f6d/registry/worker/src/demo_registry_operator/activities/builds/codebuild/execution.py#L82-L92) and the call in [`workflows/demo_project_artifact_build.py` lines 53–69](https://github.com/temporal-sa/tmprl-demo-cloud-registry/blob/19751dcea5d976fd3df0a1976966bebd648a2f6d/registry/worker/src/demo_registry_operator/workflows/demo_project_artifact_build.py#L53-L69)

```python
# activities/builds/codebuild/execution.py (abbreviated)
build_id = codebuild_build_id_from_heartbeat()
if build_id:
    activity.heartbeat({..., "codebuildBuildId": build_id, "phase": "Reattaching"})
    return _start_result({**plan, "codebuildBuildId": build_id})
# ... otherwise start a new CodeBuild run and heartbeat its ID

# workflows/demo_project_artifact_build.py (abbreviated)
await workflow.execute_activity(
    build_activities.wait_for_project_image_build,
    ...,
    start_to_close_timeout=timedelta(hours=1),
    heartbeat_timeout=timedelta(minutes=2),
)
```

**What you need to understand:**

- The activity records the CodeBuild build ID in its heartbeat. Temporal keeps the last heartbeat's details, and a retry reads them through `activity.info().heartbeat_details`.
- If a worker dies, the heartbeat timeout (1 to 2 minutes) tells Temporal the attempt is gone, and it schedules a retry on another worker. That retry picks up the same build.
- Starting and waiting are separate activities, so a slow build never holds a start attempt open, and each has its own timeout.
- The customer doesn't write a job table, a lease or a recovery sweeper. The heartbeat is the checkpoint.

**Docs:** [Activity heartbeats](https://docs.temporal.io/encyclopedia/detecting-activity-failures#activity-heartbeat)

### Promote a candidate only after it proves it's healthy

**When to use it:** beat 4.

**Where it is:** [`workflows/demo_project_kubernetes_runtime.py` lines 198–226](https://github.com/temporal-sa/tmprl-demo-cloud-registry/blob/19751dcea5d976fd3df0a1976966bebd648a2f6d/registry/worker/src/demo_registry_operator/workflows/demo_project_kubernetes_runtime.py#L198-L226)

```python
readiness_result = await self._wait_for_candidate_readiness(demo_project, smoke_plan)
if not readiness_result.get("ok"):
    cleanup_status = await self._cleanup_failed_candidate(
        ..., failure_phase="CandidateNotReady", ...
    )
    ...
smoke_result = await self._run_smoke_check(project, demo_project, smoke_plan)
if not smoke_result.get("ok"):
    cleanup_status = await self._cleanup_failed_candidate(
        ..., failure_phase="SmokeCheckFailed", ...
    )
# ... only then: self._promote_candidate(...)
```

**What you need to understand:**

- Each build deploys as a versioned candidate (`<component>-git-<sha>`) beside the live version. The public route moves only at promotion.
- A candidate that never gets ready, or fails the health check, is cleaned up, and the live version keeps serving.
- The smoke check polls with backoff, so an app that takes a while to boot isn't failed early.

### Undo a failed first provision

**When to use it:** beat 5, or when someone asks what happens if step 4 of 6 fails.

**Where it is:** [`workflows/demo_project.py` lines 2300–2323](https://github.com/temporal-sa/tmprl-demo-cloud-registry/blob/19751dcea5d976fd3df0a1976966bebd648a2f6d/registry/worker/src/demo_registry_operator/workflows/demo_project.py#L2300-L2323)

```python
async def _compensate_first_time_external_resources(self) -> None:
    # Newest target first, so a project with additional namespaces unwinds in
    # the reverse of the order it provisioned.
    for context in reversed(contexts):
        ...
        # Full teardown of resources created THIS pass, mirroring the finalize
        # order (api key -> service account -> namespace -> secrets).
        await self._compensate_temporal_candidate_api_key(plan, state)
        await self._compensate_temporal_service_account(plan, state)
        await self._compensate_temporal_namespace(plan, state)
```

**What you need to understand:**

- This is a saga. The workflow remembers what it created in this pass and, if the first provision fails, undoes it in reverse order.
- An established project keeps its live version when a later reconcile fails. Only a first provision is unwound.
- The workflow's state survives worker restarts, so the compensation still runs if the operator restarts mid-failure.

**Docs:** [Saga pattern](https://docs.temporal.io/design-patterns/saga-pattern)

### Prove cleanup before calling it done

**When to use it:** beat 5.

**Where it is:** [`workflows/onboarding_preview.py` lines 179–199](https://github.com/temporal-sa/tmprl-demo-cloud-registry/blob/19751dcea5d976fd3df0a1976966bebd648a2f6d/registry/worker/src/demo_registry_operator/workflows/onboarding_preview.py#L179-L199)

```python
async def _tear_down(self) -> None:
    ...
    await workflow.execute_activity(
        activities.delete_onboarding_preview, ...
    )
    self._status.step = "Waiting for cleanup"
    await workflow.execute_activity(
        activities.wait_onboarding_preview_cleanup,
        ...,
        start_to_close_timeout=CLEANUP_TIMEOUT,
        heartbeat_timeout=CLEANUP_HEARTBEAT_TIMEOUT,
        retry_policy=activities.CLEANUP_RETRY_POLICY,
    )
    self._status.phase = "Ended"
```

**What you need to understand:**

- The control panel's environment is a preview DemoProject owned by this workflow. Deleting it triggers the operator's finalize, and the second activity heartbeats until the namespace, source watch and catalog status are all gone.
- The cleanup retry policy has no attempt limit: teardown must eventually happen.
- The same workflow ends the environment when its 2-hour TTL runs out, so an abandoned demo doesn't keep consuming Actions.

### Gate operator releases on a canary workflow

**When to use it:** beat 6.

**Where it is:** [`projects/operator/registry.yaml` lines 166–171](https://github.com/temporal-sa/tmprl-demo-cloud-registry/blob/19751dcea5d976fd3df0a1976966bebd648a2f6d/projects/operator/registry.yaml#L166-L171) and [`workflows/operator_canary.py` lines 54–67](https://github.com/temporal-sa/tmprl-demo-cloud-registry/blob/19751dcea5d976fd3df0a1976966bebd648a2f6d/registry/worker/src/demo_registry_operator/workflows/operator_canary.py#L54-L67)

```yaml
versioning:
  enabled: true
  rollout:
    strategy: AllAtOnce
    gate:
      workflowType: RegistryOperatorCanaryWorkflow
```

```python
canary_results = await asyncio.gather(
    *(
        workflow.execute_child_workflow(
            auth_canary_workflow.RegistryOperatorAuthCanaryWorkflow.run,
            branch_request,
            ...
        )
        for branch_request in branch_requests
    ),
    return_exceptions=True,
)
```

**What you need to understand:**

- The Temporal Worker Controller runs the gate workflow on the new version before sending it any traffic. If the gate fails, the new version never takes over.
- Each canary branch creates a real test project, waits for it to go live, deletes it, and fails unless it can prove cleanup.
- Workflows default to Pinned, so a running workflow keeps the version it started on and the operator code changes without patching. Long-lived entity workflows move to the newest version when they continue-as-new, which they do with `AUTO_UPGRADE`.

**Docs:** [Worker Versioning](https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning)

## Reset and tear down

- Between runs, click **Tear it down** and wait for **Your last demo environment ended…**. Then click **Provision a demo environment** again.
- If you leave without tearing down, the environment ends on its own after 2 hours.
- Close the control panel tab when you're done. While a provision or teardown is running, the panel reads the workflow's status every few seconds, and each read is a billable Query. It stops reading when the tab is hidden or nothing is in progress.

## When something goes wrong

| Symptom | Likely cause | Fix |
|---|---|---|
| **The sample demo isn't configured yet** | The sample's commit isn't pinned in `registry/web/demo/sample-project.json` | Pin `source.branch` to a commit of the sample repo and roll out the web image |
| **Sign in to use the control panel** | You're not signed in, or your session expired | Click **Sign in** |
| **Watch the provisioning workflow** opens a page you can't read | Namespace access hasn't applied yet | Click **Request Namespace Access**, wait a few minutes, refresh |
| **Provisioning failed** after **Build images** | The sample repo's build broke at the pinned commit | Check the build in CodeBuild (off the shared screen), fix the sample repo, pin a new commit |
| **This test deploy is ending. Try again once it has ended.** | You clicked provision while a teardown was still running | Wait for teardown to finish |
| Progress sits on **Temporal Cloud and image repositories** for a long time | Temporal Cloud namespace creation is slow | Switch to your warm environment and come back to the live one |

## Run it locally

The provisioning path needs the hosted platform: the operator, the EKS cluster, AWS and the Temporal Cloud bootstrap credentials. You can't run it on a laptop. You can run the control panel's web server locally to work on the UI.

1. Install the web server's dependencies: `pip install -r registry/web/requirements.txt`.
2. Start a local Temporal dev server: `temporal server start-dev --disable-config-file` (no profile, so it can't point at a real Cloud namespace).
3. Run the server. `AUTH_MODE=bootstrap-allow` skips Google sign-in and acts as `bootstrap@temporal.io`, so use it only on your laptop:

   ```sh
   AUTH_MODE=bootstrap-allow \
   TEMPORAL_ADDRESS=localhost:7233 TEMPORAL_NAMESPACE=default \
   python registry/web/server.py
   ```

4. Open `http://localhost:8080/control`. Without an operator worker, a provision request waits on the preview workflow and times out. Use this to work on layout and copy, and the web tests (`pytest registry/web/tests`) for behavior.

## Keeping links current

Before an event, list the linked files that changed since their permalink:

```sh
grep -oE 'blob/[0-9a-f]{40}/[^#)]+' RUNBOOK.md | sort -u | while IFS=/ read -r _ sha file; do
  git diff --quiet "$sha" HEAD -- "$file" || echo "changed since ${sha:0:7}: $file"
done
```

A file on the list may only have moved lines. Check the snippet against the current code, then refresh the permalink.
