# UI-driven catalog onboarding

Any signed-in catalog user can bring a demo repo into the catalog from the web
UI. Temporal workflows on the blue operator drive every step. The process ends
when the user opens a pull request against this repo. A repo owner reviews and
merges that PR, and from then on the existing GitOps path takes over.

Instruqt workshops come later. The workflow is shaped so they can plug in.

## Principles

- **Deterministic where possible.** Repo checks, rendering the DemoProject, and
  validation are plain code. The validator in `scripts/validate_projects.py` is
  the gate, both in the workflow and in CI.
- **An agent where judgment helps.** A Claude Agent SDK agent drafts the
  DemoProject, explains each field, answers questions about one section, and
  suggests code changes to the demo repo. Its output is advisory and always goes
  through the validator.
- **The PR is the hand-off.** Once the user opens the registry PR, onboarding is
  done from the catalog's point of view. The workflow does not wait for the
  merge.
- **Blue only.** Onboarding runs on `registry-management-blue`.
- **No patching.** The operator runs PINNED Worker Versioning. Long waits use
  the existing `check_worker_version` → continue-as-new pattern.

## Flow

`CatalogOnboardingWorkflow` runs one per user
(`catalog-onboarding/<color>/<email>`), so each user has one open onboarding.

1. **Repo.** The user enters a GitHub repo in `temporal-sa`, `temporalio` or
   `temporal-community`. The repo must be public. An activity resolves the
   default branch and pins the commit SHA.
2. **Draft.** The workflow holds the draft DemoProject. The UI edits it through
   Updates, either as form fields or as raw YAML. Every edit runs the validator
   against the draft plus the projects already in the cluster. Errors come back
   with a JSON pointer, so the UI can mark the field.
3. **Agent (milestone 2).** The agent drafts the document, and helps with one
   section at a time.
4. **Secrets (milestone 3).** Users enter project secret values in the UI. The
   web server writes them straight to Secrets Manager, so values never enter
   Temporal.
5. **Test deploy (milestone 4).** An optional 48-hour preview deployment.
6. **Hand-off.** When the draft is valid, the UI opens GitHub's new-file page
   with the YAML prefilled
   (`/new/main?filename=projects/demo/<name>.yaml&value=…`). The user commits it
   to a branch and opens the PR as themselves. They paste the PR link back. An
   activity checks that the PR adds that file with exactly the validated
   content. The workflow then completes.

The workflow gives up after 14 days with no edits.

## Agent (milestone 2)

The page offers "Draft with the agent" (the whole DemoProject) and "Ask the
agent about this" on each form section. The `request_agent` update queues a run
and returns at once. The workflow's main loop runs `run_onboarding_agent`, and
the page polls status. The run's latest step comes from the activity's
heartbeat details, which the web server reads with `describe`. The result is a
**proposal**: the operator validates it the same way as any edit, and the user
applies it or ignores it. The agent never edits the draft itself.
`cancel_agent` stops a run. Cancellation reaches the activity on its next
heartbeat, so Stop takes up to about 24 seconds.

**Job and pod** (`activities/onboarding_agent.py`, `registry/onboarding-agent/`)

- One Kubernetes Job per run in `tmprl-dem-cld-onboarding-agents`, with a
  quota of 6 pods, a 25-minute deadline, and no retries. The Job name derives
  from the run ID, so a retried activity reattaches to a Job that is still
  running.
- The pod has no service-account token and no AWS or Temporal credentials. It
  runs as non-root with a read-only root filesystem and all capabilities
  dropped. An init container applies the isolated-executor firewall, which
  allows only DNS and public HTTP(S).
- The image is the operator project's `onboarding-agent` build. The operator
  gets its URI as `DEMO_REGISTRY_ONBOARDING_AGENT_IMAGE`, the same way it gets
  the Temporal Cloud proxy image. The Python Agent SDK wheel bundles the Claude
  Code CLI, so the image needs no Node.

**Model access**

- Each run mints a LiteLLM key through the Instruqt broker: $2, one hour,
  models `claude-*` and `anthropic/*`, no default model, so the SDK picks.
  Mints carry `track_id=registry-onboarding`.
- The operator signs with the existing `v1` credential. The
  `onboarding-litellm-broker` ExternalSecret copies only the
  `BROKER_HMAC_SECRETS_JSON` property; the broker and its secret are unchanged.
- The key is minted inside the activity and goes only into a Secret owned by
  the Job, so it never enters workflow history and is deleted with the Job. It
  is revoked when the run ends.
- The pod reaches the gateway at `https://litellm-instruqt.tmprl-demo.cloud`
  with `ANTHROPIC_BASE_URL` and `ANTHROPIC_AUTH_TOKEN`. The gateway gets a
  `claude-*` → `anthropic/claude-*` route, because the SDK sends bare model
  names.

**Grounding**

- The request carries the catalog contract as of the operator's own commit:
  both schemas, the validator, and `projects/README.md` from the operator image,
  plus five deployed example projects read live from the cluster.
- Buildkit guides 07 and 12 are copied into the agent image
  (`registry/onboarding-agent/context/buildkit/`), since the buildkit repo is
  private. The old `skills/tmprl-demo-cloud-onboarding` skill is not used.

**Agent run** (`registry/onboarding-agent/agent.py`)

- The pod clones the repo at the pinned commit.
- The agent gets Read, Grep, Glob, Bash, Write and Edit, with bypassed
  permissions, because the pod is the sandbox.
- It loads no settings, hooks or CLAUDE.md from the repo.
- It stops at 80 turns or $1.50.
- It writes the draft to a file and checks it with the validator. The wrapper
  gives it one bounded repair pass if validation still fails.
- Its edits to the checkout become a patch, new files included. The activity
  turns its findings into a Claude Code prompt. The agent opens no PRs.
- Output goes to stdout as `@@progress`, `@@result` (chunked base64 JSON) and
  `@@error` lines, which the activity reads from the pod log.

**Verified locally on 2026-10-12:**

- I ran `agent.py` with a real minted key against
  `temporalio/money-transfer-project-template-go` through the live gateway. It
  took 11 turns and cost $0.14.
- It found the repo is a worker plus a CLI starter. It drafted a worker-only
  project that passes the validator, patched the hardcoded task queue, and
  added a Dockerfile.
- It flagged the missing web app and the fixed workflow ID.

The image build, the firewall, and running inside a Job are not verified
locally; they get exercised on first deploy.

**Deploying it**

- Merging rolls the operator: there's a new build, and the operator now watches
  more of the repo.
- Flux applies the new `onboarding-agents` Kustomization, the RBAC change for
  `batch/jobs`, and the broker ExternalSecret.
- The gateway ConfigMap and the `config-revision` annotation roll the LiteLLM
  pods.
- No canary covers the agent. After deploy: open "Add a Demo", check a public
  repo, run "Draft with the agent", and watch progress, the proposal, Apply,
  and the patch download.

## Secrets (milestone 3)

- New web endpoints write project secrets to
  `tmprl-dem-cld/<name>/<secret>`, tagged with the submitter and onboarding ID.
  They refuse to overwrite a secret someone else owns or one that belongs to a
  project already merged.
- The web pod gets Secrets Manager write access, scoped to `tmprl-dem-cld/*`
  with an explicit deny on `tmprl-dem-cld/platform/*`.
- The shared-secret picker lists `tmprl-dem-cld/platform/shared/*` by name.
  Property names come from tags on each shared secret.
- Secrets from PRs that never merge are tagged now, so a later sweep can remove
  them.

## Test deploy (milestone 4)

`OnboardingPreviewWorkflow` (`onboarding-preview/<name>`) applies a preview
DemoProject directly to the cluster, the way the operator canary does.

- **Separate name.** The preview is named `pv-<name>`, so a merge can't collide
  with it. A `secrets-from: <name>` annotation points secret lookup at the real
  project's path.
- **Hidden.** A `registry.tmprl-demo.cloud/preview` label keeps it out of
  `catalog_payload`.
- **Auth.** `ingress.temporalAuthRequired` is forced to `true`. Any signed-in
  user can open the preview, so it can be shared for feedback.
- **Gated.** It only runs when the draft validates. One preview per user, plus
  a global cap.
- **48 hours.** A durable timer, then delete and wait for the finalizer. Ending
  early is an Update.

## Milestones

1. **Core.**
   - Accept all three orgs in the schema and validator.
   - Make the validator check an in-memory draft and return errors with JSON
     pointers.
   - Ship the validator and schemas in the operator image.
   - The onboarding workflow and edit loop.
   - The onboarding page: form, YAML tab, hand-off.
2. **Agent pod.** Done on this branch; see above.
3. **Secrets.** Project secret entry and the shared-secret picker.
4. **Test deploys.**
5. **Instruqt.**
6. **Automatic demo-repo PRs**, after the rest works end to end.

## Out of scope for now

- Editing existing catalog entries. GitHub's prefilled new-file page only works
  for new files.
- An operator canary for onboarding.
- Waiting for the PR to merge.
