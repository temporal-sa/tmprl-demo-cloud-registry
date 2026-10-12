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

The agent runs as a Kubernetes Job, one per request, in its own namespace with
resource limits.

- **Isolation.** The pod uses the isolated-executor profile
  (`isolated_executor.py`): no service-account token, no AWS or Temporal
  credentials, runs as non-root, and private address ranges are firewalled.
- **Model access.** For each run, the operator mints a LiteLLM virtual key
  through the Instruqt broker: $2 budget, 1 hour, Anthropic models only, no
  default model, so the SDK picks its own models. The operator signs requests
  with the existing broker credential (key ID `v1`). It reads only the
  `BROKER_HMAC_SECRETS_JSON` property of
  `tmprl-dem-cld/platform/instruqt-litellm/broker`, never the whole secret,
  which also holds the LiteLLM master key. The broker and its secret are not
  changed. Mints carry `track_id=registry-onboarding`, so spend can be found.
- **Gateway route.** The gateway needs a `claude-*` → `anthropic/claude-*`
  route, because the SDK sends plain model names. This only adds a route.
- **Repo access.** Repos are public, so the pod clones the repo itself.
- **Grounding.** The agent image is built from this repo, so it carries the
  schema, `projects/README.md`, example projects, the validator, and the
  buildkit guides 07 and 12 at the operator's own commit. The old
  `skills/tmprl-demo-cloud-onboarding` skill is stale and is not used.
- **Tools.** Read, Grep, Glob and Bash, with turn and cost limits. The agent can
  run the validator on its own draft.
- **Output.** The pod prints a result JSON: the draft, a reason for each field
  with `file:line` citations, findings, and a `git diff` of suggested demo-repo
  changes. The activity reads it from the pod's logs. Suggested code changes go
  to the user as a patch plus a ready-made Claude Code prompt. The agent opens
  no PRs for now.
- **Temporal.** One activity creates the Job. Its name derives from the
  workflow ID and request number, so a retry reattaches. The activity mints the
  key inside itself, so the key never enters history. It reports progress as
  heartbeat details, and deletes the Job if cancelled.

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
2. **Agent pod.** Context builder, image, Job activity, broker key minting,
   draft and section-help modes, patch-plus-prompt output.
3. **Secrets.** Project secret entry and the shared-secret picker.
4. **Test deploys.**
5. **Instruqt.**
6. **Automatic demo-repo PRs**, after the rest works end to end.

## Out of scope for now

- Editing existing catalog entries. GitHub's prefilled new-file page only works
  for new files.
- An operator canary for onboarding.
- Waiting for the PR to merge.
