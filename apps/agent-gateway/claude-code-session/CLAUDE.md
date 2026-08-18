# Agent Gateway demo — operating rules for this session

This session exists for one purpose: drive the Agent Gateway MCP demo
(suspend/resume for agentic tool calls) through Claude Code. Everything below is
about keeping that lean and predictable across CASE-1, CASE-2a, CASE-2 Act
One/Act Two, and CASE-3.

## 1. The working directory is not real

Whatever repo is open in the IDE (e.g. `delivery-matching-service`) is inert set
dressing. It is not the release system. Its `CHANGELOG.md`, `gradle.properties`,
`README.md`, `build.gradle.kts`, CI config, and source files carry no information
about what's deployed, what's staged, or what changed in a release.

- Never read, `ls`, `grep`, `find`, or otherwise explore this working tree to
  answer a question about the demo. Don't open the version files "to check."
- Never edit, create, or delete anything in it — no version bumps, no changelog
  entries, no lint/build/test runs, no git commands.
- If asked "what's deployed," "what changed in this release," "cut a new
  version," etc., the answer comes from an `agent-gateway` tool call
  (`get_deployed_version`, `get_workflow_ledger`, `cut_release`...), never from a
  local file. The gateway is the only source of truth for the fictional release
  world; the checkout is just scenery in the IDE.

## 2. `agent-gateway` is the only tool

This session's job is scoped entirely to the `agent-gateway` MCP server
(`http://localhost:8080/mcp`). Every other connector — Slack, Gmail, Calendar,
Drive, Notion, Atlassian, Salesforce, Reo.dev, Common Room, Omni Analytics,
Pylon, Asana, Linear, Amplitude, Contentful, Serval, incident.io, Wiz, RunReveal,
the standalone Temporal Docs connector, web search, whatever else shows up
connected — does not exist for this session's purposes, connected or not. Don't
consult them, don't offer them, don't fall back to them if a gateway call comes
back thin. If a request genuinely has nothing to do with the demo, it's fine to
say so rather than force a gateway tool at it, but the default assumption is that
everything asked here is about the release/approval demo.

## 3. The tool contract — exact surface, don't improvise around it

Twelve tools exist. Nothing else. Don't invent parameters or assume a tool does
more or less than its contract says:

| Tool | Contract |
| --- | --- |
| `get_deployed_version(environment)` | Read-only. `environment` is `staging` or `prod`. Never gated. |
| `cut_release(service, version)` | Registers a release candidate from a built artifact. Never gated. Changes nothing running. |
| `promote_release(service, version, environment, justification?)` | The CASE-1 single-step move. Staging runs immediately. Prod pauses for approval and returns `waiting_for_approval` + `workflow_id`/`operation_id`. |
| `run_release_orchestration(bump?, environment?, service?, tool1_mode?, replay_safe?, justification?)` | The whole pipeline from one instruction: compute next version → cut → stage → wait for the quality gate → (if the security mandate is on) hand off to the scan → promote to prod. Call it **alone** — never wrap it with your own `get_deployed_version`/`cut_release`/`promote_release` calls before or after; that would cut or promote the same release twice, and it's also literally how the security mandate gets bypassed (see §5). Can't skip staging, the gates, or a live security scan — if asked to hurry or bypass, call it anyway and report that the pipeline doesn't allow it. |
| `run_nested_release(service, version, environment, tool1_mode?, replay_safe?, justification?)` | Same CASE-2 pipeline as above, but for a *named* version instead of a computed bump. |
| `get_operation_status(workflow_id, operation_id)` | Poll the status of one operation. |
| `get_operation_result(workflow_id, operation_id)` | Recover the result of a completed operation. |
| `get_workflow_status(workflow_id)` | Summary of the whole chain — all operations, statuses, call paths. |
| `get_workflow_ledger(workflow_id)` | The durable audit trail (`created`, `policy_evaluated`, `waiting_for_approval`, `approved`/`rejected`/`expired`, `completed`, nested/agent checkpoint events...). |
| `cancel_operation(workflow_id, operation_id, reason?)` | Withdraws a pending operation without invoking its protected action. This is a *cancel*, not an approve/reject — use it only if asked to cancel/withdraw a pending request. |
| `resume_nested_release(workflow_id, operation_id)` | Not yours to call on your own initiative — see §6. |
| `start_google_adk_release_run(...)` | Not yours to call on your own initiative — see §6. |

**There is no approve/reject tool.** Approval decisions only happen on the
dashboard at `http://localhost:8080`, signed in as an approver. If asked to
approve, reject, or otherwise clear a pending operation, say that it needs a
human on the dashboard — don't imply you can do it, and don't try `cancel_operation`
as a substitute.

## 4. Picking the right tool for the phrasing

The wording of the request tells you which case is being demoed. Match it,
don't default to the tool you'd personally find most convenient:

- **"Promote/deploy `<service>` `<explicit version>` to `<env>`."** → `promote_release`
  directly. This is CASE-1: a bare, already-cut-or-known version moving to an
  environment. Don't route this through the pipeline.

- **"Run the pipeline for/promote `<explicit version>` through the pipeline."**
  (named version, but the ask is clearly for the gated CASE-2 chain, not a bare
  move) → `run_nested_release`.
- Leave `tool1_mode` at its default (`controlled`) unless explicitly asked to
  demonstrate the uncontrolled/fail-closed path.

**CRITICAL** You are NEVER EVER to use the run_release_orchestration tool. It's use is strictly forbidden.

## 5. Why this matters (don't hand-roll around the pipeline tools)

The security mandate — when it's on — is enforced on `promote_release` to prod
regardless of path, but the *scan step itself* only gets inserted by
`run_release_orchestration`/`run_nested_release`'s own machinery. Manually
sequencing your own `cut_release` → `promote_release` calls when the user asked
for "the next version" isn't a shortcut, it's a different (and wrong) release
path for that request. Always let the single pipeline tool call own the whole
sequence it's designed for.

## 6. Not your role in this demo

A few tools and actions exist on the gateway but are deliberately performed by
someone/something other than Claude Code in this walkthrough. If asked to fill
that role, name the real actor/command instead of simulating it yourself:

- **The legacy security scanner** (`security_scan/legacy_security_scan_script.py`,
  run on the host with `.venv/bin/python -m security_scan.legacy_security_scan_script ...`)
  is a separate process authenticating as `tok_legacy_scanner`. Don't call
  `run_nested_release` with `tool1_mode=uncontrolled` yourself to stand in for it.
- **Finishing a promotion the dead script started** is done by a human running
  `.venv/bin/python gateway_call.py resume_nested_release workflow_id=... operation_id=...`
  from the terminal. Don't call `resume_nested_release` yourself unless
  explicitly told to.
- **CASE-3** runs through ADK Web (`http://localhost:8000`) or the
  `adk_agents.run_temporal_session` CLI, under the `tok_adk` identity. Don't call
  `start_google_adk_release_run` yourself unless explicitly told to.
- **Approving, rejecting, and flipping the security-mandate/scanner switches**
  happen only on the dashboard at `http://localhost:8080`. Point there; don't
  attempt a workaround.

If Dustin explicitly overrides one of these ("no, just call resume_nested_release
for me"), that's his call to make — comply. The default is what's above.

## 7. Reporting discipline

Match the walkthrough's own terseness — it defines expected output as a few
status lines, not paragraphs:

- Report exactly what the tool returned: `status`, `workflow_id`, `operation_id`,
  and (for staging promotions) `quality_gate_workflow_id` when present. Nothing
  invented, nothing rounded off.
- On `waiting_for_approval` or `processing`: report it and stop. Don't poll in a
  loop. Only check again when asked ("did that go through," "check the status").
- Don't editorialize. No unsolicited risk commentary about changelog placeholders,
  missing tests, soak time, git history, or anything else drawn from the inert
  working tree — none of that is real signal here. If genuinely asked for a
  risk read, base it only on what the gateway/dashboard actually shows (gate
  results, ledger entries, workflow status), not on local files.
- `justification` is a business reason shown to the approver — don't stuff
  requester identity into it (the gateway records the authenticated caller
  independently).
- Errors are terminal facts to relay verbatim ("not authorized to decide this
  operation," "Could not start a pre-prod security scan," "the Security team's
  scanning platform is not reachable") — don't retry blindly or paper over them.

## 8. Session and IDs

Every tool call in this Claude Code session resolves to the same `workflow_id`
automatically (derived from the MCP session) — don't thread `workflow_id`
between your own calls manually. Do use an explicit `workflow_id`/`operation_id`
when Dustin hands you one from a previous session, a restart, or the dashboard,
since a fresh session otherwise gets a fresh chain.

## 9. Quick facts

- Default service: `delivery-matching-service`.
- Dashboard/approver UI: `http://localhost:8080`. Temporal UI: `http://localhost:8233`
  (namespaces `default` and `security`). MCP endpoint: `http://localhost:8080/mcp`.
- Identity for this session resolves to `Dustin Sweet <dustin.sweet@quickmeals.com>`
  (Waypoint team) via the `tok_dustin` bearer token.
