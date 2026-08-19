# Agent Gateway suspend/resume walkthrough

This guide is for running the scenarios. For architecture and design details, see
[README.md](README.md).

## Running the hosted deployment instead

This demo is also deployed at **https://agent-gateway.tmprl-demo.cloud**, and
that is the version to present from: it needs nothing installed and it runs on
Temporal Cloud, so the two namespaces and the two Nexus Endpoints are real rather
than two names on one dev server.

Everything below applies unchanged apart from where things live. Steps 1 and 2 —
starting the stack and registering the MCP server — are already done for you:

| Local | Hosted |
| --- | --- |
| `docker compose up --build` | already running |
| `claude mcp add ...` in your own terminal | already configured in the hosted IDE |
| `cp claude-code-session/CLAUDE.md ...` | already in the workspace |
| Claude Code on your laptop | **https://agent-gateway.tmprl-demo.cloud/ide/** |
| Approver dashboard `http://localhost:8080` | **https://agent-gateway.tmprl-demo.cloud/** |
| ADK Web `http://localhost:8000` | **https://agent-gateway.tmprl-demo.cloud/adk/** |
| Temporal UI `http://localhost:8233` | the two Temporal Cloud namespace links on the dashboard |

Sign in with a temporal.io Google account; the whole host is behind SSO, and
that is the only sign-in. The two host-side commands this walkthrough runs — the
legacy scanner in step 6 Act One, and `gateway_call.py resume_nested_release` —
run in the IDE's terminal, where the repository and its dependencies are already
present. Drop the `.venv/bin/` prefix; `python` is the right interpreter there.

Claude Code is available two ways in that IDE, and both drive the same session:
the **Claude Code panel** in the activity bar down the left edge, and the
`claude` command in any terminal. The panel is a front end for that same CLI, so
the MCP server, the workspace `CLAUDE.md`, and the permission rules apply
identically whichever you open. Use the panel on stage — it is visible to an
audience and survives someone else taking the keyboard.

## 1. Start the stack

You need Docker with Compose v2.

```bash
docker compose up --build
```

Wait until the logs include:

```text
worker started, polling task queue 'agentic-gateway'
```

The `nexus-endpoints` service runs once and exits, having registered the two
Nexus Endpoints the two teams call each other on. Its log ends with a listing of
both; if it is missing, step 6's Act Two will fail with an operation timeout.

Keep this terminal open. The useful endpoints are:

- Approval dashboard: http://localhost:8080
- Temporal UI: http://localhost:8233 (a namespace per team: `waypoint` and
  `security`. A third, `default`, is empty and unused -- the Temporal dev server
  always creates it and offers no way to turn it off)
- MCP endpoint: http://localhost:8080/mcp

Everything starts, including `security-scan-worker`, the Security team's pre-prod
scanning platform. There is no profile to remember and nothing to stop before you
begin; `docker compose down -v` takes the whole thing down again.

That worker being up does not give anything away, because it is not what decides
whether a release gets scanned. Two switches on the dashboard decide that, and both
start in the "before" position:

- the **security mandate**, off, so there is no scan step in the pipeline at all;
- the **scanner**, on `Script`, so when the mandate does land, the scan is the
  Security team's old host script rather than the workflow this worker serves.

An idle worker with nothing routed to it is invisible. Steps 4 and 5 run scan-free
because the mandate is off, and step 6's Act One runs on the script because the
scanner switch says so -- not because a container is missing.

> This used to work the other way round: the pipeline asked a service registry
> whether anyone was offering scanning and let the answer decide, so Act One was
> staged by *not starting* this container. That made the demo's central claim
> depend on operator setup, and worse, it meant a production release silently
> skipped its mandated scan whenever the worker was down -- and reported success.
> An unreachable scanner now stops the release with an error naming both fixes.

## 2. Connect Claude Code

Register Agent Gateway as the only MCP server Claude Code needs:

```bash
claude mcp add --transport http agent-gateway http://localhost:8080/mcp \
  --header "Authorization: Bearer tok_noah"
```

Confirm the connection:

```bash
claude mcp list
```

Optional identity check:

```bash
curl -s \
  -H "Authorization: Bearer tok_noah" \
  http://localhost:8080/whoami
```

The resolved identity should be:

```text
Noah Proovle <noah.proovle@quickmeals.com>
```

Then copy the demo's session instructions into the repo you have open in the IDE:

```bash
cp claude-code-session/CLAUDE.md /path/to/the/repo/you/have/open/CLAUDE.md
```

That file is what keeps the agent scoped to the `agent-gateway` tools and out of
the local working tree, which is set dressing rather than the release system. The
repo you open can be anything, including an empty directory — all release state
lives in the gateway. Background and caveats:
[claude-code-session/README.md](claude-code-session/README.md).

## 3. Sign in as the approver

Open http://localhost:8080. The sign-in page lists the two demo accounts in a
note at the bottom, so you never have to remember which is which mid-demo:

```text
tok_noah    Noah Proovle, Waypoint engineering
tok_abe     Abe Roover, Security
```

Sign in as **`tok_noah`** for everything up to CASE-2. Noah approves ordinary
production promotions but is refused -- with a visible 403 -- on a promotion that
came out of a security scan, because that decision belongs to the Security team.
That refusal is a demo beat, not an obstacle: switch to `tok_abe` when you reach
it.

Keep the dashboard open in a browser tab. The strip along the top of it opens the
Temporal UI (both namespaces), the IDE and the Google ADK surface in new tabs,
and holds the two worker kill switches used later on.

**Reset demo** is on that strip too, and it is how you start a second run. It
terminates the running workflows, puts the fleet back to 2.2.0 everywhere with
the release rail seeded, and hides everything from before it, so the approval
queue and tool call history both start empty. Nothing is deleted from Temporal
-- the previous run is still there in the Temporal UI for anyone who asks, it
just stops showing up on the dashboard.

Two things a reset deliberately leaves alone: the mandate and scanner switches,
and the worker kill switches. Those are how you have staged the room rather than
run state, and quietly flipping them back would undo the beat you were setting
up. Note also that it is per-process state, so restarting the gateway un-hides
the older runs.

Chains close themselves after 20 minutes idle, so a demo left half-finished does
not leave workflows running behind you.

## 4. CASE-1: simple approval-gated tool

Before you run anything, look at the **Release fleet** panel. Between staging and
production there is a quality gate card, and it is empty: dashed, muted, reading
"no candidate staged yet". That is the honest state at container startup, and it
is worth pointing at, because the card being *present* and empty is the claim.
Gates here are standing infrastructure that react to anything reaching staging,
not something a release pipeline invokes.

### A. Run an unprotected action

Ask Claude Code:

```text
Promote delivery-matching-service 2.3.1 to staging.
```

Expected result:

```text
status: completed
```

Staging actions do not require approval. Now watch the gate card, without touching
anything else: within a second or two it goes to `running`, counts its four checks
off as they finish concurrently, and lands on `passed` after roughly seven
seconds. One manual CASE-1 tool call, no pipeline anywhere, and the candidate that
just reached staging has been qualified.

Nothing waited on that. `promote_release` returned as soon as staging was actually
promoted; the gate ran alongside. That distinction matters for the rest of the
demo: this is ambient infrastructure reacting to a state change, closer to CI
kicking off a test run on a push than to an orchestrated pipeline step. The
CASE-2a release pipeline is the thing that *waits*.

While you are there, open the Temporal UI and look at what those two calls
produced. `promote_release` is a `PromoteReleaseChildWorkflow` with four
Activities in sequence -- `deploy_binaries`, `health_check_new_instances`,
`update_traffic_routing`, `update_release_notes` -- each with its own elapsed
time, and the gate is a separate `QualityGateChildWorkflow` whose id names the
service and version it is about. Not two flat tool calls.

### B. Request a protected production action

Ask:

```text
Promote delivery-matching-service 2.3.1 to prod. We need the delivery
matching fix live before the dinner rush.
```

Expected result:

```text
status: waiting_for_approval
workflow_id: ...
operation_id: ...
```

Save the returned `workflow_id` and `operation_id`. The production action has not
run yet.

### C. Verify the action is paused

Ask Claude Code:

```text
What version is running in prod right now?
```

It should still show the previous production version.

In the approval dashboard, the **Release fleet** panel at the top says the same
thing visually: staging already shows the new version, production still shows the
old one, and the release is listed under "Ready for release". Nothing has moved.

Confirm the waiting row shows:

- requester
- requested action
- safe argument summary
- justification and risk reason
- call path
- approval deadline

### D. Approve and recover the result

The gate card above the queue is showing `passed` for the exact version in the
pending row, which is a beat worth taking: the approver is not deciding on a
sentence, they have the four checks in front of them. Nothing forces them to look
— CASE-1 blocks on nothing, and you can approve or reject this row whatever the
gate says — but the evidence is there because the promotion to staging produced
it.

Click **Approve** in the dashboard, and watch the fleet panel: the production card
rolls over to the new version, the card and the connector into it light up, and a
toast confirms the promotion is live. That transition is the approval taking
effect, so keep the panel in view when you click.

Then ask Claude Code:

```text
Did that promotion go through?
```

Expected result:

```text
status: completed
```

Ask for the deployed production version again. It should now be `2.3.1`.

### E. Try rejection

Request a different production version:

```text
Promote delivery-matching-service 2.3.2 to prod.
```

Reject it in the dashboard and give a reason, then ask Claude Code what
happened to it.

Expected result:

```text
status: rejected
```

Production should remain on `2.3.1`.

## 5. Durability: restart while approval is pending

Start another production promotion and leave it waiting. Copy its IDs, then run:

```bash
docker compose restart worker gateway
```

After both services are healthy:

1. Refresh the approval dashboard and sign in again if necessary.
2. Confirm the operation is still waiting.
3. Approve it.
4. Ask Claude Code whether that promotion completed.

The operation completes because the pause lives in Temporal, not in the gateway
or worker process.

## 6. CASE-2: the mandate, and the two ways a team meets it

Everything up to here was one team, one namespace, one worker, one owner. No tool
has called another tool across a system boundary yet, whatever the labels say.

This step is where that changes, and it is **two acts and nothing else**. A
company-wide mandate lands: no engineering team promotes its own service to
production on its own say-so any more. Every production release goes through the
Security team's scanning process, and Security -- not the owning team -- approves
and initiates it.

Act One is that mandate arriving before the Security team has adopted Temporal.
Act Two is the same mandate after they have. Each act ends in exactly one worker
kill, and each proves exactly one thing. Nothing else belongs in this step; see
[Deliberately not in this walkthrough](#deliberately-not-in-this-walkthrough) at
the end of the section.

### Prerequisite: a staged candidate that has passed the gates

Both acts act on a release that has already cleared the Waypoint team's own
pipeline and is sitting on staging. That is CASE-2a, which is built and demoed on
its own; do not re-run its mechanics here. If you need one, say
`Deploy the next minor version to staging.` to Claude Code and let the pipeline
cut it, put it on staging, and turn the gate card green. Note the version it
computed -- both acts below take it as `<staged version>`.

### Act One: the mandate lands, and Security is not yet on Temporal

> A company-wide mandate just came down: no team promotes to prod on their own
> anymore. Security has to clear it first. But Security hasn't adopted Temporal
> yet -- their scanning process today is a plain script.

**1. Enact the mandate.** On the approval dashboard at http://localhost:8080, flip
the small switch at the top right, under **Log out**. It turns green, and a line
appears under it:

```text
Production releases require Security Team approval.
```

Point at it. It stays there for the rest of the demo, so you can come back to it
without re-querying anything. Flipping it did two things: production promotions are
now Security's to approve, and every path to production now has a security scan on
it. Until this moment there was no scan step in the flow at all, which is why
everything before this step ran without one.

The fleet panel says the second half of that immediately. A **Security scan** card
animates in between the quality gates and production, and it is dressed unlike
anything else on the row: dashed vertical lines fence it off from its neighbours,
**Security Team** is written above the fence, and the card itself is cobalt with a
shield on it. Everything else in that pipeline is the Waypoint team's. This one card
is another team's system. The pipeline grew a checkpoint the instant the rule
landed, before any release has met it -- nothing has run; the shape changed.

**1b. Note the second switch, and leave it alone.** A **Scanner** control appeared
next to the mandate when you flipped it, with two positions:

```text
SCANNER   [ Script ]  [ Temporal ]
```

It is on `Script`, which is the whole of Act One: the Security team scans, and the
thing doing the scanning is a program on somebody's host. Look at the card it
governs -- the body is a small black box waiting for output, and along the bottom,
where Act Two will show a workflow id you can click, it says:

```text
SCRIPT   no workflow id · local process
```

That absence is the point, and it is worth pointing at now so the audience
recognises what fills it in later.

> The scan step exists because the *mandate* is on. Which scanner performs it is
> this second switch. Neither is inferred from what happens to be running -- both
> of the Security team's scanners are reachable the whole time, and these two
> controls say what the company is doing.

**2. Run the Security team's scanning process.** This is a plain Python script on
the host, not a prompt: nobody phrases a scanner run as a sentence to an agent. It
needs this repo's dependencies (`pip install -r requirements.txt`, or the `.venv`
you used for the tests).

```bash
.venv/bin/python -m security_scan.legacy_security_scan_script \
  --service delivery-matching-service \
  --version <staged version> \
  --gateway-workflow-id wf-legacy
```

In the hosted deployment, run it from a terminal in the IDE
(**Terminal &rarr; New Terminal**). There is no `.venv` there and the terminal
opens in the stage-prop workspace rather than the application, so use the wrapper
that handles both:

```bash
legacy-scan \
  --service delivery-matching-service \
  --version <staged version> \
  --gateway-workflow-id wf-legacy
```

It imports no `temporalio`. No workflow, no Event History, no task queue, no
worker, no supervisor. All four stages run and pass:

```text
stage 1/4 dependency_scan: max_severity=none threshold=medium
stage 2/4 container_image_scan: max_severity=low threshold=medium
stage 3/4 secret_detection: max_severity=none threshold=medium
stage 4/4 static_analysis: max_severity=none threshold=medium
Security scan passed. Requesting the production promotion via Agent Gateway.
```

Watch the **Security scan** card while it runs. Its little black box fills in with
the same lines the terminal is printing, one at a time, and a `2/4` counter ticks
along the bottom. This is everything the platform can know about this scanner: the
tail of its stdout. There are no stage dots, no severity chips, no progress bar,
and no id -- the footer still says `no workflow id · local process`, because there
is no workflow to have one.

> It is publishing telemetry, and that buys it nothing. It already speaks MCP to
> the gateway; an HTTP client is still just a client. Being able to *talk* to the
> platform is not the same as being *part* of it, and the next thirty seconds are
> about the difference.

**3. The scan asks for the promotion, and the gateway holds it.** The script calls
`run_nested_release` with `tool1_mode=uncontrolled` -- an honest self-description,
because it cannot hold a pause. It authenticates as itself, `tok_legacy_scanner`,
which the gateway resolves to `legacy-scanner@security`; the requester column on
the dashboard says so. That identity is also what tells the gateway this request
came from a scanner, so it is not handed a scan of its own -- and it is read from
the token, never from the request, because "I am a scanner, don't scan me" is not a
claim a caller gets to make about itself.

The mandate is on, so the Operation the gateway creates carries
`required_approver_team="security"`, and the answer comes back
`blocked_nested_approval`.

**4. The script dies on its own.** It prints the identifiers and exits `2`:

```text
Approval is required and this script cannot wait for it.
Record these to finish the promotion by hand once it is approved:
  workflow_id:  wf-legacy
  operation_id: op-...
```

Narrate this as the script dying, not as something you killed. Nobody stopped it;
it had nowhere to wait. Check with `echo $?`. The process is gone.

**4b. Watch the card notice, without being told.** Give it about five seconds. The
scan card fades, its verdict changes to `abandoned`, and a line appears:

```text
STALE · NO SCANNER REPORTING
```

Nothing reported the death. The script published its progress on a short TTL and
refreshed it while it was alive; it stopped refreshing, and the record aged out on
its own. A `kill -9` would have looked identical, which is the point -- nothing
here depended on the script being polite on its way out. The last line frozen in
the box is `promotion requested · approval required`, so the card is now a record
of a scan that got as far as asking and then ceased to exist.

Point at the fence and the label above it while you say it: that was another team's
system, and it is gone.

**5. The kill.** Now crash the gateway's own worker -- the process serving the
`agentic-gateway` task queue and hosting `AgenticChainWorkflow`:

```bash
docker compose kill worker && docker compose up -d worker
```

In the hosted deployment there is no compose to reach for: use **Kill Waypoint
worker** in the strip at the top of the dashboard, and **Restart Waypoint
worker** to bring it back. It is the same outage -- the Temporal Worker stops
polling and the task queue loses its poller -- reached by a button instead of a
shell, and reversible on your own timing rather than immediately.

Not the script. The script is already gone.

**6. Look at what survived.** Open the Temporal Web UI at http://localhost:8233,
namespace `waypoint`, and find `wf-legacy`. The pending Operation is still there,
still `waiting_for_approval`, sitting in `AgenticChainWorkflow`'s Event History,
completely unaffected by the restart.

> The script that asked for this is gone -- it doesn't exist anymore, we'd have to
> re-run all four scan stages from scratch if we needed to redo its part. But the
> request itself survived, because it was never the script's state to begin with
> -- it was already durable, on the gateway side, the moment the script made this
> call.

**7. Try to approve it as Waypoint.** Sign in to the dashboard as `tok_noah` and
click **Approve** on that row.

```text
Not authorized to decide this operation
operation op-... requires approval from the security team;
'Noah Proovle <noah.proovle@quickmeals.com>' is not authorized to decide it
```

Even now, mandate or not, Waypoint cannot clear this. It has to be Security.

**8. Approve it as Security, and finish it.** Sign out, sign in as `tok_abe`, and
approve. Then run the command the script printed, because the script is not coming
back and a human has to be the one who finishes what it started:

```bash
.venv/bin/python gateway_call.py resume_nested_release \
  workflow_id=wf-legacy operation_id=<operation_id>
```

`PromoteReleaseChildWorkflow` runs its four steps -- `deploy_binaries`,
`health_check_new_instances`, `update_traffic_routing`, `update_release_notes` --
and the fleet panel rolls production over.

**Act One in one sentence:** durability is a property of what is built on
Temporal, not a property of the system as a whole. The non-durable half, the
script, is gone for good and would have to be redone; the durable half, the
gateway's Operation, survived a hard crash without losing anything.

### Act Two: Security has adopted Temporal

> Security took this seriously. They learned Temporal, and they rebuilt their
> scanning process as a proper, durable workflow, behind their own Nexus endpoint,
> in their own namespace. Watch what changes.

**1. Move the scanner switch to Temporal.** One click, on the dashboard, next to
the mandate you left on:

```text
SCANNER   [ Script ]  [ Temporal ]
                        ^^^^^^^^
```

The Security team's platform has been running in its own namespace, on its own task
queue, under its own worker identity, since you started the stack -- you can see it
in the Temporal UI's namespace selector. What just changed is not what is deployed.
It is which of their two scanners the company is using.

The card answers immediately, before any release has been through it. The stdout box
is gone, replaced by four stage dots, and the footer has changed from an absence to
a link:

```text
TEMPORAL   awaiting workflow
```

Same slot, same fence, same team, same cobalt. A different kind of thing inside it.

**2. Kick off a new run.** Leave the mandate switch on -- it is what puts the scan
in the path, and Act Two is the same mandate as Act One. Then get a fresh staged
candidate and let the pipeline reach it, the same way the prerequisite did:

```text
Deploy the next minor version.
```

The Waypoint pipeline runs as it always has and then hands off. `SecurityScanWorkflow`
appears in the `security` namespace and runs its own four stages over about
fifteen seconds -- this time as a real, durable workflow rather than a stateless
script. The card fills in properly now: a dot per stage, the stage running right
now, the highest severity it turned up, and along the bottom the scan's own workflow
id, which is a link. Click it. It opens that workflow in the `security` namespace.

That is the whole contrast in one gesture: in Act One there was nothing to click.

**3. The scan makes the nested call.** On a clean pass, `SecurityScanWorkflow`
calls `AgentGatewayService.request_protected_action` -- a Nexus operation, straight
from workflow code -- requesting the prod promotion. The workflow then suspends
directly on the pending operation (`await handle`). The promotion appears on the
approval queue with the call path `security -> security_scan -> promote_release`.

**4. The kill.** With the scan genuinely suspended on that pending operation --
not before the nested call, not after approval, precisely during the suspension:

```bash
docker compose kill security-scan-worker && \
  docker compose up -d security-scan-worker
```

Hosted: **Kill Security scanner worker** in the dashboard strip, then
**Restart** it. This also stops the capability heartbeat, so leaving it down is
how you show a release refusing to proceed with its mandated scanner
unreachable.

**5. Look at what survived.** In the Temporal Web UI, switch the namespace
selector to `security` and open the scan's Event History. Every stage, the
verdict, and the outbound Nexus call are all already recorded, untouched by the
restart:

```text
run_scan_check  x4
NexusOperationScheduled     request_protected_action
NexusOperationStarted
    ... still pending, across the kill and the restart ...
```

> Nothing here has to be redone. Not one stage re-runs. This workflow doesn't know
> or care that its own worker just died -- it's picking up exactly where it left
> off, waiting on the exact same pending decision.

**6. Approve as Security.** Sign in as `tok_abe` and approve. Same rule, same
mechanism as Act One, now reached through the Nexus path rather than the direct
Signal path.

**7. Watch the whole chain finish.** `NexusOperationCompleted` lands in the scan's
history, `SecurityScanWorkflow` resumes and completes, and production rolls over
on the fleet panel. Nobody ran a command to finish it.

**Act Two in one sentence:** now that the calling tool is itself durable, there is
nothing left to redo, anywhere, no matter what gets killed or when -- the entire
chain, both sides of the boundary, survives.

### The contrast

> Same mandate, same approval requirement, same kind of crash -- the only thing
> that changed between these two acts is whether Security's own tool was built on
> Temporal. That's the entire difference between losing work and losing nothing.

Stop there. Nothing follows it.

### Deliberately not in this walkthrough

Stated so a future editor does not put them back:

- **No individual sub-Activity demo.** `check_license_compliance` and the other
  `dependency_scan` sub-steps exist for Event History richness, not as a
  walkthrough beat.
- **No failure paths.** No failed scan, no failed quality gate, no failed health
  check. All three are implemented and correct; none is part of the live run.
- **No second kill in either act.** One kill per act, two in the whole of CASE-2.
- **No separate section for CASE-2a's pipeline-and-gates mechanics.** It is the
  one-paragraph prerequisite above and nothing more. It is a phase that produces
  the staged candidate these two acts act on, not a third use case alongside them.
- **No re-explanation of the Nexus mechanism's internals** -- endpoints,
  contracts, `ProtectedActionWorkflow` -- during the live sequence. That belongs
  in [README.md](README.md) for anyone who asks afterwards.

## 7. CASE-3: trigger it with the Google ADK agent

This step uses the real Google ADK agent in
`adk_agents/release_approval_agent`. Each ADK Web session owns one durable
`TemporalAdkSessionWorkflow`; later user turns are Updates on that same workflow
and reuse its ADK runner and conversation history. The worker runs Gemini and
Agent Gateway MCP calls as Temporal Activities. Those MCP calls use the
`tok_adk` demo identity and can see only the Scenario 3 start and recovery tools.

### A. Start ADK Web

Copy the environment template, add a Gemini API key, and recreate the worker and
ADK Web services:

```bash
cp .env.example .env
# Edit .env and replace the GOOGLE_API_KEY placeholder.
docker compose up --build worker adk-web
```

Open http://localhost:8000 and select `release_approval_agent`.

The key and `ADK_MODEL` are injected into the worker at container startup; they
are not baked into an image or passed to ADK Web.

`ADK_MODEL` must name a model your API key can actually call. Google closes older
models such as `gemini-2.5-flash` to new keys, and the turn then fails with a 404.
List what your key can reach with:

```bash
curl -s "https://generativelanguage.googleapis.com/v1beta/models?key=$GOOGLE_API_KEY"
```

If you prefer a terminal client, start the same Temporal execution model with:

```bash
docker compose exec worker python -m adk_agents.run_temporal_session \
  --session-id walkthrough-temporal-adk-1 \
  --prompt 'Promote delivery-matching-service 2.5.0 to prod. The rollout is validated and ready to go.'
```

### B. Trigger Scenario 3

Send this message to the ADK agent:

```text
Promote delivery-matching-service 2.5.0 to prod. The rollout is validated and
ready to go.
```

Expected result:

```text
Status: processing
Workflow ID: wf-adk-...
Operation ID: op-...
```

With the security mandate on and the scanner set to `Temporal`, `processing`
means the companion `wf-adk-...:gateway` chain is durably waiting on the scan.
If the request has already reached a human gate (or platform scanning is not in
the path), the status is `waiting_for_approval` instead. Both states keep the ADK
turn attached to the same terminal callback. After that callback reaches the
outer ADK workflow, the one-run companion closes gracefully rather than waiting
through the reusable chain's 24-hour idle window.

Nothing in that sentence is demo scaffolding. Service, version, and environment
are the only facts the agent needs, and it picks the durable run key itself:
`release-delivery-matching-service-2.5.0-prod`.

That key is what makes the run recoverable. When no separate idempotency key is
given, Agent Gateway also uses it as the caller key, so asking for the same
service, version, and environment again recovers the same run instead of opening
a second approval. To stage a fresh approval, promote a different version or name
your own key: `use agent_run_id walkthrough-adk-run-2`.

The autonomous workflow checkpoint in Temporal shows:

```text
step: waiting_for_gateway_governance
next_step: await_gateway_resolution
governance_workflow_id: wf-adk-...:gateway
protected_action_executed: false
dependent_action_executed: false
```

### C. Approve and watch Dashy resume

Leave the ADK turn open. After the scan creates the promotion, sign in as
`tok_abe` and approve the Security-owned operation in the Agent Gateway dashboard.
The gateway sends `agent_gateway_approval_resolved` to the originating
`TemporalAdkSessionWorkflow`. That workflow sends Dashy an internal resume/status
user turn and the final answer appears automatically in the same ADK conversation.

Expected result:

```text
status: completed
result:
  protected_action: ...
  dependent_action:
    followup_recorded: true
```

The dependent action runs only after the approved production action succeeds.

If the browser disconnects, reopen the same ADK Web session and send `resume` or
`check the status`. The proxy uses the workflow and active turn Update IDs saved
in ADK session state and reattaches without starting another release run. A later
normal prompt becomes another Update on the same workflow. If that workflow is no
longer running, the proxy creates and saves a new workflow ID. `docker compose
restart adk-web` preserves the local demo session database; removing or
recreating the container does not.

## 8. Inspect the audit trail

For any workflow, ask the matching authenticated MCP connection:

```text
Show me the approval ledger for that run.
```

Look for events such as:

- `created`
- `policy_evaluated`
- `waiting_for_approval`
- `approved`, `rejected`, `expired`, or `canceled`
- `completed`
- `tool1_checkpointed`, `tool1_resuming`, or `tool1_replayed`
- `agent_checkpointed` and `agent_run_completed`

For a step 6 Act Two run, the handoff and its resolution are in the same ledger:

- `scan_capability_discovered`, then `security_scan_handoff` with the scan
  workflow id and the **endpoint** it was reached on. No namespace is recorded,
  because the pipeline was never told one
- `controlled_caller_notified` when the decision was handed to the workflow
  holding the request open, or `controlled_caller_unreachable` if it could not
  be. Both are same-namespace: the boundary is crossed by the Nexus operation
  completing, not by this
- `security_scan_pipeline_settled` when the parked pipeline operation was closed
  out
- `security_scan_failed` when the Security team reported a blocking finding
  instead

The same execution history is visible in the Temporal UI at
http://localhost:8233. For an Act Two run there are three executions across two
namespaces: the chain and its `protected-action::...` adapter under `waypoint`,
and the scan's own under `security`. None is a subset of the others, which is
the point -- each team's system keeps its own record of what it did, under its
own retention policy.

## 9. Troubleshooting

### Claude Code does not see the tools

```bash
claude mcp list
docker compose logs gateway
```

Confirm the MCP URL is `http://localhost:8080/mcp`.

### The requester is unverified

Re-register the MCP server with the `Authorization` header and verify `/whoami`
returns the expected principal.

### An operation stays in processing

```bash
docker compose logs worker mock-tool
```

Then poll `get_operation_result` again. Long-running calls intentionally convert
from a blocking response to asynchronous recovery.

### The prerequisite pipeline returns `processing`, not `waiting_for_approval`

The scanner switch is on `Temporal`, so the pipeline handed off to the Security
team's workflow. That is step 6's Act Two arriving during Act One. Move the switch
back to `Script` on the dashboard. Nothing needs restarting -- and note that the
worker running is *not* the cause; it always runs.

### Act Two fails with "Could not start a pre-prod security scan"

The Nexus operation timed out, which means nothing was polling `security-tq` in
the `security` namespace for Nexus tasks. Two causes, in order of likelihood:

**A stale image.** The worker can be running older code than you are reading.
Check that the Nexus handler is even in the image:

```bash
docker compose exec security-scan-worker ls /app/security_scan/
```

If `nexus_handlers.py` is missing, rebuild:

```bash
docker compose up -d --build security-scan-worker
```

**Missing endpoints.** Confirm both are registered:

```bash
docker compose exec temporal temporal operator nexus endpoint list --address temporal:7233
```

You want `agent-gateway` → `default`/`agentic-gateway` and `security` →
`security`/`security-tq`. If they are absent, rerun the registration
with `docker compose up nexus-endpoints`.

### Act Two returns `waiting_for_approval` instead of `processing`

The scanner switch is still on `Script`, so the gateway inserted no scan step and
opened the production promotion itself. Move it to `Temporal` and run again. This is
the *only* cause: the switch is what decides, and it does not fall back.

### A release fails with "the Security team's scanning platform is not reachable"

The scanner switch is on `Temporal` and their worker is not answering. The release
stops rather than promoting past a checkpoint the mandate says it needs, and it
does not go anywhere near an approver.

This is deliberate, and it is the one behaviour worth understanding: it used to
degrade silently -- no provider meant no scan step, production got promoted anyway,
and the run reported success. Confirm the worker:

```bash
docker compose logs security-scan-worker
```

```text
security scan worker started, namespace 'security', task queue 'security-tq'
```

Then ask the registry directly:

```bash
curl -s -X POST localhost:9000/invoke -H 'content-type: application/json' \
  -d '{"tool_name":"get_security_scan_status","arguments":{}}'
```

`available: true` means the handoff will work on the next run. Note how little the
answer contains -- a provider and an endpoint, no namespace and no task queue --
which is the same restraint the code shows. Registration is a heartbeat with a TTL,
so give it about five seconds after the worker starts, and expect the same delay
after any `mock-tool` recreate. The other fix is to put the scanner switch back on
`Script` and run their host script by hand, which the error message says too.

### The security scan card says `abandoned` / `STALE · NO SCANNER REPORTING`

Working as intended, and only possible on `Script`. The legacy scanner publishes its
progress on a short TTL and refreshes it while it is alive. When the process ends --
at the approval pause, or from a `kill`, or from a crash -- nothing refreshes it and
the record ages out. Nobody reported the death because there was nobody left to
report it. Run the script again and the card comes back to life.

A card on `Temporal` never does this: that record has a workflow behind it whose
Event History is authoritative, so silence there means a worker is busy or
restarting, not that the scan stopped existing.

### The security scan card is stuck partway through

The scan itself is fifteen seconds, so give it that first. If it stays put, the
security scan worker is not processing tasks. The chain is unaffected -- the
scan's workflow keeps its place and picks up where it left off when a worker
returns, which is exactly what Act Two's kill demonstrates on purpose:

```bash
docker compose logs security-scan-worker
docker compose start security-scan-worker
```

### A pipeline operation is stuck in `waiting_for_dependency`

It handed off to the scan and is waiting on a verdict. Note that a scan worker
which was never running at all does **not** produce this: the handoff operation
has a thirty second schedule-to-close timeout, so that case fails fast with
"Could not start a pre-prod security scan" instead (see above).

This state means the scan was started and then stopped progressing -- the scan
worker died after the handler ran. The scan itself has no deadline, so the
operation waits indefinitely. Bring the worker back and the scan resumes where
it left off:

```bash
docker compose logs security-scan-worker
docker compose start security-scan-worker
```

If the scan's workflow has completed or failed and the pipeline operation still
has not moved, look for `controlled_caller_unreachable` in the chain ledger.

### Start over

Stop the stack while preserving Temporal data:

```bash
docker compose down
```

To delete all demo workflow history and start completely clean:

```bash
docker compose down -v
```

The `-v` permanently removes the demo's Temporal volume. Everything comes up with
`docker compose up -d` and goes down with `docker compose down -v` -- there are no
profiles and no per-service steps in either direction.

Both dashboard switches live in the gateway process and are not durable, so they
reset to the "before" position -- mandate off, scanner on `Script` -- on any gateway
restart. That is deliberate: a re-run starts from the same picture as the first one,
and setting them up again is two clicks. `mock-tool` holds the fleet and both card
projections, so recreating it alone is enough to reset the pipeline row without
touching Temporal history.
