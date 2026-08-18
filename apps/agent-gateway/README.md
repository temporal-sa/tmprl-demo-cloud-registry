# Agent Gateway: durable suspend and resume for agentic tool calls

> [!IMPORTANT]
> **This is demonstration code, not production code — deliberately so.** It was
> built to show what Temporal makes possible, quickly, and it was scoped for that:
> some best practices were knowingly set aside, and some of it may simply be
> wrong. Please read [What this code is, and what it is
> not](#what-this-code-is-and-what-it-is-not) before you read the code.

A runnable implementation of all three scenarios in the "Suspend/Resume Primitive
for Agentic Work" requirements. An MCP server (the Agent Gateway) pauses protected
tool calls for human approval. Temporal holds the chain, nested Tool1 checkpoint,
autonomous-agent checkpoint, approval deadline, ledger, and recovery handles, so
execution survives process restarts and resumes from durable state.

For a step-by-step, execution-first guide, use
[WALKTHROUGH.md](WALKTHROUGH.md).

The demo scenario is a deployment flow. You work in a code session (Claude Code in
any repo, the repo does not have to be this one) and drive a release through
environments. Reading what is deployed, cutting a release, and promoting to
staging all run immediately. Promoting to prod is the protected action: the
gateway pauses it and a human approves or rejects before it proceeds.

Load [claude-code-session/CLAUDE.md](claude-code-session/CLAUDE.md) into that code
session first — it is the instruction file Temporal presented the demo with, and it
is what keeps the agent on the gateway instead of reading the repo it happens to be
sitting in. See [Load the session
instructions](#load-the-session-instructions).

## What this code is, and what it is not

This repository exists to answer one question quickly: what can Temporal do for
agentic work that has to stop and wait on a human? It answers by running. Every
scenario here executes end to end, survives worker and gateway restarts, and picks
back up from durable state. Those mechanics are real, and they are the reason to
read this code: an approval that outlives the process that asked for it, a nested
tool call that fails closed rather than guessing, two teams calling each other
across a namespace boundary through Nexus, an autonomous agent that checkpoints
its own progress. Take those ideas seriously — they transfer.

Production quality, on the other hand, was an explicit non-goal. That was a
scoping decision rather than an oversight: pursuing it would have pushed the
timeline out substantially without making the demonstration any more convincing.
Three consequences are worth stating plainly.

**Temporal best practices were followed selectively, on purpose.** Where a
practice was material to what we were showing, we followed it, and [How this maps
to Temporal best practices](#how-this-maps-to-temporal-best-practices) records
which ones and why. Where it was not material, we skipped it. [Honest
caveats](#honest-caveats) lists the shortcuts we are aware of. Neither list is
exhaustive, and parts of this code are likely to be flat wrong. So: **please do not
read this repository as the sanctioned way to build on Temporal.** Where it
disagrees with Temporal's documented guidance, the guidance is right and this is
not.

**The story is stagecraft.** The Waypoint team, the Security team,
`delivery-matching-service`, the release fleet panel — that scaffolding exists to
make a live walkthrough land in front of an audience, and it works well for that.
It earns nothing when you are reading for the patterns, and it will actively get
in your way if you try to map it onto your own organization. The durable
primitives underneath are the transferable part; the org chart around them is set
dressing, and you should feel free to discard it.

**A focused reference implementation is a different artifact, and we would be glad
to build it.** Canonical, narrow implementations of whichever of these patterns
matter to your architecture — proven patterns, best practices followed throughout,
none of the demo scaffolding — were out of scope for this build. They are very much
in scope as a next step. Either way, the highest-leverage thing you can do is
bring Temporal's Solution Architects in early, while your designs are still ideas
rather than commitments. Vetting a direction before it is built into a system
costs very little; discovering the problem afterward does not.

## Architecture

```
namespace: waypoint                             namespace: security
=============================================   ===================

Claude Code -------------MCP/HTTP-----> Agent Gateway
                                              |
Google ADK Web -> TemporalAdkSessionWorkflow -> MCP Activity
                        |                     |
                        +-- model Activity    |
                                              |
                   +--------------------------+------+
                   |                                 |
         Chain entity workflow <----- Autonomous-agent workflow
      (all governed tool calls)          (CASE-3 checkpoint shell;
                   |                      delegates the same request)
    child workflows, same namespace, same task queue:
      CutReleaseChildWorkflow      tag / archive / hash
      PromoteReleaseChildWorkflow  deploy / health / route / notes
      QualityGateChildWorkflow     4 checks, started on any staging landing
                   |                                 |
                   |  Nexus: start_security_scan     |
                   |  (endpoint "agent-gateway-security")          |
                   +-------------------------------> SecurityScanWorkflow
                                                     task queue:
                                                     security-tq
                                                          |
        ProtectedActionWorkflow <-------------------------+
        (Nexus handler, endpoint     Nexus: request_protected_action
         "agent-gateway-waypoint")   (endpoint "agent-gateway-waypoint")
                   |                     ...suspends until a human decides,
                   | Update              then the operation COMPLETES and
                   v                     the scan resumes. No callback Signal.
         Chain entity workflow
                   |
                   +------- Activities -> Mock Deploy Backend
                                              ^
                                approve/reject/cancel Signals
```

Neither team's code contains the other's namespace, task queue, or workflow type.
Both directions cross on a Nexus Endpoint, which is a cluster object naming a
target namespace and task queue; callers address it by name and learn neither.

- One long-lived workflow per agentic chain, keyed by `workflow_id`. Operations
  live in a map inside workflow state.
- Chain correlation is automatic per session. Every tool call in one Claude Code
  session resolves to the same `workflow_id`, derived from the MCP session id, so
  neither the user nor the model has to thread an id between calls. An explicit
  `workflow_id` argument overrides the session derivation.
- A tool call arrives as an Update via update-with-start. It returns fast with a
  completed result (no approval) or a waiting payload (approval required).
- Approve and reject arrive as Signals that only mutate state. The workflow's main
  loop reacts to the state change and invokes the downstream tool.
- Status and result recovery are Queries, so handlers stay short and
  Continue-As-New can drain them.
- Policy evaluation and the downstream call are Activities. Policy is a function of
  the tool and its arguments: `promote_release` to prod requires approval.
- A chain rolls over with Continue-As-New to keep history bounded, and completes
  on its own after an idle window (24 hours) once it has no pending work.
- Controlled nested Tool1 calls checkpoint before Tool2 and resume with Tool2's
  durable result. Uncontrolled Tool1 calls fail closed. An approved Tool2 is not
  executed until the caller explicitly retries, and replay is refused unless
  Tool1 advertised replay safety.
- QuickMeals is the company; **Waypoint is one engineering team inside it**, the
  one that owns the delivery-matching service and the release tooling in this
  repo. The Security team is a peer. Every persona is `@quickmeals.com`, and which
  team a persona belongs to is what decides who may approve what.
- `cut_release` and `promote_release` are **Child Workflows**, in the same
  namespace and on the same task queue as the chain that starts them, and this is
  the deliberate contrast with the security scan below. A Child Workflow is the
  right primitive here for the reasons that actually justify one: each of these is
  several genuinely distinct steps (tag, archive, hash; deploy, health check,
  route traffic, update notes), and running each as a child gives it its own Event
  History, its own independently retryable steps, and its own id in the Web UI, so
  a stalled release names the step rather than the tool call. Not "to organize
  code" and not "to reduce cost", which would be bad reasons. Same team, same
  infrastructure, more depth. `PromoteReleaseChildWorkflow` is
  environment-agnostic and contains no approval-flavored step: whether a promotion
  needs a human, and which human, stays a policy question at the gateway layer
  above it.
- **Quality gates are ambient, not a pipeline step.** Whenever a promotion lands
  on staging, from any phase, `PromoteReleaseChildWorkflow` starts a
  `QualityGateChildWorkflow` for whatever just landed there and abandons it, so
  the promotion returns immediately and the gate runs alongside. Closer to CI
  kicking off a test run on a push than to an orchestrated step. This is the one
  place CASE-1 gains new machinery, and it is deliberately not sequencing: nothing
  in CASE-1 blocks on the gate or acts on its result, an operator just has real
  evidence on the dashboard before deciding. CASE-2a's pipeline does wait, on the
  specific `(service, version)`-keyed gate workflow its own staging promotion
  started, so a different candidate's verdict can never be read as its own.
- **Some operations may only be approved by one QuickMeals team.** The operation
  carries `required_approver_team`, set at creation, and the gateway refuses a
  decision from anyone else with a 403 before the Signal is ever sent — the
  workflow enforces the same rule again on the durable side, so a Signal sent
  directly cannot walk around the UI. Two independent things set it, and there is
  only one mechanism enforcing both:
  - **Who asked.** A promotion the Security team's own pre-prod scan requested is
    theirs to stand behind. This is what makes CASE-2b's nested call load-bearing
    rather than incidental: the Security team's own workflow is the only correct
    caller because the requester identity on the operation has to genuinely
    reflect who is accountable, not a decision relayed on their behalf.
  - **The security mandate.** A runtime toggle on the dashboard, off by default.
    While it is on, *every* production promotion needs Security whoever asked,
    including the Waypoint team asking for its own service. The gateway reads the
    toggle at the trust boundary and snapshots it onto the request
    (`ToolCallRequest.security_mandate`), for the same reason it resolves the
    approver's team there: it is process configuration, and Workflow code must
    not read it. Because the value lands in Event History, an operation created
    under the mandate keeps its restriction even if the toggle is flipped off a
    moment later. The same toggle is what puts the pre-prod scan in the release
    pipeline's path at all — see below.

  Everything else — every staging promotion, every read, and production
  promotions created before the mandate lands — carries no team restriction and
  is decided by any principal in `GATEWAY_APPROVERS`, unchanged. CASE-3 delegates
  its promotion to this same rule, so it requires Security while the mandate is on.
- The pre-prod security scan (CASE-2b) is a second, independently owned system,
  not a module. `SecurityScanWorkflow` runs in the `security` namespace, on the
  `security-tq` task queue, in a worker the gateway team does not deploy, from a
  Python package that imports nothing from `gateway/`, `workflows/`,
  `activities/`, or `common/`. It is **not** a child workflow: a child shares its
  parent's namespace and lifecycle, and would look separated without being
  separated. A security team is the clearest case for this shape: it can block any
  team's release regardless of reporting line, and its platform was durable long
  before Agent Gateway existed, because a dependency/image/SAST sweep is genuinely
  long-running and sometimes stops on a human reviewing a flagged finding.
- **Both directions cross on a Nexus Endpoint.** The pipeline calls `security`;
  the scan calls `agent-gateway-waypoint`. Neither side's code names the other's namespace,
  task queue, or workflow type, so either team can restructure behind their
  endpoint without the other deploying. That property is asserted in
  `tests/test_security_scan_contract.py`, not just claimed.
- The scan is the first Tool1 in this demo that genuinely calls Tool2 *through*
  the gateway. Once it clears it starts a Nexus operation carrying the original
  `workflow_id`, so one user-visible task spans two namespaces, and then
  **suspends on that operation** for as long as the approval takes. There is no
  callback Signal and nothing to poll: the operation completing is the answer.
  Kill the Security team's worker mid-pause and its stage results, verdict, and
  pending operation all come back from its own Event History.
- `ProtectedActionWorkflow` is the gateway's Nexus handler for that request. It
  exists because a workflow-backed Nexus operation starts a *new* workflow, while
  the thing that must service the request is the already-running chain. It is
  short-lived, entirely gateway-owned, and everything it does to the chain is a
  same-namespace call.
- **The handoff is a *synchronous* Nexus operation on purpose.** An async one
  awaited for the whole scan would put a Nexus operation handle inside
  `AgenticChainWorkflow`, which continues-as-new on Temporal's suggestion and
  will certainly do so across an open-ended approval. Handles do not survive
  Continue-As-New — a pending operation is orphaned and its result dropped. A
  workflow id does survive, because it is data. Same reasoning as storing
  approval deadlines as absolute timestamps rather than Timers.
- Workflow code makes the Nexus calls directly; no client, no Activity. The two
  Activities that remain (`submit_nested_tool_call`, `signal_operation_callback`)
  connect only to their own namespace.
- **The mandate is what puts a scan in the pipeline at all.** With the toggle off
  there is no scan step in the release flow, and the pipeline does not even ask
  whether one is on offer: a run is the plain cut → stage → gate → promote pipeline
  it was before the Security team onboarded, which is what makes the CASE-1
  scenarios demoable with the Security worker running. With the toggle on the scan
  is inserted after the gates and before production.
- Given the mandate, the pipeline still *discovers* the scan rather than being
  configured for it. After the quality gates pass it asks the shared platform
  whether anyone is offering pre-prod security scanning; the Security team's worker
  heartbeats that registration while it runs, and the entry expires on its own when
  it stops. The answer is *who*, and nothing about how: which stages run, the
  severity threshold, and which versions fail all live behind the endpoint. With no
  provider answering while platform mode is selected, the release fails closed
  rather than opening an unscanned production promotion. Legacy mode inserts no
  Temporal scan because the Security team runs its host script separately. The
  approval requirement on production is the mandate's alone and holds either way.
- Each ADK Web session maps to one running `TemporalAdkSessionWorkflow`. ADK Web
  submits later user turns as Updates to that same workflow, which owns one ADK
  runner and its conversation state. A new workflow ID is created only when no
  saved workflow is running. ADK Web does not execute Gemini or MCP calls
  directly; the worker runs both through Temporal Activities with a Scenario
  3-only tool allowlist and the `release-agent@google-adk` identity.
- The Temporal ADK tool activity attaches its workflow, run, and ADK session IDs
  to MCP calls. The autonomous workflow stores a snapshotted `ToolCallRequest` and
  delegates it to a deterministic companion `AgenticChainWorkflow` (`<adk-id>:gateway`),
  which is the exact direct-call path used by Claude Code. Agent Gateway persists
  that callback with the protected run and
  sends a durable `agent_gateway_approval_resolved` signal after the operation
  completes, rejects, expires, cancels, or fails. Once the outer ADK workflow
  receives that terminal callback, it gracefully closes its one-run companion;
  reusable Claude Code session chains keep their normal 24-hour idle lifecycle.
  Dashy sends an internal resume turn to the existing ADK session and reports the
  terminal result automatically.
- Explicit caller-provided workflow IDs are accepted only from authenticated
  principals. Every workflow records its owner; lifecycle queries and retries
  must come from that owner.
- Approver identity is derived from an authenticated gateway token. The approve
  and reject forms do not accept a caller-supplied approver name.
- Every MCP tool publishes a concrete output schema as well as its input schema,
  including the shared status/recovery envelope.

## Requirements scenario mapping

| Scenario | Entry tool | Durable behavior |
| --- | --- | --- |
| CASE-1: simple tool | `promote_release` | Policy gate, wait payload, approve/reject/expire/cancel, invoke, poll result |
| CASE-2a: pipeline and gates | `run_release_orchestration` (the pipeline) or `run_nested_release` (explicit version) | Fixed release pipeline, quality gates, full call path, child operation, controlled checkpoint/resume, uncontrolled fail-closed/retry |
| CASE-2b: security scan as a separate Tool1 (walkthrough Act Two) | any path to prod, with the scanner switch on `Temporal` | Nexus handoff to another team's endpoint, a real nested Tool1 -> Tool2 call back into the same `workflow_id`, Tool1 suspending on a pending Nexus operation across the approval, fail-closed on a blocking finding |
| CASE-2b: uncontrolled Tool1 (walkthrough Act One) | `security_scan/legacy_security_scan_script.py` | A stateless process that cannot hold the pause: prints the operation id, exits non-zero, and requires a human `resume_nested_release` |
| CASE-3: autonomous agent | `start_google_adk_release_run` | Agent-run checkpoint around the same governed direct-call chain as Claude Code; mandated scan/approval first, dependent action only after terminal success |

Services in `docker-compose.yml`: `temporal` (dev server plus Web UI, with the
`waypoint` and `security` namespaces -- one per team), `nexus-endpoints` (registers the
two Nexus Endpoints, then exits), `worker`, `gateway`, `mock-tool` (the pretend
deployment backend), `adk-web` (Google ADK Web for CASE-3), `ide` (the hosted Claude Code
IDE), `edge` (the nginx front door), and
`security-scan-worker` (the Security team's scanning platform).

Every service starts with `docker compose up -d` and stops with
`docker compose down -v`. There are no Compose profiles and no per-service steps in
either direction.

### What decides whether a release gets scanned

Two switches on the dashboard, and nothing else:

| Switch | Off / left | On / right | Effect |
| --- | --- | --- | --- |
| Security mandate | off (default) | on | Off: no scan step anywhere, and production is approvable by anyone. On: every path to production has a scan on it, and the promotion is the Security team's to approve. |
| Scanner (shown only under a live mandate) | `Script` (default) | `Temporal` | `Script`: the gateway inserts no step; a human runs `legacy_security_scan_script.py`, which asks for the promotion itself. `Temporal`: the gateway routes the promotion through `SecurityScanWorkflow` over the `security` Nexus Endpoint. |

Both are snapshotted onto each request at the trust boundary and recorded in Event
History, so an operation keeps the rules it was created under even if a switch moves
a second later. Neither survives a gateway restart, which is how a re-run starts from
the same picture as the first one.

Two things are deliberately **not** inputs to that decision:

- **What is running.** `security-scan-worker` is up the whole time. Both of the
  Security team's scanners are reachable at all times; the switch says which one the
  company is using. The capability registry is still there, but only as a health
  check — if the switch says `Temporal` and their platform is not answering, the
  release **fails**, with an error naming both fixes. It used to be the decision,
  which meant a mandated scan silently evaporated whenever that worker was down and
  the run reported success.
- **Which tool the agent used.** The check runs on the single-tool
  `promote_release` path as well as inside the pipeline. A mandate an agent can step
  around by hand-rolling `cut_release` + `promote_release` instead of calling
  `run_release_orchestration` is not a mandate, and an agent improvising its own
  steps is precisely the case the boundary exists for.

The one exemption is a request that came *from* a scanner
(`caller_service=security`), which is already the output of a scan and is not handed
another one. Both scanners identify that way: the workflow states it over Nexus, the
host script gets it from its own bearer token (`tok_legacy_scanner` →
`legacy-scanner@security`). It is resolved server-side from the token and never read
from the request, because "I am a scanner, do not scan me" is not a claim a caller
gets to make about itself.

## Tools

- `get_deployed_version(environment)` returns the version currently deployed in an
  environment (staging or prod). Read only, never gated.
- `cut_release(service, version)` registers a release candidate from a built
  artifact. Changes nothing running, never gated.
- `promote_release(service, version, environment, justification)` requests that an
  environment move to a release. Test and staging run immediately; prod pauses for
  approval. `justification` is optional and is shown to the approver.
- `run_nested_release(service, version, environment, tool1_mode, replay_safe)`
  runs the CASE-2 chain for a named version. `tool1_mode=controlled` resumes
  automatically. `tool1_mode=uncontrolled` returns `blocked_nested_approval`.
- `run_release_orchestration(bump, environment, service, tool1_mode, replay_safe)`
  runs the team's release pipeline from one instruction. Every argument is
  optional: `bump` defaults to `minor`, `service` to the demo service, and
  `environment` means how far to go rather than where to put it, defaulting to the
  full pipeline. Tool1 reads the version deployed in production, computes the next
  one, cuts it, promotes it to staging, waits for the quality gate verdict on that
  exact candidate, and only then reaches the production promotion. If the gates
  fail the pipeline stops and no approval is ever requested. Same nested-approval
  mechanics, flags, and statuses as `run_nested_release`.
- Quality gates have no MCP tool and are not a pipeline step. Reaching staging is
  what starts a gate run, from any phase, and `QualityGateChildWorkflow` evaluates
  whatever just landed there. An agent cannot run the gates itself and then
  promote around them, because there is nothing for it to call: the gate reacts to
  the staging promotion rather than being invoked. What the pipeline does is wait
  for the verdict belonging to the version it staged, keyed by
  `(service, version)` so an unrelated candidate's result can never be mistaken
  for its own.
- The security scan has no MCP tool at all, and could not have one: it is not
  Agent Gateway's capability to expose. It is started by the pipeline in another
  team's namespace, and the only thing that surfaces on the MCP side is the
  `promote_release` request the scan itself makes once it clears. The same
  reasoning as the gates, one step further: an agent cannot run the scan and then
  promote around it, because an agent cannot run the scan.
- `resume_nested_release(workflow_id, operation_id)` explicitly retries an
  approved uncontrolled nested call; it succeeds only when `replay_safe=true`.
- `start_google_adk_release_run(..., agent_run_id)` starts or recovers CASE-3.
- Lifecycle tools: `get_operation_status(workflow_id, operation_id)`,
  `get_operation_result(workflow_id, operation_id)`,
  `get_workflow_status(workflow_id)`, `get_workflow_ledger(workflow_id)`, and
  `cancel_operation(workflow_id, operation_id, reason)`.

All call tools accept an optional caller idempotency key. Reusing it under the same
authenticated principal and workflow returns the same operation rather than
opening a duplicate approval gate.

The default service name in the demo is `delivery-matching-service`.

## Sync, async, and convert-to-async

Each tool is annotated in the gateway (`_TOOL_STRATEGY` in `gateway/server.py`) with
how the gateway should wait on it:

- Sync: block until the tool completes and return the real result.
- Async: return a poll handle immediately and let the tool run in the background.
- Convert (monitor-and-convert): block up to a sync budget in milliseconds; if the
  tool finishes in time return the real result, otherwise convert the call to async
  and return a poll handle while the workflow keeps running.

The tool runs as an Activity inside the workflow from the start, so the sync budget
is only a client-side wait in the gateway. Giving up on that wait does not cancel or
affect the run; it only stops the gateway from blocking, and the result is recovered
later by polling `get_operation_result`. In this demo `get_deployed_version` is sync,
and `cut_release` and `promote_release` use convert-to-async with a 5 second budget.
A prod promotion returns its waiting-for-approval response well within the budget, so
it still comes back synchronously.

## Run it

Requires Docker with Compose v2.

```
docker compose up --build
```

Wait for the worker to log that it is polling and the gateway to start. Then:

- Temporal Web UI: http://localhost:8233
- Approver dashboard: http://localhost:8080
- Gateway MCP endpoint: http://localhost:8080/mcp

Each service is also published on its own port for development. What matches the
hosted deployment, though, is the composed view behind the `edge` container, and
that is what to rehearse against:

- Approver dashboard: http://localhost:8081/
- Google ADK dev UI: http://localhost:8081/adk/
- Hosted IDE (code-server + Claude Code): http://localhost:8081/ide/
- Gateway MCP endpoint: http://localhost:8081/mcp

## Running on the Temporal demo cloud

This repository is a project in
[`temporal-sa/tmprl-demo-cloud-registry`](https://github.com/temporal-sa/tmprl-demo-cloud-registry),
deployed from `projects/demo/agent-gateway.yaml`, and served at
`https://agent-gateway.tmprl-demo.cloud`. Six things differ from a laptop, and
each of them is a consequence of the platform rather than a preference.

**Temporal Cloud, not a dev server.** The `temporal` and `nexus-endpoints`
containers exist for local development only. On the platform the operator
provisions both namespaces, both service accounts, both API keys and both Nexus
Endpoints, and injects `TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`,
`TEMPORAL_API_KEY` and `TEMPORAL_TLS` per component. `common/temporal_client.py`
reads those four variables and works unchanged against either.

**The Security team's isolation is structural.** `security-worker` is bound to a
second Temporal Cloud namespace, so it receives that namespace's credential and
the gateway never holds it. On a shared dev server the boundary was two namespace
strings; here it is two API keys in two Secrets Manager paths.

**The endpoint names are project-prefixed.** Temporal Cloud scopes Nexus Endpoint
names to an entire account, so `agent-gateway` and `security` are not this demo's
to claim. They are `agent-gateway-waypoint` and `agent-gateway-security`, as
literal constants in both `nexus_contracts.py` files.

**One hostname, one front door.** The platform gives a project a single host and
matches path prefixes without stripping them, so `edge/` is an nginx container
that owns the public route and fans out internally. It is the reason
`adk web` runs with `--url_prefix /adk`.

**Claude Code is hosted.** `ide/` is code-server with the Claude Code CLI, the
stage-prop repository and `CLAUDE.md` baked in, and `.mcp.json` pointed at
`http://gateway:8080/mcp` over cluster DNS. Nothing is installed on a presenter's
laptop, the walkthrough's two host-side scripts run in its terminal, and — because
the MCP client lives inside the cluster — the platform's SSO gate can stay on in
front of everything without a bearer-token client having to survive a browser
login.

**Nothing on disk survives.** The platform has no volumes. Every pod restart,
every rollout and every 30-day credential rotation resets the deployment backend
to its seeded 2.2.0 fleet and rebuilds the IDE's home directory. For this demo
that is mostly a feature; it is only a surprise if someone pushes mid-demo.

## Test it

Install `requirements-dev.txt` and run:

```
python3 -m pytest -q
```

The workflow tests start an ephemeral local Temporal dev server with the installed
Temporal CLI and exercise all three cases plus rejection, expiry, cancellation,
deduplication, and the uncontrolled-tool safety boundary.

## Register the gateway with Claude Code

Project scope (writes `.mcp.json`, one is already included here):

```
claude mcp add --transport http agent-gateway http://localhost:8080/mcp
```

To have the approval card show a verified requester, connect with a bearer token
that matches a principal in `GATEWAY_PRINCIPALS` (the compose file ships with
`tok_dustin`):

```
claude mcp add --transport http agent-gateway http://localhost:8080/mcp \
  --header "Authorization: Bearer tok_dustin"
```

Confirm it is connected:

```
claude mcp list
```

### Load the session instructions

Before driving any scenario, copy the demo's `CLAUDE.md` into the repo you have
open in your IDE:

```
cp claude-code-session/CLAUDE.md /path/to/the/repo/you/have/open/CLAUDE.md
```

This is the same project-instruction file Temporal used to present the demo. It
scopes the session to the `agent-gateway` tools, tells the agent that the open
working tree is inert set dressing rather than the release system, maps each
scenario's phrasing to the right tool, names the steps that are deliberately
performed by someone other than the coding agent, and keeps the reporting to a
few status lines. Without it a coding agent will read the local repo to answer
"what's deployed", reach for unrelated connectors, or hand-roll its own
`cut_release` → `promote_release` sequence around the pipeline tools — all of
which make the scenarios narrate badly.

See [claude-code-session/README.md](claude-code-session/README.md) for what each
rule is defending against and the two rough edges kept in the file as-is.

> **On the repo you have open.** When Temporal presented this demo on August 3rd,
> 2026, a dummy project (`delivery-matching-service`) was loaded into the VS Code
> IDE. It was window dressing to make the demo work — **not real code**, not the
> release system, and never read by the gateway. It lives in its own GitHub repo
> and can be provided on request. You do not need it: open any repo, or an empty
> directory, drop the `CLAUDE.md` above into it, and every scenario behaves
> identically, because all release state lives in the gateway rather than on
> disk.

## Run CASE-3 with Google ADK

The ADK Web agent at `adk_agents/release_approval_agent` is a thin proxy to one
long-lived `TemporalAdkSessionWorkflow` per ADK session. Every normal user turn
is a Temporal Update on that workflow, so the same ADK runner and conversation
history are reused. The worker uses Temporal's Google ADK plugin to run Gemini
model calls and streamable HTTP MCP calls as Activities. Its MCP toolset
authenticates as `release-agent@google-adk`, exposes only the Scenario 3 start and
lifecycle tools, and cannot see the generic `promote_release` entrypoint.

Copy the environment template and put your Gemini API key in the local `.env`
file:

```
cp .env.example .env
# Edit .env to replace GOOGLE_API_KEY and, optionally, change ADK_MODEL.
docker compose up --build worker adk-web
```

Docker Compose injects the key into the worker when the container starts. It is
not baked into any image, and both `.env` and `.env.*` are excluded from the
Docker build context. `ADK_MODEL` defaults to `gemini-3.6-flash` in the template
and is passed to new ADK session workflows, so either value can be changed
without rebuilding the image; recreate the worker after changing `.env`.

Open http://localhost:8000, select `release_approval_agent`, and ask:

```
Promote delivery-matching-service 2.5.0 to prod. The rollout is validated and
ready to go.
```

Service, version, and environment are the only facts the agent needs. It derives
the durable run key itself as `release-<service>-<version>-<environment>`, so
repeating the request recovers the same run rather than opening a second
approval. Override it by naming one: `use agent_run_id walkthrough-adk-run-2`.

`ADK_MODEL` must name a model the API key can call. Google closes older models
such as `gemini-2.5-flash` to new keys, and the session turn then fails with a
404; list the reachable models with
`curl -s "https://generativelanguage.googleapis.com/v1beta/models?key=$GOOGLE_API_KEY"`.

The agent returns the durable `workflow_id` and `operation_id` when approval is
needed. Leave the ADK turn open and decide it at http://localhost:8080. Agent
Gateway signals the originating ADK session, Dashy continues with an internal
resume prompt, and the final result appears in the same turn without manual
polling.

If the browser disconnects, reopen the same ADK Web session and send `resume` or
`check the status`. The session stores its Temporal workflow ID and active Update
ID, then reattaches instead of starting a duplicate turn. Subsequent normal
prompts are new Updates on the same workflow. If the saved workflow is no longer
running, the proxy starts a new session workflow and persists its new ID. A
normal container restart preserves the local demo session database; removing or
recreating the container does not. Use a durable ADK session service in
production.

### Use the command-line client

The command-line client starts or attaches to the same long-lived
`TemporalAdkSessionWorkflow` used by ADK Web, then submits its prompt as a
Temporal Update. It is an alternate client, not a second execution model:

With the base stack running, start a session from the worker container:

```
docker compose exec worker python -m adk_agents.run_temporal_session \
  --session-id walkthrough-temporal-adk-1 \
  --prompt 'Start an autonomous release run for delivery-matching-service 2.5.0 to prod, with agent_run_id walkthrough-temporal-adk-run-1. The rollout is validated and ready to go.'
```

The command waits while the workflow is durably paused. Approve the operation at
http://localhost:8080 with `tok_approver`. The gateway-owned approval workflow
executes the protected and dependent actions and then signals the original
`TemporalAdkSessionWorkflow`. Dashy resumes the same ADK session and the command
prints both the initial pause response and the callback-resumed final response.

The callback uses the fixed signal name
`agent_gateway_approval_resolved`. Callback headers are honored only for an
authenticated gateway principal, and arbitrary signal names are never accepted.

## Chain correlation

One Claude Code session is one agentic chain. The gateway derives the chain
`workflow_id` from the MCP session id, so every tool call in that session lands on
the same workflow without the model passing anything along. The returned
`workflow_id` is stable across the session, which is what lets the demo below show
several calls on one chain from natural prompts. If you drive the gateway from a
different client, pass an explicit `workflow_id` to place calls on a chain of your
choosing.

## Verified requester

The approval card's requester comes from a validated credential, not from anything
the agent writes. The gateway reads the bearer token on each call and looks it up
in `GATEWAY_PRINCIPALS` (a static token to identity map). A matching token sets the
requester to that identity; a missing or unknown token, or no map configured, shows
`claude-code (unverified)`. This is the demo-scale stand-in for an identity provider
issued token: bearer auth proves the caller holds the token, and the gateway, not
the agent, decides who the requester is. It satisfies the requirement that
caller-supplied identity be authorized before use. Tell the agent not to put a
"requested by" line in the justification; the requester field is authoritative on
its own.

## Demo script

Drive these with natural prompts to Claude Code. The tool names below are what the
gateway exposes; the model maps to them.

1. Read current state. Ask what is deployed in staging and prod
   (`get_deployed_version`). You get canned versions, `status: completed`, and a
   `workflow_id`. Every later call in this session lands on that same chain.

2. Cut a release, and watch it convert to async. Ask it to cut release `2.3.1` of
   `delivery-matching-service` (`cut_release`). This build runs long. The gateway
   blocks for its sync budget (5 seconds), then converts the call to async: it
   returns `status: processing` with a `workflow_id`, an `operation_id`, and
   `poll_after_seconds`, while the cut keeps running in the workflow. Poll
   `get_operation_result` with those ids; it reads `processing` until the cut
   finishes (about 12 seconds), then flips to `completed`. Cuts of other versions
   are fast and return synchronously without converting.

3. Promote to staging (not gated). Ask it to promote `2.3.1` to staging
   (`promote_release`). Runs immediately and completes. Read staging again and it
   now shows `2.3.1`.

4. Promote to prod (gated). Ask it to promote `2.3.1` to prod, with a reason such
   as "ship the matching latency fix". The response is `status: waiting_for_approval`
   with an `operation_id` and `poll_after_seconds`. Prod has not changed.

5. Approve and resume. Open http://localhost:8080, sign in with the demo approver
   token `tok_approver`, and review the operation. The waiting row shows the
   requested action, requester, arguments, your justification, the risk reason, the
   submit time, and the deadline. Approve it, then poll `get_operation_result`. The
   status flips to `completed`, reading prod now shows `2.3.1`, and the decision
   appears under "Tool Call History" with the approver and time.

6. Reject. Promote a different version to prod, reject it in the dashboard with a
   reason, then poll. The status is `rejected` with your reason, prod is unchanged,
   and the rejection shows in the history table.

7. Timeout and expire. Set a short window and trigger a prod promotion:

   ```
   docker compose stop gateway
   APPROVAL_TIMEOUT_SECONDS=20 docker compose up -d gateway
   ```

   Promote to prod, wait past the window without deciding, then poll. The status is
   `expired` and prod was not changed.

8. Durability. Trigger a prod promotion so an operation is waiting. Restart the
   worker and the gateway:

   ```
   docker compose restart worker gateway
   ```

   The waiting operation is still there. Approve it and it resumes and completes.
   Nothing was lost, because the pause lives in Temporal, not in a process.

9. Idempotency. Promote the same version to the same environment twice in one
   session. The second call returns the same `operation_id` rather than opening a
   second approval gate. On any retried downstream attempt the mock backend reports
   `idempotent_replay: true` with a stable `executed_at`, so the environment is
   never promoted twice.

10. Continue-As-New. Fire many calls on one chain to grow history. When the server
    suggests it, the chain rolls over: the `workflow_id` stays the same and a new
    Run Id appears in the Web UI. Waiting operations and their deadlines carry
    across the rollover unchanged.

11. Idle completion (optional). With the default 24 hour idle window a chain stays
    open for the length of any demo. To show the automatic close, set
    `IDLE_TIMEOUT_SECONDS` in `workflows/chain.py` to a small value such as `60`,
    rebuild the worker, run one call, and let the chain sit with nothing pending.
    After the window it completes on its own as Completed (not Terminated). A new
    prompt in the same session then starts a fresh chain run under the same
    `workflow_id`.

### CASE-2: the mandate, and the two ways a team meets it

**Setup, not a scenario.** CASE-2a is the release pipeline the Waypoint team built
for itself: one sentence (`Deploy the next minor version to staging.`) makes it
read production, compute the next version, cut it, put it on staging, and wait for
the `(service, version)`-keyed quality gate run its own staging promotion started.
It is built and demoed on its own, and everything below assumes it has already
produced a staged, gate-passed candidate. It is not a third use case alongside the
two acts; it is the phase that hands them something to act on.

**The mandate.** A company-wide policy lands: no engineering team promotes its own
service to production on its own say-so any more. Every production release goes
through the Security team's scanning process, and Security — not the owning team —
approves and initiates it. Enact it with the switch at the top right of the
dashboard, under **Log out**; while it is in force the line under it reads
"Production releases require Security Team approval." Mechanically it does two
things:

- the release pipeline routes a gate-passed candidate through the Security team's
  pre-prod scan before opening the production promotion. With the mandate off there
  is no scan step in the flow at all, which is why the CASE-1 and CASE-2a runs above
  are unaffected by it and why the Security worker can stay up throughout.
- the next production Operation, from any caller, is created with
  `required_approver_team="security"`.

There is no creation-time refusal and no second enforcement layer for the approval
half. A direct `promote_release(prod)` attempted after the mandate lands is created
normally and simply carries the restriction; the attempt to approve it is what gets
refused, on the same `_authorize_approver` path that was already there.

The two acts are the same mandate before and after the Security team adopts
Temporal. Each ends in exactly one worker kill.

#### Act One: Security is not yet on Temporal

1. Turn the mandate on.
2. Run `security_scan/legacy_security_scan_script.py` against the staged
   candidate. It imports no `temporalio` — no workflow, no Event History, no task
   queue, no worker, no supervisor. All four stages (`dependency_scan`,
   `container_image_scan`, `secret_detection`, `static_analysis`) pass.
3. It calls `run_nested_release` with `tool1_mode=uncontrolled`. The mandate is
   on, so the Operation carries `required_approver_team="security"`, and the
   answer is `blocked_nested_approval`.
4. The script prints the ids and exits `2`. Nobody killed it; it had nowhere to
   wait.
5. **The kill:** crash the gateway's own worker — the process serving
   `agentic-gateway` and hosting `AgenticChainWorkflow` — and restart it. Not the
   script, which is already gone.
6. The pending Operation is still in `AgenticChainWorkflow`'s Event History, still
   `waiting_for_approval`, untouched. The script's four scan stages would have to
   be re-run from scratch; the request would not, because it was never the
   script's state — it was durable on the gateway side from the moment the call
   was made.
7. Approving as `tok_dustin` is refused with a 403: mandate or not, Waypoint
   cannot clear this.
8. Approving as `tok_abe` works. Because the caller was uncontrolled, a human then
   finishes it with `resume_nested_release`, and `PromoteReleaseChildWorkflow`
   runs its four steps.

Durability is a property of what is built on Temporal, not of the system as a
whole. The non-durable half is gone for good; the durable half survived a hard
crash without losing anything.

#### Act Two: Security has adopted Temporal

1. `docker compose up -d security-scan-worker` brings up the Security team's own
   platform: their namespace, their task queue, their worker identity.
2. A fresh pipeline run reaches a staged, gate-passed candidate and hands off.
   `SecurityScanWorkflow` runs the same four stages itself, over about fifteen
   seconds, this time as a durable workflow rather than a stateless script.
3. On a clean pass it calls `AgentGatewayService.request_protected_action` — a
   Nexus operation straight from workflow code — and suspends on the pending
   operation (`await handle`). The promotion reaches the approval queue with the
   call path `security -> security_scan -> promote_release`.
4. **The kill:** crash `security-scan-worker` while the scan is genuinely
   suspended on that pending operation, and restart it.
5. Its Event History is intact: every stage, the verdict, and the outbound Nexus
   call already recorded. Nothing re-runs. The workflow picks up on the same
   pending decision.
6. Approving as `tok_abe` — same rule, same mechanism as Act One, reached over the
   Nexus path rather than the direct Signal path — completes the operation.
7. `SecurityScanWorkflow` resumes and finishes, and production rolls over. Nobody
   ran a command to finish it.

Now that the calling tool is itself durable, there is nothing left to redo
anywhere, no matter what gets killed or when.

Same mandate, same approval requirement, same kind of crash. The only thing that
changed between the two acts is whether Security's own tool was built on Temporal,
and that is the entire difference between losing work and losing nothing.

#### Deliberately out of the live run

Implemented and correct, but not walkthrough beats: `check_license_compliance` and
the other `dependency_scan` sub-Activities (they exist for Event History richness);
a failed scan, a failed quality gate, or a failed health check; any second kill in
either act; and any re-explanation of the Nexus mechanism's internals — endpoints,
contracts, `ProtectedActionWorkflow` — during the sequence itself. Those are
described above under "Architecture" for anyone who asks afterwards.

### CASE-3: autonomous run with the Google ADK agent

Use the ADK agent for this scenario rather than calling
`start_google_adk_release_run` from Claude Code. With the base stack still
running, start ADK Web and recreate the worker so it receives the `.env` values:

```
docker compose up --build worker adk-web
```

1. Open http://localhost:8000 and select `release_approval_agent`. This agent
   starts one durable `TemporalAdkSessionWorkflow` for the ADK session. Later
   turns are Updates on that same workflow. The worker authenticates its MCP
   Activities with `tok_adk` and restricts them to the CASE-3 start and lifecycle
   tools.
2. Ask the agent:

   ```
   Start an autonomous release run for delivery-matching-service 2.5.0 to
   prod, with agent_run_id walkthrough-adk-run-1. The rollout is validated
   and ready to go.
   ```

   The first response is `processing` while a mandated platform scan runs, or
   `waiting_for_approval` when the request has already reached its approval. Both
   include the durable `workflow_id` and `operation_id`, and both keep the ADK
   turn attached to its Temporal callback. Reusing the same `agent_run_id`
   recovers this run instead of creating a duplicate.
3. Open http://localhost:8080. With the mandate on, sign in with `tok_abe` and
   approve as Security; with it off, any configured approver may decide. Agent
   Gateway signals the ADK session, which sends Dashy an
   internal resume/status prompt. The completed result then appears in the same
   ADK turn and contains both the protected promotion and dependent autonomous
   follow-up.
4. To exercise a terminal path, repeat with a new `agent_run_id`, then reject,
   cancel, or let the request expire. Dashy reports that terminal state
   automatically and neither external action is reported as successful.

CASE-3 and the two additions above, stated explicitly rather than assumed:

- **Quality gates do not apply.** A CASE-3 run promotes straight to production
  and never touches staging, so it starts no gate run and waits on no verdict.
  The version-keyed wait CASE-2a needs cannot leak a stale verdict into this path
  because this path never reads one. The agent's tool allowlist also excludes the
  pipeline tools entirely (`start_google_adk_release_run` and lifecycle reads
  only), so it cannot reach `run_release_orchestration` even indirectly.
- **The promotion is still a Child Workflow.** `AutonomousAgentWorkflow` delegates
  the exact `ToolCallRequest` to its companion `AgenticChainWorkflow`; that chain
  runs `PromoteReleaseChildWorkflow`, the same path and type Claude Code reaches.
  Under the platform mandate it first hands off to `SecurityScanWorkflow`, and
  only the scan's resulting Security-owned promotion reaches the child.
- **`release-agent@google-adk` is a requester, never an approver.** It is not in
  `GATEWAY_APPROVERS`, so it cannot decide anything, its own runs included. The
  companion chain sets `required_approver_team=security` for production while the
  mandate is on, exactly as it does for a Claude Code direct promotion.

If the browser disconnects while waiting, reopen the same ADK session and send
`resume` or `check the status`; the web proxy reattaches to its saved Temporal
workflow. See
[WALKTHROUGH.md](WALKTHROUGH.md#7-case-3-trigger-it-with-the-google-adk-agent)
for the expanded flow.

## Approval payload

When a prod promotion pauses, the approver sees the full context the requirements
call for: the requested action as a sentence, the requester, an arguments summary,
the requester justification, the policy risk reason, the submit time, and the
approval window and deadline. On decision, the ledger and the "Tool Call History"
table record who approved or rejected, when, and the rejection reason.

## Release fleet panel

Above the approval queue, the dashboard shows the fleet the decision acts on: the
releases that are cut and ready under "Ready for release", then the staging card
directly above the production card, matching the promotion path. Production is
marked as the environment that requires approval. A release older than the oldest
version still running drops off the ready list, and a version running in both
environments carries a badge for each.

The queue answers "what needs a decision"; the fleet answers "what is running", so
it is cards rather than a table. It polls `GET /fleet` every two seconds and
updates in place, which is what lets an approval read as a transition: the version
rolls over, the card and the inbound connector light up, and a promotion into a
protected environment also raises a toast. The rest of the page still refreshes on
its five second reload, which pauses while a transition is playing.

The two checkpoint cards on that path behave differently from each other, and the
difference is the point. The quality gate card is always there, from container
startup, showing `idle` until something reaches staging and then moving through
`running` to `passed` or `failed` — gates are the Waypoint team's own standing
infrastructure, and an empty card saying "no candidate staged yet" is the accurate
picture rather than something to hide. The security scan card genuinely does not
exist through CASE-1 and CASE-2a, and animates into place between the gate and
production the moment the security mandate is switched on, because that is the
moment a scan is part of the release pipeline. It reads `idle` until a scan
reports, and fills in with stages and severities once one does. The card follows
the mandate rather than the arrival of scan state, for the same reason the
pipeline does: the mandate is the single thing that decides whether there is a
scan step at all, and a card keyed on the last scan to have run would have shown
the pipeline's shape trailing the rule that decides it by an entire release. Turn
the mandate off with nothing in flight and the row goes back to gate-then-
production, which is the flow it then has. The security scan card is the only one
that names an owner, because it is the only one that has a different one.
Restarting `mock-tool` clears both cards' reported state.

`GET /fleet` is a read only projection of the same in-memory state the tools
mutate, proxied from the deployment backend (`MOCK_TOOL_STATE_URL`) and gated on
the same approver identity as the dashboard. It can never disagree with what
`get_deployed_version` returns. If the backend is unreachable, the panel keeps the
last known fleet on screen and marks itself stale rather than blanking.

## How this maps to Temporal best practices

- Determinism. All non-deterministic work and all I/O live in Activities. The
  workflow uses `workflow.now()` and `workflow.logger`, never wall clock or print.
  The idle window is a code constant rather than an environment read for the same
  reason: every Worker replaying the workflow must agree on it.
- Workflow Id as a business identifier. The chain id is the `workflow_id`, derived
  once per Claude Code session so all calls in a session share one chain. It acts
  as the running-workflow uniqueness constraint. An explicit `workflow_id`
  overrides the session derivation.
- Idempotency. Deduplication rides on a caller-provided or content-derived key
  carried in workflow state, not on Update IDs, because Update IDs are scoped to
  one Execution and reset after Continue-As-New.
- Signals mutate state only. Approve and reject handlers never invoke Activities;
  the main loop reacts to state changes and performs the invocation.
- Updates return values and may invoke Activities. The tool-call Update returns the
  completed result or the waiting payload, and it validates the request first so a
  malformed call leaves no trace in history.
- Queries are read only. Status, result, and ledger reads never mutate state.
- Explicit Activity timeouts. Every Activity sets a start-to-close timeout, and the
  downstream invocation sets a retry policy with a non-retryable class for client
  errors.
- Continue-As-New done safely. CAN is called only from the main method, after
  `workflow.all_handlers_finished` drains in-flight handlers, with state forwarded
  through the input dataclass. Approval deadlines and the idle clock are stored as
  absolute times in state rather than as Timers, because Timers do not carry across
  CAN.
- Autonomous checkpoint. CASE-3 stores the agent's current step, next step, and
  side-effect flags in workflow state. A worker or gateway restart replays that
  checkpoint; the caller connection is not the source of liveness.
- Nested fail-closed boundary. CASE-2 never treats an uncontrolled Tool1 stack as
  resumable. Approval and execution are separate states, and explicit replay is
  permitted only with an advertised replay-safe contract.
- Nexus for cross-team calls, in both directions. Calls go from workflow code to
  an Endpoint; no client is created, no namespace is named, and no Activity is
  needed to keep workflow code deterministic. The two Activities that remain on
  this path talk only to their own namespace.
- Long-running work as an asynchronous Nexus operation. The caller receives an
  operation handle and suspends on it; the handler workflow's return value is
  delivered by Temporal's completion callback. Neither side polls, and neither
  side has to know the other's workflow id to wake it.
- Short-lived adapter workflows where a Nexus operation must be serviced by an
  existing long-lived execution. `ProtectedActionWorkflow` is that adapter, and
  it keeps the entity workflow out of the Nexus caller role entirely.
- Nexus operation handles are run-scoped, so a workflow that continues-as-new
  must not hold one across the boundary. The chain takes a workflow id back from
  a synchronous operation instead, for the same reason its approval deadlines are
  absolute timestamps rather than Timers.
- Peers, not parent and child. `SecurityScanWorkflow` is started by the Security
  team's own Nexus handler, in their namespace, with no parent/child link to the
  chain. A child workflow would have been easier and would have quietly
  reintroduced shared ownership and a shared lifecycle.
- Signal handlers still only mutate state. The chain's `scan_verdict_reported`
  handler and the adapter's `operation_resolved` handler each record and return;
  the Activities that follow are driven from the main loop, off durable operation
  state rather than an in-memory queue, so a Worker restart mid-notification
  picks the work back up.
- Published interfaces, not mirrored internals. Each side declares the same Nexus
  service contract -- endpoint names, operation names, payload field names -- and
  `tests/test_security_scan_contract.py` checks they agree and that neither side
  has reacquired knowledge of the other's topology.

## Honest caveats

- The mock deploy backend is a stub. The demo shows the gateway pausing and
  resuming a promotion request, not a real pipeline or GitOps sync. In production
  `promote_release` would call your CD system's promote or sync API.
- Result recovery works while the worker is up and the chain is not terminated. A
  Query needs a worker polling the task queue to serve it.
- A completed or idle-closed chain is still queryable within the namespace
  retention window. A terminated chain is not. This demo completes a chain through
  the idle timeout or the `close_chain` signal rather than terminating it, so it
  stays queryable.
- Chain lifecycle and reopen. A chain completes on its own after 24 hours with no
  pending work. It is never cut off mid-approval, because a waiting operation
  counts as pending work. After an idle close, a new prompt in the same session
  starts a fresh chain run under the same `workflow_id` via the default Allow
  Duplicate reuse policy. Prior operations remain in the closed run and are visible
  in the Web UI by Run Id, but the gateway query tools read the latest run.
- Session correlation assumes one chain per MCP session on the default stateful
  transport. A stateless deployment, or concurrent sessions that must share a
  chain, would need explicit `workflow_id` propagation instead.
- The approver dashboard reads operations within Temporal retention. A production
  UI should page visibility results and export the ledger to the organization's
  compliance store.
- ADK Web is included only as a local development surface, not as a production
  agent deployment. It contains no Gemini key or gateway bearer token; those
  runtime secrets belong to the worker. A production deployment should inject
  them from its platform secret store and replace ADK Web's local SQLite session
  database with a supported durable session service.
- The scan is scripted, not real. `run_scan_check` returns a fixed per-stage
  severity sequence so a demo run is repeatable. In a real Security platform that
  Activity invokes the actual dependency, image, secret, and SAST scanners; nothing
  else about the workflow changes.
- The scan acts on the requester's behalf. The protected-action request carries the
  chain owner as `caller_principal`, because the gateway checks a nested call
  against the chain's owner, with the scan's own identity alongside it in
  `caller_service` and the call path. A production system would model this as
  explicit delegation -- the scan holding its own principal and the gateway
  authorizing it to act for the requester -- rather than relaying the identity.
  Nexus makes this cleaner than it was but does not solve it: an endpoint
  authorizes a caller to reach a service, not to act as a particular person.
- A parked pipeline operation has no deadline of its own. A scan workflow that is
  never scheduled (no worker ever starts on `security-tq`) leaves the pipeline
  operation waiting. The handoff operation has a schedule-to-close timeout, but
  that only covers starting the scan, not the scan itself. Production should give
  the parked operation its own deadline.
- Two namespaces on one dev server is the right shape but not the full claim.
  Real independent ownership also means separate deployments, separate retention
  policy, separate operators, and endpoint-level authorization; this demo gets
  the boundary and the failure domain right and shares a cluster.
- `ProtectedActionWorkflow` is an adapter, and adapters are a cost. It exists
  only because a workflow-backed Nexus operation starts a new workflow while the
  request has to reach an existing entity workflow. If the gateway's chain were
  per-release rather than per-session, the Nexus operation could target it
  directly and this workflow would not exist.
- The Temporal Google ADK integration is currently experimental. This demo uses
  a fixed signal and authenticated callback metadata; production should also
  issue a signed, single-use callback registration token and authorize the target
  workflow type before signaling it.

## Configuration

Set via environment in `docker-compose.yml`.

- `TEMPORAL_ADDRESS` Temporal frontend address. Default `temporal:7233`.
- `TASK_QUEUE` Task queue name. Default `agentic-gateway`.
- `PROTECTED_ENVIRONMENTS` Comma separated environments that require approval for
  `promote_release`. Default `prod,production`.
- `APPROVAL_TIMEOUT_SECONDS` Approval window before an operation expires. Default
  `300`.
- `GATEWAY_PORT` Gateway HTTP port. Default `8080`.
- `GATEWAY_MCP_ALLOWED_HOSTS` comma-separated HTTP `Host` values accepted by
  MCP DNS-rebinding protection. Defaults include local clients and the Compose
  service hostname `gateway:*`.
- `GATEWAY_MCP_ALLOWED_ORIGINS` comma-separated browser origins accepted by MCP
  DNS-rebinding protection. Defaults include local browser origins.
- `GATEWAY_PRINCIPALS` JSON map of bearer token to requester identity, for
  example `{"tok_dustin":"Dustin Sweet <dustin.sweet@quickmeals.com>"}`. When unset,
  the requester is reported as `claude-code (unverified)`.
- `GATEWAY_APPROVERS` comma-separated principal identities authorized to review
  and decide operations. The compose demo authorizes `tok_approver`
  (`approver@demo`), `tok_dustin`, and `tok_abe`.
- `GATEWAY_PRINCIPAL_TEAMS` JSON map of principal to QuickMeals engineering team,
  for example `{"abe.roover@quickmeals.com":"security"}`. Keys may be the full
  principal string or just the address inside it. Consulted only for an operation
  that carries a required approver team — an operation a Security scan asked for,
  or any production promotion while the security mandate is on; everything else is
  decided by any principal in `GATEWAY_APPROVERS`. Explicit rather than inferred
  from the email domain on purpose: an implicit rule would break silently the
  first time a persona's address changed.
- `MOCK_TOOL_URL` Downstream backend endpoint used by the invoke Activity.
- `AGENT_GATEWAY_MCP_URL` streamable HTTP MCP endpoint used by the worker's
  Temporal MCP Activities. Compose sets it to `http://gateway:8080/mcp`.
- `AGENT_GATEWAY_TOKEN` bearer identity used by the worker's MCP Activities. The
  local demo defaults to `tok_adk`; production should inject a real secret.
- `ADK_MODEL` model used by the ADK agent. Default `gemini-3.6-flash`.
- `GOOGLE_API_KEY` Gemini API credential injected into the worker at runtime. It
  is deliberately absent from the Docker image and ADK Web container.
- `TEMPORAL_NAMESPACE` the namespace a worker serves, and the only one it
  connects to. `default` for the gateway worker, `security` for the security scan
  worker. There is no configuration for reaching the *other* team, because
  reaching them is a Nexus Endpoint, and endpoint names are part of the contract
  in `common/nexus_contracts.py` and `security_scan/nexus_contracts.py`.
- `SECURITY_SCAN_BACKEND_URL` shared observability backend the scan reports stage
  progress and its capability heartbeat to. Default
  `http://mock-tool:9000/invoke`.
- `SECURITY_SCAN_HEARTBEAT_SECONDS` how often the Security team's worker
  re-advertises pre-prod scanning. Default `5`; the registration's TTL is four
  times this, so stopping the worker withdraws the capability within ~20s.
- `SCAN_FAIL_VERSIONS` comma separated versions whose scan fails, so the
  fail-closed path can be shown on demand. Empty by default. Set on
  `security-scan-worker`, not on `mock-tool`: it is the Security team's verdict
  rule, and putting it anywhere else would let the caller decide its own result.

The security mandate is not an environment variable either, and deliberately not
anything durable. It is a runtime flag held by the gateway process
(`MANDATE_TOGGLE` in `gateway/server.py`), off at startup and flipped from the
switch at the top right of the dashboard, which says "Production releases require
Security Team approval." for as long as it is on and nothing at all when it is off.
It is not a Signal, not a Search Attribute, and has no Event History of its own,
because it is not domain state: it is a property of the gateway's configuration at
the moment a call arrives, the same kind of thing `GATEWAY_APPROVERS` is. Its
*effect* is durable — the value is snapshotted onto each request, so a run that
started under the mandate keeps both its scan step and its approver restriction
even if the toggle is flipped off a second later. It does not survive a gateway
restart, and does not need to: flipping it live is the demo beat.

The idle timeout is not an environment variable. It is the `IDLE_TIMEOUT_SECONDS`
constant in `workflows/chain.py`, set to 24 hours, kept in code so every Worker
agrees on it. Lower it and rebuild the worker to demo the idle close.

The scan's shape is not an environment variable either. It is
`SCAN_CHECK_SECONDS`, `SCAN_CHECK_COUNT`, `SCAN_STAGE_NAMES`, and
`SCAN_FINDING_SEVERITY_THRESHOLD` in `security_scan/models.py` -- four stages five
seconds apart, medium and above blocking -- read by their Nexus handler when it
starts a scan. How a scan runs is the Security team's decision, so it is not on
the wire at all: `StartSecurityScanInput` has no field for any of it, and the
pipeline could not set it if it wanted to.

## Production notes

- Run at least two workers for high availability. This demo runs one.
- The Temporal dev server is for development only. Use a self-hosted cluster or
  Temporal Cloud for production.
- Continue-As-New input is bounded by the 2 MB payload limit. This demo keeps every
  pending operation plus the most recent terminal operations and drops older
  terminal records. A production system offloads older records to external storage.

## Layout

```
common/models.py            shared dataclasses and enums
common/nexus_contracts.py   the gateway's half of the Nexus boundary: endpoint
                            names, service definitions, payload types
common/semver.py            version bump arithmetic for the release pipeline
gateway_call.py             call one gateway tool directly, for operator actions
workflows/chain.py          AgenticChainWorkflow (entity workflow, CAN, idle close)
workflows/autonomous_agent.py  CASE-3 durable autonomous-agent checkpoint
workflows/adk_session.py     long-lived ADK session, turn Updates, callback receiver
workflows/nexus_handlers.py  the gateway's Nexus front door for other teams
workflows/protected_action.py  short-lived adapter: holds one external tool's
                            request open until a human decides
workflows/release_children.py  the Waypoint team's own release steps as Child
                            Workflows: CutReleaseChildWorkflow,
                            PromoteReleaseChildWorkflow, and the always-visible
                            QualityGateChildWorkflow a staging landing starts
adk_agents/release_approval_agent/  Dashy agent, Temporal integration, Web proxy
adk_agents/run_temporal_session.py  CLI client for the same ADK session workflow
activities/gateway_activities.py  evaluate_policy, invoke_tool, the individual
                            steps inside the release Child Workflows, the
                            version-keyed gate wait, and the two Activities that
                            serve another team's Nexus request -- all inside the
                            gateway's own namespace
worker.py                   registers the workflow and activities
gateway/server.py           MCP HTTP server, deployment tools, approver UI
mock_tool/server.py         pretend deploy backend with idempotency and continuity
claude-code-session/CLAUDE.md  the project instructions loaded into the Claude
                            Code session that drives the demo. Prompt-side
                            scaffolding; nothing in the running system reads it
tests/                      Temporal scenarios, gateway contract, and ADK agent
                            tests. release_step_fakes.py stands in for the
                            release Child Workflows' leaf steps so the suite
                            keeps the orchestration and drops the sleeps.

security_scan/              the Security team's pre-prod scanning platform. A
                            separate package, a separate namespace, a separate
                            worker, and no imports from anything above this line.
  models.py                 their dataclasses, and their scan's shape
  nexus_contracts.py        their half of the Nexus boundary, declared
                            independently of the gateway's
  nexus_handlers.py         their Nexus front door: start_security_scan
  security_scan_workflow.py SecurityScanWorkflow: the stages, the nested call,
                            and the suspension that outlives their worker
  security_scan_activities.py  one scan stage, the three sub-steps
                            dependency_scan is made of, and reporting progress
                            for the dashboard
  worker.py                 security-tq worker and its capability heartbeat
  legacy_security_scan_script.py  the uncontrolled Tool1: a plain process, no
                            temporalio
```
