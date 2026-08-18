# `claude-code-session/` — the CLAUDE.md used to drive the demo

This folder contains one file that matters: [CLAUDE.md](CLAUDE.md). It is the
project-instruction file that was loaded into the Claude Code session driving the
Agent Gateway demo when Temporal presented it on **August 3rd, 2026**, reproduced
verbatim.

It is not part of the gateway. Nothing in the running system reads it. It is
prompt-side scaffolding for the *client* of the gateway — the coding agent — and
it exists because an unconstrained coding agent is a bad demo narrator.

## Why it exists

The demo's story is "an agent asks to do something protected, the gateway
suspends the call, a human decides, the work resumes." Telling that story
depends on the agent doing exactly one thing per prompt, calling the tool the
scenario is about, and reporting what came back.

Left to its own devices, a coding agent does the opposite of that, in ways that
are individually reasonable and collectively fatal to a live presentation:

- It explores the open repo to "verify" what's deployed, reading a changelog and
  a `gradle.properties` that are set dressing, then reports fiction as fact.
- It reaches for whatever other MCP connectors happen to be authorized —
  Slack, Jira, Drive — because the request sounds like release coordination.
- It hand-rolls a sequence of `cut_release` → `promote_release` calls when the
  scenario wanted the single pipeline tool, which quietly takes a different
  release path than the one being demonstrated.
- It polls a pending approval in a loop, burning the pause that is the entire
  point of the scene.
- It editorializes — unsolicited risk commentary about missing tests and
  changelog placeholders — drawn entirely from a fake repo.

`CLAUDE.md` closes each of those doors explicitly: the working tree is inert,
`agent-gateway` is the only tool surface, here is the exact tool contract, here
is which tool matches which phrasing, here is who performs the steps that are
deliberately *not* the coding agent's job (the legacy scanner process, the
operator resume, the ADK run, every approve/reject), and here is how terse the
reporting should be.

## How to use it

Copy it into whatever repo you have open in your IDE while you drive the demo:

```bash
cp claude-code-session/CLAUDE.md /path/to/the/repo/you/have/open/CLAUDE.md
```

Then start Claude Code in that repo and register the gateway as described in the
top-level [README](../README.md#register-the-gateway-with-claude-code) and
[WALKTHROUGH](../WALKTHROUGH.md) step 2. Claude Code picks `CLAUDE.md` up from the
working directory automatically; confirm it did with `/context` or by asking the
session what it is scoped to.

The repo you open does not have to be this one, and does not have to be a real
service — see the note below.

## About the repo that was open during the presentation

When Temporal presented this demo on August 3rd, 2026, a dummy project
(`delivery-matching-service`) was loaded into the VS Code IDE. It was
**window dressing to make the demo work** — a plausible-looking service repo for
the agent to sit inside, so that "promote this to prod" had somewhere to be asked
from. **It is not real code.** It implements nothing, it is not the release
system, and the gateway never reads it. It lives in its own GitHub repo and can
be provided on request.

It is deliberately not shipped here, because handing over a fake service
alongside a real one invites exactly the confusion the demo works hard to avoid:
readers start looking for the connection between the two, and there isn't one.
You do not need it. Open any repo you like — or an empty directory — drop this
`CLAUDE.md` in, and every scenario behaves identically, because all release state
lives in the gateway.

## Known rough edges, kept as-is

`CLAUDE.md` is checked in exactly as it was used on the day, warts included, so
that what you have is the thing that was demonstrated rather than a cleaned-up
retelling. Two things in it are worth knowing about before you reuse it:

- **§9 names the Temporal namespaces as `default` and `security`.** The current
  stack uses `waypoint` and `security`, with `default` created by the Temporal dev
  server and unused. The line is stale; it has no effect on behavior, since
  nothing in the session routes by namespace.
- **§4 forbids `run_release_orchestration` outright**, while §3 and §5 still
  describe it. That is intentional, not a contradiction left by accident: the
  presented walkthrough drives CASE-2 with *named* versions through
  `run_nested_release`, and the prohibition removes any chance of the agent
  choosing the compute-the-next-version variant mid-demo and desynchronizing
  the version numbers from the script. The §3 row is left in place so the agent
  still knows what the tool is when it comes up in conversation. If you want to
  demo the bump-driven path, delete that line.

Personal details are also left in place — the identity is `Dustin Sweet
<dustin.sweet@quickmeals.com>` via the `tok_dustin` bearer token, and §6 refers
to "Dustin" as the operator who can override. Swap in your own principal from
`GATEWAY_PRINCIPALS` if you want the approval card to show a different verified
requester.
