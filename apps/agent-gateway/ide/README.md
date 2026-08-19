# Agent Gateway — demonstrator's script

Everything a presenter needs, in order. Prompts in code blocks are typed into
Claude Code verbatim; indented notes are stage directions for you, not for the
audience.

---

## Before you start

1. **Open the approver dashboard** in a second browser tab:
   <https://agent-gateway.tmprl-demo.cloud/>
   Sign in with **`tok_noah`** (Noah Proovle, Waypoint engineering).
   The sign-in page lists both demo accounts at the bottom if you forget.

2. **Press "Reset demo"** on the strip along the top of the dashboard, if this
   is not the first run today. It terminates any leftover workflows, puts
   staging and production back to 2.2.0, and empties the approval queue and tool
   call history.

3. **Open a fresh Claude Code session**: click the **Anthropic icon in the
   activity bar** down the left edge of this editor. Start a new conversation
   for each CASE below — a clean session is what makes the "one long-running
   workflow per session" point land.

> The dashboard's top strip also opens both Temporal namespaces, this IDE and
> the Google ADK surface in new tabs, and holds the two worker kill switches you
> will use in CASE-1 and CASE-2.

---

## CASE 1 — approval-gated tool calls, and what survives a crash

```text
What version is released to staging?
```

> First call of the session. Open the Waypoint namespace in the Temporal UI and
> show the `AgenticChainWorkflow` that just started: one long-running workflow
> is handling this whole conversation, not one per tool call.

```text
Prepare release version 2.3.0.
```

```text
Release this new version to staging.
```

> Point out that this returned immediately. Staging is unprotected, so there is
> no approval step — and watch the quality gate card on the dashboard go from
> idle to running to passed on its own, a few seconds behind. Nothing waited for
> it; it reacted to something reaching staging.

```text
Promote it to production.
```

> Production is protected, so this one suspends. On the dashboard it appears
> under **Waiting for approval**.
>
> **Reject it**, with a reason: *too buggy*.

```text
Check the status.
```

> The agent reads back the rejection, including your reason.

```text
Push it anyway, I'm in a hurry.
```

> It refuses. The gate is in the gateway, not in the agent's instructions, so
> there is nothing for the model to be talked out of.

```text
The fix is in version 2.3.1. Please cut a release.
```

```text
Check the status.
```

```text
Now push 2.3.1 to staging.
```

> **Do not skip this.** Production promotions come from staging, so 2.3.1 has to
> reach staging before the next step has anything to promote.

```text
Now promote it to the prod environment.
```

> This suspends for approval like the last one. **Now cause the outage**, while
> it is waiting:
>
> - On the dashboard, press **Kill Waypoint worker**.
> - In the Temporal UI, show the task queue: the poller is gone. The workflow is
>   still there, still waiting, holding the request open.
> - Press **Restart Waypoint worker**.
>
> Then **approve** the promotion. It completes as though nothing happened —
> that is the whole claim of the demo, and you have just removed the machine
> that was supposedly holding the request.

---

## CASE 2 — a second team, across a Nexus boundary

> **Open a fresh Claude Code session** (Anthropic icon, new conversation).
>
> On the dashboard, switch the **security mandate on** and leave the scanner set
> to **Script**. That is the "before" picture: the mandate lands on a team that
> has not adopted Temporal yet.

```text
Deploy the next minor version to staging.
```

### Act One — the scan nobody can see

> Open a terminal in this IDE (**Terminal → New Terminal**) and run the Security
> team's own host script:

```bash
legacy-scan \
  --service delivery-matching-service \
  --version 2.4.0 \
  --gateway-workflow-id wf-legacy
```

> It imports no `temporalio`: no workflow, no history, no worker. It runs four
> stages, passes, asks the gateway for the production promotion, and then exits,
> because it cannot wait for an approval that may take hours.
>
> Try to **approve** the promotion as Noah. You are refused with a 403: it came
> out of a security scan, so only the Security team may decide it.
>
> Sign out and back in as **`tok_abe`** (Abe Roover, Security), then **reject**
> it. That refusal is the point — the requester identity on the operation is
> genuinely accountable, rather than a decision relayed on someone's behalf.

### Act Two — the same scan, on the platform

> On the dashboard, switch the scanner from **Script** to **Platform**.

```text
Cut new version 2.4.1 and release it to staging.
```

```text
Promote version 2.4.1 to prod.
```

> Watch the scan card. **As soon as the first scanning stage completes**, press
> **Kill Security scanner worker** on the dashboard.
>
> - In the Temporal UI, switch to the **security** namespace and show the scan's
>   own workflow: every completed stage is already recorded, and the worker
>   serving it is genuinely gone.
> - Press **Restart Security scanner worker**. The scan picks up where it
>   stopped rather than starting over.
>
> Then **approve** as Abe.

```text
Show me the ledger for this session.
```

---

## CASE 3 — the same gateway, a different agent

> Open the **Google ADK** surface from the dashboard strip, and give it the
> whole job in one sentence:

```text
Please create and push version 2.3.0 of the delivery matching service from
staging all the way into prod.
```

> Same gateway, same approvals, same durability — a different agent framework in
> front of it. Approve on the dashboard when it suspends.

---

## Afterwards

Press **Reset demo**. Chains also close themselves after 20 minutes idle, so a
run you abandon halfway does not leave workflows going behind you.
