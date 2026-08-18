#!/bin/sh
# Bring up the hosted IDE with Claude Code ready to drive the demo on the first
# prompt.
#
# Everything here runs on every container start rather than at build time, and
# that is the point: this platform has no volumes, so the filesystem is rebuilt
# from the image on every pod start, every rollout, and every 30-day credential
# rotation. Anything baked at build time that depends on a runtime value -- and
# the API-key approval below depends on the key itself -- would be wrong the
# moment the key changed.
set -eu

WORKSPACE="${WORKSPACE:-$HOME/workspace/delivery-matching-service}"

mkdir -p "$HOME/.claude"

# ---------------------------------------------------------------- ~/.claude.json
#
# Suppresses the three first-run prompts that would otherwise greet a presenter
# on stage: the onboarding wizard, "do you want to use this API key?", and the
# workspace trust dialog.
#
# These three keys are undocumented internals of Claude Code, so treat them as
# best-effort: if a future version stops honouring them the demo still works, it
# just costs two clicks before the first prompt. That is why nothing below is
# fatal and why the file is rewritten rather than merged -- a stale approval for
# a rotated key is worse than no approval at all.
#
# The API-key approval is recorded against the LAST 20 CHARACTERS of the key,
# which is why it has to be computed here from the live value.
API_KEY_SUFFIX=""
if [ -n "${ANTHROPIC_API_KEY:-}" ]; then
  API_KEY_SUFFIX=$(printf '%s' "$ANTHROPIC_API_KEY" | tail -c 20)
fi

WORKSPACE="$WORKSPACE" API_KEY_SUFFIX="$API_KEY_SUFFIX" \
  python3 - "$HOME/.claude.json" <<'PY'
import json, os, sys

approved = [os.environ["API_KEY_SUFFIX"]] if os.environ.get("API_KEY_SUFFIX") else []
state = {
    "hasCompletedOnboarding": True,
    "customApiKeyResponses": {"approved": approved, "rejected": []},
    "projects": {
        os.environ["WORKSPACE"]: {
            "hasTrustDialogAccepted": True,
            "hasCompletedProjectOnboarding": True,
        }
    },
}
with open(sys.argv[1], "w") as fh:
    json.dump(state, fh, indent=2)
PY

# ----------------------------------------------------- ~/.claude/settings.json
#
# The MCP approval lives HERE, in user settings, and deliberately not in the
# workspace's own .claude/settings.json. A project-committed approval is ignored
# in a folder Claude Code has not been told to trust, and the server then sits at
# "Pending approval" -- which on stage looks exactly like the gateway being down.
# User settings are honoured regardless of workspace trust.
#
# The permission rules are a mechanical version of what claude-code-session's
# CLAUDE.md already asks for in prose. §1 says the working tree is inert set
# dressing and must never be edited or explored; a prompt-only guard is precisely
# the thing that gives way under stage pressure, so it is enforced here too.
cat > "$HOME/.claude/settings.json" <<'EOF'
{
  "enableAllProjectMcpServers": true,
  "disableRemoteControl": true,
  "env": {
    "DISABLE_AUTOUPDATER": "1",
    "DISABLE_TELEMETRY": "1",
    "DISABLE_ERROR_REPORTING": "1",
    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1"
  },
  "permissions": {
    "defaultMode": "acceptEdits",
    "allow": [
      "mcp__agent-gateway__get_deployed_version",
      "mcp__agent-gateway__cut_release",
      "mcp__agent-gateway__promote_release",
      "mcp__agent-gateway__run_nested_release",
      "mcp__agent-gateway__get_operation_status",
      "mcp__agent-gateway__get_operation_result",
      "mcp__agent-gateway__get_workflow_status",
      "mcp__agent-gateway__get_workflow_ledger",
      "mcp__agent-gateway__cancel_operation"
    ],
    "deny": [
      "Edit(./**)",
      "Write(./**)",
      "Bash(git commit:*)",
      "Bash(git push:*)",
      "Bash(gradle:*)",
      "Bash(./gradlew:*)"
    ]
  }
}
EOF

# The MCP server definition. Written into the workspace because that is where a
# presenter would look for it, and because Claude Code expands ${VAR} in `url`
# and `headers`, so the bearer token stays in the environment rather than on disk.
if [ -f /opt/agent-gateway-ide/mcp.json ]; then
  cp /opt/agent-gateway-ide/mcp.json "$WORKSPACE/.mcp.json"
fi

echo "hosted IDE starting: workspace=$WORKSPACE mcp=${AGENT_GATEWAY_MCP_URL:-http://gateway:8080/mcp}"
if [ -z "${ANTHROPIC_API_KEY:-}" ]; then
  echo "WARNING: ANTHROPIC_API_KEY is not set; Claude Code will ask you to sign in." >&2
fi

# --auth none on purpose: access control for this component is the platform SSO
# gate (ingress.temporalAuthRequired in the DemoProject CR), which requires a
# temporal.io Google session before any request reaches /ide/. A code-server
# password would be a second layer, but it is one more secret to provision and
# one more thing to type on stage, and no other project in this registry adds an
# app-level password behind the SSO gate.
#
# The consequence, stated plainly so it is not a surprise: anyone who clears SSO
# gets a shell in this pod, and ANTHROPIC_API_KEY is in its environment. Use a
# dedicated, low-limit key for this demo -- never a shared org key.
exec code-server \
  --bind-addr 0.0.0.0:8080 \
  --auth none \
  --disable-telemetry \
  --disable-update-check \
  --disable-workspace-trust \
  "$WORKSPACE"
