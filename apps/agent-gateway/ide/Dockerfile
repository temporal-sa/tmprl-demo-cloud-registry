# The hosted IDE: code-server with Claude Code in its terminal, opened on the
# stage-prop repository, wired to the gateway's MCP endpoint over cluster DNS.
#
# It is built FROM the same base and with the same Python dependencies as the app
# image on purpose. The walkthrough needs a shell that can run two host-side
# commands -- security_scan.legacy_security_scan_script for CASE-2 Act One, and
# gateway_call.py resume_nested_release to finish what the dead script started --
# and hosting the IDE is what makes "the presenter installs nothing" true for
# those as well as for Claude Code.
FROM python:3.12-slim

# git and ripgrep are what an editor expects to find; less and procps are what a
# person expects to find the moment they open a terminal.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
         curl ca-certificates git ripgrep less procps \
    && rm -rf /var/lib/apt/lists/*

# Non-root, and not just as hygiene: this container is reachable from a browser
# and holds an Anthropic API key, so the blast radius of getting a shell in it
# should not include root.
RUN useradd --create-home --uid 1000 --shell /bin/bash demo

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# The demo application itself, so the terminal can run its scripts.
COPY . .

# The MCP server definition, staged outside the workspace. The entrypoint copies
# it in, because the workspace is what a presenter sees and this is not something
# they should have to create.
RUN mkdir -p /opt/agent-gateway-ide
COPY ide/mcp.json /opt/agent-gateway-ide/mcp.json
COPY ide/entrypoint.sh /usr/local/bin/ide-entrypoint
RUN chmod +x /usr/local/bin/ide-entrypoint

USER demo
ENV HOME=/home/demo \
    PATH=/home/demo/.local/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    WORKSPACE=/home/demo/workspace/delivery-matching-service

# Claude Code's native installer, and code-server standalone. Both land under
# ~/.local, which is why they are installed after USER rather than before.
RUN curl -fsSL https://claude.ai/install.sh | bash -s stable
RUN curl -fsSL https://code-server.dev/install.sh | sh -s -- --method standalone

# The stage dressing. This tree is inert by design -- claude-code-session's
# CLAUDE.md §1 forbids reading it -- so its only job is to look like a service
# repository in the file explorer while a presenter talks about releases.
COPY --chown=demo:demo ide/workspace /home/demo/workspace

# The operating rules for the session, from the single copy this repository
# keeps. Placed at the workspace root, which is where the demo's own README tells
# a human to put it.
COPY --chown=demo:demo claude-code-session/CLAUDE.md \
     /home/demo/workspace/delivery-matching-service/CLAUDE.md

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/ide-entrypoint"]
