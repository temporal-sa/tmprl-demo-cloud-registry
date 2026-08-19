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

# The one host-side command the walkthrough asks a presenter to type, wrapped so
# it works from the workspace directory their terminal actually opens in.
COPY ide/legacy-scan /usr/local/bin/legacy-scan
RUN chmod +x /usr/local/bin/legacy-scan

USER demo
ENV HOME=/home/demo \
    PATH=/home/demo/.local/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    WORKSPACE=/home/demo/workspace/delivery-matching-service

# Claude Code's native installer, and code-server standalone. Both land under
# ~/.local, which is why they are installed after USER rather than before.
RUN curl -fsSL https://claude.ai/install.sh | bash -s stable
RUN curl -fsSL https://code-server.dev/install.sh | sh -s -- --method standalone

# Anthropic's Claude Code extension, so the presenter has a panel in the
# activity bar rather than only a terminal command. code-server resolves
# extensions from Open VSX rather than Microsoft's marketplace; the official
# extension is published there as Anthropic.claude-code.
#
# The extension is a front end for the `claude` CLI installed above, not a
# replacement for it -- it shells out to that binary, so both are required and
# the terminal remains a working entry point.
#
# Deliberately unpinned, matching the `stable` channel the CLI installer uses:
# a pinned extension against a moving CLI is the pairing most likely to break,
# and the image is rebuilt on every deploy anyway.
RUN code-server --install-extension Anthropic.claude-code

# The stage dressing. This tree is inert by design -- claude-code-session's
# CLAUDE.md §1 forbids reading it -- so its only job is to look like a service
# repository in the file explorer while a presenter talks about releases.
COPY --chown=demo:demo ide/workspace /home/demo/workspace

# The operating rules for the session, from the single copy this repository
# keeps. Placed at the workspace root, which is where the demo's own README tells
# a human to put it.
COPY --chown=demo:demo claude-code-session/CLAUDE.md \
     /home/demo/workspace/delivery-matching-service/CLAUDE.md

# The presenter's script, and the file the editor opens on load.
#
# It has to be called README.md, and that is not a naming preference. VS Code's
# only "open something on startup" setting is workbench.startupEditor, and its
# `readme` mode opens the workspace root's README.md specifically -- there is no
# setting that names an arbitrary file, and this code-server build has no
# ?payload=openFile support either. So the file that opens is the readme, and
# the readme is the script.
#
# The stage-prop repository's own README moved to docs-service-overview.md
# rather than being deleted: the tree still needs to read as a service
# repository, it just no longer competes for the one filename that opens.
COPY --chown=demo:demo ide/README.md \
     /home/demo/workspace/delivery-matching-service/README.md

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/ide-entrypoint"]
