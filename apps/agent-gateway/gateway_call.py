"""Call one Agent Gateway MCP tool directly, without an agent in the loop.

For operator actions: things a person would never phrase as a prompt because they
are scenario switches rather than intent. The CASE-2 safety boundary is the case
this exists for -- nobody types "use an uncontrolled Tool1", but you do want to
show what happens when the orchestrator cannot suspend.

    python gateway_call.py run_release_orchestration tool1_mode=uncontrolled
    python gateway_call.py run_release_orchestration tool1_mode=uncontrolled replay_safe=true
    python gateway_call.py resume_nested_release workflow_id=wf-abc123 operation_id=op-def456

Values are parsed as JSON when they can be, so replay_safe=true is a boolean and
bump=minor stays a string. A plain curl cannot do this: streamable HTTP MCP
requires an initialize handshake and a session id first.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client

GATEWAY_MCP_URL = os.getenv("GATEWAY_MCP_URL", "http://localhost:8080/mcp")
GATEWAY_TOKEN = os.getenv("GATEWAY_TOKEN", "tok_dustin")


def _parse_value(raw: str):
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def _parse_arguments(pairs: list[str]) -> dict:
    arguments = {}
    for pair in pairs:
        if "=" not in pair:
            raise SystemExit(f"arguments must be key=value, got {pair!r}")
        key, _, raw = pair.partition("=")
        arguments[key] = _parse_value(raw)
    return arguments


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tool")
    parser.add_argument("arguments", nargs="*", metavar="key=value")
    parser.add_argument("--url", default=GATEWAY_MCP_URL)
    parser.add_argument("--token", default=GATEWAY_TOKEN)
    args = parser.parse_args()

    async with streamablehttp_client(
        args.url, headers={"Authorization": f"Bearer {args.token}"}
    ) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool(
                args.tool, _parse_arguments(args.arguments)
            )
            if result.isError:
                text = result.content[0].text if result.content else "tool error"
                print(text, file=sys.stderr)
                raise SystemExit(1)
            print(
                json.dumps(
                    result.structuredContent
                    or [block.text for block in result.content],
                    indent=2,
                    sort_keys=True,
                )
            )


if __name__ == "__main__":
    asyncio.run(main())
