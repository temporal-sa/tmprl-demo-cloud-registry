"""Ziggy's tools: inline agent-harness tools that change what the demos page shows.

They run in the workflow through ``AgentWorkflowRunner.run_tool``, so every call
lands on the agent's event stream as ``tool_start`` / ``tool_end`` (or
``tool_error``). The page they edit is injected per turn and hidden from the
model. Each docstring is the tool's description for the model; the activity
builds the schemas (activities/catalog_helper.tool_definitions), because demo
ids are an enum of the catalog the call sees.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from temporal_agent_harness.harness import agent

from demo_registry_operator.workflows.catalog_helper_messages import HelperView


@dataclass
class HelperPage:
    """The demos page as this turn leaves it, and what the turn did to it."""

    view: HelperView
    # The catalog's demos, id to title.
    titles: dict[str, str]
    # What the page shows under Ziggy's reply, e.g. "Showing 3 demos".
    actions: list[str] = field(default_factory=list)

    def known(self, demo_ids: list[str]) -> list[str]:
        return list(dict.fromkeys(i for i in demo_ids if i in self.titles))


@agent.tool_defn(inherently_safe=True)
async def show_demos(demo_ids: list[str], page: agent.Injected[HelperPage]) -> str:
    """Filter the demos page to exactly these demos, best match first. Replaces any
    earlier filter. Use it whenever you recommend demos."""

    ids = page.known(demo_ids)
    if not ids:
        raise ValueError("Pick at least one demo id from the catalog.")
    page.view.demo_ids = ids
    page.view.highlights = [h for h in page.view.highlights if h in ids]
    page.actions.append(f"Showing {len(ids)} demo{'s' if len(ids) != 1 else ''}")
    return f"The page now shows {len(ids)} demo(s): {', '.join(page.titles[i] for i in ids)}."


@agent.tool_defn(inherently_safe=True)
async def highlight_demos(demo_ids: list[str], page: agent.Injected[HelperPage]) -> str:
    """Put a glowing halo on these demos to call them out, usually the one or two to
    start with. Replaces earlier highlights; an empty list removes them."""

    ids = page.known(demo_ids)
    page.view.highlights = ids
    if not ids:
        page.actions.append("Removed highlights")
        return "Highlights removed."
    hidden = [i for i in ids if page.view.demo_ids is not None and i not in page.view.demo_ids]
    note = (
        f" Note: {', '.join(page.titles[i] for i in hidden)} isn't in your current filter, so its halo won't show."
        if hidden
        else ""
    )
    page.actions.append(f"Highlighted {len(ids)}")
    return f"Highlighted {', '.join(page.titles[i] for i in ids)}.{note}"


@agent.tool_defn(inherently_safe=True)
async def clear_view(page: agent.Injected[HelperPage]) -> str:
    """Remove your filter and highlights so the page shows every demo."""

    page.view.demo_ids = None
    page.view.highlights = []
    page.actions.append("Showing every demo")
    return "The page shows every demo again."


HELPER_TOOLS = {tool.__name__: tool for tool in (show_demos, highlight_demos, clear_view)}
