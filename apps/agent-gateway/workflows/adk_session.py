"""Long-lived Google ADK session owned by one Temporal workflow."""

from __future__ import annotations

import asyncio
from typing import Any

from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from google.adk.runners import InMemoryRunner
    from google.adk.utils.context_utils import Aclosing
    from google.genai import types

    from adk_agents.release_approval_agent.temporal_integration import (
        build_temporal_agent,
    )
    from common.adk_resume import build_adk_resume_prompt
    from common.models import (
        AGENT_GATEWAY_APPROVAL_SIGNAL,
        AdkSessionTurnInput,
        AdkSessionWorkflowInput,
        AdkSessionWorkflowResult,
        AdkTemporalSessionCallback,
        ApprovalResolution,
    )


@workflow.defn
class TemporalAdkSessionWorkflow:
    """Own one Dashy runner and accept all turns for an ADK Web session."""

    @workflow.init
    def __init__(self, input: AdkSessionWorkflowInput) -> None:
        self._input = input
        self._session_id = input.session_id
        self._ready = False
        self._runner: InMemoryRunner | None = None
        self._turn_lock = asyncio.Lock()
        self._turns: dict[str, AdkSessionWorkflowResult] = {}
        self._waiting_operation_id: str | None = None
        self._approval: ApprovalResolution | None = None

    @workflow.run
    async def run(
        self,
        input: AdkSessionWorkflowInput,
    ) -> None:
        info = workflow.info()
        if not self._session_id:
            self._session_id = f"adk-session-{workflow.uuid4()}"
        callback = AdkTemporalSessionCallback(
            workflow_id=info.workflow_id,
            run_id=info.run_id,
            session_id=self._session_id,
        )
        agent = build_temporal_agent(
            model=input.model,
            callback=callback,
        )
        self._runner = InMemoryRunner(
            agent=agent,
            app_name="dashy_temporal_session",
        )
        await self._runner.session_service.create_session(
            app_name="dashy_temporal_session",
            user_id=input.user_id,
            session_id=self._session_id,
        )
        self._ready = True

        # The workflow is the lifetime boundary for the ADK session. It remains
        # open for subsequent user turns, browser reconnects, and approval
        # callbacks. If it is closed or terminated, the Web proxy starts a new
        # workflow and stores that new ID in ADK session state.
        await workflow.wait_condition(lambda: False)

    @workflow.update
    async def run_turn(
        self,
        turn: AdkSessionTurnInput,
    ) -> AdkSessionWorkflowResult:
        await workflow.wait_condition(lambda: self._ready)
        async with self._turn_lock:
            assert self._runner is not None
            self._waiting_operation_id = None
            self._approval = None
            result = AdkSessionWorkflowResult(
                workflow_id=workflow.info().workflow_id,
                session_id=self._session_id,
                turn_id=turn.turn_id,
                initial_response="",
            )
            self._turns[turn.turn_id] = result

            try:
                (
                    result.initial_response,
                    self._waiting_operation_id,
                ) = await self._run_agent_turn(
                    self._runner,
                    self._input.user_id,
                    turn.prompt,
                )
            except Exception as err:
                # A model or MCP Activity that exhausted its retries would
                # otherwise leave the browser waiting on an Update that never
                # resolves. Report the failure as the turn's answer instead.
                return self._fail_turn(result, err)

            if self._waiting_operation_id is not None:
                await workflow.wait_condition(
                    lambda: (
                        self._approval is not None
                        and self._approval.operation_id
                        == self._waiting_operation_id
                    )
                )
                assert self._approval is not None
                result.approval = self._approval
                try:
                    (
                        result.resumed_response,
                        _,
                    ) = await self._run_agent_turn(
                        self._runner,
                        self._input.user_id,
                        build_adk_resume_prompt(self._approval),
                    )
                except Exception as err:
                    return self._fail_turn(result, err)

            result.complete = True
            self._waiting_operation_id = None
            return result

    @workflow.signal(name=AGENT_GATEWAY_APPROVAL_SIGNAL)
    def approval_resolved(self, resolution: ApprovalResolution) -> None:
        if resolution.adk_session_id != self._session_id:
            return
        if (
            self._waiting_operation_id is not None
            and resolution.operation_id != self._waiting_operation_id
        ):
            return
        self._approval = resolution

    def _fail_turn(
        self,
        result: AdkSessionWorkflowResult,
        err: Exception,
    ) -> AdkSessionWorkflowResult:
        """Close a turn that could not reach the model or gateway."""

        result.error = f"{type(err).__name__}: {err}"
        if not result.initial_response:
            result.initial_response = (
                "This session could not complete the turn: "
                f"{result.error}"
            )
        result.complete = True
        self._waiting_operation_id = None
        workflow.logger.warning(
            "adk session turn failed",
            extra={"turn_id": result.turn_id, "error": result.error},
        )
        return result

    @workflow.query
    def get_turn_status(
        self,
        turn_id: str,
    ) -> AdkSessionWorkflowResult | None:
        return self._turns.get(turn_id)

    async def _run_agent_turn(
        self,
        runner: InMemoryRunner,
        user_id: str,
        message: str,
    ) -> tuple[str, str | None]:
        final_text = ""
        waiting_operation_id: str | None = None
        async with Aclosing(
            runner.run_async(
                user_id=user_id,
                session_id=self._session_id,
                new_message=types.Content(
                    role="user",
                    parts=[types.Part(text=message)],
                ),
            )
        ) as events:
            async for event in events:
                if not event.content or not event.content.parts:
                    continue
                for part in event.content.parts:
                    if part.text:
                        final_text = part.text
                    operation_id = self._waiting_operation_from_part(part)
                    if operation_id is not None:
                        waiting_operation_id = operation_id
        return final_text, waiting_operation_id

    @staticmethod
    def _waiting_operation_from_part(part: Any) -> str | None:
        function_response = getattr(part, "function_response", None)
        if (
            function_response is None
            or function_response.name != "start_google_adk_release_run"
        ):
            return None
        response = function_response.response
        if not isinstance(response, dict):
            return None
        structured = response.get("structuredContent", response)
        if not isinstance(structured, dict):
            return None
        # Platform scanning returns processing before a human approval exists.
        # It is still a durable pause: the autonomous workflow has registered
        # this session as its callback target and will signal the terminal result.
        if structured.get("status") not in {
            "waiting_for_approval",
            "processing",
        }:
            return None
        operation_id = structured.get("operation_id")
        return str(operation_id) if operation_id else None
