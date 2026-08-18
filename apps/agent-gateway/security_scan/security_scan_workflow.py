from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from security_scan.models import (
        SecurityScanInput,
        CheckCveInput,
        CheckLicenseInput,
        GenerateSbomInput,
        ScanCheckInput,
        ScanResult,
        ScanState,
        ScanStatusView,
        SCAN_FINDING_SEVERITY_THRESHOLD,
        SEVERITY_ORDER,
        PublishScanStateInput,
        stage_name,
    )
    from security_scan.nexus_contracts import (
        AGENT_GATEWAY_ENDPOINT,
        AgentGatewayService,
        ProtectedActionRequest,
        ToolOutcomeReport,
    )
    from security_scan.security_scan_activities import (
        check_cve_database,
        check_license_compliance,
        generate_sbom,
        publish_scan_state,
        run_scan_check,
    )

# The stage that has internal structure. Every other stage is one Activity.
DEPENDENCY_SCAN_STAGE = 1

CHECK_TIMEOUT = timedelta(seconds=15)
PUBLISH_TIMEOUT = timedelta(seconds=10)
# The gateway holds this open across a human decision, so there is no useful
# upper bound short of the approval window itself plus slack.
PROTECTED_ACTION_TIMEOUT = timedelta(minutes=30)
REPORT_TIMEOUT = timedelta(seconds=30)


@workflow.defn
class SecurityScanWorkflow:
    """The Security team's pre-prod scan, and the checkpoint that outlives a pause.

    This is Tool1 in the requirements document's sense: a tool that is owned and
    operated by another team, is independently durable, and can therefore be told
    "waiting_for_approval" and survive it. Nothing about that is simulated here.
    This workflow runs in the security namespace, on the security-tq task queue,
    in a worker process the Agent Gateway team does not deploy. It is not a child
    workflow of the chain: the two are peers, correlated by a shared workflow_id
    value that is passed in explicitly.

    A scan is the honest version of this claim. It is genuinely long-running and
    genuinely variable -- a dependency scan against a fresh advisory feed, an
    image scan over a rebuilt base layer, and every so often a flagged finding
    that stops the whole thing until a reviewer looks at it. Something that has to
    hold its place across that cannot be a request-scoped process, and the
    Security team's platform was durable long before Agent Gateway existed.

    The shape of the run is:

        run the scan stages, one at a time, entirely on its own
        if any stage finds something at or above the threshold, stop; the gateway
            is never asked
        otherwise, call Agent Gateway's Nexus endpoint to request the promotion
        SUSPEND on that operation until a human somewhere decides

    The stop is the whole point, and it is a real suspension of this workflow's
    own execution rather than of Agent Gateway's. Kill this worker while the
    operation is pending and the stage results, the verdict, and the pending
    operation all come back on replay, with the wait resuming exactly where it
    was.

    Note how little this workflow knows about the other side. It calls an
    endpoint by name. It does not know Agent Gateway's namespace, holds no
    credentials for it, names none of its workflows, and does not construct any
    of its internal types. There is no callback Signal to receive, because the
    Nexus operation's own completion is the answer.
    """

    @workflow.init
    def __init__(self, input: SecurityScanInput) -> None:
        self._input = input
        self._state = ScanState(
            phase="running_scan",
            stage_count=input.stage_count,
            gateway_workflow_id=input.workflow_id,
        )

    @workflow.run
    async def run(self, input: SecurityScanInput) -> ScanResult:
        await self._publish()

        # Phase 1: the scan itself. Entirely local to the Security team; Agent
        # Gateway does not know this is happening and does not need to. Each
        # stage is its own Activity so it lands in Event History individually: a
        # worker restart mid-scan resumes at the next un-run stage instead of
        # rescanning from the top, which is the property being demonstrated when
        # the container is killed between the second and third stage.
        for stage_number in range(1, input.stage_count + 1):
            check_input = ScanCheckInput(
                service=input.service,
                version=input.version,
                stage_number=stage_number,
                stage_count=input.stage_count,
                scripted_outcome=input.scripted_outcome,
                scan_workflow_id=workflow.info().workflow_id,
                environment=input.environment,
            )
            if stage_number == DEPENDENCY_SCAN_STAGE:
                stage = await self._run_dependency_scan_stage(check_input)
            else:
                stage = await workflow.execute_activity(
                    run_scan_check,
                    check_input,
                    start_to_close_timeout=CHECK_TIMEOUT,
                    retry_policy=RetryPolicy(maximum_attempts=3),
                )
            self._state.stage_results.append(stage)
            self._state.stages_completed = stage_number
            if not stage["passed"]:
                return await self._fail_scan(stage)
            await self._publish()
            if stage_number < input.stage_count:
                await workflow.sleep(timedelta(seconds=input.check_seconds))

        self._state.verdict = "pass"

        # Phase 2: the nested call. The scan is now itself a tool caller, and it
        # goes through Agent Gateway rather than promoting anything directly,
        # because promoting to production is a protected action and the Security
        # team has no more right to do it unsupervised than anyone else does.
        #
        # Straight from workflow code. No Activity, no client, no credentials for
        # anyone else's namespace: a Nexus call to a published endpoint.
        gateway = workflow.create_nexus_client(
            service=AgentGatewayService, endpoint=AGENT_GATEWAY_ENDPOINT
        )
        handle = await gateway.start_operation(
            AgentGatewayService.request_protected_action,
            ProtectedActionRequest(
                gateway_workflow_id=input.workflow_id,
                tool_name="promote_release",
                arguments={
                    "service": input.service,
                    "version": input.version,
                    "environment": input.environment,
                },
                idempotency_key=(
                    input.idempotency_key or workflow.info().workflow_id
                ),
                caller_service="security",
                caller_principal=input.requester,
                caller_workflow_id=workflow.info().workflow_id,
                origin_operation_id=input.origin_operation_id,
                justification=(
                    f"Pre-prod security scan cleared all {input.stage_count} "
                    f"stages for {input.service} {input.version} with no "
                    f"findings at or above {SCAN_FINDING_SEVERITY_THRESHOLD} "
                    f"severity"
                ),
            ),
            schedule_to_close_timeout=PROTECTED_ACTION_TIMEOUT,
        )
        self._state.gateway_workflow_id = input.workflow_id
        self._state.gateway_operation_token = handle.operation_token
        self._state.phase = "awaiting_prod_approval"
        await self._publish()

        # Phase 3: the checkpoint. Execution genuinely stops here, on a pending
        # Nexus operation, for as long as it takes a person to decide.
        #
        # Everything above this line -- every stage, the verdict, the outbound
        # operation -- is already in this workflow's own Event History, in this
        # namespace, under this team's retention policy. Kill the security scan
        # worker now and replay rebuilds all of it without re-running a single
        # stage, and this await picks up exactly where it left off.
        outcome = await handle
        self._state.gateway_operation_id = outcome.operation_id

        if outcome.status == "completed":
            self._state.phase = "completed"
            self._state.promotion_result = outcome.result
        elif outcome.status in ("rejected", "canceled"):
            self._state.phase = "rejected"
            self._state.error = outcome.reason or f"the promotion was {outcome.status}"
        elif outcome.status == "expired":
            self._state.phase = "expired"
            self._state.error = outcome.reason or "approval window expired"
        else:
            self._state.phase = "failed"
            self._state.error = (
                outcome.reason or f"Agent Gateway returned {outcome.status!r}"
            )
        await self._publish()
        return self._result()

    # ------------------------------------------------------------- handlers
    #
    # There is no resolution Signal handler here any more, and its absence is
    # the clearest single measure of what Nexus bought. Agent Gateway used to
    # need this workflow's id and a signal name to wake it; now the operation
    # this workflow is suspended on simply completes.

    @workflow.query
    def get_status(self) -> ScanStatusView:
        return ScanStatusView(
            scan_workflow_id=workflow.info().workflow_id,
            service=self._input.service,
            version=self._input.version,
            environment=self._input.environment,
            phase=self._state.phase,
            stages_completed=self._state.stages_completed,
            stage_count=self._state.stage_count,
            stage_results=list(self._state.stage_results),
            verdict=self._state.verdict,
            gateway_workflow_id=self._state.gateway_workflow_id,
            gateway_operation_token=self._state.gateway_operation_token,
            gateway_operation_id=self._state.gateway_operation_id,
            promotion_result=self._state.promotion_result,
            error=self._state.error,
        )

    # -------------------------------------------------------------- helpers

    async def _run_dependency_scan_stage(self, input: ScanCheckInput) -> dict:
        """Stage 1, which is three sub-steps rather than one.

        Sequenced inline here rather than extracted to its own Child Workflow.
        The four top-level stages are already individually visible as Activities,
        and this is the only one with genuine internal structure: resolve the
        transitive tree, then check that resolved tree twice, once against the
        advisory feed and once against the licence allow-list. Both checks depend
        on the SBOM existing, which is why they follow it.

        The stage result keeps exactly the shape every other stage produces, so
        the workflow's own pass/fail logic, the dashboard card, and the failure
        report do not have to know this stage is built differently. What it adds
        is sub_checks, for anyone who wants to see which part found what.
        """
        sbom = await workflow.execute_activity(
            generate_sbom,
            GenerateSbomInput(service=input.service, version=input.version),
            start_to_close_timeout=CHECK_TIMEOUT,
            retry_policy=RetryPolicy(maximum_attempts=2),
        )
        cve_result = await workflow.execute_activity(
            check_cve_database,
            CheckCveInput(
                service=input.service,
                version=input.version,
                sbom_ref=sbom["sbom_ref"],
                stage_number=input.stage_number,
                scripted_outcome=input.scripted_outcome,
            ),
            start_to_close_timeout=CHECK_TIMEOUT,
            retry_policy=RetryPolicy(maximum_attempts=2),
        )
        license_result = await workflow.execute_activity(
            check_license_compliance,
            CheckLicenseInput(
                service=input.service,
                version=input.version,
                sbom_ref=sbom["sbom_ref"],
            ),
            start_to_close_timeout=CHECK_TIMEOUT,
            retry_policy=RetryPolicy(maximum_attempts=2),
        )
        findings = cve_result["findings"] + license_result["findings"]
        max_severity = self._highest_severity(
            [cve_result["max_severity"], license_result["max_severity"]]
        )
        return {
            "stage": input.stage_number,
            "stage_name": stage_name(input.stage_number),
            "findings": findings,
            "max_severity": max_severity,
            # The licence check never fails, so this is the CVE check's verdict
            # in practice. Written as the combination anyway, because that is
            # what it means and a future second failing sub-check should not
            # need this line rewritten.
            "passed": not findings,
            "sub_checks": {
                "generate_sbom": {
                    "sbom_ref": sbom["sbom_ref"],
                    "dependency_count": sbom["dependency_count"],
                },
                "check_cve_database": cve_result,
                "check_license_compliance": license_result,
            },
        }

    @staticmethod
    def _highest_severity(severities: list[str]) -> str:
        """The worst level on the ladder, or the first unrecognised one.

        An unknown level is treated as the answer rather than skipped, which
        matches severity_at_or_above: a scanner reporting something this platform
        does not understand is a reason to stop, not to ignore it.
        """
        worst = "none"
        for severity in severities:
            if severity not in SEVERITY_ORDER:
                return severity
            if SEVERITY_ORDER.index(severity) > SEVERITY_ORDER.index(worst):
                worst = severity
        return worst

    async def _fail_scan(self, stage: dict) -> ScanResult:
        """One finding at or above the threshold ends the scan.

        Deliberately not averaged, rescanned past, or given a second chance. The
        gateway is never called, so a release that failed its security scan never
        reaches the approval queue at all -- the same fail-closed shape the
        quality gate already has, one checkpoint further along.
        """
        self._state.phase = "scan_failed"
        self._state.verdict = "fail"
        finding_word = "finding" if stage["findings"] == 1 else "findings"
        self._state.error = (
            f"{stage['stage_name']} (stage {stage['stage']} of "
            f"{self._state.stage_count}) reported {stage['findings']} "
            f"{finding_word} at {stage['max_severity']} severity, at or above "
            f"the {SCAN_FINDING_SEVERITY_THRESHOLD} threshold"
        )
        await self._publish()
        await self._report_verdict("fail", self._state.error)
        return self._result()

    async def _report_verdict(self, verdict: str, reason: str | None) -> None:
        """Tell Agent Gateway the promotion is not coming.

        Only needed when the scan closes red. A clean scan reports itself by
        requesting the promotion; a failed one never asks for anything, and
        without this the release pipeline operation that handed off would sit
        waiting on a request that is never going to arrive.

        Synchronous, because there is nothing to wait for.
        """
        if not self._input.origin_operation_id:
            return
        gateway = workflow.create_nexus_client(
            service=AgentGatewayService, endpoint=AGENT_GATEWAY_ENDPOINT
        )
        try:
            await gateway.execute_operation(
                AgentGatewayService.report_tool_outcome,
                ToolOutcomeReport(
                    gateway_workflow_id=self._input.workflow_id,
                    origin_operation_id=self._input.origin_operation_id,
                    caller_workflow_id=workflow.info().workflow_id,
                    outcome=verdict,
                    reason=reason,
                    detail={
                        "stage_results": list(self._state.stage_results),
                        "severity_threshold": SCAN_FINDING_SEVERITY_THRESHOLD,
                    },
                ),
                schedule_to_close_timeout=REPORT_TIMEOUT,
            )
        except Exception as err:
            # The scan's own verdict is already durable here. Failing to deliver
            # it does not change what the scan found, so it is recorded and the
            # workflow still completes with the right answer.
            workflow.logger.error(
                "could not deliver the scan verdict to Agent Gateway",
                extra={"error": str(err)},
            )

    async def _publish(self) -> None:
        """Push scan progress to the shared observability backend.

        Visibility only. A publish failure must never fail a security scan, so
        this swallows Activity errors rather than propagating them: whether the
        dashboard can draw a card has nothing to do with what the scan found.
        """
        try:
            await workflow.execute_activity(
                publish_scan_state,
                PublishScanStateInput(
                    scan_workflow_id=workflow.info().workflow_id,
                    service=self._input.service,
                    version=self._input.version,
                    environment=self._input.environment,
                    phase=self._state.phase,
                    stages_completed=self._state.stages_completed,
                    stage_count=self._state.stage_count,
                    severity_threshold=SCAN_FINDING_SEVERITY_THRESHOLD,
                    stage_results=list(self._state.stage_results),
                    verdict=self._state.verdict,
                    gateway_operation_id=self._state.gateway_operation_id,
                    error=self._state.error,
                ),
                start_to_close_timeout=PUBLISH_TIMEOUT,
                retry_policy=RetryPolicy(maximum_attempts=2),
            )
        except ActivityError as err:
            workflow.logger.warning(
                "could not publish scan state", extra={"error": str(err)}
            )

    def _result(self) -> ScanResult:
        return ScanResult(
            phase=self._state.phase,
            verdict=self._state.verdict,
            stage_results=list(self._state.stage_results),
            gateway_operation_id=self._state.gateway_operation_id,
            promotion_result=self._state.promotion_result,
            error=self._state.error,
        )
