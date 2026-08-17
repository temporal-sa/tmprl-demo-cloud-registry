"""The Security team's own data types, and the shape of their scan.

Everything here is internal to this team: the severity ladder they judge findings
against, which stages a scan runs, and the dataclasses their workflow and
Activities pass around. None of it is on the wire, and none of it is Agent
Gateway's business -- the caller cannot see a stage name or a threshold, let alone
set one.

The wire contract lives in security_scan/nexus_contracts.py, stated as an
interface. Agent Gateway declares the mirror image in common/nexus_contracts.py,
and neither imports the other: they are different services owned by different
teams, and a shared internal package between them would be a lie about the
boundary. The payload converter matches on field names, so the contract is the
field names, and it is checked by tests/test_security_scan_contract.py rather than
by the type system.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Optional


SECURITY_TASK_QUEUE = "security-tq"
SECURITY_NAMESPACE = "security"

# Severity ladder, low to high. The threshold is a level on this ladder rather
# than a number, because a level is how a scanner's findings actually arrive.
SEVERITY_ORDER = ["none", "low", "medium", "high", "critical"]

# The highest severity a stage may report and still pass. Medium and above fails,
# so "none" and "low" clear. Fixed, so the finding on the dashboard is legible
# and the same on every run.
SCAN_FINDING_SEVERITY_THRESHOLD = "medium"

# The scan runs for about fifteen seconds of live wall clock: long enough that
# the stage counter reads as an observable wait (and long enough to kill the
# worker mid-scan and show the workflow pick up where it left off), short enough
# that nobody watching a demo loses interest. A real pre-prod scan takes
# considerably longer than this, and sometimes stops entirely on a human
# reviewing a flagged finding, which is the reason it has to be able to hold its
# own state at all. These are constants rather than dashboard inputs on purpose.
# Changing them is a deliberate decision about the demo, not a knob to reach for.
#
# The budget: dependency_scan is three sub-Activities of its own and takes ~3.5s,
# the other three stages are instant, and there are three four-second gaps
# between the four stages. 3.5 + 12 is a hair over fifteen, which is where this
# has always been aimed.
SCAN_CHECK_SECONDS = 4
SCAN_CHECK_COUNT = 4

# What each check stands in for. A real scan runs these as distinct stages
# against the staged artifact, and which stage found something is the useful part
# of a failure report: "the scan failed" is not actionable, "the dependency scan
# found a high" is.
SCAN_STAGE_NAMES = [
    "dependency_scan",
    "container_image_scan",
    "secret_detection",
    "static_analysis",
]


def severity_at_or_above(severity: str, threshold: str) -> bool:
    """True when `severity` sits at or above `threshold` on the ladder.

    An unrecognised severity counts as at-or-above rather than below. A scanner
    reporting a level this platform does not know is a reason to stop, not a
    reason to wave a release through.
    """
    try:
        return SEVERITY_ORDER.index(severity) >= SEVERITY_ORDER.index(threshold)
    except ValueError:
        return True


def stage_name(stage_number: int) -> str:
    """Name the nth stage, 1-indexed.

    Clamped rather than indexed straight: the test suite runs a shortened
    two-stage scan, and a stage count raised past the named stages should not
    become an IndexError inside an Activity.
    """
    index = min(max(stage_number, 1), len(SCAN_STAGE_NAMES)) - 1
    return SCAN_STAGE_NAMES[index]


def scan_fail_versions() -> set[str]:
    """Versions whose scan fails, so the fail-closed path can be demoed.

    Read at call time rather than at import, and read on this side of the
    boundary rather than relayed in by the caller. Which releases the scan
    rejects is the Security team's business; a pipeline that could tell the scan
    what verdict to reach would not be much of a checkpoint.
    """
    return {
        item.strip()
        for item in os.getenv("SCAN_FAIL_VERSIONS", "").split(",")
        if item.strip()
    }


@dataclass
class SecurityScanInput:
    """One pre-prod security scan, gating one promotion.

    workflow_id is Agent Gateway's chain workflow id, not this workflow's own.
    It is the correlation context that ties two namespaces to one user-visible
    task: when the scan calls back to request the promotion, the nested call has
    to land on the chain the operator started, not open a new one.
    """

    workflow_id: str
    service: str
    version: str
    environment: str = "prod"
    check_seconds: int = SCAN_CHECK_SECONDS
    stage_count: int = SCAN_CHECK_COUNT
    # "pass" | "fail" | "flaky_then_pass". Scripted so a demo run is repeatable.
    scripted_outcome: str = "pass"
    idempotency_key: str = ""
    # The operation in Agent Gateway's chain that handed off to this scan and is
    # parked until it resolves. Echoed back on both the pass and fail paths.
    origin_operation_id: str = ""
    # Who asked for the release. The scan acts on their behalf when it calls the
    # gateway back, so the gateway can check the request against the chain's
    # owner.
    requester: str = ""


@dataclass
class ScanCheckInput:
    service: str
    version: str
    stage_number: int
    stage_count: int
    scripted_outcome: str
    scan_workflow_id: str
    environment: str = "prod"


@dataclass
class ScanCheckResult:
    stage: int
    stage_name: str
    findings: int
    max_severity: str
    passed: bool


# ---------------------------------------------------- dependency_scan internals
#
# The first stage is three sub-steps rather than one, because a dependency scan
# genuinely is: resolve the transitive tree into an SBOM, check that SBOM against
# an advisory feed, and check the same SBOM's licences. The other three stages
# stay single Activities -- they are already visible enough individually, and
# giving every stage internal structure would be depth for its own sake.
#
# Only the CVE check is scripted to fail. The licence check always passes, so
# there is exactly one controllable failure lever for this stage, expressed
# through the top-level scripted_outcome the same way it always was. Two
# independently failing sub-checks could disagree with no narratively visible
# reason, which would make the demo's failure story harder to explain rather
# than richer.


@dataclass
class GenerateSbomInput:
    service: str
    version: str


@dataclass
class CheckCveInput:
    service: str
    version: str
    sbom_ref: str
    stage_number: int
    scripted_outcome: str


@dataclass
class CheckLicenseInput:
    service: str
    version: str
    sbom_ref: str


@dataclass
class ScanState:
    phase: str = "running_scan"
    stages_completed: int = 0
    stage_count: int = SCAN_CHECK_COUNT
    stage_results: list[dict[str, Any]] = field(default_factory=list)
    verdict: Optional[str] = None
    # The Nexus operation this workflow is suspended on. Held from the moment
    # the request is made, and this team's own handle on the pending call.
    gateway_operation_token: Optional[str] = None
    # Agent Gateway's internal id for the operation, which arrives only with the
    # outcome. The scan does not have it while it waits and does not need it: it
    # is waiting on an operation it owns a token for, not polling someone else's
    # record by id.
    gateway_operation_id: Optional[str] = None
    gateway_workflow_id: Optional[str] = None
    promotion_result: Optional[Any] = None
    error: Optional[str] = None


@dataclass
class ScanStatusView:
    """Read-only projection for operators and for the Temporal UI."""

    scan_workflow_id: str
    service: str
    version: str
    environment: str
    phase: str
    stages_completed: int
    stage_count: int
    stage_results: list[dict[str, Any]] = field(default_factory=list)
    verdict: Optional[str] = None
    gateway_workflow_id: Optional[str] = None
    gateway_operation_token: Optional[str] = None
    gateway_operation_id: Optional[str] = None
    promotion_result: Optional[Any] = None
    error: Optional[str] = None


@dataclass
class ScanResult:
    phase: str
    verdict: Optional[str] = None
    stage_results: list[dict[str, Any]] = field(default_factory=list)
    gateway_operation_id: Optional[str] = None
    promotion_result: Optional[Any] = None
    error: Optional[str] = None


@dataclass
class PublishScanStateInput:
    """Publish scan progress to the shared observability backend.

    The dashboard's security scan card is rendered from this, the same way the
    quality gate card is rendered from the backend's gate state. Purely for
    visibility: the workflow's own decision never depends on it, so a failure to
    publish must never fail the scan.
    """

    scan_workflow_id: str
    service: str
    version: str
    environment: str
    phase: str
    stages_completed: int
    stage_count: int
    severity_threshold: str
    stage_results: list[dict[str, Any]] = field(default_factory=list)
    verdict: Optional[str] = None
    gateway_operation_id: Optional[str] = None
    error: Optional[str] = None


# The wire contract with Agent Gateway used to live here: a hand-copied version
# of their internal request struct, plus a Signal payload, plus a test that
# grepped their source to check the copies had not drifted. All of it is gone.
# What replaced it is security_scan/nexus_contracts.py, which states an
# interface rather than mirroring an implementation.
