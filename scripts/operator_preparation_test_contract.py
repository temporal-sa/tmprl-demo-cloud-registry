"""Finite owned-kind SA/D/S preparation proof. No CLI or raw private output."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import selectors
import subprocess
import time
from decimal import Decimal

from operator_kube_test_compiler import CompilerContractFailure, compile_profile

LABEL = "operator.local/run-id"
MANDATORY = "TestOwnedAPIAndNetworkBoundary"
PLAN_TEST = "TestNativePreparationPlanAcrossCleanWaitHandoff"
FAULT_TEST = "TestRuntimeServiceAccountReceiptBeforeFailedObservation"
TESTS = (MANDATORY, PLAN_TEST, FAULT_TEST)
PHASES = {"plan-wait": ("wait", "missing-bootstrap", "changed-bootstrap", "ready", "sa-drift"), "sa403": ("sa-fault", "sa-inspect")}
FAILURE_CODES = frozenset({
    "ChildExecutionFailed", "PreparationPureBootstrapDenialMismatch", "PreparationAcceptedBootstrapUnavailable", "PreparationCheckpointInvalid", "PreparationWorkerAdmissionFailed", "PreparationSADriftAdmitted", "PreparationDesiredUnavailable", "PreparationNamespaceAcceptanceFailed", "PreparationBoundPairRenderFailed", "PreparationTypedPairUnavailable", "PreparationSAObservationFaultMissing", "PreparationSAHistoricalReceiptOrUnknownCurrentPolicyLost", "PreparationSAFaultKindLost", "PreparationSAFaultTargetLost", "PreparationCurrentMinimalSAPolicyUnproven", "PreparationDeploymentApplyFailed", "PreparationServiceAllocationUnproven", "PreparationDeploymentObservationFailed", "PreparationTaintDidNotPreventReadiness", "PreparationCurrentReadyControllerFactsUnproven", "PreparationFinalJournalUnavailable", "PreparationOriginalSAFaultLost", "PreparationCanonicalWriteUnproven", "PreparationJournalUnavailable", "PreparationQuiesceFailed", "PreparationPrivateReceiptInvalid", "PreparationRawPayloadEscapedReceipt", "PreparationPrivateInputInvalid",
})


class PreparationProofFailure(RuntimeError):
    pass


def pass_record(line):
    match = re.fullmatch(r"--- PASS: (" + "|".join(map(re.escape, TESTS)) + r") \(((?:0|[1-9][0-9]{0,2})\.[0-9]{2})s\)", line)
    if not match or Decimal(match[2]) > 450:
        raise PreparationProofFailure("PreparationPassDurationInvalid")
    # Reconstruct only independently allowlisted name + actual finite duration.
    return match[1], "--- PASS: " + match[1] + " (" + format(Decimal(match[2]), ".2f") + "s)"


def attach_output(docker, environment, name):
    """One fixed attach, aggregate hard stream bound, stderr counted/discarded."""
    process, selector, primary = None, None, None
    try:
        deadline = time.monotonic() + 450
        process = subprocess.Popen([docker, "start", "--attach", name], env=environment, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ, "out")
        selector.register(process.stderr, selectors.EVENT_READ, "err")
        output, total = bytearray(), 0
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise PreparationProofFailure("PreparationAttachDeadlineExceeded")
            for key, _ in selector.select(min(remaining, 1)):
                data = os.read(key.fileobj.fileno(), 65536)
                if not data:
                    selector.unregister(key.fileobj)
                    continue
                total += len(data)
                if total > 1024**2:
                    raise PreparationProofFailure("PreparationPrivateOutputOverflow")
                if key.data == "out":
                    output.extend(data)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise PreparationProofFailure("PreparationAttachDeadlineExceeded")
        try:
            code = process.wait(timeout=remaining)
        except subprocess.TimeoutExpired:
            raise PreparationProofFailure("PreparationAttachDeadlineExceeded") from None
        try:
            return code, output.decode("utf8", errors="strict")
        except UnicodeDecodeError:
            raise PreparationProofFailure("PreparationPrivateOutputEncodingInvalid") from None
    except BaseException as error:
        primary = error if isinstance(error, PreparationProofFailure) else PreparationProofFailure("PreparationAttachFailed")
        raise primary from None
    finally:
        try:
            if process is not None:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=10)
                for stream in (process.stdout, process.stderr):
                    if stream is not None:
                        stream.close()
        except Exception:
            if primary is None:
                raise PreparationProofFailure("PreparationAttachCleanupUnconfirmed") from None
            primary.add_note("PreparationAttachCleanupUnconfirmed")
        finally:
            if selector is not None:
                selector.close()


def preparation_mode(enabled):
    if type(enabled) is not bool:
        raise PreparationProofFailure("PreparationModeInvalid")
    return TESTS if enabled else None


def compile_preparation_binary(socket, directory, destination):
    try:
        return compile_profile("candidate-preparation", socket, directory, destination)
    except CompilerContractFailure as error:
        raise PreparationProofFailure("PreparationCompiler" + str(error)) from None


def finite_progress(raw):
    """Closed progress prefix only; never interpolate a private line/error."""
    if type(raw) is not str or len(raw) > 1024**2:
        raise PreparationProofFailure("PreparationPrivateOutputOverflow")
    phases, failed, skipped, assertions = {name: [] for name in PHASES}, [], [], []
    last_case, last_phase = "none", "none"
    for line in raw.splitlines():
        if line.startswith("PREPARATION-PROGRESS "):
            try:
                value = json.loads(line.removeprefix("PREPARATION-PROGRESS "))
            except ValueError:
                raise PreparationProofFailure("PreparationProgressInvalid") from None
            if type(value) is not dict or set(value) != {"case", "phase"} or type(value["case"]) is not str or value["case"] not in PHASES or type(value["phase"]) is not str:
                raise PreparationProofFailure("PreparationProgressInvalid")
            name = value["case"]
            expected = PHASES[name]
            if len(phases[name]) >= len(expected) or value["phase"] != expected[len(phases[name])]:
                raise PreparationProofFailure("PreparationProgressInvalid")
            phases[name].append(value["phase"])
            last_case, last_phase = name, value["phase"]
        elif line.startswith("PREPARATION-FAILURE "):
            try:
                value = json.loads(line.removeprefix("PREPARATION-FAILURE "))
            except ValueError:
                raise PreparationProofFailure("PreparationAssertionCodeInvalid") from None
            if type(value) is not dict or set(value) != {"case", "phase", "code"} or value["case"] != last_case or value["phase"] != last_phase or type(value["code"]) is not str or value["code"] not in FAILURE_CODES or any(item["case"] == value["case"] for item in assertions):
                raise PreparationProofFailure("PreparationAssertionCodeInvalid")
            assertions.append({"case": last_case, "phase": last_phase, "code": value["code"]})
        else:
            match = re.fullmatch(r"--- (FAIL|SKIP): (" + "|".join(map(re.escape, TESTS)) + r") \([0-9.]+s\)", line)
            if match:
                values = failed if match[1] == "FAIL" else skipped
                if match[2] in values:
                    raise PreparationProofFailure("PreparationProgressInvalid")
                values.append(match[2])
    return {"case": last_case, "phase": last_phase, "completedPhases": sum(len(value) for value in phases.values()), "failedCases": failed, "failedCount": len(failed), "skippedCount": len(skipped), "assertionFailures": assertions}, phases


def public_output(raw):
    """Release only exact cases and finite evidence; discard all other bytes."""
    progress, phases = finite_progress(raw)
    if progress["failedCount"] or progress["skippedCount"] or progress["assertionFailures"]:
        raise PreparationProofFailure("PreparationNativeCaseFailed")
    passes, names, summaries = [], [], []
    for line in raw.splitlines():
        if line.startswith("--- PASS:"):
            name, canonical = pass_record(line)
            names.append(name)
            passes.append(canonical)
        elif "--- SKIP:" in line or "--- FAIL:" in line:
            raise PreparationProofFailure("PreparationNativeCaseFailed")
        elif line.startswith("PREPARATION-PUBLIC "):
            try:
                value = json.loads(line.removeprefix("PREPARATION-PUBLIC "))
            except ValueError:
                raise PreparationProofFailure("PreparationPublicEvidenceInvalid") from None
            if not isinstance(value, dict) or set(value) != {"case", "waiting", "processExited", "canonical", "noRepost", "promotion"} or value["case"] not in {"plan-wait", "sa403"} or value["promotion"] != "PromotionNotAuthorized" or any(type(value[key]) is not bool or not value[key] for key in ("processExited", "canonical", "noRepost")) or type(value["waiting"]) is not bool or value["waiting"] != (value["case"] == "plan-wait"):
                raise PreparationProofFailure("PreparationPublicEvidenceInvalid")
            summaries.append(value)
        # Other stdout (including private child lines), stderr, exceptions and
        # authoring/provider text are never printed or returned by this mode.
    if any(names.count(name) != 1 for name in TESTS) or len(names) != len(TESTS) or sorted(value["case"] for value in summaries) != ["plan-wait", "sa403"] or any(tuple(phases[name]) != expected for name, expected in PHASES.items()):
        raise PreparationProofFailure("PreparationExactEvidenceIncomplete")
    return "\n".join(passes) + "\n", summaries


def check_container(value, run, image, network, connection):
    config, host = value["Config"], value["HostConfig"]
    if value["Image"] != image or config.get("Labels", {}).get(LABEL) != run or config.get("User") != "65532:65532" or host.get("NetworkMode") != network or set(value["NetworkSettings"]["Networks"]) != {network}:
        raise PreparationProofFailure("PreparationContainerIdentityInvalid")
    if not host.get("ReadonlyRootfs") or host.get("Privileged") or host.get("CapAdd") or set(host.get("CapDrop", [])) != {"ALL"} or host.get("PidMode") or host.get("IpcMode") != "private" or host.get("Devices") or host.get("Binds") or host.get("SecurityOpt") != ["no-new-privileges"] or host.get("Dns") != ["127.0.0.1"]:
        raise PreparationProofFailure("PreparationContainerPrivilegeInvalid")
    if host.get("Tmpfs") != {"/tmp": "rw,noexec,nosuid,nodev,size=64m,uid=65532,gid=65532,mode=0700"} or host.get("Memory") != 768 * 1024**2 or host.get("MemorySwap") != 768 * 1024**2 or host.get("PidsLimit") != 128 or host.get("NanoCpus") != 2 * 10**9:
        raise PreparationProofFailure("PreparationContainerResourceInvalid")
    mounts = value.get("Mounts", [])
    binds = [item for item in mounts if item.get("Type") == "bind"]
    if len(binds) != 1 or Path(binds[0]["Source"]).resolve() != connection.resolve() or binds[0].get("Destination") != "/connection.json" or binds[0].get("RW") or any(item.get("Type") not in {"bind", "tmpfs"} or item.get("Type") == "tmpfs" and item.get("Destination") not in {"/tmp", "/dev/shm"} for item in mounts) or any(value["NetworkSettings"].get("Ports", {}).values()):
        raise PreparationProofFailure("PreparationContainerMountInvalid")
    actual_env = config.get("Env", [])
    required = {"OPERATOR_LOCAL_KUBE_CONNECTION=/connection.json", "OPERATOR_LOCAL_CLUSTER_RUN=" + run, "OPERATOR_LOCAL_PREPARATION_FIXTURE=1"}
    if len(set(actual_env)) != len(actual_env) or not required <= set(actual_env) or any(item not in required and not item.startswith("PATH=") for item in actual_env) or sum(item.startswith("PATH=") for item in actual_env) > 1:
        raise PreparationProofFailure("PreparationContainerEnvironmentInvalid")


def run_preparation_proof(docker, environment, run, network, image, connection):
    name = run + "-preparation"
    identifier, primary, attempted = None, None, False

    def command(arguments, timeout=90):
        result = subprocess.run([docker, *arguments], env=environment, capture_output=True, text=True, timeout=timeout)
        if result.returncode or len(result.stdout) > 1024**2:
            raise PreparationProofFailure("PreparationDockerCommandFailed")
        return result.stdout

    def owned():
        result = subprocess.run([docker, "inspect", "--type=container", name], env=environment, capture_output=True, text=True, timeout=30)
        if result.returncode:
            if result.returncode == 1 and result.stdout == "[]\n" and result.stderr in {"Error: No such container: " + name + "\n", "Error response from daemon: No such container: " + name + "\n"}:
                return None
            raise PreparationProofFailure("PreparationOwnedInspectionFailed")
        value = json.loads(result.stdout)[0]
        if value["Name"] != "/" + name or identifier is not None and value["Id"] != identifier:
            raise PreparationProofFailure("PreparationOwnedIdentityChanged")
        check_container(value, run, image, network, connection)
        return value

    try:
        attempted = True
        identifier = command(["create", "--pull=never", "--name", name, "--label", LABEL + "=" + run, "--network", network, "--read-only", "--cap-drop=ALL", "--security-opt", "no-new-privileges", "--ipc=private", "--dns", "127.0.0.1", "--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=64m,uid=65532,gid=65532,mode=0700", "--memory=768m", "--memory-swap=768m", "--cpus=2", "--pids-limit=128", "--mount", "type=bind,source=" + str(connection) + ",target=/connection.json,readonly", "--env", "OPERATOR_LOCAL_KUBE_CONNECTION=/connection.json", "--env", "OPERATOR_LOCAL_CLUSTER_RUN=" + run, "--env", "OPERATOR_LOCAL_PREPARATION_FIXTURE=1", image, "-test.v", "-test.timeout=420s", "-test.run=^(" + "|".join(TESTS) + ")$"]).strip()
        if owned()["State"]["Status"] != "created":
            raise PreparationProofFailure("PreparationExecutedBeforeBoundary")
        exit_code, raw = attach_output(docker, environment, name)
        # Never print raw stdout/stderr even on failure. Source fixture holds
        # accepted authoring bytes/private pins; finite public receipts only.
        completed = owned()
        if exit_code or completed["State"]["Running"] or completed["State"]["ExitCode"] != 0 or completed["State"].get("OOMKilled"):
            evidence, _ = finite_progress(raw)
            raise PreparationProofFailure("PreparationExecutionFailed " + json.dumps(evidence, sort_keys=True))
        output, summaries = public_output(raw)
        print("PASS finite preparation evidence " + json.dumps(summaries, sort_keys=True), flush=True)
        return output
    except BaseException as error:
        primary = error if isinstance(error, PreparationProofFailure) else PreparationProofFailure("PreparationBoundaryOrExecutionFailed")
        raise primary from None
    finally:
        if attempted:
            try:
                if owned() is not None:
                    command(["rm", "--force", name])
                    if owned() is not None:
                        raise PreparationProofFailure("PreparationRemovalUnconfirmed")
            except Exception:
                if primary is None:
                    raise PreparationProofFailure("PreparationOwnedCleanupUnconfirmed") from None
                primary.add_note("PreparationOwnedCleanupUnconfirmed")
            else:
                print("Owned private preparation container cleanup verified.", flush=True)
