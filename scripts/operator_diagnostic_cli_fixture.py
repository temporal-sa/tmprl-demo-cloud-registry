"""One closed privileged local diagnostic OS command proof; private output."""
from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path
import re
import subprocess
import tarfile

from operator_preparation_test_contract import attach_output

LABEL = "operator.local/run-id"
TESTS = ("TestOwnedAPIAndNetworkBoundary", "TestPrivilegedLocalDiagnosticCommandReadsAndRevokes")


class DiagnosticCLIProofFailure(RuntimeError):
    pass


def validate_artifact_proof(proof):
    keys = {"compilerInputSHA256", "staticELFSHA256", "commandELFSHA256"}
    if type(proof) is not dict or set(proof) != keys or any(type(value) is not str or re.fullmatch(r"[a-f0-9]{64}", value) is None for value in proof.values()):
        raise DiagnosticCLIProofFailure("DiagnosticArtifactProofInvalid")


def add_command_artifact(archive, path, proof):
    validate_artifact_proof(proof)
    data = path.read_bytes()
    if path.is_symlink() or not path.is_file() or len(data) > 128 * 1024**2 or hashlib.sha256(data).hexdigest() != proof["commandELFSHA256"]:
        raise DiagnosticCLIProofFailure("DiagnosticCommandArtifactChanged")
    for name, data, mode in (("registry-diagnostics", data, 0o755), ("diagnostic-cli-proof.json", json.dumps(proof, sort_keys=True).encode(), 0o444)):
        info = tarfile.TarInfo(name)
        info.size, info.mode = len(data), mode
        archive.addfile(info, io.BytesIO(data))


def public_output(raw):
    if not isinstance(raw, str) or len(raw) > 1024**2:
        raise DiagnosticCLIProofFailure("DiagnosticPrivateOutputInvalid")
    names, output = [], []
    for line in raw.splitlines():
        if line.startswith("--- PASS:"):
            match = re.fullmatch(r"--- PASS: (" + "|".join(TESTS) + r") \(((?:0|[1-9][0-9]{0,2})\.[0-9]{2})s\)", line)
            if not match or float(match[2]) > 450:
                raise DiagnosticCLIProofFailure("DiagnosticExactCaseInvalid")
            names.append(match[1]); output.append(match[0])
        elif "--- FAIL:" in line or "--- SKIP:" in line:
            raise DiagnosticCLIProofFailure("DiagnosticNativeCaseFailed")
    if names != list(TESTS):
        raise DiagnosticCLIProofFailure("DiagnosticExactCasesIncomplete")
    return "\n".join(output) + "\n"


def check_container(value, run, image, network, connection):
    config, host = value["Config"], value["HostConfig"]
    if value["Image"] != image or config.get("Labels", {}).get(LABEL) != run or config.get("User") != "65532:65532" or host.get("NetworkMode") != network or set(value["NetworkSettings"]["Networks"]) != {network}:
        raise DiagnosticCLIProofFailure("DiagnosticContainerIdentityInvalid")
    if not host.get("ReadonlyRootfs") or host.get("Privileged") or host.get("CapAdd") or set(host.get("CapDrop", [])) != {"ALL"} or host.get("PidMode") or host.get("IpcMode") != "private" or host.get("Devices") or host.get("Binds") or host.get("SecurityOpt") != ["no-new-privileges"] or host.get("Dns") != ["127.0.0.1"]:
        raise DiagnosticCLIProofFailure("DiagnosticContainerPrivilegeInvalid")
    if host.get("Tmpfs") != {"/tmp": "rw,noexec,nosuid,nodev,size=64m,uid=65532,gid=65532,mode=0700"} or host.get("Memory") != 768 * 1024**2 or host.get("MemorySwap") != 768 * 1024**2 or host.get("PidsLimit") != 128 or host.get("NanoCpus") != 2 * 10**9:
        raise DiagnosticCLIProofFailure("DiagnosticContainerResourceInvalid")
    mounts = value.get("Mounts", [])
    binds = [item for item in mounts if item.get("Type") == "bind"]
    if len(binds) != 1 or Path(binds[0]["Source"]).resolve() != connection.resolve() or binds[0].get("Destination") != "/connection.json" or binds[0].get("RW") or any(item.get("Type") not in {"bind", "tmpfs"} or item.get("Type") == "tmpfs" and item.get("Destination") not in {"/tmp", "/dev/shm"} for item in mounts) or any(value["NetworkSettings"].get("Ports", {}).values()):
        raise DiagnosticCLIProofFailure("DiagnosticContainerMountInvalid")
    required = {"OPERATOR_LOCAL_KUBE_CONNECTION=/connection.json", "OPERATOR_LOCAL_CLUSTER_RUN=" + run, "OPERATOR_LOCAL_DIAGNOSTIC_CLI=1"}
    actual = config.get("Env", [])
    if len(set(actual)) != len(actual) or not required <= set(actual) or any(item not in required and not item.startswith("PATH=") for item in actual) or sum(item.startswith("PATH=") for item in actual) > 1:
        raise DiagnosticCLIProofFailure("DiagnosticContainerEnvironmentInvalid")


def run_diagnostic_proof(docker, environment, run, network, image, connection, proof):
    validate_artifact_proof(proof)
    name, identifier, attempted, primary = run + "-diagnostic-cli", None, False, None

    def command(arguments, timeout=90):
        result = subprocess.run([docker, *arguments], env=environment, capture_output=True, text=True, timeout=timeout)
        if result.returncode or len(result.stdout) > 1024**2:
            raise DiagnosticCLIProofFailure("DiagnosticDockerCommandFailed")
        return result.stdout

    def owned():
        result = subprocess.run([docker, "inspect", "--type=container", name], env=environment, capture_output=True, text=True, timeout=30)
        if result.returncode:
            if result.returncode == 1 and result.stdout == "[]\n" and result.stderr in {"Error: No such container: " + name + "\n", "Error response from daemon: No such container: " + name + "\n"}:
                return None
            raise DiagnosticCLIProofFailure("DiagnosticOwnedInspectionFailed")
        value = json.loads(result.stdout)[0]
        if value["Name"] != "/" + name or identifier is not None and value["Id"] != identifier:
            raise DiagnosticCLIProofFailure("DiagnosticOwnedIdentityChanged")
        check_container(value, run, image, network, connection)
        return value

    try:
        attempted = True
        identifier = command(["create", "--pull=never", "--name", name, "--label", LABEL + "=" + run, "--network", network, "--read-only", "--cap-drop=ALL", "--security-opt", "no-new-privileges", "--ipc=private", "--dns", "127.0.0.1", "--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=64m,uid=65532,gid=65532,mode=0700", "--memory=768m", "--memory-swap=768m", "--cpus=2", "--pids-limit=128", "--mount", "type=bind,source=" + str(connection) + ",target=/connection.json,readonly", "--env", "OPERATOR_LOCAL_KUBE_CONNECTION=/connection.json", "--env", "OPERATOR_LOCAL_CLUSTER_RUN=" + run, "--env", "OPERATOR_LOCAL_DIAGNOSTIC_CLI=1", image, "-test.v", "-test.timeout=420s", "-test.run=^(" + "|".join(TESTS) + ")$"]).strip()
        if owned()["State"]["Status"] != "created":
            raise DiagnosticCLIProofFailure("DiagnosticExecutedBeforeBoundary")
        code, raw = attach_output(docker, environment, name)  # reviewed aggregate stream/deadline bound
        completed = owned()
        if code or completed["State"]["Running"] or completed["State"]["ExitCode"] != 0 or completed["State"].get("OOMKilled"):
            raise DiagnosticCLIProofFailure("DiagnosticNativeExecutionFailed")
        return public_output(raw)
    except BaseException as error:
        primary = error if isinstance(error, DiagnosticCLIProofFailure) else DiagnosticCLIProofFailure("DiagnosticBoundaryOrExecutionFailed")
        raise primary from None
    finally:
        if attempted:
            try:
                if owned() is not None:
                    command(["rm", "--force", name])
                    if owned() is not None:
                        raise DiagnosticCLIProofFailure("DiagnosticRemovalUnconfirmed")
            except Exception:
                if primary is None:
                    raise DiagnosticCLIProofFailure("DiagnosticOwnedCleanupUnconfirmed") from None
                primary.add_note("DiagnosticOwnedCleanupUnconfirmed")
            else:
                print("Owned private diagnostic CLI container cleanup verified.", flush=True)
