#!/usr/bin/env python3
"""Compile Go tests in pinned Linux isolation; execute in no-network scratch.

Acquire dependencies first: python3 scripts/prepare_operator_kube_tests.py --go-only
Acquire compiler separately: python3 scripts/prepare_operator_go_compiler.py --acquire
Run: python3 scripts/run_operator_go_unit_tests.py
Owned metadata process proof: python3 scripts/run_operator_go_unit_tests.py --metadata-process
Owned local candidate TLS proof: python3 scripts/run_operator_go_unit_tests.py --credential-observation-wire
Owned actor/read lifecycle proof: python3 scripts/run_operator_go_unit_tests.py --credential-staging-observation-wire
Owned real CLI FD3 proof: python3 scripts/run_operator_go_unit_tests.py --aws-permission-invocation-process

Package initialization happens only after container isolation. Images include
the static binaries and explicitly selected public schema/project fixtures.
There are no host mounts, cloud credentials, or Docker socket inside tests.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import io
import json
import os
import re
import selectors
import time
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import uuid

from operator_go_linux_compiler import Compiler

ROOT = Path(__file__).resolve().parents[1]
LABEL = "operator.local/run-id"

AWS_MODE = "aws-permission-invocation-process"
AWS_TARGET = "./testinfra/awspermission/..."
AWS_TAG = "awspermission_process_integration"
AWS_TEST = "TestOwnedInvocationFD3ProcessBoundary"
AWS_PACKAGE = "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/testinfra/awspermission"
AWS_CASES = ("real-init-and-preserved-fd3", "ordinary-go-pipe-clears-flag", "absent-fd3", "malformed-stream", "stalled-writer-deadline", "private-metadata-error", "replay-and-init-no-reset")
AWS_COMMANDS = (("run", "./testinfra/awspermission/cmd/run-aws-readproof"), ("init", "./testinfra/awspermission/cmd/init-aws-readproof-journal"))
AWS_COUNTERS = {"InitSuccess": 2, "InitRefused": 1, "RunProcesses": 7, "SourceIdentity": 1, "ReplayDenied": 1, "PreReadDenied": 4, "RawNonblockPreserved": True, "OrdinaryFlagCleared": True, "StalledDeadline": True, "MaterialAbsent": True, "ChildrenReaped": True}


class AWSInvocationProofFailure(RuntimeError):
    """Closed private-runtime diagnostic; never accept provider text."""
    CODES = {"OutputInvalid", "Unproven", "DeadlineExceeded", "OutputOverflow", "EncodingInvalid", "CaptureUnproven", "CaptureCleanupUnconfirmed"}

    def __init__(self, code, last="none", passed=0):
        self.code = code if type(code) is str and code in self.CODES else "CaptureUnproven"
        self.last = last if type(last) is str and last in (*AWS_CASES, "none") else "none"
        self.passed = passed if type(passed) is int and 0 <= passed <= len(AWS_CASES) else 0
        suffix = " " + json.dumps({"lastCase": self.last, "passedSubcases": self.passed}, sort_keys=True) if self.code == "Unproven" else ""
        super().__init__("AWSInvocationProcess" + self.code + suffix)


def compile_invocation_commands(compiler, manifest, directory):
    """Only two fixed commands, built in the already frozen native compiler."""
    expected = [{"path": AWS_PACKAGE, "workdir": "testinfra/awspermission", "binary": "package-0"}]
    records = {item["path"]: item for item in manifest.get("files", [])}
    if manifest.get("target") != AWS_TARGET or manifest.get("tags") != AWS_TAG or manifest.get("packages") != expected or type(manifest.get("digest")) is not str or re.fullmatch(r"[a-f0-9]{64}", manifest["digest"]) is None:
        raise RuntimeError("AWSInvocationCompilerSelectionInvalid")
    for _, target in AWS_COMMANDS:
        record = records.get("workspace/operator-v2/" + target.removeprefix("./") + "/main.go", {})
        if type(record.get("sha256")) is not str or re.fullmatch(r"[a-f0-9]{64}", record["sha256"]) is None or type(record.get("bytes")) is not int or not 0 < record["bytes"] <= 16*1024**2:
            raise RuntimeError("AWSInvocationCommandSourceMissing")
    paths, metadata = {}, {"CompilerInputSHA256": manifest["digest"]}
    for name, target in AWS_COMMANDS:
        destination = directory / ("aws-proof-" + name)
        if destination.exists() or destination.is_symlink():
            raise RuntimeError("AWSInvocationExportAlreadyExists")
        compiler.execution_boundary()
        compiler.command(["exec", "--workdir", "/workspace/operator-v2", compiler.container, "/usr/local/go/bin/go", "build", "-o", "/tmp/out/aws-proof-" + name, target], timeout=300)
        size, digest = compiler.extract("/tmp/out/aws-proof-" + name, destination)
        if type(size) is not int or not 0 < size <= 256*1024**2 or type(digest) is not str or re.fullmatch(r"[a-f0-9]{64}", digest) is None or destination.is_symlink() or not destination.is_file() or destination.stat().st_size != size:
            raise RuntimeError("AWSInvocationCommandExportInvalid")
        with destination.open("rb") as stream:
            if hashlib.file_digest(stream, "sha256").hexdigest() != digest:
                raise RuntimeError("AWSInvocationCommandExportHashChanged")
        paths[name] = destination
        metadata[name.capitalize() + "SHA256"] = digest
    return paths, metadata


def invocation_process_output(output, code=0):
    """Discard tainted child/Go text; release only closed names and counters."""
    if type(output) is not str or len(output) > 1024**2:
        raise AWSInvocationProofFailure("OutputInvalid")
    passed, top, evidence, last = [], 0, [], "none"
    failed = code != 0
    for line in output.splitlines():
        if line.startswith("=== RUN   " + AWS_TEST + "/"):
            candidate = line.removeprefix("=== RUN   " + AWS_TEST + "/")
            if candidate in AWS_CASES:
                last = candidate
        if "--- FAIL:" in line or "--- SKIP:" in line or line == "FAIL":
            failed = True
        if "--- PASS:" in line:
            match = re.fullmatch(r"\s*--- PASS: " + re.escape(AWS_TEST) + r"(?:/(" + "|".join(map(re.escape, AWS_CASES)) + r"))? \([0-9.]+s\)", line)
            if match is None:
                failed = True
            elif match[1] is None:
                top += 1
            else:
                passed.append(match[1])
        if line.startswith("AWSINVOCATION-PUBLIC "):
            try:
                value = json.loads(line.removeprefix("AWSINVOCATION-PUBLIC "))
            except (ValueError, TypeError):
                failed = True
                continue
            if type(value) is not dict or set(value) != set(AWS_COUNTERS) or any(type(value[key]) is not type(expected) or value[key] != expected for key, expected in AWS_COUNTERS.items()):
                failed = True
            else:
                evidence.append(value)
    if failed or top != 1 or len(passed) != len(AWS_CASES) or any(passed.count(name) != 1 for name in AWS_CASES) or len(evidence) != 1 or "PASS" not in output.splitlines():
        raise AWSInvocationProofFailure("Unproven", last, min(len(passed),len(AWS_CASES)))
    return 1, evidence[0]


def capture_invocation_process(arguments, environment):
    """One selected scratch run; hard dual-stream bound, never raw disclosure."""
    process, selector, primary = None, None, None
    try:
        deadline = time.monotonic() + 150
        process = subprocess.Popen(list(map(str, arguments)), env=environment, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ, "out")
        selector.register(process.stderr, selectors.EVENT_READ, "err")
        output, total = bytearray(), 0
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AWSInvocationProofFailure("DeadlineExceeded")
            for key, _ in selector.select(min(remaining, 1)):
                data = os.read(key.fileobj.fileno(), 65536)
                if not data:
                    selector.unregister(key.fileobj)
                    continue
                total += len(data)
                if total > 1024**2:
                    raise AWSInvocationProofFailure("OutputOverflow")
                if key.data == "out":
                    output.extend(data)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AWSInvocationProofFailure("DeadlineExceeded")
        code = process.wait(timeout=remaining)
        try:
            raw = output.decode("utf8", errors="strict")
        except UnicodeDecodeError:
            raise AWSInvocationProofFailure("EncodingInvalid") from None
        invocation_process_output(raw, code)
        return raw
    except BaseException as error:
        # Preserve only locally manufactured finite failures, never raw Popen,
        # selector, decoder, Docker stderr or provider exceptions.
        if type(error) is AWSInvocationProofFailure:
            primary = AWSInvocationProofFailure(error.code, error.last, error.passed)
        else:
            primary = AWSInvocationProofFailure("CaptureUnproven")
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
                raise AWSInvocationProofFailure("CaptureCleanupUnconfirmed") from None
            primary.add_note("AWSInvocationProcessCaptureCleanupUnconfirmed")
        finally:
            if selector is not None:
                try:
                    selector.close()
                except Exception:
                    if primary is None:
                        raise AWSInvocationProofFailure("CaptureCleanupUnconfirmed") from None
                    primary.add_note("AWSInvocationProcessCaptureCleanupUnconfirmed")


@dataclass(frozen=True)
class UnitMode:
    name: str
    target: str
    tags: str = ""
    package: str | None = None
    test: str | None = None
    timeout: int = 120
    copy_fixtures: bool = True


def mode_from_flags(metadata_process=False, credential_observation_wire=False, credential_staging_observation_wire=False, aws_permission_invocation_process=False):
    """Finite reviewed modes only; no target/tag/environment passthrough."""
    flags = (metadata_process, credential_observation_wire, credential_staging_observation_wire, aws_permission_invocation_process)
    if any(type(value) is not bool for value in flags) or sum(flags) > 1:
        raise ValueError("Choose at most one finite Go proof mode")
    if metadata_process:
        return UnitMode("metadata-process", "./internal/providers/localmetadata", "localmetadata_integration", "internal/providers/localmetadata", "TestOwnedTwoProcessCAS", copy_fixtures=False)
    if credential_observation_wire:
        return UnitMode("credential-observation-wire", "./internal/credentialobservation", "credentialobservation_integration", "internal/credentialobservation", "TestCandidateObservationLocalTLSWireBoundary", 90, copy_fixtures=False)
    if credential_staging_observation_wire:
        return UnitMode("credential-staging-observation-wire", "./internal/credentialstaging", "credentialobservation_integration", "internal/credentialstaging", "TestActorStagedObservationLocalTLSLifecycle", 90, copy_fixtures=False)
    if aws_permission_invocation_process:
        return UnitMode(AWS_MODE, AWS_TARGET, AWS_TAG, "testinfra/awspermission", AWS_TEST, 120, copy_fixtures=False)
    return UnitMode("default", "./...")


def marker_environment(mode, *, selected_test):
    # Never use this in the compiler environment or the independent /proof
    # container. This marker is scoped to the selected scratch test process.
    if mode.name in ("credential-observation-wire", "credential-staging-observation-wire") and selected_test:
        return ("REGISTRY_CREDENTIAL_OBSERVATION_TLS_TIER=owned-isolated",)
    if mode.name == AWS_MODE and selected_test:
        return ("REGISTRY_AWS_INVOCATION_PROCESS_TIER=owned-isolated",)
    return ()


def check_cases(output, mode):
    if mode.name == AWS_MODE:
        return invocation_process_output(output)[0]
    if "--- SKIP:" in output or "--- FAIL:" in output or "FAIL" in output.splitlines():
        raise RuntimeError("Selected Go contracts failed or skipped; refusing a partial proof")
    passes = [line for line in output.splitlines() if line.startswith("--- PASS:")]
    if not passes or "PASS" not in output.splitlines():
        raise RuntimeError("Selected Go test package reported no passing top-level contracts")
    if mode.test is not None and (len(passes) != 1 or not passes[0].startswith("--- PASS: " + mode.test + " (")):
        raise RuntimeError("Exact selected Go proof did not PASS once; refusing a partial proof")
    return len(passes)


def argument_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--docker-socket", default="/var/run/docker.sock")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--metadata-process", action="store_true", help="Compile only the opt-in localmetadata two-process CAS proof; execute its one exact test under the mandatory isolated boundary")
    mode.add_argument("--credential-observation-wire", action="store_true", help="Compile only the opt-in owned local candidate TLS proof; never execute this tag on the host")
    mode.add_argument("--credential-staging-observation-wire", action="store_true", help="Compile only the opt-in owned actor/read TLS lifecycle proof; never execute this tag on the host")
    mode.add_argument("--aws-permission-invocation-process", action="store_true", help="Compile only the owned real CLI synthetic FD3 proof plus exactly two fixed commands; never run this tag on host")
    return parser


def main():
    parser = argument_parser()
    args = parser.parse_args()
    mode = mode_from_flags(args.metadata_process, args.credential_observation_wire, args.credential_staging_observation_wire, args.aws_permission_invocation_process)
    docker, go = shutil.which("docker"), shutil.which("go")
    socket = Path(args.docker_socket).resolve()
    if not docker or not go or not socket.is_socket():
        parser.error("Existing Go and an explicit local Docker Unix socket required")
    run_id = "operator-unit-" + uuid.uuid4().hex[:12]
    image = "operator-local-unit:" + run_id
    with tempfile.TemporaryDirectory(prefix="operator-unit-") as directory:
        temporary = Path(directory).resolve()
        home = temporary / "home"
        home.mkdir()
        environment = {
            "PATH": os.defpath, "HOME": str(home), "DOCKER_CONFIG": str(home),
            "DOCKER_HOST": "unix://" + str(socket), "GOTOOLCHAIN": "local", "GOPROXY": "off", "GOSUMDB": "off",
            "CGO_ENABLED": "0", "GOOS": "linux", "GOCACHE": str(ROOT / ".venv/operator-go-cache"),
            "GOMODCACHE": str(ROOT / ".venv/operator-go-modules"),
        }

        def command(arguments, *, cwd=None, timeout=180):
            result = subprocess.run(list(map(str, arguments)), env=environment, cwd=cwd, capture_output=True, text=True, timeout=timeout, check=False)
            if result.returncode:
                print((result.stderr or result.stdout)[-14000:])
                raise RuntimeError("Isolated Go unit command failed: " + str(arguments[0]))
            return result.stdout

        environment["GOARCH"] = json.loads(command([docker, "version", "--format", "{{json .Server}} "]))["Arch"]
        bridge = json.loads(command([docker, "network", "inspect", "bridge"]))[0]
        gateway = bridge["IPAM"]["Config"][0]["Gateway"]
        compiler_directory = temporary / "compiler"
        compiler_directory.mkdir(mode=0o700)
        compiler = Compiler(str(socket), compiler_directory)
        compilation_failure = None
        invocation_binaries = None
        try:
            packages, proof, manifest = compiler.compile(mode.target, mode.tags)
            if mode.name == AWS_MODE:
                invocation_binaries = compile_invocation_commands(compiler, manifest, compiler_directory)
            print("Verified public compiler input manifest: sha256:" + manifest["digest"], flush=True)
        except BaseException as error:
            compilation_failure = error
            raise
        finally:
            try:
                compiler.close()
            except Exception as error:
                if compilation_failure is None:
                    raise
                compilation_failure.add_note(str(error))
                print(str(error), flush=True)
        if not packages:
            raise RuntimeError("No Go test packages selected")
        if mode.package is not None and (len(packages) != 1 or packages[0][1] != Path(mode.package)):
            raise RuntimeError("Finite proof mode selected an unexpected package")
        archive = temporary / "unit.tar"
        with tarfile.open(archive, "w") as tar:
            def add(path, name, executable=False):
                if path.is_symlink() or path.resolve() != path:
                    raise RuntimeError("Refusing a symlink in public test fixtures")
                data = path.read_bytes()
                info = tarfile.TarInfo(name)
                info.size, info.mode = len(data), 0o555 if executable else 0o444
                tar.addfile(info, io.BytesIO(data))

            add(proof, "proof", True)
            if invocation_binaries is not None:
                paths, metadata = invocation_binaries
                for name in ("run", "init"):
                    add(paths[name], "aws-readproof/" + name, True)
                raw = json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode()
                info = tarfile.TarInfo("aws-readproof/binaries.json")
                info.size, info.mode = len(raw), 0o444
                tar.addfile(info, io.BytesIO(raw))
                print("Frozen real CLI binaries " + json.dumps(metadata, sort_keys=True), flush=True)
            for _, relative, binary in packages:
                add(binary, "tests/" + binary.name, True)
                info = tarfile.TarInfo("workspace/operator-v2/" + str(relative))
                info.type, info.mode = tarfile.DIRTYPE, 0o555
                tar.addfile(info)
            # Fixtures share the immutable precompile snapshot. Reading live
            # repo files here would let later source edits change the evidence.
            records = {item["path"]: item for item in manifest["files"]}
            with tarfile.open(compiler_directory / "compiler-inputs.tar") as frozen:
                for name in manifest["fixtures"] if mode.copy_fixtures else ():
                    entry, record = frozen.getmember(name), records[name]
                    if not entry.isfile() or entry.size != record["bytes"]:
                        raise RuntimeError("Frozen public fixture metadata differs")
                    data = frozen.extractfile(entry).read()
                    if hashlib.sha256(data).hexdigest() != record["sha256"]:
                        raise RuntimeError("Frozen public fixture content differs")
                    info = tarfile.TarInfo(name)
                    info.size, info.mode = len(data), 0o444
                    tar.addfile(info, io.BytesIO(data))
        created = False
        containers = []
        failure = None
        try:
            command([docker, "image", "import", "--platform", "linux/" + environment["GOARCH"], "--change", "USER 65532:65532", "--change", f"LABEL {LABEL}={run_id}", archive, image])
            created = True
            def isolated(arguments, workdir="/", *, selected_test=False):
                name = run_id + "-" + str(len(containers))
                containers.append(name)
                marker = [item for value in marker_environment(mode, selected_test=selected_test) for item in ("--env", value)]
                arguments = [docker, "run", "--rm", "--name", name, "--network=none", "--read-only", "--cap-drop=ALL", "--security-opt", "no-new-privileges", "--tmpfs", "/tmp:rw,noexec,nosuid,size=64m", "--env", "HOME=/tmp/home", "--env", "OPERATOR_UNIT_HOST_GATEWAY=" + gateway, *marker, "--workdir", workdir, image, *arguments]
                if mode.name == AWS_MODE and selected_test:
                    return capture_invocation_process(arguments, environment)
                return command(arguments, timeout=150)
            print(isolated(["/proof"]).strip(), flush=True)
            total_cases = 0
            for package, relative, binary in packages:
                exact_test = ["-test.run=^" + mode.test + "$", "-test.count=1"] if mode.test is not None else []
                output = isolated(["/tests/" + binary.name, "-test.v", "-test.timeout=" + str(mode.timeout) + "s", *exact_test], "/workspace/operator-v2/" + str(relative), selected_test=True)
                try:
                    count = check_cases(output, mode)
                except RuntimeError:
                    if mode.name != AWS_MODE:
                        print(output)
                    raise
                if mode.name == AWS_MODE:
                    print("PASS finite AWS invocation process evidence " + json.dumps(invocation_process_output(output)[1], sort_keys=True), flush=True)
                print(f"PASS {package}: {count} top-level cases (container network=none)", flush=True)
                total_cases += count
            print(f"All {len(packages)} Go test packages / {total_cases} top-level cases passed without skips, host mounts or external networking.", flush=True)
            return 0
        except Exception as error:
            failure = error
            raise
        finally:
            cleanup_errors = []
            for name in containers:
                result = subprocess.run([docker, "inspect", "--type=container", name], env=environment, capture_output=True, text=True)
                if result.returncode == 0:
                    owned = json.loads(result.stdout)[0]
                    if owned["Config"].get("Labels", {}).get(LABEL) != run_id or owned["HostConfig"]["NetworkMode"] != "none":
                        cleanup_errors.append("Container ownership changed; refusing removal")
                    else:
                        result = subprocess.run([docker, "rm", "-f", name], env=environment, capture_output=True, text=True)
                        if result.returncode: cleanup_errors.append(result.stderr)
                elif "No such" not in result.stderr:
                    cleanup_errors.append(result.stderr)
            if created:
                owned = json.loads(command([docker, "image", "inspect", image]))[0]
                if owned["Config"].get("Labels", {}).get(LABEL) != run_id:
                    cleanup_errors.append("Image ownership changed; refusing removal")
                else:
                    result = subprocess.run([docker, "image", "rm", image], env=environment, capture_output=True, text=True)
                    if result.returncode: cleanup_errors.append(result.stderr)
            if cleanup_errors:
                detail = "Owned unit sandbox cleanup failed: " + "\n".join(cleanup_errors)
                if failure is None: raise RuntimeError(detail)
                failure.add_note(detail)


if __name__ == "__main__":
    raise SystemExit(main())
