#!/usr/bin/env python3
"""Run reviewed Python stdlib build-helper tests in a sealed Linux container.

Acquire first: python3 scripts/prepare_operator_python_tests.py
Execute: python3 scripts/run_operator_python_tests.py

Only the two explicit public helper/test files and trusted proof harness are
copied into an owned image. No Dockerfile/build instruction or test is executed
while staging. Execution has no host mounts, Docker socket, inherited provider
credentials or network; native process creation/exec is disabled before tests.
Linux amd64/arm64 Docker only, with fail-closed boundary probes. No PyPI installs.
"""
from __future__ import annotations

import argparse
import io
import ipaddress
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import uuid

from prepare_operator_python_tests import PYTHON_IMAGE

ROOT = Path(__file__).resolve().parents[1]
LABEL = "operator.local/run-id"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--docker-socket", default="/var/run/docker.sock")
    parser.add_argument("--self-check", action="store_true", help="First prove skipped tests and caught extra I/O attempts fail the entire isolated run")
    args = parser.parse_args()
    docker = shutil.which("docker")
    socket = Path(args.docker_socket).resolve()
    if not docker or not socket.is_socket():
        parser.error("Existing Docker CLI and explicit local Unix socket required")
    run_id = "operator-python-" + uuid.uuid4().hex[:12]
    image = "operator-local-python:" + run_id
    with tempfile.TemporaryDirectory(prefix="operator-python-") as directory:
        environment = {"PATH": os.defpath, "HOME": directory, "DOCKER_CONFIG": directory,
                       "DOCKER_HOST": "unix://" + str(socket)}

        def command(arguments, *, data=None, timeout=120, expected_failure=None):
            result = subprocess.run(list(map(str, arguments)), env=environment, input=data,
                                    capture_output=True, timeout=timeout, check=False)
            if expected_failure is not None:
                output = (result.stdout + result.stderr).decode(errors="replace")
                if result.returncode == 0 or expected_failure not in output:
                    print(output[-14000:])
                    raise RuntimeError("Expected isolated negative control did not fail precisely")
                return "Negative control rejected: " + expected_failure
            if result.returncode:
                print((result.stdout + result.stderr).decode(errors="replace")[-14000:])
                raise RuntimeError("Isolated Python command failed: " + str(arguments[0]))
            return result.stdout.decode()

        engine = json.loads(command([docker, "version", "--format", "{{json .Server}} "]))
        base = json.loads(command([docker, "image", "inspect", PYTHON_IMAGE]))[0]
        if (engine.get("Os") != "linux" or engine.get("Arch") not in {"amd64", "arm64"}
                or base.get("Os") != "linux" or base.get("Architecture") != engine["Arch"] or base["Config"].get("Volumes")):
            parser.error("Native Linux amd64/arm64 Docker and matching pinned image without implicit volumes required; emulation is not supported")
        digest = PYTHON_IMAGE.split("@", 1)[1]
        if not any(value.endswith("@" + digest) for value in base.get("RepoDigests", [])):
            parser.error("Pinned Python digest not acquired; run prepare_operator_python_tests.py")
        bridge = json.loads(command([docker, "network", "inspect", "bridge"]))[0]
        gateway = str(ipaddress.IPv4Address(bridge["IPAM"]["Config"][0]["Gateway"]))
        archive = io.BytesIO()
        with tarfile.open(fileobj=archive, mode="w") as tar:
            for name in ("operator-tests", "operator-tests/buildspec"):
                entry = tarfile.TarInfo(name)
                entry.type, entry.mode = tarfile.DIRTYPE, 0o555
                tar.addfile(entry)
            assets = {
                "operator-tests/harness.py": ROOT / "scripts/operator_python_test_harness.py",
                "operator-tests/buildspec/runner.py": ROOT / "operator-v2/internal/buildspec/runner.py",
                "operator-tests/buildspec/runner_test.py": ROOT / "operator-v2/internal/buildspec/runner_test.py",
            }
            for name, source in assets.items():
                if source.resolve() != source or not source.is_file():
                    raise RuntimeError("Only explicit regular public test assets may be staged")
                contents = source.read_bytes()
                entry = tarfile.TarInfo(name)
                entry.size, entry.mode = len(contents), 0o444
                tar.addfile(entry, io.BytesIO(contents))
            contents = b"synthetic immutable fixture; no user data\n"
            entry = tarfile.TarInfo("operator-tests/immutable.txt")
            entry.size, entry.mode = len(contents), 0o444
            tar.addfile(entry, io.BytesIO(contents))
        containers = []
        image_attempted = False
        failure = None
        try:
            # Stopped staging only: docker cp/commit do not execute image code.
            staging = run_id + "-staging"
            containers.append((staging, base["Id"]))
            command([docker, "create", "--pull=never", "--name", staging, "--label", f"{LABEL}={run_id}",
                     "--network=none", "--entrypoint", "/usr/local/bin/python3", PYTHON_IMAGE])
            command([docker, "cp", "-", staging + ":/"], data=archive.getvalue())
            image_attempted = True
            command([docker, "commit", "--change", f"LABEL {LABEL}={run_id}",
                     "--change", "USER 65532:65532", staging, image])
            owned_image = json.loads(command([docker, "image", "inspect", image]))[0]
            if owned_image["Config"].get("Labels", {}).get(LABEL) != run_id:
                raise RuntimeError("Generated image ownership not verified")
            cases = []
            if args.self_check:
                cases.extend([("skipped", "stdlib helper contracts failed, skipped or empty"),
                              ("caught-network", "unexpected caught process/network attempt")])
            cases.append((None, None))
            for index, (negative, expected_failure) in enumerate(cases):
                execution = run_id + "-tests-" + str(index)
                containers.append((execution, owned_image["Id"]))
                command([docker, "create", "--pull=never", "--name", execution, "--label", f"{LABEL}={run_id}",
                         "--network=none", "--read-only", "--user", "65532:65532", "--cap-drop=ALL",
                         "--security-opt", "no-new-privileges", "--pids-limit", "8", "--memory", "256m",
                         "--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=64m,mode=1777",
                         "--env", "HOME=/tmp/home", "--env", "TMPDIR=/tmp",
                         "--env", "OPERATOR_PYTHON_HOST_GATEWAY=" + gateway,
                         "--entrypoint", "/usr/local/bin/python3", image, "-I", "-B", "-S", "/operator-tests/harness.py",
                         *(["--negative=" + negative] if negative else [])])
                sandbox = json.loads(command([docker, "inspect", "--type=container", execution]))[0]
                host = sandbox["HostConfig"]
                if (sandbox["Image"] != owned_image["Id"] or host["NetworkMode"] != "none" or not host["ReadonlyRootfs"]
                        or host.get("Binds") or host.get("Privileged") or "ALL" not in host.get("CapDrop", [])
                        or "no-new-privileges" not in host.get("SecurityOpt", [])
                        or any(mount["Type"] != "tmpfs" or mount["Destination"] != "/tmp" for mount in sandbox.get("Mounts", []))
                        or set(host.get("Tmpfs", {})) != {"/tmp"}):
                    raise RuntimeError("Refusing to start an unproven test container boundary")
                print(command([docker, "start", "--attach", execution], timeout=120, expected_failure=expected_failure).strip(), flush=True)
            return 0
        except Exception as error:
            failure = error
            raise
        finally:
            errors = []

            def inspect(arguments):
                result = subprocess.run([docker, *arguments], env=environment, capture_output=True, text=True, timeout=30)
                if result.returncode:
                    if "No such" not in result.stderr:
                        errors.append(result.stderr[-1500:])
                    return None
                return json.loads(result.stdout)[0]

            def remove(arguments):
                result = subprocess.run([docker, *arguments], env=environment, capture_output=True, text=True, timeout=60)
                if result.returncode:
                    errors.append(result.stderr[-1500:])

            for name, expected_image in reversed(containers):
                try:
                    owned = inspect(["inspect", "--type=container", name])
                    if owned is not None:
                        if (owned["Config"].get("Labels", {}).get(LABEL) != run_id or owned["Image"] != expected_image
                                or owned["HostConfig"]["NetworkMode"] != "none" or owned["HostConfig"].get("Binds")):
                            errors.append("Container ownership changed; refusing removal")
                        else:
                            remove(["rm", "--force", owned["Id"]])
                except Exception as error:
                    errors.append(str(error))
            if image_attempted:
                try:
                    owned = inspect(["image", "inspect", image])
                    if owned is not None:
                        if owned["Config"].get("Labels", {}).get(LABEL) != run_id:
                            errors.append("Image ownership changed; refusing removal")
                        else:
                            remove(["image", "rm", image])
                except Exception as error:
                    errors.append(str(error))
            if errors:
                detail = "Owned Python sandbox cleanup failed: " + "\n".join(errors)
                if failure is None:
                    raise RuntimeError(detail)
                failure.add_note(detail)
                print(detail)
            else:
                print("Owned Python staging/test containers and generated image cleaned; public base retained.", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
