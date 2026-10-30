#!/usr/bin/env python3
"""Run the opt-in owned wrapper exec helper in a no-network scratch container.

This separate tier permits native process replacement. Default wrapper tests
use fake executors and do not spawn. Uses existing offline Go modules and an
explicit local Docker Unix socket; there are no host mounts/cloud credentials
inside the container, no AWS calls, and no authored build commands.
"""
from __future__ import annotations

import argparse
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import uuid

ROOT = Path(__file__).resolve().parents[1]
LABEL = "operator.local/wrapper-run-id"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docker-socket", default="/var/run/docker.sock")
    args = parser.parse_args()
    docker, go = shutil.which("docker"), shutil.which("go")
    socket = Path(args.docker_socket).resolve(strict=True)
    if not docker or not go or not socket.is_socket():
        parser.error("Existing Go and explicit local Docker Unix socket required")
    run_id = "wrapper-native-" + uuid.uuid4().hex[:12]
    image = "operator-local-wrapper:" + run_id
    with tempfile.TemporaryDirectory(prefix="wrapper-native-") as directory:
        temporary = Path(directory)
        home = temporary / "home"
        home.mkdir()
        env = {"PATH": os.defpath, "HOME": str(home), "DOCKER_CONFIG": str(home),
               "DOCKER_HOST": "unix://" + str(socket), "GOTOOLCHAIN": "local", "GOENV": "off", "GOWORK": "off",
               "GOPROXY": "off", "GOSUMDB": "off", "GOTELEMETRY": "off", "CGO_ENABLED": "0", "GOOS": "linux",
               "GOCACHE": str(ROOT / ".venv/operator-go-cache"), "GOMODCACHE": str(ROOT / ".venv/operator-go-modules")}

        def command(argv, *, cwd=None, timeout=180):
            result = subprocess.run(list(map(str, argv)), cwd=cwd, env=env, capture_output=True, text=True, timeout=timeout, check=False)
            if result.returncode:
                raise RuntimeError("Owned wrapper harness command failed: " + str(argv[0]) + "\n" + (result.stderr or result.stdout)[-8000:])
            return result.stdout

        arch = json.loads(command([docker, "version", "--format", "{{json .Server}} "]))["Arch"]
        if arch not in ("amd64", "arm64"):
            raise RuntimeError("Unsupported local daemon architecture")
        env["GOARCH"] = arch
        gateway = json.loads(command([docker, "network", "inspect", "bridge"]))[0]["IPAM"]["Config"][0]["Gateway"]
        binary, proof, wrapper = temporary / "wrapper-tests", temporary / "proof", temporary / "registry-lambda-wrapper"
        command([go, "test", "-mod=readonly", "-c", "-tags=wrapper_integration", "-o", binary, "./internal/lambdawrapper"], cwd=ROOT / "operator-v2")
        command([go, "build", "-mod=readonly", "-o", proof, ROOT / "scripts/operator_unit_boundary.go"], cwd=ROOT / "operator-v2")
        command([go, "build", "-mod=readonly", "-o", wrapper, "./cmd/registry-lambda-wrapper"], cwd=ROOT / "operator-v2")
        archive = temporary / "image.tar"
        with tarfile.open(archive, "w") as tar:
            for path, name in ((binary, "wrapper-tests"), (proof, "proof"), (wrapper,"registry-lambda-wrapper")):
                data = path.read_bytes()
                info = tarfile.TarInfo(name)
                info.size, info.mode = len(data), 0o555
                tar.addfile(info, io.BytesIO(data))
        created = False
        containers = []
        primary = None
        try:
            command([docker, "image", "import", "--platform", "linux/" + arch, "--change", "USER 65532:65532", "--change", f"LABEL {LABEL}={run_id}", archive, image])
            created = True
            for index, argv in enumerate((["/proof"], ["/wrapper-tests", "-test.v", "-test.timeout=30s", "-test.run=^TestNative(ProcessHandoff|BinaryBootstrapFD|BootstrapStalledWriterAndWrongDescriptor)$"])):
                name = run_id + "-" + str(index)
                containers.append(name)
                output = command([docker, "run", "--rm", "--name", name, "--network=none", "--read-only", "--cap-drop=ALL", "--security-opt", "no-new-privileges", "--tmpfs", "/tmp:rw,noexec,nosuid,size=32m", "--env", "WRAPPER_NATIVE_BINARY=/registry-lambda-wrapper", "--env", "OPERATOR_UNIT_HOST_GATEWAY=" + gateway, image, *argv], timeout=60)
                if index == 1 and ("--- PASS: TestNativeProcessHandoff" not in output or "--- PASS: TestNativeBinaryBootstrapFD" not in output or "--- PASS: TestNativeBootstrapStalledWriterAndWrongDescriptor" not in output or "--- SKIP:" in output):
                    raise RuntimeError("Native wrapper proof did not complete")
                print(output.strip(), flush=True)
            print("Owned native exec handoff passed in network=none with no host mounts or cloud credentials.")
        except BaseException as error:
            primary = error
            raise
        finally:
            cleanup_errors = []
            for name in containers:
                result = subprocess.run([docker, "inspect", "--type=container", name], env=env, capture_output=True, text=True)
                if result.returncode == 0:
                    owned = json.loads(result.stdout)[0]
                    if owned["Config"].get("Labels", {}).get(LABEL) != run_id or owned["HostConfig"]["NetworkMode"] != "none":
                        cleanup_errors.append("Container ownership changed; refusing removal")
                    else:
                        result = subprocess.run([docker, "rm", "-f", name], env=env, capture_output=True, text=True)
                        if result.returncode:
                            cleanup_errors.append("Owned wrapper container removal failed")
                elif "No such" not in result.stderr:
                    cleanup_errors.append("Cannot verify owned wrapper container cleanup")
            if created:
                owned = json.loads(command([docker, "image", "inspect", image]))[0]
                if owned["Config"].get("Labels", {}).get(LABEL) != run_id:
                    cleanup_errors.append("Image ownership changed; refusing removal")
                elif subprocess.run([docker, "image", "rm", image], env=env, capture_output=True).returncode:
                    cleanup_errors.append("Owned wrapper image removal failed")
            if cleanup_errors:
                detail = "; ".join(cleanup_errors)
                if primary is None:
                    raise RuntimeError(detail)
                primary.add_note(detail)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
