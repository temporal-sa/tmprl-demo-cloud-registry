#!/usr/bin/env python3
"""Execute reviewed diagnostics contracts with the preacquired pinned Node image.

No acquisition command or mutable-tag fallback is issued by this runner. Docker
BuildKit can still resolve immutable FROM metadata through daemon registry
access; --network=none on build does NOT isolate that metadata lookup. Only the
started test container has the proved kernel network boundary. A no-RUN
Dockerfile copies exactly fourteen public
files into a derived owned image. Before starting, export inspection verifies
their exact bytes and credential/control-path absence. Then native Node proves
the kernel/environment boundary and runs the pure contracts in that SAME
network=none, read-only, nonroot, empty-capability container. No host mounts or
Docker socket inside; no real browser, diagnostic API/session or provider proof.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import ipaddress
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import tarfile
import tempfile
import threading
import uuid

from operator_node_test_contract import Docker, EXPECTED_CASES, PUBLIC_FILES, ROOT

LABEL = "operator.local/node-test-run"
FORBIDDEN = tuple(PurePosixPath(home) / item for home in ("root", "home/node") for item in (".aws", ".kube", ".ssh", ".docker", ".netrc", ".npmrc")) + tuple(map(PurePosixPath, ("run/secrets", "var/run/secrets", "run/docker.sock", "var/run/docker.sock")))


def export_proof(docker: Docker, name: str, expected: dict[str, bytes]):
    process = subprocess.Popen([docker.binary, "export", name], env=docker.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    def stop():
        if process.poll() is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
    deadline = threading.Timer(60, stop)
    deadline.daemon = True
    deadline.start()
    class BoundedExport:
        total = 0
        def read(self, size):
            data = process.stdout.read(size)
            self.total += len(data)
            if self.total > 512 * 1024 * 1024:
                raise RuntimeError("Public Node image export exceeds its bound")
            return data
    found = set()
    try:
        with tarfile.open(fileobj=BoundedExport(), mode="r|") as archive:
            for index, member in enumerate(archive):
                if index > 100000:
                    raise RuntimeError("Public Node image member count exceeds its bound")
                path = PurePosixPath(member.name.removeprefix("./"))
                if path.is_absolute() or ".." in path.parts:
                    raise RuntimeError("Unexpected unsafe image member path")
                if any(path == prefix or prefix in path.parents for prefix in FORBIDDEN):
                    raise RuntimeError("Credential/control path in the test image; refusing execution")
                if path == PurePosixPath("reviewed") or PurePosixPath("reviewed") in path.parents:
                    if member.isdir():
                        continue
                    key = str(path)
                    if key not in expected or not member.isfile() or member.mode & 0o777 != 0o444 or member.size != len(expected[key]):
                        raise RuntimeError("Unexpected reviewed image content/type/mode")
                    source = archive.extractfile(member)
                    if source is None or source.read(member.size + 1) != expected[key]:
                        raise RuntimeError("Reviewed image content differs from the frozen public source")
                    found.add(key)
        if process.wait(timeout=30) != 0 or found != set(expected):
            raise RuntimeError("Unstarted image export proof incomplete")
    finally:
        deadline.cancel()
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=30)


def check_container(value, run_id, image_id):
    config, host = value["Config"], value["HostConfig"]
    if config.get("Labels", {}).get(LABEL) != run_id or value["Image"] != image_id or config.get("User") != "65532:65532" or host.get("NetworkMode") != "none" or not host.get("ReadonlyRootfs") or host.get("Binds") or host.get("Privileged") or host.get("Devices") or host.get("CapAdd") or set(host.get("CapDrop") or []) != {"ALL"} or "no-new-privileges" not in host.get("SecurityOpt", []) or host.get("PidMode") == "host" or host.get("IpcMode") == "host":
        raise RuntimeError("Owned Node container boundary configuration mismatch")
    if set(host.get("Tmpfs") or {}) != {"/tmp"} or any(mount.get("Type") != "tmpfs" or mount.get("Destination") != "/tmp" for mount in value.get("Mounts", [])):
        raise RuntimeError("Unexpected mount in Node container")
    keys = {item.split("=", 1)[0] for item in config.get("Env", [])}
    if keys - {"PATH", "NODE_VERSION", "YARN_VERSION", "HOME", "TZ", "OPERATOR_UNIT_HOST_GATEWAY"}:
        raise RuntimeError("Unexpected environment in Node test container")


def check_cases(output):
    names = re.findall(r"^ok [0-9]+ - (.+)$", output, flags=re.MULTILINE)
    if names != list(EXPECTED_CASES) or re.search(r"(?:^not ok|^(?:ok|not ok)[^\n]*#\s*(?:SKIP|TODO)\b)", output, re.MULTILINE | re.IGNORECASE):
        raise RuntimeError("Node case allowlist/order or skip/failure proof mismatch")
    for label, value in (("tests", len(EXPECTED_CASES)), ("pass", len(EXPECTED_CASES)), ("fail", 0), ("cancelled", 0), ("skipped", 0), ("todo", 0)):
        if not re.search(r"^# " + label + r" " + str(value) + r"$", output, re.MULTILINE):
            raise RuntimeError("Node aggregate contract proof incomplete")
    if "Verified Node boundary and diagnostic contracts completed in the same container." not in output:
        raise RuntimeError("Native same-container boundary proof missing")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--docker-socket", default="/var/run/docker.sock")
    args = parser.parse_args()
    run_id = "node-contract-" + uuid.uuid4().hex[:12]
    image = "operator-local-node:" + run_id
    container = run_id
    with tempfile.TemporaryDirectory(prefix="operator-node-") as directory:
        temporary = Path(directory)
        home = temporary / "home"
        home.mkdir()
        docker = Docker(args.docker_socket, home)
        docker.verify_base()  # No pull/fallback: acquisition has its own entrypoint.
        snapshot = {}
        for relative in PUBLIC_FILES:
            path = ROOT / relative
            if path.is_symlink() or path.resolve() != path or not path.is_file():
                raise RuntimeError("Only canonical regular public fixture files are allowed")
            data = path.read_bytes()
            if len(data) > 128 * 1024:
                raise RuntimeError("Public test asset exceeds its explicit bound")
            snapshot["reviewed/" + relative] = data
        source_digest = hashlib.sha256(json.dumps({name: hashlib.sha256(data).hexdigest() for name, data in snapshot.items()}, sort_keys=True).encode()).hexdigest()
        context = temporary / "context.tar"
        dockerfile = ("FROM " + docker.base + "\nCOPY assets/ /reviewed/\nLABEL " + LABEL + "=" + run_id + "\n").encode()
        with tarfile.open(context, "w") as archive:
            info = tarfile.TarInfo("Dockerfile")
            info.size, info.mode = len(dockerfile), 0o444
            archive.addfile(info, io.BytesIO(dockerfile))
            for name, data in snapshot.items():
                info = tarfile.TarInfo("assets/" + name.removeprefix("reviewed/"))
                info.size, info.mode = len(data), 0o444
                archive.addfile(info, io.BytesIO(data))
        gateway = json.loads(docker.command(["network", "inspect", "bridge"]))[0]["IPAM"]["Config"][0]["Gateway"]
        ipaddress.IPv4Address(gateway)
        build_attempted = create_attempted = False
        primary = None
        try:
            build_attempted = True
            with context.open("rb") as source:
                result = subprocess.run([docker.binary, "build", "--pull=false", "--network=none", "--platform", "linux/" + docker.arch, "--tag", image, "-"], env=docker.env, stdin=source, capture_output=True, timeout=180, check=False)
            if result.returncode:
                raise RuntimeError("No-RUN offline Node image staging failed")
            staged = json.loads(docker.command(["image", "inspect", image]))[0]
            if staged["Config"].get("Labels", {}).get(LABEL) != run_id or staged["Os"] != "linux" or staged["Architecture"] != docker.arch:
                raise RuntimeError("Staged Node image identity mismatch")
            create_attempted = True
            docker.command(["create", "--pull=never", "--name", container, "--label", f"{LABEL}={run_id}", "--network=none", "--read-only", "--cap-drop=ALL", "--security-opt", "no-new-privileges", "--user", "65532:65532", "--pids-limit", "64", "--memory", "256m", "--cpus", "1", "--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=32m", "--env", "HOME=/tmp/home", "--env", "TZ=UTC", "--env", "OPERATOR_UNIT_HOST_GATEWAY=" + gateway, "--entrypoint", "/usr/local/bin/node", staged["Id"], "/reviewed/scripts/operator_node_boundary.mjs"])
            owned = json.loads(docker.command(["inspect", "--type=container", container]))[0]
            check_container(owned, run_id, staged["Id"])
            if owned["State"]["Status"] != "created":
                raise RuntimeError("Container executed before content verification")
            export_proof(docker, container, snapshot)
            output = docker.command(["start", "--attach", container], timeout=60)
            completed = json.loads(docker.command(["inspect", "--type=container", container]))[0]
            check_container(completed, run_id, staged["Id"])
            if completed["State"]["Running"] or completed["State"]["ExitCode"] != 0 or completed["State"].get("OOMKilled"):
                raise RuntimeError("Owned Node tests failed or exceeded boundary resources")
            try:
                check_cases(output)
            except RuntimeError:
                # Only reviewed public fixtures run here, after image/env/mount
                # proof. Keep their bounded test output reviewable on failure.
                print(output[-8000:])
                raise
            for relative in PUBLIC_FILES:
                if (ROOT / relative).read_bytes() != snapshot["reviewed/" + relative]:
                    raise RuntimeError("Public diagnostic source changed during test snapshot")
            print(output.strip())
            print(f"PASS {len(EXPECTED_CASES)} reviewed Node contracts; linux/{docker.arch}; public assets sha256:{source_digest}; native kernel proof and exact image contents verified.")
        except BaseException as error:
            primary = error
            raise
        finally:
            cleanup = []
            if create_attempted:
                try:
                    owned = docker.inspect_optional("container", container)
                    if owned is not None:
                        if owned["Config"].get("Labels", {}).get(LABEL) != run_id:
                            raise RuntimeError("Container ownership changed; refusing removal")
                        docker.command(["rm", "--force", container])
                        if docker.inspect_optional("container", container) is not None:
                            raise RuntimeError("Owned container removal unconfirmed")
                except Exception:
                    cleanup.append("owned Node container cleanup could not be verified")
            if build_attempted:
                try:
                    owned = docker.inspect_optional("image", image)
                    if owned is not None:
                        if owned["Config"].get("Labels", {}).get(LABEL) != run_id:
                            raise RuntimeError("Image ownership changed; refusing removal")
                        docker.command(["image", "rm", image])
                        if docker.inspect_optional("image", image) is not None:
                            raise RuntimeError("Owned image removal unconfirmed")
                except Exception:
                    cleanup.append("owned Node image cleanup could not be verified")
            if cleanup:
                message = "; ".join(cleanup)
                if primary is None:
                    raise RuntimeError(message)
                primary.add_note(message)


if __name__ == "__main__":
    main()
