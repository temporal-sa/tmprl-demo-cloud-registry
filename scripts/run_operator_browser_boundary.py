#!/usr/bin/env python3
"""Finite offline Chromium launch/sandbox proof. No cluster or page server.

Acquire separately with prepare_operator_browser_tests.py --acquire. Execution
never pulls/builds, runs npm, downloads browsers or accepts a URL/command. Exact
public driver and two reviewed scripts are copied into a NEVER-STARTED staging
container, committed, then verified by an unstarted full-image export. Only the
owned network=none execution container runs, with a native boundary probe before
driver import. Chromium sandbox failure is terminal; there is no weak fallback.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import ipaddress
import json
from pathlib import Path, PurePosixPath
import subprocess
import tarfile
import tempfile
import threading
import uuid

from operator_browser_contract import CACHE, Docker, LABEL, PUBLIC_FILES, ROOT, TMP_OPTIONS, browser_seccomp, check_container, check_output, driver_files

FORBIDDEN = tuple(PurePosixPath(home) / item for home in ("root", "home/pwuser") for item in (".aws", ".kube", ".ssh", ".docker", ".netrc", ".npmrc")) + tuple(map(PurePosixPath, ("run/secrets", "var/run/secrets", "run/docker.sock", "var/run/docker.sock")))
EMPTY_PUBLIC_DIRECTORY = PurePosixPath("root/.ssh")


def admitted_image_path(item):
    path = PurePosixPath(item.name.removeprefix("./"))
    if path.is_absolute() or ".." in path.parts:
        raise RuntimeError("Unexpected browser image path")
    # Exact immutable official base contains this one EMPTY directory. Mode,
    # owner and type were independently observed in a never-started audit.
    # Every descendant is still forbidden; only a COMPLETE export can prove
    # emptiness. No file/link/key or other credential prefix is exempted.
    if path == EMPTY_PUBLIC_DIRECTORY:
        if not item.isdir() or item.size != 0 or item.mode != 0o700 or item.uid != 0 or item.gid != 0:
            raise RuntimeError("Pinned empty public directory metadata differs")
        return path
    for prefix in FORBIDDEN:
        if path == prefix or prefix in path.parents:
            raise RuntimeError("Browser image contains credential/control prefix: " + str(prefix))
    return path


def regular_bytes(path, bound):
    if path.is_symlink() or path.resolve() != path or not path.is_file() or path.stat().st_size > bound:
        raise RuntimeError("Only canonical bounded regular public fixture files are admitted")
    data = path.read_bytes()
    if len(data) > bound:
        raise RuntimeError("Public source exceeded bound during read")
    return data


def export_proof(docker, container, expected):
    """Streaming full export, including absent credential/control path proof."""
    process = subprocess.Popen([docker.binary, "export", container], env=docker.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    deadline = threading.Timer(180, process.kill)
    deadline.daemon = True
    deadline.start()
    class Bounded:
        total = 0
        def read(self, size):
            data = process.stdout.read(size)
            self.total += len(data)
            if self.total > 3 * 1024**3:
                raise RuntimeError("Official browser filesystem exceeds reviewed export bound")
            return data
    found = set()
    try:
        with tarfile.open(fileobj=Bounded(), mode="r|") as archive:
            for index, item in enumerate(archive):
                if index >= 250000:
                    raise RuntimeError("Unexpected browser image path/count")
                path = admitted_image_path(item)
                if path == PurePosixPath("reviewed") or PurePosixPath("reviewed") in path.parents:
                    if item.isdir():
                        if item.mode & 0o777 != 0o555:
                            raise RuntimeError("Unexpected reviewed directory mode")
                        continue
                    name = str(path)
                    if name in found or name not in expected or not item.isfile() or item.mode & 0o777 != 0o444 or item.size != len(expected[name]):
                        raise RuntimeError("Unexpected staged public driver/source member")
                    source = archive.extractfile(item)
                    if source is None or source.read(item.size + 1) != expected[name]:
                        raise RuntimeError("Staged public driver/source content differs")
                    found.add(name)
        if process.wait(timeout=30) != 0 or found != set(expected):
            raise RuntimeError("Browser unstarted export proof incomplete")
    finally:
        deadline.cancel()
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=30)


def stage_tar(snapshot):
    stream = io.BytesIO()
    directories = set()
    for path in snapshot:
        directories.update(str(parent) for parent in PurePosixPath(path).parents if str(parent) != ".")
    with tarfile.open(fileobj=stream, mode="w") as archive:
        for name in sorted(directories, key=lambda name: (name.count("/"), name)):
            info = tarfile.TarInfo(name)
            info.type, info.mode = tarfile.DIRTYPE, 0o555
            archive.addfile(info)
        for name, data in sorted(snapshot.items()):
            info = tarfile.TarInfo(name)
            info.size, info.mode = len(data), 0o444
            archive.addfile(info, io.BytesIO(data))
    return stream.getvalue()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docker-socket", default="/var/run/docker.sock")
    args = parser.parse_args()
    run = "browser-boundary-" + uuid.uuid4().hex[:12]
    staging, execution, image = run + "-stage", run + "-proof", "operator-local-browser:" + run
    with tempfile.TemporaryDirectory(prefix="operator-browser-") as directory:
        temporary = Path(directory)
        docker = Docker(args.docker_socket, temporary)
        base = docker.verify_base()
        core = regular_bytes(CACHE / "playwright-core-1.56.0.tgz", 4 * 1024**2)
        seccomp = browser_seccomp(regular_bytes(CACHE / "seccomp.json", 65536))
        profile = temporary / "seccomp.json"
        profile.write_bytes(seccomp)
        profile.chmod(0o444)
        snapshot = {"reviewed/" + name: data for name, data in driver_files(core).items()}
        for relative in PUBLIC_FILES:
            snapshot["reviewed/" + relative] = regular_bytes(ROOT / relative, 65536)
        digest = hashlib.sha256(json.dumps({name: hashlib.sha256(data).hexdigest() for name, data in sorted(snapshot.items())}, sort_keys=True).encode()).hexdigest()
        gateway = json.loads(docker.command(["network", "inspect", "bridge"]))[0]["IPAM"]["Config"][0]["Gateway"]
        ipaddress.IPv4Address(gateway)
        image_id, primary = None, None
        stage_id = execution_id = None
        stage_attempted = execution_attempted = commit_attempted = False

        def owned_container(name, expected_id, expected_image):
            value = docker.inspect_optional("container", name)
            if value is not None and (value["Name"] != "/" + name or expected_id is not None and value["Id"] != expected_id or value["Image"] != expected_image or value["Config"].get("Labels", {}).get(LABEL) != run):
                raise RuntimeError("Owned browser container identity changed")
            return value

        try:
            stage_attempted = True
            stage_id = docker.command(["create", "--pull=never", "--network=none", "--name", staging, "--label", LABEL + "=" + run, "--entrypoint", "/usr/bin/true", base["Id"]]).strip()
            staged = owned_container(staging, stage_id, base["Id"])
            if staged["State"]["Status"] != "created":
                raise RuntimeError("Staging container must never execute")
            result = subprocess.run([docker.binary, "cp", "-", staging + ":/"], env=docker.env, input=stage_tar(snapshot), capture_output=True, timeout=120)
            if result.returncode:
                raise RuntimeError("Exact public browser staging copy failed")
            owned_container(staging, stage_id, base["Id"])
            commit_attempted = True
            image_id = docker.command(["commit", "--change", "USER 1001:1001", "--change", "LABEL " + LABEL + "=" + run, "--change", "LABEL operator.local/source-digest=" + digest, staging, image], timeout=180).strip()
            docker.command(["rm", staging])
            staged_image = docker.inspect_optional("image", image)
            if staged_image is None or staged_image["Id"] != image_id or staged_image["Architecture"] != docker.arch or staged_image["Os"] != "linux" or staged_image["Config"].get("Labels", {}).get(LABEL) != run:
                raise RuntimeError("Owned staged browser image identity differs")
            execution_attempted = True
            execution_id = docker.command(["create", "--pull=never", "--name", execution, "--label", LABEL + "=" + run,
                "--network=none", "--read-only", "--cap-drop=ALL", "--security-opt", "no-new-privileges", "--security-opt", "seccomp=" + str(profile),
                "--user", "1001:1001", "--ipc=private", "--shm-size=256m", "--tmpfs", "/tmp:" + TMP_OPTIONS,
                "--pids-limit=192", "--memory=1g", "--memory-swap=1g", "--cpus=2", "--env", "HOME=/tmp/home", "--env", "TMPDIR=/tmp", "--env", "XDG_CACHE_HOME=/tmp/cache",
                "--env", "OPERATOR_UNIT_HOST_GATEWAY=" + gateway, "--entrypoint", "/usr/bin/node", image_id, "/reviewed/scripts/operator_browser_boundary.mjs"]).strip()
            value = owned_container(execution, execution_id, image_id)
            check_container(value, run, image_id, seccomp)
            if value["State"]["Status"] != "created":
                raise RuntimeError("Browser executed before complete image proof")
            export_proof(docker, execution, snapshot)
            print("Immutable browser-only public snapshot sha256:" + digest + "; linux/" + docker.arch + "; " + str(len(snapshot)) + " exact regular files; derived seccomp sha256:" + hashlib.sha256(seccomp).hexdigest() + ". Starting network=none proof.", flush=True)
            result = subprocess.run([docker.binary, "start", "--attach", execution], env=docker.env, capture_output=True, text=True, timeout=90)
            if len(result.stdout) + len(result.stderr) > 65536:
                raise RuntimeError("Finite browser proof output exceeded bound")
            output = result.stdout
            completed = owned_container(execution, execution_id, image_id)
            check_container(completed, run, image_id, seccomp)
            if result.returncode or completed["State"]["Running"] or completed["State"]["ExitCode"] != 0 or completed["State"].get("OOMKilled"):
                # This tier contains only pinned public code + synthetic probes,
                # with no session/provider/private payloads to echo. Preserve
                # bounded evidence of the exact failed stage for review.
                print((result.stdout + result.stderr)[-6000:])
                raise RuntimeError("Finite browser proof failed/resource bound exceeded")
            check_output(output)
            print(output.strip())
            print("PASS exact5 browser-only cases; no cluster/page/provider proof.")
        except BaseException as error:
            primary = error
            raise
        finally:
            errors = []
            for name, identifier, expected_image, attempted in ((execution, execution_id, image_id, execution_attempted), (staging, stage_id, base["Id"], stage_attempted)):
                if not attempted:
                    continue
                try:
                    if owned_container(name, identifier, expected_image) is not None:
                        docker.command(["rm", "--force", name])
                        if docker.inspect_optional("container", name) is not None:
                            raise RuntimeError("Owned container removal unconfirmed")
                except Exception:
                    errors.append("owned browser container cleanup unconfirmed")
            if commit_attempted:
                try:
                    value = docker.inspect_optional("image", image)
                    if value is not None:
                        labels = value["Config"].get("Labels", {})
                        if image_id is not None and value["Id"] != image_id or labels.get(LABEL) != run or labels.get("operator.local/source-digest") != digest:
                            raise RuntimeError("Owned browser image identity changed")
                        docker.command(["image", "rm", image])
                        if docker.inspect_optional("image", image) is not None:
                            raise RuntimeError("Owned image removal unconfirmed")
                except Exception:
                    errors.append("owned browser image cleanup unconfirmed")
            if errors:
                message = "; ".join(errors)
                if primary is None:
                    raise RuntimeError(message)
                primary.add_note(message)
            else:
                print("Owned browser staging/execution containers and derived image cleanup verified.", flush=True)


if __name__ == "__main__":
    main()
