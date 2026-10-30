#!/usr/bin/env python3
"""Acquire one pinned official native Linux Go compiler; never compile project code.

Acquisition: python3 scripts/prepare_operator_go_compiler.py --acquire
Metadata only by default. Compilation/execution is separately network denied.
Pins: Docker official golang1.27.3-bookworm native manifests/configurations.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import uuid

VERSION = "go1.27.3"
PINS = {
    "arm64": {"manifest": "ae675cdbf1bb782b6de4a56b157c8f91a5c89a594d8fa6475bb71efc4be3cfcb", "config": "dba766c2797d15d2b6e35651bd5acb02764b27611e9b89bba1100a54a0167bf7"},
    "amd64": {"manifest": "e482b31b5d30c31f9e495f4b8cbdf41429b2d74955dfbcd9c0a904483d738390", "config": "f5d48661d11d8c3f1e8a3b477f2865c72b4ff7651a7c593d49f4b77b2fc1a804"},
}


def reference(architecture):
    if architecture not in PINS:
        raise RuntimeError("Only native Linux amd64/arm64 Go compiler images are supported")
    return "docker.io/library/golang@sha256:" + PINS[architecture]["manifest"]


def verify_image(docker, environment, architecture):
    result = subprocess.run([docker, "image", "inspect", reference(architecture)], env=environment,
                            capture_output=True, text=True, check=True, timeout=30)
    value = json.loads(result.stdout)[0]
    manifests = {prefix + "@sha256:" + PINS[architecture]["manifest"] for prefix in ("docker.io/library/golang", "library/golang", "golang")}
    if value["Id"] != "sha256:" + PINS[architecture]["config"] or value["Os"] != "linux" or value["Architecture"] != architecture or not manifests.intersection(value.get("RepoDigests", [])) or "GOLANG_VERSION=1.27.3" not in value["Config"].get("Env", []) or value["Config"].get("OnBuild") or value["Config"].get("Volumes"):
        raise RuntimeError("Pinned compiler manifest/config/platform/version metadata differs")
    return value["Id"]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--docker-socket", default="/var/run/docker.sock")
    parser.add_argument("--acquire", action="store_true", help="Explicit public official-image acquisition only; no project compilation/tests")
    args = parser.parse_args()
    docker = shutil.which("docker")
    socket = Path(args.docker_socket).resolve()
    if not docker or not socket.is_socket():
        parser.error("Existing explicit local Docker Unix socket/CLI required")
    with tempfile.TemporaryDirectory(prefix="operator-go-compiler-acquire-") as directory:
        environment = {"PATH": os.defpath, "HOME": directory, "DOCKER_CONFIG": directory, "DOCKER_HOST": "unix://" + str(socket)}
        version = subprocess.run([docker, "version", "--format", "{{json .Server}}"], env=environment, capture_output=True, text=True, check=True, timeout=30)
        architecture = json.loads(version.stdout)["Arch"]
        image = reference(architecture)
        print("Pinned native compiler: " + image, flush=True)
        if not args.acquire:
            print("No acquisition requested; use --acquire separately from isolated compilation.")
            return
        subprocess.run([docker, "pull", "--platform", "linux/" + architecture, image], env=environment, check=True, timeout=600)
        verify_image(docker, environment, architecture)
        # This trusted image-only version probe executes no repository code,
        # receives no host files/socket/credentials and cannot use networking.
        run_id = "operator-go-version-" + uuid.uuid4().hex[:12]
        try:
            result = subprocess.run([docker, "run", "--rm", "--name", run_id, "--label", "operator.local/run-id=" + run_id, "--network=none", "--read-only", "--user", "65532:65532", "--cap-drop=ALL", "--security-opt", "no-new-privileges", "--env", "HOME=/nonexistent", "--entrypoint", "/usr/local/go/bin/go", image, "version"], env=environment, capture_output=True, text=True, check=True, timeout=30)
            if result.stdout.strip() != "go version " + VERSION + " linux/" + architecture:
                raise RuntimeError("Native pinned compiler executable version differs")
        finally:
            inspect = subprocess.run([docker, "inspect", "--type=container", run_id], env=environment, capture_output=True, text=True, timeout=30)
            if inspect.returncode == 0:
                owned = json.loads(inspect.stdout)[0]
                if owned["Name"] != "/" + run_id or owned["Config"].get("Labels", {}).get("operator.local/run-id") != run_id or owned["Image"] != "sha256:" + PINS[architecture]["config"] or owned["HostConfig"]["NetworkMode"] != "none":
                    raise RuntimeError("Refusing compiler version-probe cleanup: ownership changed")
                subprocess.run([docker, "rm", "-f", run_id], env=environment, capture_output=True, text=True, check=True, timeout=30)
            elif "No such" not in inspect.stderr:
                raise RuntimeError("Cannot establish owned compiler version-probe cleanup")
        print("Verified official native Go1.27.3 manifest/config/executable; no project code compiled.")


if __name__ == "__main__":
    main()
