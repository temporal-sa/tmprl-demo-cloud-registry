#!/usr/bin/env python3
"""Acquire pinned public kind/node tools separately from isolated execution."""
import argparse
import hashlib
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
VERSION = "v0.33.0"
NODE = "kindest/node:v1.37.0@sha256:a1ed56cfb0e7b93589bdf97c8cd566405a265939e3620fc4f5de89adff580ae5"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docker-socket", default="/var/run/docker.sock")
    parser.add_argument("--go-only", action="store_true", help="Acquire only pinned Go module dependencies")
    args = parser.parse_args()
    go = shutil.which("go")
    if not go:
        parser.error("Existing Go toolchain required")
    with tempfile.TemporaryDirectory(prefix="operator-go-acquire-") as directory:
        target = Path(directory)
        for name in ("go.mod", "go.sum"):
            source = ROOT / "operator-v2" / name
            if source.exists():
                shutil.copyfile(source, target / name)
        environment = {
            "PATH": os.defpath, "HOME": directory, "GOTOOLCHAIN": "local",
            "GOMODCACHE": str(ROOT / ".venv/operator-go-modules"),
            "GOCACHE": str(ROOT / ".venv/operator-go-cache"),
            "GOPROXY": "https://proxy.golang.org", "GOSUMDB": "sum.golang.org",
        }
        subprocess.run([go, "mod", "download"], cwd=target, env=environment, check=True)
    if args.go_only:
        print("Public Go module acquisition complete; no tests executed.")
        return
    system = {"Darwin": "darwin", "Linux": "linux"}.get(platform.system())
    architecture = {"arm64": "arm64", "aarch64": "arm64", "x86_64": "amd64"}.get(platform.machine())
    if not system or not architecture:
        parser.error("Only native macOS/Linux amd64/arm64 tools are supported")
    tools = ROOT / ".venv/tools"
    tools.mkdir(parents=True, exist_ok=True)
    asset = f"kind-{system}-{architecture}"
    url = f"https://github.com/kubernetes-sigs/kind/releases/download/{VERSION}/{asset}"
    binary = urllib.request.urlopen(url, timeout=60).read()
    expected = urllib.request.urlopen(url + ".sha256sum", timeout=60).read().decode().split()[0]
    if hashlib.sha256(binary).hexdigest() != expected:
        raise RuntimeError("Public kind checksum verification failed")
    (tools / "kind").write_bytes(binary)
    (tools / "kind").chmod(0o755)
    docker = shutil.which("docker")
    if not docker:
        parser.error("Existing local Docker engine and CLI required")
    with tempfile.TemporaryDirectory(prefix="operator-tools-") as directory:
        environment = {"PATH": os.defpath, "HOME": directory, "DOCKER_CONFIG": directory}
        subprocess.run([docker, "--host", "unix://" + str(Path(args.docker_socket).resolve()), "pull", NODE], env=environment, check=True)
    print("Pinned kind checksum verified and node image acquired; no tests executed.")


if __name__ == "__main__":
    main()
