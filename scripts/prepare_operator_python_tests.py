#!/usr/bin/env python3
"""Acquire the pinned public Docker Official Python image; execute no tests.

Run separately before scripts/run_operator_python_tests.py. The anonymous Docker
client gets an empty temporary HOME/config and an explicit local Unix socket.
The human-readable version tag is documentation; only the digest is acquired.
"""
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

# Docker Official python:3.15.2-slim-bookworm index, resolved anonymously from
# docker.io/library/python on 2026-10-30. Includes Linux amd64 and arm64 images.
PYTHON_IMAGE = "docker.io/library/python:3.15.2-slim-bookworm@sha256:4e03260adeeebafccd084f3584e6b84823aba5651cfb53ef8a078ed441ddf9cd"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docker-socket", default="/var/run/docker.sock")
    args = parser.parse_args()
    docker = shutil.which("docker")
    socket = Path(args.docker_socket).resolve()
    if not docker or not socket.is_socket():
        parser.error("Existing Docker CLI and explicit local Unix socket required")
    with tempfile.TemporaryDirectory(prefix="operator-python-acquire-") as directory:
        environment = {"PATH": os.defpath, "HOME": directory, "DOCKER_CONFIG": directory,
                       "DOCKER_HOST": "unix://" + str(socket)}
        subprocess.run([docker, "pull", PYTHON_IMAGE], env=environment, check=True, timeout=300)
    print("Pinned public Python image acquired anonymously; no tests executed.")


if __name__ == "__main__":
    main()
