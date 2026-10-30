#!/usr/bin/env python3
"""Explicit public-only acquisition; no package hooks/browser execution.

Usage: python3 scripts/prepare_operator_browser_tests.py --acquire
Exact official native image, driver SHA512 and reviewed seccomp SHA256 only.
No npm/npx, transitive install, ambient Docker config/proxy or browser downloads.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import tempfile
import urllib.request

from operator_browser_contract import CACHE, DRIVER_URL, Docker, INDEX, MAX_ARCHIVE, SECCOMP_URL, driver_files, existing_cache_matches, verify_seccomp
from prepare_operator_node_tests import NoRedirect


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--acquire", action="store_true", required=True)
    parser.add_argument("--docker-socket", default="/var/run/docker.sock")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="browser-public-acquire-") as directory:
        docker = Docker(args.docker_socket, Path(directory))
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())

        def get(url, bound, headers=None):
            with opener.open(urllib.request.Request(url, headers=headers or {}), timeout=60) as response:
                data = response.read(bound + 1)
            if len(data) > bound:
                raise RuntimeError("Pinned public artifact exceeds its bound")
            return data

        accept = {"Accept": "application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.docker.distribution.manifest.v2+json"}
        base = "https://mcr.microsoft.com/v2/playwright/manifests/"
        index = get(base + INDEX, 65536, accept)
        if "sha256:" + hashlib.sha256(index).hexdigest() != INDEX:
            raise RuntimeError("Official browser index digest mismatch")
        platforms = [item for item in json.loads(index)["manifests"] if item.get("platform") == {"os": "linux", "architecture": docker.arch}]
        if len(platforms) != 1 or platforms[0]["digest"] != docker.pin["manifest"]:
            raise RuntimeError("Official browser native manifest mismatch")
        manifest = get(base + docker.pin["manifest"], 65536, accept)
        if "sha256:" + hashlib.sha256(manifest).hexdigest() != docker.pin["manifest"] or json.loads(manifest)["config"]["digest"] != docker.pin["config"]:
            raise RuntimeError("Official browser native config mismatch")
        core, seccomp = get(DRIVER_URL, MAX_ARCHIVE), get(SECCOMP_URL, 65536)
        files = driver_files(core)
        verify_seccomp(seccomp)
        if CACHE.parent.resolve(strict=True) != CACHE.parent or CACHE.is_symlink():
            raise RuntimeError("Browser cache must have a canonical owned path")
        CACHE.mkdir(mode=0o700, exist_ok=True)
        if CACHE.resolve(strict=True) != CACHE:
            raise RuntimeError("Browser cache canonical identity changed")
        for name, data in (("playwright-core-1.56.0.tgz", core), ("seccomp.json", seccomp)):
            path = CACHE / name
            try:
                fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o444)
            except FileExistsError:
                if not existing_cache_matches(path, data):
                    raise RuntimeError("Existing browser cache differs; refusing overwrite") from None
            else:
                with os.fdopen(fd, "wb") as stream:
                    stream.write(data)
                    stream.flush()
                    os.fsync(stream.fileno())
        docker.command(["image", "pull", "--platform", "linux/" + docker.arch, docker.base], timeout=600)
        docker.verify_base()
        print(json.dumps({"platform": "linux/" + docker.arch, "index": INDEX, **docker.pin, "driverFiles": len(files), "driverBytes": sum(map(len, files.values())), "driverArchiveSHA256": hashlib.sha256(core).hexdigest(), "seccompSHA256": hashlib.sha256(seccomp).hexdigest()}, sort_keys=True))
        print("Pinned public browser acquisition complete; no package/browser/provider execution.")


if __name__ == "__main__":
    main()
