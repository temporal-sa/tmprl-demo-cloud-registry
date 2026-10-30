#!/usr/bin/env python3
"""Explicit public acquisition of the pinned official Node test image only.

Usage: python3 scripts/prepare_operator_node_tests.py --acquire
Authenticity relies on reviewed official library/node publication over HTTPS
and exact immutable manifest/config digests; no source-build attestation is
claimed. Anonymous registry tokens remain private in memory and are never
printed or passed in argv. No user Docker config, proxy or credentials loaded.
Execution uses a separate offline runner with no mutable tags or tool download.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import ssl
import tempfile
import urllib.request

from operator_node_test_contract import Docker, INDEX, PRIMARY_SOURCES, VERSION


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise RuntimeError("Public registry metadata redirects are disabled")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--acquire", action="store_true", required=True)
    parser.add_argument("--docker-socket", default="/var/run/docker.sock")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="node-public-acquisition-") as directory:
        docker = Docker(args.docker_socket, Path(directory))
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect(), urllib.request.HTTPSHandler(context=ssl.create_default_context()))

        def get(url, headers=None, limit=2 * 1024 * 1024):
            try:
                with opener.open(urllib.request.Request(url, headers=headers or {}), timeout=30) as response:
                    data = response.read(limit + 1)
                    if len(data) > limit:
                        raise RuntimeError("Public registry metadata exceeds its bound")
                    return data
            except Exception:
                raise RuntimeError("Pinned public registry metadata acquisition failed") from None

        token = json.loads(get("https://auth.docker.io/token?service=registry.docker.io&scope=repository:library/node:pull", limit=65536))["token"]
        headers = {"Authorization": "Bearer " + token, "Accept": "application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json"}
        index = get("https://registry-1.docker.io/v2/library/node/manifests/" + INDEX, headers)
        if "sha256:" + hashlib.sha256(index).hexdigest() != INDEX:
            raise RuntimeError("Official Node index digest mismatch")
        entries = [item for item in json.loads(index)["manifests"] if item.get("platform", {}).get("os") == "linux" and item.get("platform", {}).get("architecture") == docker.arch]
        if len(entries) != 1 or entries[0]["digest"] != docker.pin["manifest"]:
            raise RuntimeError("Official Node native platform manifest mismatch")
        manifest = get("https://registry-1.docker.io/v2/library/node/manifests/" + docker.pin["manifest"], headers)
        if "sha256:" + hashlib.sha256(manifest).hexdigest() != docker.pin["manifest"] or json.loads(manifest)["config"]["digest"] != docker.pin["config"]:
            raise RuntimeError("Official Node manifest/config digest mismatch")
        docker.command(["image", "pull", "--platform", "linux/" + docker.arch, docker.base], timeout=240)
        docker.verify_base()
        print(json.dumps({"version": VERSION, "platform": "linux/" + docker.arch, "index": INDEX, **docker.pin, "sources": PRIMARY_SOURCES}, sort_keys=True))
        print("Pinned public Node acquisition verified. No tests/provider calls ran; image is a shared public cache.")


if __name__ == "__main__":
    main()
