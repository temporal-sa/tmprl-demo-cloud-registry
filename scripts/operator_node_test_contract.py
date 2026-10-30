"""Reviewed public Node test artifact pins and explicit local Docker helpers."""
from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess

ROOT = Path(__file__).resolve().parents[1]
VERSION = "26.13.0"
INDEX = "sha256:bc0a2afd3b74ce12222fc8bab99271e66257f021078f5f4a5d687a77d280c1d9"
PINS = {
    "amd64": {"manifest": "sha256:cd820d60a7f3ae057389f3e1bcf1a0ac09dd57c428fe14f191dcbc5c56ac5fe2", "config": "sha256:08fc0796a2b9a31dd1e7a5be3dd2f583e2bba76a13b21a6b04c5e642fd352df5"},
    "arm64": {"manifest": "sha256:29b974c22ff1c2508ca4c62da1414429b484a3d2a8f8f32520114c6975991eed", "config": "sha256:2cc41b716b20489bd428e6670d18f46371aa7c5aeba4020b662603506642f7e7"},
}
PRIMARY_SOURCES = [
    "https://raw.githubusercontent.com/docker-library/official-images/master/library/node",
    "https://github.com/nodejs/docker-node/tree/34e1b0030b415bdea959ccb15afda976a0761346/26/bookworm-slim",
    "https://registry-1.docker.io/v2/library/node/manifests/" + INDEX,
]
PUBLIC_FILES = (
    "registry/web/static/diagnostics.mjs",
    "registry/web/tests/diagnostics.test.mjs",
    "registry/web/tests/fixtures/go-diagnostic-view-v1.json",
    "scripts/operator_node_boundary.mjs",
    "registry/web/static/diagnostic-page.html",
    "registry/web/static/diagnostic-page.mjs",
    "registry/web/static/diagnostic-page.css",
    "registry/web/tests/diagnostic-page.test.mjs",
    "scripts/operator_browser_assertions.mjs",
    "scripts/tests/browser_assertions.test.mjs",
    "scripts/operator_browser_page.mjs",
    "scripts/tests/browser_page_protocol.test.mjs",
    "scripts/operator_browser_source_unavailable.mjs",
    "scripts/tests/browser_source_unavailable_protocol.test.mjs",
)
EXPECTED_CASES = (
    "standalone page import, construction and disabled mode make no requests",
    "one explicit load composes fixed authorized binding and real diagnostic panel",
    "binding scope shape and feature flag reject substitutions before diagnostic fetch",
    "discovery transport and body bounds fail closed without private error rendering",
    "authorization failure on rediscovery or diagnostic refresh clears an earlier view",
    "mounted page pins first enabled canonical scope through clear and lifecycle",
    "discovery close, replacement and timeout cannot mount a late protected view",
    "page lifecycle clears and aborts without polling or automatic resume",
    "standalone HTML uses only the fixed module and exposes no bootstrap identity",
    "actual Go Model.Read JSON fixture preserves the cross-language wire contract",
    "import and disabled/unbound panels perform zero requests or timers",
    "cause, cleanup, evidence and publication remain separate with private references dropped",
    "schema1 current, reconciling, stale, unknown and not-observed contracts",
    "archived, stale or future route evidence never confirms current serving",
    "retry certainty requires authoritative observation; category/configuration never invent schedule",
    "timestamps retain independent observation meanings",
    "scope/version/type/size mismatch fails closed and no project-name UID substitution exists",
    "untrusted text renders literally with no HTML or locator attributes",
    "manual client uses fixed same-origin endpoint, safe fetch options and no polling",
    "authorization failures clear old protected state and do not claim project existence",
    "late success cannot restore protected state after a newer denial or explicit clear",
    "cross-origin/redirect/cacheable/non-JSON/cross-scope/unavailable sources fail without raw error rendering",
    "body size is bounded before decoding; announced overflow and malformed UTF-8 are refused",
    "manual read deadline aborts and clears a stalled body; destruction prevents late rendering",
    "untrusted config cannot select an arbitrary diagnostic endpoint or leak invalid origin text",
    "exact internal namespace and seccomp report is required",
    "failure metadata contains only finite selected numeric kernel fields",
    "live renderer needs additional seccomp and PID and UID namespace evidence",
    "namespace bounding ceiling must equal exact bounded current kernel full set",
    "page module imports without runtime prerequisites or native side effects",
    "bootstrap origin is exact private frontend IP and fixed owned port",
    "bootstrap cannot select commands aliases arbitrary scopes or unverified writer",
    "malformed synthetic cookies fail with only finite privacy-safe error",
    "source outage module imports without runtime or driver side effects",
    "source outage phase requires exact completed first read and actual close evidence",
    "source outage finish refuses polling provider repost and unclosed resources",
    "source outage unknown phases and private fields yield only finite failure",
)


class Docker:
    def __init__(self, socket: str, private_home: Path):
        self.binary = shutil.which("docker")
        endpoint = Path(socket).resolve(strict=True)
        if not self.binary or not endpoint.is_socket():
            raise RuntimeError("Existing Docker and explicit local Unix socket required")
        self.env = {"PATH": os.defpath, "HOME": str(private_home), "DOCKER_CONFIG": str(private_home), "DOCKER_HOST": "unix://" + str(endpoint)}
        server = json.loads(self.command(["version", "--format", "{{json .Server}}"]))
        if server.get("Os") != "linux" or server.get("Arch") not in PINS:
            raise RuntimeError("Only explicit native Linux amd64/arm64 daemon platforms are reviewed")
        self.arch = server["Arch"]
        self.pin = PINS[self.arch]
        self.base = "docker.io/library/node@" + self.pin["manifest"]

    def command(self, argv, *, timeout=180):
        result = subprocess.run([self.binary, *map(str, argv)], env=self.env, capture_output=True, text=True, timeout=timeout, check=False)
        if result.returncode:
            # No provider/token/config text is echoed. These helpers never load
            # user Docker config; acquisition and execution have separate entrypoints.
            raise RuntimeError("Explicit owned Node Docker command failed: " + str(argv[0]))
        if len(result.stdout) > 2 * 1024 * 1024:
            raise RuntimeError("Owned Node command output exceeds its bound")
        return result.stdout

    def verify_base(self):
        value = json.loads(self.command(["image", "inspect", self.base]))[0]
        if value["Id"] != self.pin["config"] or value["Os"] != "linux" or value["Architecture"] != self.arch:
            raise RuntimeError("Pinned Node image platform/config identity mismatch")
        digests = {"docker.io/library/node@", "library/node@", "node@"}
        if not any(prefix + self.pin["manifest"] in value.get("RepoDigests", []) for prefix in digests):
            raise RuntimeError("Exact official Node manifest reference not present locally")
        config = value["Config"]
        env = dict(item.split("=", 1) for item in config.get("Env", []))
        if env.get("NODE_VERSION") != VERSION or set(env) - {"PATH", "NODE_VERSION", "YARN_VERSION"} or config.get("OnBuild") or config.get("Volumes"):
            raise RuntimeError("Unexpected pinned Node image execution configuration")
        return value

    def inspect_optional(self, kind, name):
        result = subprocess.run([self.binary, "inspect", "--type=" + kind, name], env=self.env, capture_output=True, text=True, timeout=30, check=False)
        if result.returncode == 0:
            return json.loads(result.stdout)[0]
        if "No such" in result.stderr:
            return None
        raise RuntimeError("Cannot establish owned Node resource absence")
