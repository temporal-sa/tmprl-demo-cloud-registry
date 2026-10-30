"""Finite public browser pins, archive admission and disposable Docker boundary.

Only trusted local fixture pages are in scope. These old pinned browser bytes
are not a recommendation for browsing arbitrary websites.
"""
from __future__ import annotations

import base64
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import stat
import tarfile

from operator_node_test_contract import Docker as NodeDocker

ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / ".venv/operator-browser"
VERSION = "1.56.0"
CHROMIUM = "141.0.7390.37"
INDEX = "sha256:35246d87a7c88ea9b771c65d33171b2611b02a8253b4b12ce6f94376c55f99f2"
PINS = {
    "arm64": {"manifest": "sha256:d4d089b3fc9cc1cbf6b1b534ebd3062e85cc3d01d4ba1ae25d878a85ee6ff14f", "config": "sha256:6426c518eaeda3cec6c41fd25704b90c52f0628139fcb5e1ea5d0e338100c522"},
    "amd64": {"manifest": "sha256:1e83c10ccee0065f527953861d176a828a31a84324688ecb03f11912296c8444", "config": "sha256:86e2829695532ecd3624885d15514a8ccb304fe46be05b9b561f0b0d4ae6f000"},
}
DRIVER_URL = "https://registry.npmjs.org/playwright-core/-/playwright-core-1.56.0.tgz"
DRIVER_INTEGRITY = "1SXl7pMfemAMSDn5rkPeZljxOCYAmQnYLBTExuh6E8USHXGSX3dx6lYZN/xPpTz1vimXmPA9CDnILvmJaB8aSQ=="
SECCOMP_URL = "https://raw.githubusercontent.com/microsoft/playwright/v1.56.0/utils/docker/seccomp_profile.json"
SECCOMP_HASH = "cc3e61cabda6bbc1e53e54d27ba4d55a9d3be829b6dd1a596f4a7b31b1cc7849"
MAX_ARCHIVE = 4 * 1024 * 1024
MAX_UNPACKED = 9 * 1024 * 1024
LABEL = "operator.local/browser-run"
PUBLIC_FILES = ("scripts/operator_browser_boundary.mjs", "scripts/operator_browser_assertions.mjs")
CASES = ("native-boundary", "chromium-version", "internal-sandbox-status", "renderer-kernel-sandbox", "browser-closed")
TMP_OPTIONS = "rw,noexec,nosuid,nodev,size=128m,uid=1001,gid=1001,mode=0700"


def existing_cache_matches(path, expected):
    """No-follow identity/type/size check before a strictly bounded read."""
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        return False
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_size != len(expected):
            return False
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            return stream.read(len(expected) + 1) == expected
    finally:
        os.close(descriptor)


def verify_seccomp(data):
    if len(data) > 65536 or hashlib.sha256(data).hexdigest() != SECCOMP_HASH:
        raise RuntimeError("Exact reviewed Playwright seccomp source is required")
    value = json.loads(data)
    if value.get("defaultAction") != "SCMP_ACT_ERRNO":
        raise RuntimeError("Seccomp default denial is required")
    return data


def browser_seccomp(data):
    """Exact upstream policy plus one syscall needed inside Chrome's userns.

    Docker resolves upstream capability-conditional rules against OUTER caps
    at container creation; cap-dropALL otherwise omits chroot even inside a new
    user namespace. This grants no outer capability: parent must prove EPERM.
    """
    value = json.loads(verify_seccomp(data))
    matches = [item for item in value["syscalls"] if "chroot" in item.get("names", [])]
    expected = {"names": ["chroot"], "action": "SCMP_ACT_ALLOW", "args": [], "comment": "", "includes": {"caps": ["CAP_SYS_CHROOT"]}, "excludes": {}}
    if matches != [expected]:
        raise RuntimeError("Reviewed upstream conditional chroot rule differs")
    value["syscalls"].append({"names": ["chroot"], "action": "SCMP_ACT_ALLOW", "args": [], "includes": {}, "excludes": {}})
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def driver_files(data):
    """Return exact regular bytes, never extract archive paths or run hooks."""
    if len(data) > MAX_ARCHIVE or hashlib.sha512(data).digest() != base64.b64decode(DRIVER_INTEGRITY):
        raise RuntimeError("Matching pinned Playwright driver integrity is required")
    return admitted_driver_files(data)


def admitted_driver_files(data):
    # Separate parser enables synthetic hostile-archive tests without replacing
    # the immutable digest authority in driver_files.
    files, total, members = {}, 0, 0
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as archive:
        for item in archive:
            members += 1
            path = PurePosixPath(item.name)
            if members > 600 or path.is_absolute() or ".." in path.parts or not path.parts or path.parts[0] != "package" or str(path) != item.name:
                raise RuntimeError("Unsafe driver archive path/count")
            if item.isdir():
                continue
            if not item.isfile() or len(path.parts) < 2 or str(path) in files or item.size > MAX_UNPACKED:
                raise RuntimeError("Driver archive must contain distinct bounded regular files")
            total += item.size
            if total > MAX_UNPACKED:
                raise RuntimeError("Driver unpacked bytes exceed bound")
            source = archive.extractfile(item)
            contents = source.read(item.size + 1) if source else b""
            if len(contents) != item.size:
                raise RuntimeError("Driver member is truncated")
            files[str(path)] = contents
    # Check actual npm manifest, not a user-supplied transitive installation.
    package = json.loads(files.get("package/package.json", b"null"))
    if not isinstance(package, dict) or package.get("name") != "playwright-core" or package.get("version") != VERSION or package.get("dependencies") or package.get("optionalDependencies") or package.get("scripts"):
        raise RuntimeError("Only exact dependency-free, hook-free playwright-core is admitted")
    if "package/index.mjs" not in files or "package/lib/server/chromium/chromium.js" not in files:
        raise RuntimeError("Driver entrypoint/browser implementation is missing")
    paths = set(files)
    if any(str(parent) in paths for name in paths for parent in PurePosixPath(name).parents):
        raise RuntimeError("Driver archive contains file/parent collisions")
    return files


class Docker(NodeDocker):
    """Reuse scrubbed explicit-Unix-socket transport, not Node image policy."""
    def __init__(self, socket, private_home):
        super().__init__(socket, private_home)
        self.pin = PINS[self.arch]
        self.base = "mcr.microsoft.com/playwright@" + self.pin["manifest"]

    def verify_base(self):
        # Config lookup also permits the already cached exact index reference;
        # no daemon pull/build resolution can run during execution.
        value = json.loads(self.command(["image", "inspect", self.pin["config"]]))[0]
        if value["Id"] != self.pin["config"] or value["Os"] != "linux" or value["Architecture"] != self.arch:
            raise RuntimeError("Pinned official browser platform/config mismatch")
        refs = set(value.get("RepoDigests", []))
        if not any("mcr.microsoft.com/playwright@" + digest in refs for digest in (INDEX, self.pin["manifest"])):
            raise RuntimeError("Official immutable browser publication reference is absent")
        config = value["Config"]
        env = dict(item.split("=", 1) for item in config.get("Env", []))
        if set(env) != {"PATH", "LANG", "LC_ALL", "PLAYWRIGHT_BROWSERS_PATH"} or env["PLAYWRIGHT_BROWSERS_PATH"] != "/ms-playwright" or config.get("OnBuild") or config.get("Volumes") or config.get("Entrypoint"):
            raise RuntimeError("Unexpected official browser image startup state")
        return value


def check_container(value, run, image, seccomp):
    config, host = value["Config"], value["HostConfig"]
    if value["Image"] != image or config.get("Labels", {}).get(LABEL) != run or config.get("User") != "1001:1001" or host.get("NetworkMode") != "none" or not host.get("ReadonlyRootfs") or host.get("Privileged") or host.get("Binds") or host.get("Devices") or host.get("CapAdd") or set(host.get("CapDrop", [])) != {"ALL"}:
        raise RuntimeError("Browser isolation/identity differs; no weaker fallback")
    if host.get("IpcMode") != "private" or host.get("PidMode") or host.get("ShmSize") != 256 * 1024**2 or host.get("Memory") != 1024**3 or host.get("MemorySwap") != 1024**3 or host.get("NanoCpus") != 2 * 10**9 or host.get("PidsLimit") != 192:
        raise RuntimeError("Browser namespaces/resource bounds differ")
    security = host.get("SecurityOpt", [])
    # Docker normalizes a profile file into the exact JSON in inspect.
    if "no-new-privileges" not in security or len(security) != 2:
        raise RuntimeError("Browser security options differ")
    encoded = [entry.removeprefix("seccomp=") for entry in security if entry.startswith("seccomp=")]
    if len(encoded) != 1 or json.loads(encoded[0]) != json.loads(seccomp):
        raise RuntimeError("Browser seccomp policy differs from reviewed source")
    if host.get("Tmpfs") != {"/tmp": TMP_OPTIONS} or any(item.get("Type") != "tmpfs" or item.get("Destination") not in {"/tmp", "/dev/shm"} for item in value.get("Mounts", [])):
        raise RuntimeError("Unexpected browser mount")
    allowed = {"PATH", "LANG", "LC_ALL", "PLAYWRIGHT_BROWSERS_PATH", "HOME", "TMPDIR", "XDG_CACHE_HOME", "OPERATOR_UNIT_HOST_GATEWAY"}
    if {item.split("=", 1)[0] for item in config.get("Env", [])} != allowed:
        raise RuntimeError("Unexpected browser environment")


def check_output(output):
    lines = output.strip().splitlines()
    if lines != ["PASS browser " + item for item in CASES]:
        raise RuntimeError("Exact finite browser proof did not PASS without omissions/skips")
