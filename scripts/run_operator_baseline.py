#!/usr/bin/env python3
"""Run an audited operator baseline without network or ambient credentials.

Usage: python3 scripts/run_operator_baseline.py [--self-check | --list]

Requires macOS sandbox-exec and an existing worker virtual environment (or
--python PATH). No dependencies are downloaded. This deliberately does not run
Temporal-server, Kubernetes-cluster, or cloud integration tests. The sandbox
restricts home-directory reads to code/runtime fixtures and writes to temporary
files; it is a development guard for trusted tests, not a hostile-code sandbox.
Python audit-visible I/O attempts fail the run even if caught. Native-code
network attempts are OS-denied, but need not be visible to the Python audit log.
"""

from __future__ import annotations

import argparse
import ctypes
import errno
import importlib.util
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile


ROOT = Path(__file__).resolve().parents[1]
TESTS = ROOT / "registry/worker/tests"
# Keep this explicit: broad discovery imports optional dependencies and may opt
# into native Temporal networking. Extend only after auditing the added tests.
BASELINE = (
    "test_config.py",
    "test_derive_and_defaults.py",
    "test_retry_policies.py",
    "test_provider_failure_retries.py",
    "test_rollout_failures.py",
    "test_namespace_workspace_failures.py",
    "test_demo_project_workflow.py",
    "test_temporal_cloud_saga_golden.py",
    "test_promotion.py",
)
REQUIRED = ("pytest", "temporalio", "kubernetes", "boto3", "yaml", "jsonschema", "cryptography", "urllib3")
GUARD_EXIT = 86


def sandbox_profile(real_home: Path, temporary: Path, runtime: Path, environment_root: Path) -> str:
    def path(value: Path) -> str:
        return json.dumps(str(value.resolve()))

    readable = (
        ROOT / "registry/worker",
        ROOT / "scripts",
        ROOT / "schemas",
        ROOT / "projects",
        runtime,
        environment_root / "lib",
        environment_root / "pyvenv.cfg",
    )
    exclusions = "\n".join(f"(require-not (subpath {path(p)}))" for p in readable)
    # File metadata remains readable so Python can traverse ancestors; contents
    # outside the code/runtime allowlist under the real home are denied.
    return f"""(version 1)
(allow default)
(deny network*)
(deny file-read-data (require-all (subpath {path(real_home)}) {exclusions}))
(deny file-read* (subpath {path(temporary / 'credential-store')}))
(deny file-read* (subpath {path(real_home / '.aws')}))
(deny file-read* (subpath {path(real_home / '.kube')}))
(deny file-read* (subpath {path(real_home / '.ssh')}))
(deny file-read* (regex #"(^|/)\\.env($|\\.)"))
(deny file-write* (require-all
  (require-not (subpath {path(temporary)}))
  (require-not (literal "/dev/null"))))
"""


def isolated_environment(temporary: Path) -> dict[str, str]:
    home = temporary / "home"
    home.mkdir()
    empty = temporary / "empty-config"
    empty.write_text("")
    return {
        "PATH": os.defpath,
        "HOME": str(home),
        "TMPDIR": str(temporary),
        "LANG": "en_US.UTF-8",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "AWS_CONFIG_FILE": str(empty),
        "AWS_SHARED_CREDENTIALS_FILE": str(empty),
        "AWS_ACCESS_KEY_ID": "offline-test-only",
        "AWS_SECRET_ACCESS_KEY": "offline-test-only",
        "AWS_EC2_METADATA_DISABLED": "true",
        "KUBECONFIG": str(empty),
        "TEMPORAL_ADDRESS": "127.0.0.1:7233",
        "TEMPORAL_NAMESPACE": "offline-test-only",
        "TEMPORAL_TLS": "false",
        "OPERATOR_BASELINE_TEMP": str(temporary),
    }


def verify_native_boundary(temporary: Path) -> None:
    """Probe OS enforcement using loopback and synthetic credential data only."""
    libc = ctypes.CDLL(None, use_errno=True)
    descriptor = libc.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    if descriptor < 0:
        blocked = ctypes.get_errno() in (errno.EPERM, errno.EACCES)
    else:
        # Darwin sockaddr_in: length, family, network-order port, IPv4, padding.
        address = bytes([16, socket.AF_INET, 0, 9, 127, 0, 0, 1]) + bytes(8)
        try:
            result = libc.connect(descriptor, address, len(address))
            blocked = result == -1 and ctypes.get_errno() in (errno.EPERM, errno.EACCES)
        finally:
            libc.close(descriptor)
    if not blocked:
        raise RuntimeError("OS network denial is not active; refusing to collect tests")
    try:
        (temporary / "credential-store/credentials").read_text()
    except PermissionError:
        pass
    else:
        raise RuntimeError("Credential-file denial is not active; refusing to collect tests")
    try:
        Path(os.environ["OPERATOR_BASELINE_WRITE_PROBE"]).write_text("must not be written")
    except PermissionError:
        pass
    else:
        raise RuntimeError("Write restriction is not active; refusing to collect tests")


class IOGuard:
    def __init__(self) -> None:
        self.attempts: list[str] = []

    def audit(self, event: str, arguments: tuple[object, ...]) -> None:
        blocked = event in {
            "socket.connect", "socket.bind", "socket.getaddrinfo",
            "socket.gethostbyname", "socket.gethostbyaddr", "socket.sendto",
            "subprocess.Popen", "os.system", "os.posix_spawn", "os.exec", "os.fork",
        }
        if blocked:
            # Avoid recording arguments: they might contain URLs or credentials.
            self.attempts.append(event)
            raise PermissionError(f"Offline operator baseline blocked {event}")

    def result(self, pytest_result: int) -> int:
        if self.attempts:
            print("OFFLINE GUARD VIOLATION: " + ", ".join(sorted(set(self.attempts))), flush=True)
            print("The run fails even when a test catches this Python audit-visible exception.", flush=True)
            return GUARD_EXIT
        return pytest_result


def child(mode: str) -> int:
    temporary = Path(os.environ["OPERATOR_BASELINE_TEMP"])
    verify_native_boundary(temporary)
    if mode == "baseline":
        missing = [name for name in REQUIRED if importlib.util.find_spec(name) is None]
        if missing:
            print("Missing baseline dependencies: " + ", ".join(missing), file=sys.stderr)
            print("Prepare the selected Python environment separately; this runner never installs packages.", file=sys.stderr)
            return 2
        print("urllib3's optional IPv6 loopback capability probe runs under OS network denial before the strict test guard.", flush=True)
        import urllib3  # noqa: F401 - deliberate import-time loopback probe, already denied by the OS

    guard = IOGuard()
    sys.addaudithook(guard.audit)
    if mode in {"network", "process"}:
        try:
            if mode == "network":
                socket.getaddrinfo("offline-probe.invalid", 443)
            else:
                subprocess.run([sys.executable, "-c", "pass"], check=True)
        except PermissionError:
            pass
        return guard.result(0)
    if mode == "boundary":
        print("OS network, credential-file read, and outside-temporary-directory write denial verified.", flush=True)
        return 0
    import pytest

    result = pytest.main([
        "-c", "/dev/null", "--rootdir", str(TESTS), "--confcutdir", str(TESTS),
        "-p", "no:cacheprovider", "--basetemp", str(temporary / "pytest"),
        "-q", "-ra", *[str(TESTS / name) for name in BASELINE],
    ])
    return guard.result(int(result))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--python", type=Path, help="Existing Python environment with worker test dependencies")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--self-check", action="store_true", help="Verify network, credential-file, and caught-I/O denial")
    mode.add_argument("--list", action="store_true", help="List audited selection and excluded integration tier")
    parser.add_argument("--_child", choices=("baseline", "boundary", "network", "process"), help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args._child:
        return child(args._child)
    if args.list:
        print("Audited baseline files:\n" + "\n".join(BASELINE))
        print("Excluded: every file not listed, including local Temporal history/replay, cluster, and cloud integration tests.")
        return 0
    sandbox = shutil.which("sandbox-exec")
    if sys.platform != "darwin" or not sandbox:
        parser.error("This first runner requires macOS sandbox-exec; no weaker fallback is enabled.")
    python = (args.python or ROOT / "registry/worker/.venv/bin/python").expanduser().absolute()
    if not python.is_file():
        parser.error("An existing worker virtual environment is required; use --python PATH.")
    # This command only inspects interpreter metadata; it imports no repo code.
    runtime = Path(subprocess.check_output(
        [sandbox, "-p", "(version 1)(allow default)(deny network*)", str(python), "-I", "-S", "-c", "import sys; print(sys.base_prefix)"],
        env={"PATH": os.defpath}, text=True,
    ).strip())
    environment_root = python.absolute().parent.parent
    if not (environment_root / "pyvenv.cfg").is_file():
        environment_root = runtime
    with (
        tempfile.TemporaryDirectory(prefix="operator-baseline-", dir="/private/tmp") as directory,
        tempfile.TemporaryDirectory(prefix="operator-baseline-write-probe-", dir="/private/tmp") as outside_directory,
    ):
        temporary = Path(directory).resolve()
        credentials = temporary / "credential-store"
        credentials.mkdir()
        (credentials / "credentials").write_text("synthetic self-check fixture, never a real credential")
        environment = isolated_environment(temporary)
        write_probe = Path(outside_directory) / "synthetic-fixture"
        write_probe.write_text("unchanged synthetic fixture")
        environment["OPERATOR_BASELINE_WRITE_PROBE"] = str(write_probe)
        profile = sandbox_profile(Path.home(), temporary, runtime, environment_root)
        modes = ("boundary", "network", "process") if args.self_check else ("baseline",)
        print("Offline baseline: macOS OS network denial; restricted real-home reads; temporary writes; no inherited credentials.", flush=True)
        print("Integration tier excluded: local Temporal history/replay, Kubernetes cluster, and cloud tests.", flush=True)
        for selected in modes:
            result = subprocess.run(
                [sandbox, "-p", profile, str(python), "-I", "-B", str(Path(__file__).resolve()), "--_child", selected],
                env=environment, cwd=TESTS, check=False,
            )
            expected = GUARD_EXIT if selected in {"network", "process"} else 0
            if result.returncode != expected:
                print(f"{selected} returned {result.returncode}; expected {expected}.", file=sys.stderr)
                return result.returncode or 1
            if write_probe.read_text() != "unchanged synthetic fixture":
                print("Write-denial fixture unexpectedly changed.", file=sys.stderr)
                return 1
        if args.self_check:
            print("All sandbox and caught-I/O self-checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
