#!/usr/bin/env python3
"""Run selected operator history/recovery tests against an owned local server.

Acquire dependencies separately (no installation happens during execution):
  uv venv .venv/operator-tests --python registry/worker/.venv/bin/python
  uv pip sync --python .venv/operator-tests/bin/python scripts/requirements-operator-tests.lock
Run:
  python3 scripts/run_operator_temporal_tests.py [--self-check | --list]

macOS sandbox-exec is required. The trusted Temporal CLI process has loopback
network access; tests can reach only its allocated frontend port. All other
client networking and child processes are prohibited. State lives in temporary
files; real-home contents are restricted to code/runtime fixtures. Python
audit-visible violations fail the run even when caught; native networking is
OS-enforced and not necessarily visible to Python audit hooks.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_operator_baseline as offline

ROOT = offline.ROOT
TESTS = offline.TESTS
SELECTION = (
    TESTS / "test_rollout_failure_history.py",
    TESTS / "test_namespace_workspace_history.py",
    TESTS / "test_isolated_lambda_cleanup_history.py",
    ROOT / "scripts/tests/test_operator_temporal_recovery.py",
)
REQUIRED = (*offline.REQUIRED, "anthropic", "httpx2")


class LocalIOGuard(offline.IOGuard):
    def __init__(self, port: int) -> None:
        super().__init__()
        self.port = port

    def audit(self, event: str, arguments: tuple[object, ...]) -> None:
        if event == "socket.connect":
            address = arguments[1]
            if isinstance(address, tuple) and address[:2] == ("127.0.0.1", self.port):
                return
        if event == "socket.getaddrinfo":
            if arguments[:2] in (("127.0.0.1", self.port), ("127.0.0.1", str(self.port))):
                return
        super().audit(event, arguments)


async def native_client_probe(port: int) -> None:
    from temporalio.client import Client

    client = await asyncio.wait_for(Client.connect(f"127.0.0.1:{port}"), 10)
    await asyncio.wait_for(client.workflow_service.get_system_info(
        __import__("temporalio.api.workflowservice.v1", fromlist=["GetSystemInfoRequest"]).GetSystemInfoRequest()
    ), 10)
    try:
        await asyncio.wait_for(Client.connect("127.0.0.1:9"), 3)
    except Exception as error:
        # A timeout or an absent listener is insufficient proof of OS denial.
        message = str(error).lower()
        if not any(value in message for value in ("operation not permitted", "permission denied", "os error 1", "os error 13")):
            raise RuntimeError("Native SDK denial was not proven: " + type(error).__name__) from error
    else:
        raise RuntimeError("Native SDK unexpectedly reached an unapproved loopback port")


class OwnedServerPlugin:
    def __init__(self, port: int, namespaces: list[str]) -> None:
        self.port = port
        self.namespaces = iter(namespaces)
        self.skipped = 0

    def pytest_configure(self, config) -> None:
        from temporalio.client import Client
        from temporalio.testing import WorkflowEnvironment

        async def start_owned(cls, **kwargs):
            unsupported = set(kwargs) - {"dev_server_existing_path"}
            if unsupported:
                raise ValueError("Unaudited start_local options: " + ", ".join(sorted(unsupported)))
            namespace = next(self.namespaces)
            client = await Client.connect(f"127.0.0.1:{self.port}", namespace=namespace)
            return WorkflowEnvironment.from_client(client)

        self.original_start_local = WorkflowEnvironment.__dict__["start_local"]
        WorkflowEnvironment.start_local = classmethod(start_owned)

    def pytest_runtest_logreport(self, report) -> None:
        if report.skipped:
            self.skipped += 1

    def pytest_unconfigure(self, config) -> None:
        from temporalio.testing import WorkflowEnvironment
        WorkflowEnvironment.start_local = self.original_start_local


def child(mode: str) -> int:
    temporary = Path(os.environ["OPERATOR_BASELINE_TEMP"])
    port = int(os.environ["OPERATOR_LOCAL_TEMPORAL_PORT"])
    offline.verify_native_boundary(temporary)
    missing = [name for name in REQUIRED if importlib.util.find_spec(name) is None]
    if missing:
        print("Missing test dependencies: " + ", ".join(missing), file=sys.stderr)
        print("Acquire the locked test environment separately; see --help.", file=sys.stderr)
        return 2
    # Same narrowly documented import-time IPv6 probe as the offline baseline;
    # it remains OS-denied before the stricter test audit phase starts.
    import urllib3  # noqa: F401
    guard = LocalIOGuard(port)
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
        asyncio.run(native_client_probe(port))
        print("Verified native Temporal client reaches owned server and OS-denies other loopback ports; credential reads/writes and Python DNS/process probes remain restricted.", flush=True)
        return guard.result(0)
    sys.path.insert(0, str(ROOT / "registry/worker/src"))
    sys.path.insert(0, str(TESTS))
    import pytest
    plugin = OwnedServerPlugin(port, json.loads(os.environ["OPERATOR_LOCAL_TEMPORAL_NAMESPACES"]))
    result = pytest.main([
        "-c", "/dev/null", "--rootdir", str(TESTS), "--confcutdir", str(TESTS),
        "-p", "no:cacheprovider", "--basetemp", str(temporary / "pytest"),
        "-q", "-ra", *map(str, SELECTION),
    ], plugins=[plugin])
    if plugin.skipped:
        print(f"Refusing a partial history proof: {plugin.skipped} selected cases skipped.", file=sys.stderr)
        result = 1
    return guard.result(int(result))


def stop(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)


def owns_listener(pid: int, port: int) -> bool:
    result = subprocess.run([
        "/usr/sbin/lsof", "-nP", "-iTCP:" + str(port),
        "-sTCP:LISTEN", "-Fp",
    ], capture_output=True, text=True, env={"PATH": os.defpath}, check=False)
    owners = {line for line in result.stdout.splitlines() if line.startswith("p")}
    return owners == {f"p{pid}"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--python", type=Path, default=ROOT / ".venv/operator-tests/bin/python")
    parser.add_argument("--temporal", type=Path, help="Existing trusted Temporal CLI binary; never downloaded by this runner")
    parser.add_argument("--timeout", type=float, default=300, help="Maximum seconds for the selected tests")
    parser.add_argument("--go", action="store_true", help="Compile and run Go coordinator histories instead of Python histories; module cache must be prepared separately")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--self-check", action="store_true")
    modes.add_argument("--list", action="store_true")
    parser.add_argument("--_child", choices=("boundary", "network", "process", "tests"), help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args._child:
        return child(args._child)
    if args.list:
        selected = (ROOT / "operator-v2/internal/coordinator/local_history_test.go",) if args.go else SELECTION
        print("Selected local history/recovery test files:\n" + "\n".join(str(path.relative_to(ROOT)) for path in selected))
        print("Excluded: all other tests, time-skipping downloads, real providers and Kubernetes/cloud integration.")
        return 0
    sandbox = shutil.which("sandbox-exec")
    python = args.python.expanduser().absolute()
    temporal = args.temporal or Path(shutil.which("temporal") or "/nonexistent/temporal")
    temporal = temporal.expanduser().absolute()
    if sys.platform != "darwin" or not sandbox or not Path("/usr/sbin/lsof").is_file():
        parser.error("Requires macOS sandbox-exec and lsof; no weaker fallback is enabled.")
    if not python.is_file() or not temporal.is_file():
        parser.error("Prepare an existing locked test Python environment and Temporal CLI; see --help.")
    metadata_profile = "(version 1)(allow default)(deny network*)"
    runtime = Path(subprocess.check_output([
        sandbox, "-p", metadata_profile, str(python), "-I", "-S", "-c", "import sys; print(sys.base_prefix)",
    ], env={"PATH": os.defpath}, text=True).strip())
    environment_root = python.parent.parent
    if not (environment_root / "pyvenv.cfg").is_file():
        environment_root = runtime
    with (
        tempfile.TemporaryDirectory(prefix="operator-temporal-", dir="/private/tmp") as directory,
        tempfile.TemporaryDirectory(prefix="operator-temporal-write-probe-", dir="/private/tmp") as outside,
    ):
        temporary = Path(directory).resolve()
        (temporary / "credential-store").mkdir()
        (temporary / "credential-store/credentials").write_text("synthetic credential fixture")
        write_probe = Path(outside) / "synthetic-fixture"
        write_probe.write_text("unchanged synthetic fixture")
        environment = offline.isolated_environment(temporary)
        go_binary = temporary / "coordinator-history.test"
        if args.go and not args.self_check:
            go = shutil.which("go")
            if not go:
                parser.error("An existing Go compiler is required for --go")
            # Compilation does not execute package init or test code. All test
            # execution occurs below inside the owned one-port OS boundary.
            build_environment = dict(environment, GOTOOLCHAIN="local", GOPROXY="off", GOSUMDB="off", CGO_ENABLED="0",
                                     GOCACHE=str(ROOT / ".venv/operator-go-cache"), GOMODCACHE=str(ROOT / ".venv/operator-go-modules"))
            subprocess.run([go, "test", "-c", "-tags", "temporal_integration", "-o", str(go_binary), "./internal/coordinator"],
                           env=build_environment, cwd=ROOT / "operator-v2", check=True, timeout=180)
        environment["OPERATOR_BASELINE_WRITE_PROBE"] = str(write_probe)
        environment["DEMO_REGISTRY_TEST_TEMPORAL_HISTORY"] = "1"
        run_id = uuid.uuid4().hex[:12]
        namespaces = [f"operator-local-{run_id}-{index}" for index in range(12)]
        # The CLI cannot inherit a listener. Verify process ownership before
        # permitting the test client to connect, and monitor server lifetime.
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        environment["OPERATOR_LOCAL_TEMPORAL_PORT"] = str(port)
        environment["OPERATOR_LOCAL_TEMPORAL_NAMESPACES"] = json.dumps(namespaces)
        environment["TEMPORAL_ADDRESS"] = f"127.0.0.1:{port}"
        profile = offline.sandbox_profile(Path.home(), temporary, runtime, environment_root)
        # SBPL accepts the special localhost host, not numeric IP literals.
        client_profile = profile + f'\n(allow network-outbound (remote ip "localhost:{port}"))\n'
        server_profile = profile + '\n(allow network-outbound (remote ip "localhost:*"))\n(allow network-inbound (local ip "localhost:*"))\n(allow network-bind (local ip "localhost:*"))\n'
        version = subprocess.check_output([
            sandbox, "-p", server_profile, str(temporal), "--version",
        ], env=environment, cwd=temporary, text=True).strip()
        print("Trusted local server: " + version, flush=True)
        log_path = temporary / "temporal.log"
        command = [sandbox, "-p", server_profile, str(temporal), "server", "start-dev", "--headless", "--ip", "127.0.0.1", "--port", str(port), "--db-filename", str(temporary / "temporal.db")]
        for namespace in namespaces:
            command.extend(["--namespace", namespace])
        server = None
        client = None
        try:
            with log_path.open("w") as log:
                server = subprocess.Popen(command, env=environment, cwd=temporary, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
                deadline = time.monotonic() + 40
                while not owns_listener(server.pid, port):
                    if server.poll() is not None or time.monotonic() > deadline:
                        raise RuntimeError("Owned Temporal server did not become ready")
                    time.sleep(0.1)
                print(f"Owned local Temporal PID {server.pid}, frontend 127.0.0.1:{port}; clients cannot reach other loopback ports or external endpoints.", flush=True)
                selected_modes = ("boundary", "network", "process") if args.self_check else ("boundary", "tests")
                for mode in selected_modes:
                    if server.poll() is not None or not owns_listener(server.pid, port):
                        raise RuntimeError("Owned frontend listener identity changed; refusing test execution")
                    if args.go and mode == "tests":
                        go_profile = client_profile + '\n(deny process-exec (require-not (literal ' + json.dumps(str(go_binary)) + ')))\n'
                        client_command = [sandbox, "-p", go_profile, str(go_binary), "-test.v", "-test.run", "^TestGoLocal", "-test.timeout", str(int(args.timeout)) + "s"]
                    else:
                        client_command = [sandbox, "-p", client_profile, str(python), "-I", "-B", str(Path(__file__).resolve()), "--_child", mode]
                    client = subprocess.Popen(client_command, env=environment, cwd=TESTS, start_new_session=True)
                    deadline = time.monotonic() + args.timeout
                    while client.poll() is None:
                        if server.poll() is not None:
                            raise RuntimeError("Owned server exited during testing; refusing any reused frontend")
                        if time.monotonic() > deadline:
                            raise TimeoutError("Local history test time budget exceeded")
                        time.sleep(0.1)
                    expected = offline.GUARD_EXIT if mode in {"network", "process"} else 0
                    if server.poll() is not None:
                        raise RuntimeError("Owned server exited before the test phase completed")
                    if client.returncode != expected:
                        return client.returncode or 1
                    if write_probe.read_text() != "unchanged synthetic fixture":
                        raise RuntimeError("Synthetic write restriction fixture changed")
                return 0
        except (RuntimeError, TimeoutError) as error:
            print(str(error), file=sys.stderr)
            if log_path.exists():
                print("Owned server log tail:\n" + "\n".join(log_path.read_text().splitlines()[-15:]), file=sys.stderr)
            return 1
        finally:
            if client is not None:
                stop(client)
            if server is not None:
                stop(server)
                if owns_listener(server.pid, port):
                    raise RuntimeError("Owned server listener remained after teardown")
                print("Owned server stopped; temporary state removed on exit.", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
