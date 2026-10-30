"""Finite private-stdio browser fixture for the existing owned kind harness.

No CLI, command/server override, acquisition or deployment entrypoint. Runtime
stdio now includes a synthetic cookie/private pins: NEVER echo raw child output
or exception details. Only finite phase/code/case counters may be published.
The browser has no host mount. The Go fixture has ONE read-only bind containing
only this owned cluster's synthetic mTLS connection JSON, plus bounded tmpfs.
"""
from __future__ import annotations

import hashlib
import io
import ipaddress
import json
import os
from pathlib import Path
import re
import selectors
import subprocess
import tarfile
import time
from dataclasses import dataclass
from types import MappingProxyType

from operator_browser_contract import CACHE, Docker, LABEL, ROOT, TMP_OPTIONS, browser_seccomp, driver_files
from run_operator_browser_boundary import export_proof, regular_bytes, stage_tar

KUBE_LABEL = "operator.local/run-id"
TEST = "TestLocalDiagnosticBrowserAfterWriterExit"
MANDATORY = "TestOwnedAPIAndNetworkBoundary"
COMPILER_TARGET = "./integration"
COMPILER_TAGS = "integration,browser_integration"
PUBLIC_ASSETS = tuple("registry/web/static/" + name for name in ("diagnostic-page.html", "diagnostic-page.mjs", "diagnostic-page.css", "diagnostics.mjs"))
PUBLIC_SCRIPTS = ("scripts/operator_browser_assertions.mjs", "scripts/operator_browser_page.mjs")
PAGE_CASES = ("native-network", "chromium-sandbox", "static-no-read", "session-denial", "cause-and-warning", "clear-view", "logout-and-scope", "policy-revocation", "no-repost-and-privacy", "browser-closed")
SOURCE_TEST = "TestLocalDiagnosticSourceUnavailableAfterWriterExit"
SOURCE_CASES = ("native-network", "chromium-sandbox", "initial-authorized-view", "source-disconnected", "unavailable-cleared", "evidence-and-no-repost", "browser-closed")


@dataclass(frozen=True)
class BrowserVariant:
    test: str
    cases: tuple[str, ...]
    scripts: tuple[str, ...]
    entrypoint: str
    go_prefix: str
    node_prefix: str
    marker: str


def browser_variant(source_unavailable=False):
    # Exactly two internal modes, not a general test/script/phase injection API.
    if type(source_unavailable) is not bool:
        raise BrowserFixtureFailure("BrowserModeInvalid")
    if source_unavailable:
        return BrowserVariant(SOURCE_TEST, SOURCE_CASES, (*PUBLIC_SCRIPTS, "scripts/operator_browser_source_unavailable.mjs"), "/reviewed/scripts/operator_browser_source_unavailable.mjs", "OWNED-BROWSER-SOURCE ", "SOURCE-BROWSER-PHASE ", "browser-source")
    return BrowserVariant(TEST, PAGE_CASES, PUBLIC_SCRIPTS, "/reviewed/scripts/operator_browser_page.mjs", "OWNED-BROWSER ", "BROWSER-PHASE ", "browser-page")


def process_variant(kind):
    if kind not in ("go", "browser", "go-source", "browser-source"):
        raise BrowserFixtureFailure("BrowserProcessKindInvalid")
    return browser_variant(kind in ("go-source", "browser-source"))


class BrowserFixtureFailure(RuntimeError):
    pass


def compilation_mode(enabled, *, source_unavailable=False):
    if type(enabled) is not bool or type(source_unavailable) is not bool or source_unavailable and not enabled:
        raise BrowserFixtureFailure("BrowserModeInvalid")
    return (COMPILER_TARGET, COMPILER_TAGS, (MANDATORY, browser_variant(source_unavailable).test)) if enabled else None


def freeze_browser_assets():
    assets = MappingProxyType({name: regular_bytes(ROOT / name, 65536) for name in PUBLIC_ASSETS})
    hashes = {name: hashlib.sha256(contents).hexdigest() for name, contents in assets.items()}
    digest = hashlib.sha256(json.dumps(hashes, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return assets, {"files": hashes, "combinedSHA256": digest}


def add_browser_assets(archive, assets):
    # Tar bytes and audit manifest use the SAME frozen map, never later reads
    # of a repository which another author may have legitimately changed.
    if set(assets) != set(PUBLIC_ASSETS) or any(type(value) is not bytes or not 0 < len(value) <= 65536 for value in assets.values()):
        raise BrowserFixtureFailure("BrowserPublicAssetSnapshotInvalid")
    for name in PUBLIC_ASSETS:
        contents = assets[name]
        info = tarfile.TarInfo("browser-assets/" + Path(name).name)
        info.size, info.mode = len(contents), 0o444
        archive.addfile(info, io.BytesIO(contents))


def compile_browser_binary(socket, directory, destination):
    # Preserve the accepted browser wrapper and proof shape; the common helper
    # has only TWO fixed internal profiles, never arbitrary tags or targets.
    from operator_kube_test_compiler import CompilerContractFailure, compile_profile
    try:
        return compile_profile("diagnostic-browser", socket, directory, destination)
    except CompilerContractFailure as error:
        raise BrowserFixtureFailure("BrowserCompiler" + str(error)) from None


def check_server_container(value, run, image, api, ui, frontend, connection):
    host, config = value["HostConfig"], value["Config"]
    if value["Image"] != image or config.get("Labels", {}).get(LABEL) != run or config.get("Labels", {}).get(KUBE_LABEL) != run or config.get("User") != "65532:65532" or host.get("NetworkMode") != api or set(value["NetworkSettings"]["Networks"]) != {api, ui}:
        raise BrowserFixtureFailure("BrowserServerIdentityInvalid")
    if not host.get("ReadonlyRootfs") or host.get("Privileged") or host.get("CapAdd") or set(host.get("CapDrop", [])) != {"ALL"} or host.get("Devices") or host.get("Binds") or host.get("PidMode") or host.get("IpcMode") != "private" or host.get("SecurityOpt") != ["no-new-privileges"]:
        raise BrowserFixtureFailure("BrowserServerPrivilegeInvalid")
    if host.get("Memory") != 768 * 1024**2 or host.get("MemorySwap") != 768 * 1024**2 or host.get("NanoCpus") != 2 * 10**9 or host.get("PidsLimit") != 128 or host.get("Tmpfs") != {"/tmp": "rw,noexec,nosuid,nodev,size=64m,uid=65532,gid=65532,mode=0700"}:
        raise BrowserFixtureFailure("BrowserServerResourcesInvalid")
    mounts = value.get("Mounts", [])
    binds = [mount for mount in mounts if mount.get("Type") == "bind"]
    if len(binds) != 1 or Path(binds[0]["Source"]).resolve() != connection.resolve() or binds[0].get("Destination") != "/connection.json" or binds[0].get("RW") or any(mount.get("Type") not in {"bind", "tmpfs"} or mount.get("Type") == "tmpfs" and mount.get("Destination") not in {"/tmp", "/dev/shm"} for mount in mounts):
        raise BrowserFixtureFailure("BrowserServerUnexpectedMount")
    endpoint = value["NetworkSettings"]["Networks"][ui]
    if endpoint.get("IPAMConfig", {}).get("IPv4Address") != frontend or endpoint.get("IPAddress", "") not in {"", frontend} or value.get("State", {}).get("Running") and endpoint.get("IPAddress") != frontend or any(value["NetworkSettings"].get("Ports", {}).values()):
        raise BrowserFixtureFailure("BrowserServerFrontendInvalid")
    required = {"OPERATOR_LOCAL_KUBE_CONNECTION", "OPERATOR_LOCAL_CLUSTER_RUN", "OPERATOR_LOCAL_BROWSER_FIXTURE"}
    keys = {item.split("=", 1)[0] for item in config.get("Env", [])}
    if not required <= keys or keys - required - {"PATH"}:
        raise BrowserFixtureFailure("BrowserServerEnvironmentInvalid")


def phase_record(value, phase):
    schemas = {
        "ready": {"phase", "run", "origin", "target", "cookie", "writerExited"},
        "revoked": {"phase", "sourceReads"},
        "finished": {"phase", "sourceReads", "managedPosts", "writerExited", "serverClosed"},
        "revoke": {"phase"}, "finish": {"phase"},
    }
    if not isinstance(value, dict) or phase not in schemas or set(value) != schemas[phase] or value.get("phase") != phase:
        raise BrowserFixtureFailure("BrowserPhaseInvalid")
    if phase in {"revoked", "finished"} and (type(value["sourceReads"]) is not int or value["sourceReads"] != 4):
        raise BrowserFixtureFailure("BrowserReadCountInvalid")
    if phase == "finished" and (value["managedPosts"] != 1 or type(value["managedPosts"]) is not int or value["writerExited"] is not True or value["serverClosed"] is not True):
        raise BrowserFixtureFailure("BrowserFinishEvidenceInvalid")
    return value


def source_phase_record(value, phase):
    if phase in ("ready", "finish"):
        return phase_record(value, phase)
    schemas = {
        "disconnect-source": {"phase"},
        "source-disconnected": {"phase", "sourceReads", "proxyClosed", "evidenceRetained"},
        "finished": {"phase", "sourceReads", "managedPosts", "writerExited", "serverClosed", "proxyClosed", "evidenceRetained"},
    }
    if not isinstance(value, dict) or phase not in schemas or set(value) != schemas[phase] or value.get("phase") != phase:
        raise BrowserFixtureFailure("BrowserSourcePhaseInvalid")
    if phase in ("source-disconnected", "finished"):
        if type(value["sourceReads"]) is not int or value["sourceReads"] != (2 if phase == "finished" else 1) or value["proxyClosed"] is not True or value["evidenceRetained"] is not True:
            raise BrowserFixtureFailure("BrowserSourceEvidenceInvalid")
    if phase == "finished" and (type(value["managedPosts"]) is not int or value["managedPosts"] != 1 or value["writerExited"] is not True or value["serverClosed"] is not True):
        raise BrowserFixtureFailure("BrowserSourceFinishInvalid")
    return value


def ui_network_absent(returncode, stdout, stderr, name):
    # Exact observed Docker29.9.2 result after successful owned removal; do not
    # treat daemon/permission errors, foreign names or arbitrary text as absence.
    return type(returncode) is int and returncode == 1 and stdout == "[]\n" and stderr == "Error response from daemon: network " + name + " not found\n"


def inspect_ui_network(docker, name):
    result = subprocess.run([docker.binary, "inspect", "--type=network", name], env=docker.env, capture_output=True, text=True, timeout=30)
    if result.returncode:
        if ui_network_absent(result.returncode, result.stdout, result.stderr, name):
            return None
        raise BrowserFixtureFailure("BrowserOwnedNetworkInspectionFailed")
    if len(result.stdout) > 65536 or result.stderr:
        raise BrowserFixtureFailure("BrowserOwnedNetworkInspectionOverflow")
    value = json.loads(result.stdout)
    if not isinstance(value, list) or len(value) != 1 or not isinstance(value[0], dict):
        raise BrowserFixtureFailure("BrowserOwnedNetworkInspectionInvalid")
    return value[0]


def finite_browser_failure(line, completed, *, source_unavailable=False):
    variant = browser_variant(source_unavailable)
    steps = (*variant.cases, "bootstrap", "unknown")
    pattern = r"FAIL " + variant.marker + " (" + "|".join(map(re.escape, steps)) + r") (AssertionOrBoundaryFailed|CloseUnconfirmed)"
    match = re.fullmatch(pattern, line)
    if match is None or list(completed) != list(variant.cases[:len(completed)]):
        raise BrowserFixtureFailure("BrowserPageFailureRecordInvalid")
    return {"step": match[1], "code": match[2], "completedCases": list(completed), "completedCount": len(completed)}


def owned_network(value, run):
    if value.get("Labels", {}).get(KUBE_LABEL) != run or not value.get("Internal") or value.get("Driver") != "bridge" or value.get("EnableIPv6") or value.get("Options", {}).get("com.docker.network.bridge.gateway_mode_ipv4") != "isolated":
        raise BrowserFixtureFailure("BrowserNetworkIsolationInvalid")
    configs = value.get("IPAM", {}).get("Config", [])
    if len(configs) != 1 or configs[0].get("Gateway"):
        raise BrowserFixtureFailure("BrowserNetworkGatewayPresent")
    network = ipaddress.ip_network(configs[0]["Subnet"])
    if network.version != 4 or network.num_addresses < 16 or not network.is_private:
        raise BrowserFixtureFailure("BrowserNetworkSubnetInvalid")
    return network


def check_browser_container(value, run, image_id, network, address, profile):
    config, host = value["Config"], value["HostConfig"]
    required = {"NetworkMode": network, "ReadonlyRootfs": True, "IpcMode": "private", "ShmSize": 256 * 1024**2, "Memory": 1024**3, "MemorySwap": 1024**3, "NanoCpus": 2 * 10**9, "PidsLimit": 192, "Tmpfs": {"/tmp": TMP_OPTIONS}}
    if value["Image"] != image_id or config.get("Labels", {}).get(LABEL) != run or config.get("User") != "1001:1001" or any(host.get(key) != expected for key, expected in required.items()):
        raise BrowserFixtureFailure("BrowserContainerBoundaryInvalid")
    if host.get("Privileged") or host.get("Binds") or host.get("Devices") or host.get("CapAdd") or host.get("PidMode") or host.get("Dns") != ["127.0.0.1"] or set(host.get("CapDrop", [])) != {"ALL"} or any(m.get("Type") != "tmpfs" or m.get("Destination") not in {"/tmp", "/dev/shm"} for m in value.get("Mounts", [])):
        raise BrowserFixtureFailure("BrowserContainerPrivilegeOrMountInvalid")
    security = host.get("SecurityOpt", [])
    profiles = [item.removeprefix("seccomp=") for item in security if item.startswith("seccomp=")]
    if "no-new-privileges" not in security or len(security) != 2 or len(profiles) != 1 or json.loads(profiles[0]) != json.loads(profile):
        raise BrowserFixtureFailure("BrowserContainerSeccompInvalid")
    networks = value["NetworkSettings"]["Networks"]
    endpoint = networks.get(network, {})
    if set(networks) != {network} or endpoint.get("IPAMConfig", {}).get("IPv4Address") != address or endpoint.get("IPAddress", "") not in {"", address} or value.get("State", {}).get("Running") and endpoint.get("IPAddress") != address or any(value["NetworkSettings"].get("Ports", {}).values()):
        raise BrowserFixtureFailure("BrowserContainerFrontendMembershipInvalid")
    if {item.split("=", 1)[0] for item in config.get("Env", [])} != {"PATH", "LANG", "LC_ALL", "PLAYWRIGHT_BROWSERS_PATH", "HOME", "TMPDIR", "XDG_CACHE_HOME"}:
        raise BrowserFixtureFailure("BrowserContainerEnvironmentInvalid")


class PrivateProcess:
    """Bounded private streams; only exact protocol/case records leave here."""
    def __init__(self, argv, env, deadline, kind):
        process_variant(kind)  # Reject unknown kinds before any process starts.
        self.process = subprocess.Popen(argv, env=env, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.deadline, self.kind = deadline, kind
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.process.stdout, selectors.EVENT_READ, "out")
        self.selector.register(self.process.stderr, selectors.EVENT_READ, "err")
        self.buffer, self.total, self.lines, self.cases = b"", 0, [], []

    def send(self, value):
        data = json.dumps(value, separators=(",", ":")).encode() + b"\n"
        if len(data) > 8192:
            raise BrowserFixtureFailure("BrowserProtocolWriteOverflow")
        try:
            self.process.stdin.write(data)
            self.process.stdin.flush()
        except (BrokenPipeError, OSError):
            raise BrowserFixtureFailure("BrowserProtocolWriteFailed") from None

    def next_line(self):
        while True:
            if b"\n" in self.buffer:
                line, self.buffer = self.buffer.split(b"\n", 1)
                if len(line) > 8192:
                    raise BrowserFixtureFailure("BrowserProtocolLineOverflow")
                return line.decode("utf8", errors="strict")
            remaining = self.deadline - time.monotonic()
            if remaining <= 0:
                raise BrowserFixtureFailure("BrowserFixtureDeadlineExceeded")
            if not self.selector.get_map():
                if self.buffer:
                    raise BrowserFixtureFailure("BrowserProtocolPartialLine")
                return None
            for key, _ in self.selector.select(min(remaining, 1)):
                data = os.read(key.fileobj.fileno(), 65536)
                if not data:
                    self.selector.unregister(key.fileobj)
                    continue
                self.total += len(data)
                if self.total > 1024**2:
                    raise BrowserFixtureFailure("BrowserPrivateOutputOverflow")
                # stderr may contain cookie/DOM/argv diffs. Count and discard.
                if key.data == "out":
                    self.buffer += data

    def accept_line(self, line):
        variant = process_variant(self.kind)
        if self.kind in ("browser", "browser-source"):
            if line.startswith("PASS " + variant.marker + " "):
                name = line.removeprefix("PASS " + variant.marker + " ")
                if len(self.cases) >= len(variant.cases) or name != variant.cases[len(self.cases)]:
                    raise BrowserFixtureFailure("BrowserPageCaseOrderInvalid")
                self.cases.append(name)
            elif line.startswith("FAIL " + variant.marker + " "):
                # Reconstruct only allowlisted enums/counts, NEVER echo the
                # original private line or a thrown/Playwright error object.
                failure = finite_browser_failure(line, self.cases, source_unavailable=self.kind == "browser-source")
                print("Finite diagnostic-browser failure " + json.dumps(failure, sort_keys=True), flush=True)
                raise BrowserFixtureFailure("BrowserPageAssertionFailed:" + failure["step"] + ":" + failure["code"] + ":completed=" + str(failure["completedCount"]))
            else:
                raise BrowserFixtureFailure("BrowserPageOutputInvalid")
        else:
            if re.fullmatch(r"--- PASS: (?:" + MANDATORY + "|" + variant.test + r") \([0-9]+\.[0-9]+s\)", line):
                self.lines.append(line)
            elif "--- FAIL:" in line or "--- SKIP:" in line or re.fullmatch(r"\s+\S+\.go:[0-9]+:\s+Browser[A-Za-z]+", line):
                raise BrowserFixtureFailure("BrowserGoFixtureAssertionFailed")
            # Other Go harness lines are private and discarded, never echoed.

    def record(self, phase):
        variant = process_variant(self.kind)
        prefix = variant.node_prefix if self.kind in ("browser", "browser-source") else variant.go_prefix
        while (line := self.next_line()) is not None:
            if line.startswith(prefix):
                try:
                    value = json.loads(line.removeprefix(prefix))
                except (ValueError, TypeError):
                    raise BrowserFixtureFailure("BrowserProtocolJSONInvalid") from None
                return source_phase_record(value, phase) if self.kind in ("go-source", "browser-source") else phase_record(value, phase)
            self.accept_line(line)
        raise BrowserFixtureFailure("BrowserProtocolUnexpectedExit")

    def finish(self):
        self.process.stdin.close()
        while (line := self.next_line()) is not None:
            self.accept_line(line)
        if self.process.wait(timeout=max(0.1, self.deadline - time.monotonic())) != 0:
            raise BrowserFixtureFailure("BrowserChildExitFailed")

    def close(self):
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=10)
        self.selector.close()
        for stream in (self.process.stdin, self.process.stdout, self.process.stderr):
            if not stream.closed:
                stream.close()


def run_browser_fixture(socket, temporary, run, api_network, node_ip, gateway, go_image, connection, *, source_unavailable=False):
    """Only called by finite owned-kind browser mode after node identity proof."""
    variant = browser_variant(source_unavailable)
    home = temporary / "browser-home"
    home.mkdir(mode=0o700)
    docker = Docker(socket, home)
    base = docker.verify_base()
    profile = browser_seccomp(regular_bytes(CACHE / "seccomp.json", 65536))
    profile_path = temporary / "browser-seccomp.json"
    profile_path.write_bytes(profile)
    profile_path.chmod(0o444)
    snapshot = {"reviewed/" + name: contents for name, contents in driver_files(regular_bytes(CACHE / "playwright-core-1.56.0.tgz", 4 * 1024**2)).items()}
    for name in variant.scripts:
        snapshot["reviewed/" + name] = regular_bytes(ROOT / name, 65536)
    digest = hashlib.sha256(json.dumps({name: hashlib.sha256(data).hexdigest() for name, data in sorted(snapshot.items())}, sort_keys=True).encode()).hexdigest()
    ui, stage, server, browser, image = run + "-ui", run + "-browser-stage", run + "-browser-server", run + "-browser", "operator-local-browser:" + run
    ids, expected_images, memberships = {}, {}, {}
    image_id, primary, ui_created, commit_attempted = None, None, False, False
    processes = []
    phase = "BrowserPreparation"

    def inspect_owned(name):
        value = docker.inspect_optional("container", name)
        if value is not None:
            networks = set(value["NetworkSettings"]["Networks"])
            partial = name == server and networks in ({api_network}, {api_network, ui}) or name == stage and networks <= {"none"} and value["HostConfig"].get("NetworkMode") == "none"
            if value["Name"] != "/" + name or value["Config"].get("Labels", {}).get(LABEL) != run or value["Image"] != expected_images[name] or name in ids and value["Id"] != ids[name] or networks != memberships[name] and not partial:
                raise BrowserFixtureFailure("BrowserOwnedContainerIdentityChanged")
        return value

    def running(name):
        end = time.monotonic() + 10
        while time.monotonic() < end:
            value = inspect_owned(name)
            if value is None:
                raise BrowserFixtureFailure("BrowserOwnedContainerMissing")
            if value["State"]["Running"]:
                return value
            if value["State"]["Status"] == "exited":
                raise BrowserFixtureFailure("BrowserOwnedContainerPrematureExit")
            time.sleep(0.05)
        raise BrowserFixtureFailure("BrowserOwnedContainerStartUnconfirmed")

    try:
        ui_created = True
        docker.command(["network", "create", "--internal", "--label", KUBE_LABEL + "=" + run, "--opt", "com.docker.network.bridge.gateway_mode_ipv4=isolated", ui])
        subnet = owned_network(json.loads(docker.command(["network", "inspect", ui]))[0], run)
        api_subnet = owned_network(json.loads(docker.command(["network", "inspect", api_network]))[0], run)
        if subnet.overlaps(api_subnet):
            raise BrowserFixtureFailure("BrowserNetworkSubnetsOverlap")
        frontend_ip, browser_ip = str(subnet.network_address + 1), str(subnet.network_address + 2)
        expected_images[stage], memberships[stage] = base["Id"], {"none"}
        # NetworkSettings reports an empty map for network=none on some Engine
        # releases; verify HostConfig none explicitly, not by broad membership.
        ids[stage] = docker.command(["create", "--pull=never", "--network=none", "--name", stage, "--label", LABEL + "=" + run, "--entrypoint", "/usr/bin/true", base["Id"]]).strip()
        staged = docker.inspect_optional("container", stage)
        memberships[stage] = set(staged["NetworkSettings"]["Networks"])
        if staged["HostConfig"]["NetworkMode"] != "none" or inspect_owned(stage)["State"]["Status"] != "created":
            raise BrowserFixtureFailure("BrowserStagingExecuted")
        result = subprocess.run([docker.binary, "cp", "-", stage + ":/"], env=docker.env, input=stage_tar(snapshot), capture_output=True, timeout=120)
        if result.returncode:
            raise BrowserFixtureFailure("BrowserPublicImageCopyFailed")
        commit_attempted = True
        image_id = docker.command(["commit", "--change", "USER 1001:1001", "--change", "LABEL " + LABEL + "=" + run, "--change", "LABEL operator.local/source-digest=" + digest, stage, image], timeout=180).strip()
        inspect_owned(stage)
        docker.command(["rm", stage])
        expected_images[browser], memberships[browser] = image_id, {ui}
        ids[browser] = docker.command(["create", "-i", "--pull=never", "--name", browser, "--label", LABEL + "=" + run, "--network", ui, "--ip", browser_ip, "--dns", "127.0.0.1", "--read-only", "--cap-drop=ALL", "--security-opt", "no-new-privileges", "--security-opt", "seccomp=" + str(profile_path), "--user", "1001:1001", "--ipc=private", "--shm-size=256m", "--tmpfs", "/tmp:" + TMP_OPTIONS, "--pids-limit=192", "--memory=1g", "--memory-swap=1g", "--cpus=2", "--env", "HOME=/tmp/home", "--env", "TMPDIR=/tmp", "--env", "XDG_CACHE_HOME=/tmp/cache", "--entrypoint", "/usr/bin/node", image_id, variant.entrypoint]).strip()
        check_browser_container(inspect_owned(browser), run, image_id, ui, browser_ip, profile)
        export_proof(docker, browser, snapshot)
        print("Immutable diagnostic-browser public snapshot sha256:" + digest + "; derived seccomp sha256:" + hashlib.sha256(profile).hexdigest() + "; browser source/native boundary awaits execution.", flush=True)
        expected_images[server], memberships[server] = go_image, {api_network}
        ids[server] = docker.command(["create", "-i", "--pull=never", "--name", server, "--label", LABEL + "=" + run, "--network", api_network, "--read-only", "--cap-drop=ALL", "--security-opt", "no-new-privileges", "--ipc=private", "--dns", "127.0.0.1", "--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=64m,uid=65532,gid=65532,mode=0700", "--pids-limit=128", "--memory=768m", "--memory-swap=768m", "--cpus=2", "--mount", "type=bind,source=" + str(connection) + ",target=/connection.json,readonly", "--env", "OPERATOR_LOCAL_KUBE_CONNECTION=/connection.json", "--env", "OPERATOR_LOCAL_CLUSTER_RUN=" + run, "--env", "OPERATOR_LOCAL_BROWSER_FIXTURE=1", go_image, "-test.v", "-test.timeout=420s", "-test.run=^(" + MANDATORY + "|" + variant.test + ")$"]).strip()
        inspect_owned(server)
        docker.command(["network", "connect", "--ip", frontend_ip, ui, server])
        memberships[server] = {api_network, ui}
        check_server_container(inspect_owned(server), run, go_image, api_network, ui, frontend_ip, connection)
        deadline = time.monotonic() + 450
        phase = "BrowserFailedWriterAndReaderBootstrap"
        go_process = PrivateProcess([docker.binary, "start", "--attach", "--interactive", server], docker.env, deadline, "go-source" if source_unavailable else "go")
        processes.append(go_process)
        check_server_container(running(server), run, go_image, api_network, ui, frontend_ip, connection)
        go_process.send({"phase": "start", "run": run, "frontendIP": frontend_ip})
        ready = go_process.record("ready")
        if ready["run"] != run or ready["origin"] != "http://" + frontend_ip + ":8080" or ready["writerExited"] is not True:
            raise BrowserFixtureFailure("BrowserReadyOriginIdentityInvalid")
        phase = "BrowserNativeAndDOM"
        node_process = PrivateProcess([docker.binary, "start", "--attach", "--interactive", browser], docker.env, deadline, "browser-source" if source_unavailable else "browser")
        processes.append(node_process)
        check_browser_container(running(browser), run, image_id, ui, browser_ip, profile)
        node_process.send({**ready, "frontendIP": frontend_ip, "browserIP": browser_ip, "nodeIP": node_ip, "hostGateway": gateway})
        if source_unavailable:
            node_process.record("disconnect-source")
            go_process.send({"phase": "disconnect-source"})
            disconnected = go_process.record("source-disconnected")
            node_process.send(disconnected)
            phase = "BrowserSourceConnectorUnavailable"
        else:
            node_process.record("revoke")
            go_process.send({"phase": "revoke"})
            revoked = go_process.record("revoked")
            node_process.send(revoked)
            phase = "BrowserLiveRevocation"
        node_process.record("finish")
        go_process.send({"phase": "finish"})
        finished = go_process.record("finished")
        if not source_unavailable and finished["sourceReads"] != revoked["sourceReads"]:
            raise BrowserFixtureFailure("BrowserRevokedSourceReadChanged")
        node_process.send(finished)
        node_process.finish()
        go_process.finish()
        expected_go = (MANDATORY, variant.test)
        passed_go = [re.fullmatch(r"--- PASS: (Test[A-Za-z0-9_]+) \([0-9]+\.[0-9]+s\)", line)[1] for line in go_process.lines]
        if node_process.cases != list(variant.cases) or len(passed_go) != 2 or any(passed_go.count(name) != 1 for name in expected_go):
            raise BrowserFixtureFailure("BrowserExactCasesIncomplete")
        for name in (server, browser):
            value = inspect_owned(name)
            if value["State"]["Running"] or value["State"]["ExitCode"] != 0 or value["State"].get("OOMKilled"):
                raise BrowserFixtureFailure("BrowserOwnedProcessExitInvalid")
        check_browser_container(inspect_owned(browser), run, image_id, ui, browser_ip, profile)
        if source_unavailable:
            print("PASS exact7 diagnostic-browser-source-unavailable assertions; own native sandbox, authorized source503, retained journal evidence and protected DOM clear; raw private stdio suppressed.", flush=True)
        else:
            print("PASS exact10 diagnostic-browser assertions; native frontend/API/host denial and real live policy revocation; raw private stdio suppressed.", flush=True)
        return "\n".join(go_process.lines) + "\n"
    except BaseException as error:
        detail = str(error) if isinstance(error, BrowserFixtureFailure) else "Failed"
        primary = BrowserFixtureFailure(phase + ":" + detail)
        raise primary from None
    finally:
        cleanup = []
        for process in processes:
            try:
                process.close()
            except Exception:
                cleanup.append("BrowserOwnedAttachCleanupUnconfirmed")
        for name in (browser, server, stage):
            if name not in expected_images:
                continue
            try:
                if inspect_owned(name) is not None:
                    docker.command(["rm", "--force", name])
                    if docker.inspect_optional("container", name) is not None:
                        raise BrowserFixtureFailure("BrowserContainerRemovalUnconfirmed")
            except Exception:
                cleanup.append("BrowserOwnedContainerCleanupUnconfirmed")
        if commit_attempted:
            try:
                value = docker.inspect_optional("image", image)
                if value is not None:
                    if image_id is not None and value["Id"] != image_id or value["Config"].get("Labels", {}).get(LABEL) != run or value["Config"].get("Labels", {}).get("operator.local/source-digest") != digest:
                        raise BrowserFixtureFailure("BrowserImageOwnershipChanged")
                    docker.command(["image", "rm", image])
                    if docker.inspect_optional("image", image) is not None:
                        raise BrowserFixtureFailure("BrowserImageRemovalUnconfirmed")
            except Exception:
                cleanup.append("BrowserOwnedImageCleanupUnconfirmed")
        if ui_created:
            try:
                value = inspect_ui_network(docker, ui)
                if value is not None:
                    owned_network(value, run)
                    if value.get("Containers"):
                        raise BrowserFixtureFailure("BrowserNetworkNotEmpty")
                    docker.command(["network", "rm", ui])
                    if inspect_ui_network(docker, ui) is not None:
                        raise BrowserFixtureFailure("BrowserNetworkRemovalUnconfirmed")
            except Exception:
                cleanup.append("BrowserOwnedNetworkCleanupUnconfirmed")
        if cleanup:
            detail = ";".join(cleanup)
            if primary is None:
                raise BrowserFixtureFailure(detail)
            primary.add_note(detail)
        else:
            print("Owned diagnostic-browser containers/image/frontend network cleanup verified.", flush=True)
