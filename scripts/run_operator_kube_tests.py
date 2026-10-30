#!/usr/bin/env python3
"""Run real Kubernetes API/controller tests on an owned, isolated kind cluster.

Acquire public prerequisites separately: python3 scripts/prepare_operator_kube_tests.py
Execute: python3 scripts/run_operator_kube_tests.py
Requires Docker Engine 28+ with internal isolated gateway support, a local Unix
socket, Go, and the prepared kind/node image. The test container receives only
owned local credentials and no Docker socket. No ambient kubeconfig is read.

The pinned kind custom-network mode is experimental. With isolated gateways,
kind prints a terminal host-kubeconfig export error because no API port is
published. Only that exact error is recognized; the runner exports the internal
kubeconfig and independently verifies the owned API and controllers. Other
bootstrap errors fail. The synthetic node hosts fixture enables no host access.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import ipaddress
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
import uuid

from prepare_operator_kube_tests import NODE
from verify_operator_fixture_image import verify_fixture_image

ROOT = Path(__file__).resolve().parents[1]
LABEL = "operator.local/run-id"
BOUNDARY_TEST = "TestOwnedAPIAndNetworkBoundary"


def selected_tests(value):
    names = value.split(",")
    if len(names) > 16 or len(set(names)) != len(names) or any(len(name) > 128 or re.fullmatch(r"Test[A-Za-z0-9_]+", name, flags=re.ASCII) is None for name in names):
        raise argparse.ArgumentTypeError("Use 1–16 distinct exact ASCII top-level TestName values, comma-separated")
    return tuple(dict.fromkeys([BOUNDARY_TEST, *names]))


def verify_selected_results(output, names):
    passed = re.findall(r"^--- PASS: (Test[A-Za-z0-9_]+) \(", output, flags=re.MULTILINE | re.ASCII)
    if re.search(r"^\s*--- SKIP:", output, flags=re.MULTILINE) or any(passed.count(name) != 1 for name in names):
        raise RuntimeError("Selected API proof did not PASS each exact requested test once without skips")


def test_arguments(boundary_only=False, tests=None):
    requested = tests or ((BOUNDARY_TEST,) if boundary_only else ())
    seconds, container_seconds = (420, 450) if requested else (900, 930)
    arguments = ["-test.v", f"-test.timeout={seconds}s"]
    if requested:
        arguments.append("-test.run=^(" + "|".join(requested) + ")$")
    return arguments, container_seconds


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--docker-socket", default="/var/run/docker.sock")
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--boundary-only", action="store_true", help="Verify owned API/network/immutable fixture-image boundary only; do not run workload contracts")
    selection.add_argument("--tests", type=selected_tests, help="Comma-separated exact top-level Go test names (1–16); always includes the owned boundary and requires each PASS without skips")
    selection.add_argument("--diagnostic-browser", action="store_true", help="Finite owned browser+read-only diagnostic page proof only; includes mandatory API boundary, uses native pinned Linux compiler and preacquired browser, no command/origin/tag overrides")
    selection.add_argument("--diagnostic-browser-source-unavailable", action="store_true", help="Separate finite owned source-connector outage browser proof; same pinned browser compiler/assets, mandatory boundary, exactly two authorized manual source reads and retained real journal evidence")
    selection.add_argument("--candidate-preparation", action="store_true", help="Finite owned SA/D/S private Plan Waiting/process-handoff proof plus separate SA receipt fault; includes mandatory API boundary and exact native compiler profile, no generic commands/tags")
    selection.add_argument("--diagnostic-read-local", action="store_true", help="Privileged operator-local read-only diagnostic OS command proof; fixed two native artifacts, owned FD3, live policy revocation, no command or credential overrides")
    args = parser.parse_args()
    if args.diagnostic_browser or args.diagnostic_browser_source_unavailable:
        from operator_browser_kube_fixture import compilation_mode
        args.tests = compilation_mode(True, source_unavailable=args.diagnostic_browser_source_unavailable)[2]
    elif args.candidate_preparation:
        from operator_preparation_test_contract import preparation_mode
        args.tests = preparation_mode(True)
    elif args.diagnostic_read_local:
        from operator_diagnostic_cli_fixture import TESTS
        args.tests = TESTS
    test_flags, container_timeout = test_arguments(args.boundary_only, args.tests)
    docker, go = shutil.which("docker"), shutil.which("go")
    kind = ROOT / ".venv/tools/kind"
    docker_socket = Path(args.docker_socket).resolve()
    if not docker or not go or not kind.is_file() or not docker_socket.is_socket():
        parser.error("Prepare existing local Docker Unix socket, Go and pinned kind first; see --help")
    run_id = "operator-local-" + uuid.uuid4().hex[:12]
    network, node = run_id, run_id + "-control-plane"
    with tempfile.TemporaryDirectory(prefix="operator-kube-") as directory:
        temporary = Path(directory)
        home = temporary / "home"
        home.mkdir()
        environment = {
            "PATH": os.pathsep.join([str(Path(docker).parent), str(Path(go).parent), os.defpath]),
            "HOME": str(home), "DOCKER_CONFIG": str(home),
            "DOCKER_HOST": "unix://" + str(docker_socket),
            "KUBECONFIG": str(temporary / "kubeconfig"),
            "KIND_EXPERIMENTAL_PROVIDER": "docker", "KIND_EXPERIMENTAL_DOCKER_NETWORK": network,
            "GOTOOLCHAIN": "local", "GOPROXY": "off", "GOSUMDB": "off", "CGO_ENABLED": "0",
            "GOCACHE": str(ROOT / ".venv/operator-go-cache"), "GOMODCACHE": str(ROOT / ".venv/operator-go-modules"),
        }

        def command(arguments, *, data=None, capture=True, timeout=300):
            result = subprocess.run(list(map(str, arguments)), env=environment, input=data,
                                    capture_output=capture, text=True, timeout=timeout, check=False)
            if result.returncode:
                if capture:
                    print((result.stderr or result.stdout)[-6000:])
                raise RuntimeError("Local test command failed: " + str(arguments[0]))
            return result.stdout if capture else ""

        version = json.loads(command([docker, "version", "--format", "{{json .Server}} "]))
        if int(version["Version"].split(".")[0]) < 28:
            parser.error("Docker Engine 28+ isolated bridge mode is required; no weaker fallback")
        architecture = version["Arch"]
        if architecture not in {"arm64", "amd64"}:
            parser.error("Unsupported Docker architecture")
        node_image_id = json.loads(command([docker, "image", "inspect", NODE]))[0]["Id"]
        print(f"Owned cluster {run_id}; Docker {version['Version']}; acquisition is disabled during execution.", flush=True)
        browser_assets = browser_asset_proof = browser_compiler_proof = preparation_compiler_proof = diagnostic_cli_proof = None
        if args.diagnostic_browser or args.diagnostic_browser_source_unavailable:
            from operator_browser_kube_fixture import freeze_browser_assets
            browser_assets, browser_asset_proof = freeze_browser_assets()
        compile_environment = {**environment, "GOOS": "linux", "GOARCH": architecture}
        for target, package, test in (("fixture", "./integration/fixture", False), ("integration.test", "./integration", True)):
            if test and (args.diagnostic_browser or args.diagnostic_browser_source_unavailable):
                from operator_browser_kube_fixture import compile_browser_binary
                browser_compiler_proof = compile_browser_binary(args.docker_socket, temporary / "browser-compiler", temporary / target)
                continue
            if test and args.candidate_preparation:
                from operator_preparation_test_contract import compile_preparation_binary
                preparation_compiler_proof = compile_preparation_binary(args.docker_socket, temporary / "preparation-compiler", temporary / target)
                continue
            if test and args.diagnostic_read_local:
                from operator_kube_test_compiler import compile_local_diagnostics
                diagnostic_cli_proof = compile_local_diagnostics(args.docker_socket, temporary / "diagnostic-cli-compiler", temporary / target, temporary / "registry-diagnostics")
                continue
            arguments = [go, "test", "-c", "-tags", "integration"] if test else [go, "build"]
            arguments += ["-o", str(temporary / target), package]
            subprocess.run(arguments, cwd=ROOT / "operator-v2", env=compile_environment, check=True, timeout=180)
        images = []
        image_ids = {}
        network_created = False
        cluster_created = False
        test_container = run_id + "-tests"
        primary_failure = None
        try:
            for binary, purpose in (("fixture", "fixture"), ("integration.test", "tests")):
                image = f"operator-local-{purpose}:git-{run_id.removeprefix('operator-local-')}"
                archive = temporary / (purpose + ".tar")
                with tarfile.open(archive, "w") as tar:
                    info = tarfile.TarInfo(binary)
                    contents = (temporary / binary).read_bytes()
                    if binary == "integration.test" and args.diagnostic_read_local and hashlib.sha256(contents).hexdigest() != diagnostic_cli_proof["staticELFSHA256"]:
                        raise RuntimeError("Fixed diagnostic integration artifact changed before image import")
                    info.size, info.mode = len(contents), 0o755
                    tar.addfile(info, io.BytesIO(contents))
                    if binary == "integration.test" and (args.diagnostic_browser or args.diagnostic_browser_source_unavailable):
                        from operator_browser_kube_fixture import add_browser_assets
                        add_browser_assets(tar, browser_assets)
                    if binary == "integration.test" and args.diagnostic_read_local:
                        from operator_diagnostic_cli_fixture import add_command_artifact
                        add_command_artifact(tar, temporary / "registry-diagnostics", diagnostic_cli_proof)
                image_id = command([docker, "image", "import", "--platform", "linux/" + architecture,
                         "--change", f'ENTRYPOINT ["/{binary}"]', "--change", "USER 65532:65532",
                         "--change", f"LABEL {LABEL}={run_id}", archive, image]).strip()
                images.append(image)
                image_ids[image] = image_id
                if binary == "integration.test" and (args.diagnostic_browser or args.diagnostic_browser_source_unavailable):
                    print("Frozen diagnostic-browser Go image proof " + json.dumps({"testImageID": image_id, "publicAssets": browser_asset_proof, **browser_compiler_proof}, sort_keys=True), flush=True)
                elif binary == "integration.test" and args.candidate_preparation:
                    print("Frozen candidate-preparation Go image proof " + json.dumps({"testImageID": image_id, **preparation_compiler_proof}, sort_keys=True), flush=True)
                elif binary == "integration.test" and args.diagnostic_read_local:
                    print("Frozen privileged diagnostic CLI image proof " + json.dumps({"testImageID": image_id, **diagnostic_cli_proof}, sort_keys=True), flush=True)
            command([docker, "network", "create", "--internal", "--label", f"{LABEL}={run_id}",
                     "--opt", "com.docker.network.bridge.gateway_mode_ipv4=isolated", network])
            network_created = True
            details = json.loads(command([docker, "network", "inspect", network]))[0]
            if not details["Internal"] or details["Options"].get("com.docker.network.bridge.gateway_mode_ipv4") != "isolated":
                raise RuntimeError("Requested network isolation was not established")
            subnet = ipaddress.ip_network(details["IPAM"]["Config"][0]["Subnet"])
            # Isolated gateway mode reserves no bridge/gateway address; the
            # first and only node receives the first usable address.
            expected_node_ip = str(subnet.network_address + 1)
            hosts = temporary / "node-hosts"
            hosts.write_text(f"127.0.0.1 localhost\n::1 localhost\n192.0.2.1 host.docker.internal\n{expected_node_ip} {node}\n")
            config = temporary / "kind.yaml"
            # kind's entrypoint requires a nonempty DNS rewrite destination.
            # Reserved TEST-NET disables host-DNS forwarding without adding a
            # gateway or changing host/Docker configuration.
            config.write_text("kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnetworking:\n  apiServerAddress: 127.0.0.1\nnodes:\n- role: control-plane\n  extraMounts:\n  - hostPath: " + str(hosts) + "\n    containerPath: /etc/hosts\n    readOnly: true\n")
            # Set before launch so finally also cleans partially created nodes.
            cluster_created = True
            created = subprocess.run([
                str(kind), "create", "cluster", "--name", run_id, "--image", NODE,
                "--config", str(config), "--kubeconfig", environment["KUBECONFIG"],
                "--wait", "0s", "--retain",
            ], env=environment, capture_output=True, text=True, timeout=300)
            print(created.stdout + created.stderr, flush=True)
            if created.returncode:
                known_export_failure = "ERROR: failed to create cluster: failed to get api server port:" in created.stderr and "slice index out of range" in created.stderr
                if not known_export_failure:
                    raise RuntimeError("Owned cluster bootstrap failed")
                print("Isolated network has no published API port; verifying the internal API directly instead of exporting a host kubeconfig.", flush=True)
            inspected = json.loads(command([docker, "inspect", node]))[0]
            if inspected["Image"] != node_image_id or inspected["Config"]["Labels"].get("io.x-k8s.kind.cluster") != run_id or set(inspected["NetworkSettings"]["Networks"]) != {network}:
                raise RuntimeError("Owned node/network provenance verification failed")
            node_ip = inspected["NetworkSettings"]["Networks"][network]["IPAddress"]
            if node_ip != expected_node_ip:
                raise RuntimeError("Owned node address differs from isolated bootstrap mapping")
            if any(inspected["NetworkSettings"].get("Ports", {}).values()):
                raise RuntimeError("Refusing a cluster with a published host endpoint")
            command([kind, "export", "kubeconfig", "--name", run_id, "--internal",
                     "--kubeconfig", environment["KUBECONFIG"]])
            command([kind, "load", "docker-image", "--name", run_id, images[0]], capture=False)
            image_proof = verify_fixture_image(docker, environment, node, network, run_id, architecture,
                                               images[0], image_ids[images[0]], node_image_id, inspected["Id"])
            print("Verified owned native OCI manifest/config and local immutable CRI fixture alias.", flush=True)
            kubectl = [docker, "exec", node, "kubectl", "--kubeconfig=/etc/kubernetes/admin.conf"]
            command([*kubectl, "wait", "--for=condition=Ready", "nodes", "--all", "--timeout=120s"])
            command([*kubectl, "-n", "kube-system", "rollout", "status", "deployment/coredns", "--timeout=120s"])
            command([*kubectl, "-n", "kube-system", "create", "configmap", "operator-local-proof", "--from-literal=run=" + run_id])
            namespace_uid = json.loads(command([*kubectl, "get", "namespace", "kube-system", "-o", "json"]))["metadata"]["uid"]
            configuration = json.loads(command([*kubectl, "config", "view", "--raw", "-o", "json"]))
            cluster, user = configuration["clusters"][0]["cluster"], configuration["users"][0]["user"]
            bridge = json.loads(command([docker, "network", "inspect", "bridge"]))[0]
            gateway = bridge["IPAM"]["Config"][0]["Gateway"]
            connection = temporary / "connection.json"
            connection.write_text(json.dumps({
                "server": f"https://{node_ip}:6443", "nodeIp": node_ip,
                "ca": cluster["certificate-authority-data"], "certificate": user["client-certificate-data"],
                "key": user["client-key-data"], "runId": run_id, "namespaceUid": namespace_uid,
                "image": images[0], "hostGateway": gateway,
                **image_proof,
            }))
            connection.chmod(0o444)
            if args.diagnostic_browser or args.diagnostic_browser_source_unavailable:
                from operator_browser_kube_fixture import run_browser_fixture
                output = run_browser_fixture(args.docker_socket, temporary, run_id, network, node_ip, gateway, image_ids[images[1]], connection, source_unavailable=args.diagnostic_browser_source_unavailable)
                print(output, end="", flush=True)
                verify_selected_results(output, args.tests)
                return 0
            if args.candidate_preparation:
                from operator_preparation_test_contract import run_preparation_proof
                output = run_preparation_proof(docker, environment, run_id, network, image_ids[images[1]], connection)
                print(output, end="", flush=True)
                verify_selected_results(output, args.tests)
                return 0
            if args.diagnostic_read_local:
                from operator_diagnostic_cli_fixture import run_diagnostic_proof
                output = run_diagnostic_proof(docker, environment, run_id, network, image_ids[images[1]], connection, diagnostic_cli_proof)
                print(output, end="", flush=True)
                verify_selected_results(output, args.tests)
                return 0
            output = command([docker, "run", "--rm", "--name", test_container, "--network", network,
                     "--read-only", "--cap-drop=ALL", "--security-opt", "no-new-privileges",
                     "--dns", "127.0.0.1", "--tmpfs", "/tmp:rw,noexec,nosuid,size=32m",
                     "--mount", f"type=bind,source={connection},target=/connection.json,readonly",
                     "--env", "OPERATOR_LOCAL_KUBE_CONNECTION=/connection.json",
                     "--env", "OPERATOR_LOCAL_CLUSTER_RUN=" + run_id,
                     images[1], *test_flags], capture=bool(args.tests), timeout=container_timeout)
            if args.tests:
                print(output, end="", flush=True)
                verify_selected_results(output, args.tests)
            return 0
        except Exception as error:
            primary_failure = error
            if args.diagnostic_browser or args.diagnostic_browser_source_unavailable or args.candidate_preparation or args.diagnostic_read_local:
                print("Owned private fixture failed; child/provider output suppressed.", flush=True)
            else:
                diagnostic = subprocess.run([docker, "logs", "--tail", "60", node], env=environment, capture_output=True, text=True, check=False)
                print("Owned node failure evidence:\n" + (diagnostic.stdout + diagnostic.stderr)[-6000:])
            raise
        finally:
            cleanup_errors = []

            def cleanup(arguments, missing_ok=False):
                try:
                    result = subprocess.run(list(map(str, arguments)), env=environment, capture_output=True, text=True, timeout=90, check=False)
                    missing = "No such" in result.stderr or "not found" in result.stderr
                    if result.returncode and not (missing_ok and missing):
                        cleanup_errors.append((result.stderr or result.stdout)[-1500:])
                except (OSError, subprocess.TimeoutExpired) as error:
                    cleanup_errors.append(str(error))

            container = subprocess.run([docker, "inspect", "--type=container", test_container], env=environment, capture_output=True, text=True, check=False)
            if container.returncode == 0:
                try:
                    owned = json.loads(container.stdout)[0]
                    if owned["Config"].get("Labels", {}).get(LABEL) != run_id or set(owned["NetworkSettings"]["Networks"]) != {network}:
                        raise RuntimeError("Refusing test-container deletion: ownership changed")
                    cleanup([docker, "rm", "-f", test_container])
                except Exception as error:
                    cleanup_errors.append(str(error))
            elif "No such" not in container.stderr:
                cleanup_errors.append(container.stderr[-1500:])
            if cluster_created:
                try:
                    node_ids = command([docker, "ps", "-aq", "--filter", "label=io.x-k8s.kind.cluster=" + run_id]).split()
                    for node_id in node_ids:
                        owned = json.loads(command([docker, "inspect", node_id]))[0]
                        if owned["Name"] != "/" + node or owned["Config"]["Labels"].get("io.x-k8s.kind.cluster") != run_id or set(owned["NetworkSettings"]["Networks"]) != {network}:
                            raise RuntimeError("Refusing cluster deletion: node identity/network ownership changed")
                    cleanup([kind, "delete", "cluster", "--name", run_id, "--kubeconfig", environment["KUBECONFIG"]])
                except Exception as error:
                    cleanup_errors.append(str(error))
            if network_created:
                try:
                    remaining = json.loads(command([docker, "network", "inspect", network]))[0]
                    if remaining.get("Labels", {}).get(LABEL) != run_id:
                        cleanup_errors.append("Refusing network deletion: ownership changed")
                    else:
                        cleanup([docker, "network", "rm", network])
                except Exception as error:
                    cleanup_errors.append(str(error))
            for image in images:
                try:
                    existing = json.loads(command([docker, "image", "inspect", image]))[0]
                    if existing["Config"].get("Labels", {}).get(LABEL) != run_id:
                        cleanup_errors.append("Refusing generated image deletion: ownership changed")
                    else:
                        cleanup([docker, "image", "rm", image])
                except Exception as error:
                    cleanup_errors.append(str(error))
            if cleanup_errors:
                detail = "Owned cleanup incomplete: " + "\n".join(cleanup_errors)
                if primary_failure is None:
                    raise RuntimeError(detail)
                primary_failure.add_note(detail)
                print(detail, file=sys.stderr)
            else:
                print("Owned cluster, network, test containers and generated images cleaned.", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
