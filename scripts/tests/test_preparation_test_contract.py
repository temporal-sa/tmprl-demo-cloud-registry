"""Finite preparation/compiler authority and private-output guards, no I/O APIs."""
import copy
import hashlib
import json
from pathlib import Path
import sys
import tempfile
import unittest
from collections import deque
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import operator_kube_test_compiler as compiler
import operator_preparation_test_contract as proof
import run_operator_kube_tests as runner


def container():
    return {"Image": "owned-image", "Name": "/owned-run-preparation", "Config": {"User": "65532:65532", "Labels": {proof.LABEL: "owned-run"}, "Env": ["PATH=/unavailable", "OPERATOR_LOCAL_KUBE_CONNECTION=/connection.json", "OPERATOR_LOCAL_CLUSTER_RUN=owned-run", "OPERATOR_LOCAL_PREPARATION_FIXTURE=1"]}, "HostConfig": {"NetworkMode": "owned-api", "ReadonlyRootfs": True, "CapDrop": ["ALL"], "SecurityOpt": ["no-new-privileges"], "IpcMode": "private", "Dns": ["127.0.0.1"], "Tmpfs": {"/tmp": "rw,noexec,nosuid,nodev,size=64m,uid=65532,gid=65532,mode=0700"}, "Memory": 768*1024**2, "MemorySwap": 768*1024**2, "NanoCpus": 2*10**9, "PidsLimit": 128}, "NetworkSettings": {"Networks": {"owned-api": {}}, "Ports": {}}, "Mounts": [{"Type": "bind", "Source": "/tmp/owned-connection.json", "Destination": "/connection.json", "RW": False}, {"Type": "tmpfs", "Destination": "/tmp"}]}


def progress(case, phase):
    return "PREPARATION-PROGRESS " + json.dumps({"case": case, "phase": phase})


def successful_output():
    lines = [progress(case, phase) for case, phases in proof.PHASES.items() for phase in phases]
    for case in proof.PHASES:
        lines.append("PREPARATION-PUBLIC " + json.dumps({"case": case, "waiting": case == "plan-wait", "processExited": True, "canonical": True, "noRepost": True, "promotion": "PromotionNotAuthorized"}))
    return "\n".join([*lines, *("--- PASS: " + name + " (0.01s)" for name in proof.TESTS)])


def simulate_attach(chunks, clock_values=None, wait_timeout=False):
    """Fake streams only: no subprocess creation, descriptor or selector I/O."""
    process = MagicMock()
    process.stdout.fileno.return_value, process.stderr.fileno.return_value = 11, 12
    state = {"code": None, "waits": 0}
    process.poll.side_effect = lambda: state["code"]

    def wait(timeout):
        state["waits"] += 1
        if wait_timeout and state["waits"] == 1:
            raise proof.subprocess.TimeoutExpired("PRIVATE", timeout)
        state["code"] = 0
        return 0

    process.wait.side_effect = wait
    selector = MagicMock()
    registered, queued, current = {}, deque(chunks), []
    streams = {"out": process.stdout, "err": process.stderr}
    selector.register.side_effect = lambda stream, event, kind: registered.update({kind: stream})
    selector.get_map.side_effect = lambda: registered
    selector.unregister.side_effect = lambda stream: registered.pop(next(key for key, value in registered.items() if value is stream))

    def select(timeout):
        kind, data = queued.popleft()
        current.append(data)
        return [(SimpleNamespace(fileobj=streams[kind], data=kind), None)]

    selector.select.side_effect = select
    clock = (lambda: 0) if clock_values is None else MagicMock(side_effect=clock_values)
    result, error = None, None
    with patch("operator_preparation_test_contract.subprocess.Popen", return_value=process) as popen, patch("operator_preparation_test_contract.selectors.DefaultSelector", return_value=selector), patch("operator_preparation_test_contract.os.read", side_effect=lambda descriptor, limit: current.pop(0)), patch("operator_preparation_test_contract.time.monotonic", side_effect=clock):
        try:
            result = proof.attach_output("fixed-docker", {"HOME": "owned"}, "owned-name")
        except proof.PreparationProofFailure as failure:
            error = failure
        popen.assert_called_once_with(["fixed-docker", "start", "--attach", "owned-name"], env={"HOME": "owned"}, stdin=proof.subprocess.DEVNULL, stdout=proof.subprocess.PIPE, stderr=proof.subprocess.PIPE)
    return result, error, process, selector


class PreparationContractTests(unittest.TestCase):
    def test_attach_counts_and_discards_private_stderr_closes_and_waits(self):
        raw = successful_output().encode()
        result, error, process, selector = simulate_attach([("err", b"PRIVATE-cookie-body-provider-error"), ("out", raw[:25]), ("out", raw[25:]), ("out", b""), ("err", b"")])
        self.assertIsNone(error)
        self.assertEqual(result, (0, raw.decode()))
        self.assertNotIn("PRIVATE", result[1])
        process.kill.assert_not_called(); process.wait.assert_called_once()
        process.stdout.close.assert_called_once(); process.stderr.close.assert_called_once(); selector.close.assert_called_once()

    def test_attach_total_stdout_plus_stderr_limit_kills_waits_and_discards(self):
        # Neither stream alone exceeds 1MiB; their aggregate must still fail.
        chunks = [("out", b"x"*65536)]*8 + [("err", b"PRIVATE"+b"y"*(65536-7))]*9
        result, error, process, selector = simulate_attach(chunks)
        self.assertIsNone(result); self.assertEqual(str(error), "PreparationPrivateOutputOverflow")
        self.assertNotIn("PRIVATE", str(error))
        process.kill.assert_called_once(); process.wait.assert_called_once_with(timeout=10); selector.close.assert_called_once()
        process.stdout.close.assert_called_once(); process.stderr.close.assert_called_once()

    def test_attach_deadline_and_exit_wait_timeout_kill_and_reap_without_error_text(self):
        for chunks, clocks, wait_timeout in (([], [0, 451], False), ([("out", b""), ("err", b"")], None, True)):
            result, error, process, selector = simulate_attach(chunks, clocks, wait_timeout)
            self.assertIsNone(result); self.assertEqual(str(error), "PreparationAttachDeadlineExceeded")
            self.assertNotIn("PRIVATE", str(error)); process.kill.assert_called_once(); selector.close.assert_called_once()
            self.assertEqual(process.wait.call_args.kwargs, {"timeout": 10})

    def test_pass_duration_is_actual_bounded_and_reconstructed(self):
        self.assertEqual(proof.pass_record("--- PASS: " + proof.PLAN_TEST + " (123.45s)"), (proof.PLAN_TEST, "--- PASS: " + proof.PLAN_TEST + " (123.45s)"))
        for duration in ("451.00", "1000000000000000000.00", "1..0", "NaN", "Infinity", "-1.00", "1.00000", "01.00", "PRIVATE"):
            with self.assertRaises(proof.PreparationProofFailure) as error:
                proof.public_output(successful_output().replace("(0.01s)", "(" + duration + "s)", 1))
            self.assertNotIn("PRIVATE", str(error.exception))

    def test_mode_is_finite_boolean_and_always_includes_boundary(self):
        self.assertEqual(proof.preparation_mode(True), (proof.MANDATORY, proof.PLAN_TEST, proof.FAULT_TEST))
        self.assertIsNone(proof.preparation_mode(False))
        for value in (None, 0, 1, "yes", (True,)):
            with self.assertRaises(proof.PreparationProofFailure):
                proof.preparation_mode(value)

    def test_modes_mutually_exclude_before_tool_discovery(self):
        for other in ("--diagnostic-browser", "--boundary-only", "--tests=TestExample"):
            with patch.object(sys, "argv", ["runner", "--candidate-preparation", other]), patch("run_operator_kube_tests.shutil.which", side_effect=AssertionError("No tool discovery")), patch("sys.stderr"):
                with self.assertRaises(SystemExit) as error:
                    runner.main()
                self.assertEqual(error.exception.code, 2)
        self.assertEqual(runner.test_arguments(tests=proof.TESTS), (["-test.v", "-test.timeout=420s", "-test.run=^(" + "|".join(proof.TESTS) + ")$"], 450))
        self.assertEqual(runner.test_arguments(), (["-test.v", "-test.timeout=900s"], 930))

    def test_compiler_has_exact_two_immutable_profiles_and_closed_target(self):
        self.assertEqual(dict(compiler.PROFILES), {"diagnostic-browser": "integration,browser_integration", "candidate-preparation": "integration,preparation_integration"})
        self.assertEqual(compiler.TARGET, "./integration")
        with self.assertRaises(TypeError):
            compiler.PROFILES["third"] = "unsafe"
        for value in (None, True, "", "./...", "integration", "candidate-preparation,unsafe", []):
            with self.assertRaises(compiler.CompilerContractFailure):
                compiler.compile_profile(value, None, None, None)

    def test_compiler_preserves_browser_and_preparation_exact_export_and_cleanup(self):
        for profile, tags in compiler.PROFILES.items():
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                artifact = root / "verified-native-ELF"
                artifact.write_bytes(b"Reviewed Compiler already verifies bounded static native ELF")
                destination = root / "integration.test"
                from unittest.mock import MagicMock
                instance = MagicMock()
                instance.compile.return_value = ([(compiler.PACKAGE, Path("integration"), artifact)], None, {"target": "./integration", "tags": tags, "digest": "a"*64})
                with patch("operator_go_linux_compiler.Compiler", return_value=instance):
                    result = compiler.compile_profile(profile, "owned-socket", root / "compile", destination)
                instance.compile.assert_called_once_with("./integration", tags)
                instance.close.assert_called_once()
                self.assertEqual(result, {"compilerInputSHA256": "a"*64, "staticELFSHA256": hashlib.sha256(artifact.read_bytes()).hexdigest()})
                self.assertEqual(destination.stat().st_mode & 0o777, 0o555)

    def test_compiler_rejects_selection_or_manifest_drift_before_copy_and_closes(self):
        from unittest.mock import MagicMock
        for mutation in ("extra", "package", "workdir", "target", "tags", "digest"):
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory); destination = root / "integration.test"
                artifacts = [(compiler.PACKAGE, Path("integration"), root / "must-not-read")]
                manifest = {"target": compiler.TARGET, "tags": compiler.PROFILES["candidate-preparation"], "digest": "a"*64}
                if mutation == "extra": artifacts *= 2
                elif mutation == "package": artifacts[0] = ("foreign", Path("integration"), artifacts[0][2])
                elif mutation == "workdir": artifacts[0] = (compiler.PACKAGE, Path("foreign"), artifacts[0][2])
                elif mutation == "target": manifest["target"] = "./..."
                elif mutation == "tags": manifest["tags"] = "integration,browser_integration"
                else: manifest["digest"] = "PRIVATE"
                instance = MagicMock(); instance.compile.return_value = (artifacts, None, manifest)
                with patch("operator_go_linux_compiler.Compiler", return_value=instance):
                    with self.assertRaises(compiler.CompilerContractFailure) as error:
                        compiler.compile_profile("candidate-preparation", "owned", root / "compile", destination)
                self.assertNotIn("PRIVATE", str(error.exception)); self.assertFalse(destination.exists()); instance.close.assert_called_once()

    def test_public_output_releases_exact_evidence_but_discards_private_lines(self):
        output, summaries = proof.public_output(successful_output() + "\nPRIVATE raw-body cookie path SDK-error\nPREPARATION-PRIVATE PRIVATE\n")
        self.assertNotIn("PRIVATE", output)
        self.assertEqual(len(summaries), 2)
        self.assertEqual(len(output.splitlines()), 3)

    def test_public_output_rejects_missing_duplicate_skip_and_false_receipts(self):
        valid = successful_output()
        for raw in (valid.replace("--- PASS: " + proof.FAULT_TEST, "not passed " + proof.FAULT_TEST), valid + "\n--- PASS: " + proof.FAULT_TEST + " (0.01s)", valid + "\n--- SKIP: " + proof.FAULT_TEST + " (0.00s)", valid.replace('"canonical": true', '"canonical": false', 1), valid.replace('"promotion": "PromotionNotAuthorized"', '"promotion": "ReadyToPromote"', 1), valid.replace(progress("plan-wait", "ready"), "")):
            with self.assertRaises(proof.PreparationProofFailure):
                proof.public_output(raw)

    def test_failed_progress_reconstructs_only_finite_case_phase_and_counts(self):
        raw = progress("plan-wait", "wait") + "\nPRIVATE raw provider error\n--- FAIL: " + proof.PLAN_TEST + " (0.10s)\n"
        evidence, _ = proof.finite_progress(raw)
        self.assertEqual(evidence, {"case": "plan-wait", "phase": "wait", "completedPhases": 1, "failedCases": [proof.PLAN_TEST], "failedCount": 1, "skippedCount": 0, "assertionFailures": []})
        self.assertNotIn("PRIVATE", json.dumps(evidence))
        for line in (progress("PRIVATE", "wait"), progress("plan-wait", "PRIVATE"), progress("plan-wait", "ready"), progress("plan-wait", "wait") + "\n" + progress("plan-wait", "wait"), 'PREPARATION-PROGRESS {"case":"plan-wait","phase":"wait","raw":"PRIVATE"}'):
            with self.assertRaises(proof.PreparationProofFailure) as error:
                proof.finite_progress(line)
            self.assertNotIn("PRIVATE", str(error.exception))

    def test_assertion_failures_release_only_closed_code_and_matching_last_phase(self):
        prefix = progress("plan-wait", "wait") + "\n"
        value = {"case": "plan-wait", "phase": "wait", "code": "PreparationNamespaceAcceptanceFailed"}
        line = "PREPARATION-FAILURE " + json.dumps(value)
        evidence, _ = proof.finite_progress(prefix + line + "\nPRIVATE raw child/provider failure")
        self.assertEqual(evidence["assertionFailures"], [value])
        for changed in ({**value, "code": "PRIVATE"}, {**value, "phase": "ready"}, {**value, "case": "sa403"}, {**value, "raw": "PRIVATE"}):
            with self.assertRaises(proof.PreparationProofFailure) as error:
                proof.finite_progress(prefix + "PREPARATION-FAILURE " + json.dumps(changed))
            self.assertNotIn("PRIVATE", str(error.exception))
        with self.assertRaises(proof.PreparationProofFailure):
            proof.finite_progress(prefix + line + "\n" + line)

    def test_preparation_container_has_one_owned_readonly_connection_only(self):
        proof.check_container(container(), "owned-run", "owned-image", "owned-api", Path("/tmp/owned-connection.json"))
        for field, value in (("NetworkMode", "bridge"), ("ReadonlyRootfs", False), ("CapAdd", ["SYS_ADMIN"]), ("IpcMode", "host"), ("Dns", ["8.8.8.8"]), ("PidsLimit", 0), ("MemorySwap", -1), ("Tmpfs", {"/tmp": "rw,exec,size=64m"}), ("SecurityOpt", [])):
            actual = container(); actual["HostConfig"][field] = value
            with self.assertRaises(proof.PreparationProofFailure):
                proof.check_container(actual, "owned-run", "owned-image", "owned-api", Path("/tmp/owned-connection.json"))

    def test_preparation_container_denies_mount_network_env_identity_drift(self):
        fixtures = []
        for mutation in ("mount", "writable", "source", "network", "env", "env-value", "label", "user", "ports"):
            value = container()
            if mutation == "mount": value["Mounts"].append({"Type": "bind", "Source": "/var/run/docker.sock", "Destination": "/socket", "RW": False})
            elif mutation == "writable": value["Mounts"][0]["RW"] = True
            elif mutation == "source": value["Mounts"][0]["Source"] = "/tmp/foreign.json"
            elif mutation == "network": value["NetworkSettings"]["Networks"]["bridge"] = {}
            elif mutation == "env": value["Config"]["Env"].append("AWS_SECRET_ACCESS_KEY=PRIVATE")
            elif mutation == "env-value": value["Config"]["Env"][1] = "OPERATOR_LOCAL_KUBE_CONNECTION=/foreign"
            elif mutation == "label": value["Config"]["Labels"][proof.LABEL] = "foreign"
            elif mutation == "user": value["Config"]["User"] = "0:0"
            elif mutation == "ports": value["NetworkSettings"]["Ports"] = {"80/tcp": [{"HostPort": "8080"}]}
            fixtures.append(value)
        for value in fixtures:
            with self.assertRaises(proof.PreparationProofFailure):
                proof.check_container(value, "owned-run", "owned-image", "owned-api", Path("/tmp/owned-connection.json"))

    def test_finite_source_contracts_keep_names_tags_and_failure_enum_exact(self):
        root = Path(__file__).resolve().parents[2]
        plan = (root / "operator-v2/integration/zzzzz_candidate_preparation_test.go").read_text()
        fault = (root / "operator-v2/integration/zzzzz_native_serviceaccount_receipt_test.go").read_text()
        self.assertIn("func " + proof.PLAN_TEST + "(t *testing.T)", plan)
        self.assertIn("func " + proof.FAULT_TEST + "(t *testing.T)", fault)
        self.assertIn("journal.AdmissionReady", plan)
        self.assertNotIn("journal.AdmissionHandoffReady", plan)
        for text in (plan, fault): self.assertTrue(text.startswith("//go:build integration && preparation_integration\n"))
        process = (root / "operator-v2/integration/runtime_preparation_process_test.go").read_text()
        self.assertNotIn("ServiceAccountUnavailable", process)
        self.assertIn("provider.ChildUnavailable", process)
        codes = process.split("var preparationFailureCodes = []string{", 1)[1].split("}", 1)[0]
        import re
        self.assertEqual(set(re.findall(r'"([A-Za-z]+)"', codes)), proof.FAILURE_CODES)


if __name__ == "__main__":
    unittest.main()
