"""Pure stage3 protocol/authority guards; no Docker, provider or compilation."""
import copy
import hashlib
import io
import re
from pathlib import Path
import sys
import tarfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from operator_browser_kube_fixture import BrowserFixtureFailure, KUBE_LABEL, LABEL, MANDATORY, TEST, PAGE_CASES, SOURCE_TEST, SOURCE_CASES, PUBLIC_ASSETS, PUBLIC_SCRIPTS, PrivateProcess, add_browser_assets, browser_variant, check_browser_container, compilation_mode, finite_browser_failure, freeze_browser_assets, owned_network, phase_record, process_variant, source_phase_record, ui_network_absent
from operator_browser_contract import ROOT, TMP_OPTIONS


def network():
    return {"Labels": {KUBE_LABEL: "owned-run"}, "Driver": "bridge", "Internal": True, "EnableIPv6": False, "Options": {"com.docker.network.bridge.gateway_mode_ipv4": "isolated"}, "IPAM": {"Config": [{"Subnet": "172.30.0.0/24"}]}}


def browser():
    return {"Image": "owned-image", "State": {"Running": False}, "Config": {"User": "1001:1001", "Labels": {LABEL: "owned-run"}, "Env": [name + "=public" for name in ("PATH", "LANG", "LC_ALL", "PLAYWRIGHT_BROWSERS_PATH", "HOME", "TMPDIR", "XDG_CACHE_HOME")]}, "HostConfig": {"NetworkMode": "owned-ui", "ReadonlyRootfs": True, "IpcMode": "private", "ShmSize": 256 * 1024**2, "Memory": 1024**3, "MemorySwap": 1024**3, "NanoCpus": 2 * 10**9, "PidsLimit": 192, "Tmpfs": {"/tmp": TMP_OPTIONS}, "Dns": ["127.0.0.1"], "CapDrop": ["ALL"], "SecurityOpt": ["no-new-privileges", 'seccomp={"defaultAction":"SCMP_ACT_ERRNO"}']}, "NetworkSettings": {"Networks": {"owned-ui": {"IPAMConfig": {"IPv4Address": "172.30.0.2"}, "IPAddress": ""}}}, "Mounts": [{"Type": "tmpfs", "Destination": "/tmp"}]}


class BrowserKubeContractTests(unittest.TestCase):
    def test_source_mode_is_one_exact_test_same_profile_and_closed_script_set(self):
        self.assertEqual(compilation_mode(True, source_unavailable=True), ("./integration", "integration,browser_integration", (MANDATORY, SOURCE_TEST)))
        self.assertEqual(compilation_mode(True), ("./integration", "integration,browser_integration", (MANDATORY, TEST)))
        source, original = browser_variant(True), browser_variant()
        self.assertEqual(source.scripts, (*PUBLIC_SCRIPTS, "scripts/operator_browser_source_unavailable.mjs"))
        self.assertEqual(original.scripts, PUBLIC_SCRIPTS)
        self.assertEqual(original.cases, PAGE_CASES)
        self.assertEqual(source.cases, SOURCE_CASES)
        self.assertEqual(len(source.cases), 7)
        for value in ("source", 1, None, [], {}):
            with self.assertRaises(BrowserFixtureFailure):
                browser_variant(value)
        with self.assertRaises(BrowserFixtureFailure):
            compilation_mode(False, source_unavailable=True)
        with patch("operator_browser_kube_fixture.subprocess.Popen") as spawn:
            with self.assertRaises(BrowserFixtureFailure):
                PrivateProcess([], {}, 0, "PRIVATE_PROCESS_OVERRIDE")
            spawn.assert_not_called()

    def test_source_phase_requires_exact_manual_read_counts_retention_and_closure(self):
        disconnected = {"phase": "source-disconnected", "sourceReads": 1, "proxyClosed": True, "evidenceRetained": True}
        finished = {"phase": "finished", "sourceReads": 2, "managedPosts": 1, "writerExited": True, "serverClosed": True, "proxyClosed": True, "evidenceRetained": True}
        source_phase_record({"phase": "disconnect-source"}, "disconnect-source")
        source_phase_record({"phase": "finish"}, "finish")
        source_phase_record(disconnected, "source-disconnected")
        source_phase_record(finished, "finished")
        for base, phase, changes in ((disconnected, "source-disconnected", ({"sourceReads": 0}, {"sourceReads": 2}, {"sourceReads": True}, {"proxyClosed": False}, {"evidenceRetained": False}, {"endpoint": "PRIVATE_LOCATOR"})), (finished, "finished", ({"sourceReads": 3}, {"managedPosts": 2}, {"managedPosts": True}, {"writerExited": False}, {"serverClosed": False}, {"cookie": "PRIVATE_COOKIE"}))):
            for change in changes:
                with self.assertRaises(BrowserFixtureFailure) as failure:
                    source_phase_record({**base, **change}, phase)
                self.assertNotIn("PRIVATE", str(failure.exception))
        for phase in ("revoke", "revoked", "resume-source", "PRIVATE_PHASE"):
            with self.assertRaises(BrowserFixtureFailure):
                source_phase_record({"phase": phase}, phase)
        with self.assertRaises(BrowserFixtureFailure):
            source_phase_record({"phase": "disconnect-source", "command": "PRIVATE"}, "disconnect-source")

    def test_source_processes_refuse_old_case_prefixes_wrong_order_and_raw_failure(self):
        process = PrivateProcess.__new__(PrivateProcess)
        process.kind, process.lines, process.cases = "browser-source", [], []
        for name in SOURCE_CASES:
            process.accept_line("PASS browser-source " + name)
        self.assertEqual(process.cases, list(SOURCE_CASES))
        for line in ("PASS browser-page native-network", "PASS browser-source native-network", "PRIVATE_COOKIE", "FAIL browser-source PRIVATE_SCOPE AssertionOrBoundaryFailed"):
            with self.assertRaises(BrowserFixtureFailure) as failure:
                process.accept_line(line)
            self.assertNotIn("PRIVATE", str(failure.exception))
        process.cases = []
        with self.assertRaises(BrowserFixtureFailure):
            process.accept_line("PASS browser-source unavailable-cleared")
        process.kind = "go-source"
        process.accept_line("--- PASS: " + TEST + " (0.00s)")
        process.accept_line("--- PASS: " + SOURCE_TEST + " (..s)")
        self.assertEqual(process.lines, [])
        process.accept_line("--- PASS: " + MANDATORY + " (0.01s)")
        process.accept_line("--- PASS: " + SOURCE_TEST + " (12.34s)")
        self.assertEqual(len(process.lines), 2)

    def test_source_failure_report_releases_only_finite_step_and_prior_case_count(self):
        completed = list(SOURCE_CASES[:4])
        value = finite_browser_failure("FAIL browser-source unavailable-cleared AssertionOrBoundaryFailed", completed, source_unavailable=True)
        self.assertEqual(value, {"step": "unavailable-cleared", "code": "AssertionOrBoundaryFailed", "completedCases": completed, "completedCount": 4})
        for line in ("FAIL browser-page unavailable-cleared AssertionOrBoundaryFailed", "FAIL browser-source cause-and-warning AssertionOrBoundaryFailed", "FAIL browser-source unavailable-cleared PRIVATE_COOKIE", "FAIL browser-source unavailable-cleared AssertionOrBoundaryFailed PRIVATE_COOKIE"):
            with self.assertRaises(BrowserFixtureFailure) as failure:
                finite_browser_failure(line, completed, source_unavailable=True)
            self.assertNotIn("PRIVATE", str(failure.exception))

    def test_source_protocol_prefixes_are_fixed_and_foreign_prefix_cannot_be_read(self):
        process = PrivateProcess.__new__(PrivateProcess)
        process.kind, process.lines, process.cases = "go-source", [], []
        lines = iter(['OWNED-BROWSER-SOURCE {"phase":"source-disconnected","sourceReads":1,"proxyClosed":true,"evidenceRetained":true}', None])
        process.next_line = lambda: next(lines)
        self.assertEqual(process.record("source-disconnected")["sourceReads"], 1)
        process.kind, process.cases = "browser-source", []
        lines = iter(['BROWSER-PHASE {"phase":"disconnect-source"}', None])
        with self.assertRaises(BrowserFixtureFailure):
            process.record("disconnect-source")
        lines = iter(['SOURCE-BROWSER-PHASE {"phase":"disconnect-source"}', None])
        self.assertEqual(process.record("disconnect-source"), {"phase": "disconnect-source"})
        self.assertEqual(process_variant("go").test, TEST)
        self.assertEqual(process_variant("go-source").test, SOURCE_TEST)

    def test_source_mode_cli_is_mutually_exclusive_before_any_tool_or_process(self):
        from run_operator_kube_tests import main
        for other in (("--diagnostic-browser",), ("--candidate-preparation",), ("--boundary-only",), ("--tests", "TestExactCase")):
            with patch.object(sys, "argv", ["run_operator_kube_tests.py", "--diagnostic-browser-source-unavailable", *other]), patch("subprocess.Popen") as spawn, redirect_stdout(io.StringIO()), patch("sys.stderr", io.StringIO()):
                with self.assertRaises(SystemExit) as failure:
                    main()
                self.assertEqual(failure.exception.code, 2)
                spawn.assert_not_called()

    def test_ui_network_absence_requires_exact_result_name_and_no_other_error(self):
        name = "operator-local-012345abcdef-ui"
        error = "Error response from daemon: network " + name + " not found\n"
        self.assertTrue(ui_network_absent(1, "[]\n", error, name))
        for code, out, detail, target in ((0, "[]\n", error, name), (True, "[]\n", error, name), (1, "[{}]\n", error, name), (1, "[]\n", error, name + "-foreign"), (1, "[]\n", "permission denied", name), (1, "[]\n", error + "daemon disconnected", name), (1, "[]\n", "No such network", name)):
            self.assertFalse(ui_network_absent(code, out, detail, target))

    def test_finite_browser_failure_reconstructs_only_allowlisted_step_and_counts(self):
        completed = list(PAGE_CASES[:4])
        value = finite_browser_failure("FAIL browser-page cause-and-warning AssertionOrBoundaryFailed", completed)
        self.assertEqual(value, {"step": "cause-and-warning", "code": "AssertionOrBoundaryFailed", "completedCases": completed, "completedCount": 4})
        for line in ("FAIL browser-page PRIVATE_COOKIE AssertionOrBoundaryFailed", "FAIL browser-page cause-and-warning PRIVATE_COOKIE", "FAIL browser-page cause-and-warning AssertionOrBoundaryFailed PRIVATE_COOKIE"):
            with self.assertRaises(BrowserFixtureFailure) as error:
                finite_browser_failure(line, completed)
            self.assertNotIn("PRIVATE_COOKIE", str(error.exception))
        with self.assertRaises(BrowserFixtureFailure):
            finite_browser_failure("FAIL browser-page cause-and-warning AssertionOrBoundaryFailed", ["cause-and-warning"])

    def test_browser_expected_provider_code_matches_exact_typed_go_enum(self):
        # Public cross-language source contract: never derive an expected code
        # from a live failure, which could launder an incorrect provider result.
        go = (ROOT / "operator-v2/internal/providers/kubernetes/errors.go").read_text()
        node = (ROOT / "scripts/operator_browser_page.mjs").read_text()
        go_code = re.search(r'ChildUnavailable\s+Code\s*=\s*"([a-z_]+)"', go)
        node_code = re.search(r'export const CHILD_FAILURE_CODE = "([a-z_]+)"', node)
        self.assertIsNotNone(go_code)
        self.assertIsNotNone(node_code)
        self.assertEqual(go_code[1], node_code[1])

    def test_asset_audit_hashes_and_tar_share_one_immutable_snapshot(self):
        original = {name: (name + " public frozen bytes").encode() for name in PUBLIC_ASSETS}
        with patch("operator_browser_kube_fixture.regular_bytes", side_effect=lambda path, limit: original[str(path.relative_to(ROOT))]):
            assets, manifest = freeze_browser_assets()
        with self.assertRaises(TypeError):
            assets[PUBLIC_ASSETS[0]] = b"changed"
        output = io.BytesIO()
        with patch("operator_browser_kube_fixture.regular_bytes", side_effect=AssertionError("No late repository reads")):
            with tarfile.open(fileobj=output, mode="w") as archive:
                add_browser_assets(archive, assets)
        with tarfile.open(fileobj=io.BytesIO(output.getvalue()), mode="r:") as archive:
            for name in PUBLIC_ASSETS:
                member = archive.getmember("browser-assets/" + Path(name).name)
                data = archive.extractfile(member).read()
                self.assertEqual(data, original[name])
                self.assertEqual(member.mode, 0o444)
                self.assertEqual(hashlib.sha256(data).hexdigest(), manifest["files"][name])
        self.assertEqual(set(manifest["files"]), set(PUBLIC_ASSETS))
        self.assertRegex(manifest["combinedSHA256"], r"^[a-f0-9]{64}$")

    def test_asset_tar_refuses_unreviewed_paths_or_mutable_payload_types(self):
        for assets in ({"/private/credential": b"private"}, {name: bytearray(b"mutable") for name in PUBLIC_ASSETS}, {name: b"" for name in PUBLIC_ASSETS}):
            with tarfile.open(fileobj=io.BytesIO(), mode="w") as archive, self.assertRaises(BrowserFixtureFailure):
                add_browser_assets(archive, assets)

    def test_compile_mode_is_exact_target_tag_and_two_test_allowlist(self):
        self.assertEqual(compilation_mode(True), ("./integration", "integration,browser_integration", (MANDATORY, TEST)))
        self.assertIsNone(compilation_mode(False))
        for value in ("integration", 1, [], None):
            with self.assertRaises(BrowserFixtureFailure):
                compilation_mode(value)

    def test_protocol_refuses_unknown_phases_fields_and_uncertain_finish(self):
        phase_record({"phase": "revoke"}, "revoke")
        phase_record({"phase": "finished", "sourceReads": 4, "managedPosts": 1, "writerExited": True, "serverClosed": True}, "finished")
        for value, phase in (({"phase": "revoke", "command": "PRIVATE"}, "revoke"), ({"phase": "revoked", "sourceReads": True}, "revoked"), ({"phase": "finished", "sourceReads": 1, "managedPosts": 2, "writerExited": True, "serverClosed": True}, "finished"), ({"phase": "finish"}, "arbitrary")):
            with self.assertRaises(BrowserFixtureFailure) as error:
                phase_record(value, phase)
            self.assertNotIn("PRIVATE", str(error.exception))

    def test_network_needs_owned_isolated_internal_no_gateway(self):
        self.assertEqual(str(owned_network(network(), "owned-run")), "172.30.0.0/24")
        for mutation in (lambda v: v.update(Internal=False), lambda v: v.update(Driver="host"), lambda v: v["Labels"].update({KUBE_LABEL: "foreign"}), lambda v: v["IPAM"]["Config"][0].update(Gateway="172.30.0.1"), lambda v: v["Options"].update({"com.docker.network.bridge.gateway_mode_ipv4": "nat"})):
            value = network(); mutation(value)
            with self.assertRaises(BrowserFixtureFailure):
                owned_network(value, "owned-run")

    def test_browser_cannot_join_api_network_get_credentials_or_privilege(self):
        profile = b'{"defaultAction":"SCMP_ACT_ERRNO"}'
        check_browser_container(browser(), "owned-run", "owned-image", "owned-ui", "172.30.0.2", profile)
        for mutation in (lambda v: v["NetworkSettings"]["Networks"].update({"api-network": {}}), lambda v: v["Config"]["Env"].append("AWS_PROFILE=ambient"), lambda v: v["HostConfig"].update(CapAdd=["SYS_ADMIN"]), lambda v: v["HostConfig"].update(IpcMode="host"), lambda v: v["Mounts"].append({"Type": "bind", "Destination": "/connection.json"}), lambda v: v["NetworkSettings"]["Networks"]["owned-ui"]["IPAMConfig"].update(IPv4Address="172.30.0.3")):
            value = browser(); mutation(value)
            with self.assertRaises(BrowserFixtureFailure):
                check_browser_container(value, "owned-run", "owned-image", "owned-ui", "172.30.0.2", profile)

    def test_running_browser_requires_actual_not_only_planned_frontend_ip(self):
        value = browser(); value["State"]["Running"] = True
        with self.assertRaises(BrowserFixtureFailure):
            check_browser_container(value, "owned-run", "owned-image", "owned-ui", "172.30.0.2", b'{"defaultAction":"SCMP_ACT_ERRNO"}')
        value["NetworkSettings"]["Networks"]["owned-ui"]["IPAddress"] = "172.30.0.2"
        check_browser_container(value, "owned-run", "owned-image", "owned-ui", "172.30.0.2", b'{"defaultAction":"SCMP_ACT_ERRNO"}')

    def test_private_process_releases_only_finite_cases_not_raw_errors(self):
        process = PrivateProcess.__new__(PrivateProcess)
        process.kind, process.lines, process.cases = "go", [], []
        output = io.StringIO()
        with redirect_stdout(output):
            process.accept_line("=== RUN   " + TEST)
            process.accept_line("PRIVATE_COOKIE_AND_PROVIDER_TEXT")
            process.accept_line("--- PASS: " + MANDATORY + " (0.00s)")
            with self.assertRaises(BrowserFixtureFailure) as failure:
                process.accept_line("--- FAIL: " + TEST + " PRIVATE_COOKIE")
        self.assertEqual(output.getvalue(), "")
        self.assertNotIn("PRIVATE_COOKIE", str(failure.exception))
        self.assertEqual(process.lines, ["--- PASS: " + MANDATORY + " (0.00s)"])
        process.kind = "browser"
        for name in PAGE_CASES:
            process.accept_line("PASS browser-page " + name)
        self.assertEqual(process.cases, list(PAGE_CASES))
        with self.assertRaises(BrowserFixtureFailure):
            process.accept_line("PRIVATE_COOKIE_AND_RAW_ASSERTION_DIFF")


if __name__ == "__main__":
    unittest.main()
