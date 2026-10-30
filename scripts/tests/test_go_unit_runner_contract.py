"""Finite unit-runner selection/output contracts; no Docker/network/Go exec."""
from __future__ import annotations

from contextlib import redirect_stderr
import io
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from run_operator_go_unit_tests import argument_parser, check_cases, marker_environment, mode_from_flags


class UnitRunnerContract(unittest.TestCase):
    def test_finite_modes_preserve_default_and_metadata_contracts(self):
        default = mode_from_flags()
        self.assertEqual((default.target, default.tags, default.test, default.timeout, default.copy_fixtures), ("./...", "", None, 120, True))
        metadata = mode_from_flags(metadata_process=True)
        self.assertEqual((metadata.target, metadata.tags, metadata.package, metadata.test, metadata.timeout), ("./internal/providers/localmetadata", "localmetadata_integration", "internal/providers/localmetadata", "TestOwnedTwoProcessCAS", 120))
        self.assertFalse(metadata.copy_fixtures, "metadata proof must exclude runtime fixtures independently of manifest contents")
        wire = mode_from_flags(credential_observation_wire=True)
        self.assertEqual((wire.target, wire.tags, wire.package, wire.test, wire.timeout, wire.copy_fixtures), ("./internal/credentialobservation", "credentialobservation_integration", "internal/credentialobservation", "TestCandidateObservationLocalTLSWireBoundary", 90, False))
        actor = mode_from_flags(credential_staging_observation_wire=True)
        self.assertEqual((actor.target, actor.tags, actor.package, actor.test, actor.timeout, actor.copy_fixtures), ("./internal/credentialstaging", "credentialobservation_integration", "internal/credentialstaging", "TestActorStagedObservationLocalTLSLifecycle", 90, False))

    def test_mutually_exclusive_modes_and_generic_bypasses_are_rejected(self):
        for arguments in ((True, True), (True, False, True), (False, True, True), ("./foreign", False), (False, "foreign_tag"), (False, False, "foreign")):
            with self.subTest(arguments=arguments), self.assertRaises(ValueError):
                mode_from_flags(*arguments)
        for arguments in (
            ["--metadata-process", "--credential-observation-wire"],
            ["--metadata-process", "--credential-staging-observation-wire"],
            ["--credential-observation-wire", "--credential-staging-observation-wire"],
            ["--target", "./foreign"], ["--tag", "foreign"], ["--env", "AWS_PROFILE=synthetic"],
        ):
            with self.subTest(arguments=arguments), redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                argument_parser().parse_args(arguments)

    def test_marker_is_only_for_selected_wire_runtime(self):
        modes = (mode_from_flags(), mode_from_flags(metadata_process=True), mode_from_flags(credential_observation_wire=True), mode_from_flags(credential_staging_observation_wire=True))
        for mode in modes:
            self.assertEqual(marker_environment(mode, selected_test=False), ())
            expected = ("REGISTRY_CREDENTIAL_OBSERVATION_TLS_TIER=owned-isolated",) if mode.name in ("credential-observation-wire", "credential-staging-observation-wire") else ()
            self.assertEqual(marker_environment(mode, selected_test=True), expected)

    def test_finite_proof_refuses_zero_wrong_duplicate_extra_failed_or_skipped_cases(self):
        for mode in (mode_from_flags(metadata_process=True), mode_from_flags(credential_observation_wire=True), mode_from_flags(credential_staging_observation_wire=True)):
            passed = "--- PASS: " + mode.test + " (0.01s)\nPASS\n"
            self.assertEqual(check_cases(passed, mode), 1)
            changed = (
                "PASS\n", passed.replace(mode.test, "TestUnreviewed"), passed + passed,
                passed + "--- PASS: TestUnreviewed (0.01s)\n", passed.replace("--- PASS:", "--- SKIP:"),
                passed.replace("--- PASS:", "--- FAIL:"), passed + "FAIL\n", passed.replace("\nPASS\n", "\n"),
                passed + "    --- SKIP: owned/unavailable (0.00s)\n",
            )
            for output in changed:
                with self.subTest(mode=mode.name, output=output[-80:]), self.assertRaises(RuntimeError):
                    check_cases(output, mode)
        self.assertEqual(check_cases("--- PASS: TestOne (0.01s)\n--- PASS: TestTwo (0.01s)\nPASS\n", mode_from_flags()), 2)


class AWSInvocationProcessContract(unittest.TestCase):
    def test_mode_is_closed_and_does_not_change_other_modes(self):
        import run_operator_go_unit_tests as runner
        mode = mode_from_flags(aws_permission_invocation_process=True)
        self.assertEqual((mode.name, mode.target, mode.tags, mode.package, mode.test, mode.timeout, mode.copy_fixtures), (runner.AWS_MODE, runner.AWS_TARGET, runner.AWS_TAG, "testinfra/awspermission", runner.AWS_TEST, 120, False))
        self.assertEqual(marker_environment(mode, selected_test=True), ("REGISTRY_AWS_INVOCATION_PROCESS_TIER=owned-isolated",))
        self.assertEqual(marker_environment(mode, selected_test=False), ())
        for flags in ((True, False, False, True), (False, True, False, True), (False, False, True, True), (False, False, False, "unsafe")):
            with self.assertRaises(ValueError):
                mode_from_flags(*flags)
        for other in ("--metadata-process", "--credential-observation-wire", "--credential-staging-observation-wire"):
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                argument_parser().parse_args(["--aws-permission-invocation-process", other])

    @staticmethod
    def output():
        import json
        import run_operator_go_unit_tests as runner
        return "\n".join([*("=== RUN   " + runner.AWS_TEST + "/" + name for name in runner.AWS_CASES), "AWSINVOCATION-PUBLIC " + json.dumps(runner.AWS_COUNTERS), "--- PASS: " + runner.AWS_TEST + " (12.00s)", *("    --- PASS: " + runner.AWS_TEST + "/" + name + " (0.01s)" for name in runner.AWS_CASES), "PASS", ""])

    def test_private_text_is_discarded_and_exact_seven_cases_required(self):
        import run_operator_go_unit_tests as runner
        valid = self.output()
        count, evidence = runner.invocation_process_output(valid + "PRIVATE provider/body/path\n")
        self.assertEqual(count, 1)
        self.assertEqual(evidence, runner.AWS_COUNTERS)
        self.assertNotIn("PRIVATE", str(evidence))
        self.assertEqual(check_cases(valid, mode_from_flags(aws_permission_invocation_process=True)), 1)
        for raw in ("PASS\n", valid + valid, valid.replace("--- PASS: " + runner.AWS_TEST + "/absent-fd3", "not passed"), valid + "--- PASS: TestUnreviewed (0.01s)\n", valid + "    --- SKIP: private/body (0.00s)\n", valid.replace('"MaterialAbsent": true', '"MaterialAbsent": false'), valid.replace('"SourceIdentity": 1', '"SourceIdentity": true')):
            with self.assertRaises(runner.AWSInvocationProofFailure) as error:
                runner.invocation_process_output(raw)
            self.assertNotIn("PRIVATE", str(error.exception))
        with self.assertRaises(runner.AWSInvocationProofFailure):
            runner.invocation_process_output(valid, 1)
        with self.assertRaises(runner.AWSInvocationProofFailure):
            runner.invocation_process_output("x" * (1024**2 + 1))

    def test_compiler_exports_only_two_literal_commands_from_exact_snapshot(self):
        import hashlib
        import tempfile
        from unittest.mock import MagicMock
        import run_operator_go_unit_tests as runner
        manifest = {"target": runner.AWS_TARGET, "tags": runner.AWS_TAG, "digest": "a"*64, "packages": [{"path": runner.AWS_PACKAGE, "workdir": "testinfra/awspermission", "binary": "package-0"}], "files": [{"path": "workspace/operator-v2/" + target[2:] + "/main.go", "sha256": "b"*64, "bytes": 100} for _, target in runner.AWS_COMMANDS]}
        with tempfile.TemporaryDirectory() as directory:
            compiler = MagicMock(); compiler.container = "owned-compiler"
            def extract(source, destination):
                body = ("owned bounded ELF " + source).encode()
                destination.write_bytes(body)
                return len(body), hashlib.sha256(body).hexdigest()
            compiler.extract.side_effect = extract
            paths, metadata = runner.compile_invocation_commands(compiler, manifest, Path(directory))
            self.assertEqual(set(paths), {"run", "init"})
            self.assertEqual(set(metadata), {"CompilerInputSHA256", "RunSHA256", "InitSHA256"})
            self.assertEqual(compiler.execution_boundary.call_count, 2)
            self.assertEqual(compiler.command.call_count, 2)
            for (name, target), call in zip(runner.AWS_COMMANDS, compiler.command.call_args_list):
                self.assertEqual(call.args[0], ["exec", "--workdir", "/workspace/operator-v2", "owned-compiler", "/usr/local/go/bin/go", "build", "-o", "/tmp/out/aws-proof-" + name, target])
        import copy
        for mutation in ("target", "tags", "packages", "digest", "source"):
            bad = copy.deepcopy(manifest)
            if mutation == "target": bad["target"] = "./..."
            elif mutation == "tags": bad["tags"] = "unsafe"
            elif mutation == "packages": bad["packages"] = []
            elif mutation == "digest": bad["digest"] = "PRIVATE"
            else: bad["files"] = bad["files"][:1]
            compiler = MagicMock()
            with tempfile.TemporaryDirectory() as directory, self.assertRaises(RuntimeError) as error:
                runner.compile_invocation_commands(compiler, bad, Path(directory))
            self.assertNotIn("PRIVATE", str(error.exception)); compiler.command.assert_not_called()

    def test_capture_caps_during_streaming_discards_stderr_and_reaps(self):
        from types import SimpleNamespace
        from unittest.mock import MagicMock, patch
        import run_operator_go_unit_tests as runner
        for kind in ("success", "overflow", "raw-error"):
            process = MagicMock(); process.stdout = MagicMock(); process.stderr = MagicMock(); process.stdout.fileno.return_value = 101; process.stderr.fileno.return_value = 102; process.wait.return_value = 0
            process.poll.return_value = None if kind == "overflow" else 0
            selector = MagicMock(); selector.get_map.return_value = {1: "active"}
            if kind == "success":
                sequence = [(process.stdout, "out", self.output().encode()), (process.stderr, "err", b"PRIVATE never released"), (process.stdout, "out", b""), (process.stderr, "err", b"")]
            elif kind == "overflow":
                sequence = [(process.stderr, "err", b"x"*65536)]*17
            else:
                sequence = []
            position = [0]
            def select(timeout):
                stream, tag, data = sequence[position[0]]; position[0] += 1
                return [(SimpleNamespace(fileobj=stream, data=tag), 1)]
            selector.select.side_effect = select
            def read(fd, size):
                return sequence[position[0]-1][2]
            if kind == "success":
                selector.get_map.side_effect = [{1: True}]*4 + [{}]
            if kind == "raw-error":
                selector.select.side_effect = RuntimeError("PRIVATE exception body")
            with patch.object(runner.subprocess, "Popen", return_value=process), patch.object(runner.selectors, "DefaultSelector", return_value=selector), patch.object(runner.os, "read", side_effect=read):
                if kind == "success":
                    result = runner.capture_invocation_process(["owned-fixed-argv"], {"HOME": "/owned"})
                    self.assertNotIn("PRIVATE", result)
                else:
                    with self.assertRaises(runner.AWSInvocationProofFailure) as error:
                        runner.capture_invocation_process(["owned-fixed-argv"], {})
                    self.assertNotIn("PRIVATE", str(error.exception))
            if kind == "overflow": process.kill.assert_called_once(); process.wait.assert_called_once()
            process.stdout.close.assert_called_once(); process.stderr.close.assert_called_once(); selector.close.assert_called_once()

    def test_go_process_tier_is_opt_in_and_uses_real_fixed_binaries(self):
        root = Path(__file__).resolve().parents[2]
        source = (root / "operator-v2/testinfra/awspermission/invocation_process_integration_test.go").read_text()
        self.assertTrue(source.startswith("//go:build linux && awspermission_process_integration\n"))
        self.assertIn("func TestOwnedInvocationFD3ProcessBoundary(t *testing.T)", source)
        self.assertIn("syscall.Pipe2(fds, syscall.O_CLOEXEC)", source)
        self.assertIn("syscall.SetNonblock(fds[0], true)", source)
        self.assertIn("cmd.ExtraFiles = []*os.File{reader}", source)
        self.assertNotIn("CombinedOutput", source)
        self.assertNotIn("t.Skip", source)
        self.assertNotIn("configured(", source)
        self.assertNotIn("makeSDKClients(", source)


if __name__ == "__main__":
    unittest.main()
