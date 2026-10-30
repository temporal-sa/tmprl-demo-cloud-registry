"""Pure fixed selection/artifact/privacy checks; no Docker/process/acquisition."""
import copy
import hashlib
import io
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import operator_diagnostic_cli_fixture as proof
import operator_kube_test_compiler as compiler
import run_operator_kube_tests as runner
from test_preparation_test_contract import container


class DiagnosticCLIContracts(unittest.TestCase):
    def test_exact_cases_private_output_and_no_skip_or_extra_case(self):
        good="\n".join("--- PASS: "+name+" (0.01s)" for name in proof.TESTS)
        self.assertEqual(proof.public_output("PRIVATE_KEY\n"+good),good+"\n")
        for raw in (good.replace("PASS","SKIP",1),good.replace("PASS","FAIL",1),good+"\n--- PASS: TestForeign (0.01s)",good+"\n"+good,good.replace("0.01s","NaNs"),good.replace("0.01s","451.00s"),"x"*(1024**2+1)):
            with self.assertRaises(proof.DiagnosticCLIProofFailure):proof.public_output(raw)

    def test_command_artifact_is_fixed_and_checksum_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"registry-diagnostics";path.write_bytes(b"PUBLIC_ELF")
            metadata={"compilerInputSHA256":"a"*64,"staticELFSHA256":"b"*64,"commandELFSHA256":hashlib.sha256(path.read_bytes()).hexdigest()}
            output=io.BytesIO()
            with tarfile.open(fileobj=output,mode="w") as archive:proof.add_command_artifact(archive,path,metadata)
            with tarfile.open(fileobj=io.BytesIO(output.getvalue())) as archive:self.assertEqual([(m.name,m.mode) for m in archive.getmembers()],[("registry-diagnostics",0o755),("diagnostic-cli-proof.json",0o444)])
            path.write_bytes(b"CHANGED")
            with tarfile.open(fileobj=io.BytesIO(),mode="w") as archive:
                with self.assertRaises(proof.DiagnosticCLIProofFailure):proof.add_command_artifact(archive,path,metadata)

    def test_cli_container_has_only_owned_scope_and_connection(self):
        value=container();value["Name"]="/owned-run-diagnostic-cli"
        value["Config"]["Env"]=[s.replace("OPERATOR_LOCAL_PREPARATION_FIXTURE=1","OPERATOR_LOCAL_DIAGNOSTIC_CLI=1") for s in value["Config"]["Env"]]
        proof.check_container(value,"owned-run","owned-image","owned-api",Path("/tmp/owned-connection.json"))
        for change in (lambda v:v["Config"]["Env"].append("AWS_SECRET_ACCESS_KEY=PRIVATE"),lambda v:v["HostConfig"].update(NetworkMode="host"),lambda v:v["HostConfig"].update(ReadonlyRootfs=False),lambda v:v["HostConfig"].update(CapAdd=["SYS_ADMIN"]),lambda v:v["Mounts"].append({"Type":"bind","Source":"/home/user","Destination":"/host","RW":False})):
            bad=copy.deepcopy(value);change(bad)
            with self.assertRaises(proof.DiagnosticCLIProofFailure):proof.check_container(bad,"owned-run","owned-image","owned-api",Path("/tmp/owned-connection.json"))

    def test_same_snapshot_exact_two_native_artifact_selection_and_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            base=Path(directory);first=base/"first";second=base/"second";first.write_bytes(b"TEST_ELF");second.write_bytes(b"CLI_ELF")
            fake=MagicMock()
            manifest={"target":"./...","tags":"integration","digest":"a"*64}
            artifacts=[(compiler.PACKAGE,Path("integration"),first),("github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/cmd/registry-diagnostics",Path("cmd/registry-diagnostics"),second)]
            fake.compile_local_diagnostics.return_value=(artifacts,None,manifest)
            with patch("operator_go_linux_compiler.Compiler",return_value=fake):
                value=compiler.compile_local_diagnostics("/owned.sock",base/"stage",base/"integration.test",base/"registry-diagnostics")
            self.assertEqual(value["compilerInputSHA256"],"a"*64);self.assertEqual((base/"registry-diagnostics").read_bytes(),b"CLI_ELF")
            fake.compile_local_diagnostics.assert_called_once_with();fake.compile.assert_not_called();fake.close.assert_called_once_with()
            fake.reset_mock();fake.compile_local_diagnostics.return_value=(artifacts[:1],None,manifest)
            with patch("operator_go_linux_compiler.Compiler",return_value=fake):
                with self.assertRaises(compiler.CompilerContractFailure):compiler.compile_local_diagnostics("/owned.sock",base/"badstage",base/"test2",base/"cmd2")
            fake.close.assert_called_once_with()

    def test_cli_mode_is_mutually_exclusive_without_target_overrides(self):
        source=Path(runner.__file__).read_text()
        self.assertIn('selection.add_argument("--diagnostic-read-local", action="store_true"',source)
        self.assertIn('args.tests = TESTS',source)
        self.assertNotIn('add_argument("--command',source)
        arguments,_=runner.test_arguments(tests=proof.TESTS)
        self.assertEqual(arguments[-1],"-test.run=^("+"|".join(proof.TESTS)+")$")


if __name__=="__main__":unittest.main()
