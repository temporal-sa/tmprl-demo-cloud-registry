"""Pure synthetic parent contracts. No CLI, credential, Go or network execution."""
import datetime
import errno
import hashlib
import json
from pathlib import Path
import os
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import aws_readproof_parent as parent


def write(path, value, mode=0o600):
    raw = value if isinstance(value, bytes) else json.dumps(value, separators=(",", ":")).encode() + b"\n"
    path.write_bytes(raw); path.chmod(mode)
    return hashlib.sha256(raw).hexdigest()


def instant(minutes):
    return (datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0) + datetime.timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")


class Fixture:
    def __init__(self, directory):
        self.base = Path(directory).resolve(); self.base.chmod(0o700)
        root = self.base / "journal"; root.mkdir(mode=0o700); info = root.stat()
        directory_id = {"Device": info.st_dev, "Inode": info.st_ino, "UID": info.st_uid}
        spec = "f"*64
        manifest = {"Contract": "registry-aws-readproof-local-attempts-v1", "BootstrapUID": "b"*64, "SpecSHA256": spec, "Directory": directory_id}
        manifest_sha = write(root / "identity.json", manifest)
        binding = {"BootstrapUID": "b"*64, "SpecSHA256": spec, "ManifestSHA256": manifest_sha, "Directory": directory_id}
        pin = self.base / "pin.json"; pin_sha = write(pin, {"Version": 1, "Contract": "registry-aws-readproof-journal-pin-v1", "Binding": binding})
        receiver = self.base / "receiver"; receiver_sha = write(receiver, b"owned fake CLI bytes (never executed)", 0o500)
        python = self.base / "python"; python_sha = write(python, b"owned fake Python bytes (never executed)", 0o500)
        self.invocation = {"Version": 1, "Contract": "registry-aws-readproof-invocation-v1", "Account": parent.ACCOUNT, "Region": parent.REGION, "ProofID": parent.PROOF, "JournalPinSHA256": pin_sha, "BinarySHA256": receiver_sha, "SourceFreezeSHA256": "a"*64, "Config": {"Phase": "Seed", "SourceAdminRoleID": "AROA2345678901ABCDEFG", "ApprovedUntil": instant(10), "FixtureExpiresAt": instant(30), "DenyFixtureExpiresAt": instant(60), "ApprovalSHA256": spec}}
        invocation = self.base / "invocation.json"; invocation_sha = write(invocation, self.invocation)
        self.approval = dict.fromkeys(parent.FIELDS, "")
        self.approval.update(Version=1, Kind="SyntheticHandoff", Until=self.invocation["Config"]["ApprovedUntil"], InvocationPath=str(invocation), InvocationSHA256=invocation_sha, JournalRoot=str(root), JournalPinPath=str(pin), JournalPinSHA256=pin_sha, ReceiverPath=str(receiver), ReceiverSHA256=receiver_sha, SourceFreezeSHA256="a"*64, BaseExpiresAt=instant(60), SourceRoleID=self.invocation["Config"]["SourceAdminRoleID"], PythonPath=str(python), PythonSHA256=python_sha)
        self.path = self.base / "approval.json"; self.save()

    def save(self):
        self.digest = write(self.path, self.approval)


class ParentContracts(unittest.TestCase):
    def test_pin_preflight_synthetic_has_no_live_source_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            f = Fixture(directory)
            a, invocation, pin = parent._preflight(str(f.path), f.digest)
            self.assertEqual(a["Kind"], "SyntheticHandoff")
            command, env = parent._producer(a, "/owned/empty")
            self.assertEqual(command, [a["PythonPath"], "-I", "-S", "-c", "import os;os.write(1,b'" + parent.MALFORMED + "')"])
            self.assertEqual(env, {"PATH": "/unavailable", "HOME": "/owned/empty"})
            self.assertFalse(any(key.startswith("AWS_") for key in env))
            self.assertEqual(pin["Binding"]["SpecSHA256"], invocation["Config"]["ApprovalSHA256"])

    def test_all_three_expiries_and_fresh_pins_precede_dispatch(self):
        for kind in ("base", "new-outlives-base", "deny", "expired", "pin", "source", "dirty", "live-field", "unknown"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                f = Fixture(directory)
                if kind == "base": f.approval["BaseExpiresAt"] = instant(1)
                elif kind == "new-outlives-base": f.approval["BaseExpiresAt"] = instant(20)
                elif kind == "deny":
                    f.invocation["Config"]["DenyFixtureExpiresAt"] = instant(1)
                    f.approval["InvocationSHA256"] = write(Path(f.approval["InvocationPath"]), f.invocation)
                elif kind == "expired": f.approval["Until"] = instant(-1)
                elif kind == "pin": f.approval["JournalPinSHA256"] = "0"*64
                elif kind == "source": f.approval["SourceFreezeSHA256"] = "0"*64
                elif kind == "dirty": write(Path(f.approval["JournalRoot"]) / "prior.requested.json", b"retained unknown")
                elif kind == "live-field": f.approval["AWSCLIPath"] = "/should/not/read/aws"
                else: f.approval["Unknown"] = "PRIVATE"
                f.save(); pipe = MagicMock(read=3, write=4)
                with patch.object(parent, "_reserve_pipe", return_value=pipe), patch.object(parent, "_deliver") as deliver:
                    with self.assertRaises(parent.ParentFailure) as error:
                        parent.run_parent(str(f.path), f.digest)
                    self.assertNotIn("PRIVATE", str(error.exception)); deliver.assert_not_called()
                    pipe.close_read.assert_called_once(); pipe.close_write.assert_called_once()

    def test_fd3_busy_is_not_closed_and_owned_pipe_is_cloexec_nonblock(self):
        with patch.object(parent.os, "fstat", return_value=object()), patch.object(parent.os, "pipe") as create, patch.object(parent.os, "close") as close:
            with self.assertRaises(parent.ParentFailure) as error: parent._reserve_pipe()
            self.assertEqual(error.exception.code, "FD3Busy"); create.assert_not_called(); close.assert_not_called()
        with patch.object(parent.os, "fstat", side_effect=OSError(errno.EBADF, "PRIVATE")), patch.object(parent.os, "pipe", return_value=(3,4)), patch.object(parent.os, "set_inheritable") as inherit, patch.object(parent.os, "set_blocking") as blocking:
            pipe = parent._reserve_pipe()
            self.assertEqual((pipe.read,pipe.write),(3,4)); self.assertEqual([call.args for call in inherit.call_args_list], [(3,False),(4,False)]); blocking.assert_called_once_with(3,False)

    def test_private_metadata_strict_json_and_nanosecond_boundary(self):
        self.assertEqual(parent._utc_ns("2026-10-30T01:00:00.000000001Z")-parent._utc_ns("2026-10-30T01:00:00Z"),1)
        for raw in (b'{"Kind":"Seed","Kind":"Probe"}', b'{}{}'):
            with self.assertRaises(parent.ParentFailure): parent._json(raw)
        with tempfile.TemporaryDirectory() as directory:
            f=Fixture(directory); f.path.chmod(0o644)
            with self.assertRaises(parent.ParentFailure): parent._preflight(str(f.path),f.digest)

    def test_sso_only_source_and_no_ambient_credentials_in_producer(self):
        valid = ("[profile " + parent.PROFILE + "]\nsso_session=approved\nsso_account_id=" + parent.ACCOUNT + "\nsso_role_name=AWSAdministratorAccess\n[sso-session approved]\nsso_start_url=https://owned.awsapps.com/start\nsso_region=" + parent.REGION + "\n").encode()
        parent._sso_config(valid)
        for change in (b"credential_process=PRIVATE\n", b"endpoint_url=https://foreign.invalid\n", b"aws_access_key_id=PRIVATE\n"):
            with self.assertRaises(parent.ParentFailure) as error: parent._sso_config(valid + change)
            self.assertNotIn("PRIVATE",str(error.exception))
        a={"Kind":"Seed","AWSCLIPath":"/owned/aws","AWSHome":"/owned/home","AWSConfigPath":"/owned/config"}
        command,env=parent._producer(a,"/empty")
        self.assertEqual(command[:3],["/owned/aws","--profile",parent.PROFILE]); self.assertEqual(command[-4:],["configure","export-credentials","--format","process"])
        self.assertNotIn("AWS_ACCESS_KEY_ID",env); self.assertEqual(env["AWS_SHARED_CREDENTIALS_FILE"],"/unavailable");self.assertEqual(env["AWS_MAX_ATTEMPTS"],"1")

    def test_synthetic_same_process_flow_caps_and_reaps_private_output(self):
        for kind in ("success", "overflow", "raw-error"):
            with self.subTest(kind=kind),tempfile.TemporaryDirectory() as directory:
                f=Fixture(directory); a,inv,_=parent._preflight(str(f.path),f.digest); pipe=MagicMock(read=3,write=4)
                receiver,producer=MagicMock(),MagicMock();receiver.returncode=1;producer.returncode=0;receiver.poll.return_value=1;producer.poll.return_value=0;producer.stdout=None
                receiver.stdout.fileno.return_value=101;receiver.stderr.fileno.return_value=102;producer.stderr.fileno.return_value=103
                selector=MagicMock();selector.get_map.return_value={1:True}
                report={"result":{"Contract":"registry-aws-exact-secret-permission-proof-v1","SpecSHA256":"","Outcome":"Unproven","Steps":None},"diagnostic":"AWSReadProofInvalidContract"}
                sequence=[(receiver.stdout,"result",json.dumps(report).encode()),(receiver.stderr,"discard",b"PRIVATE provider error"),(producer.stderr,"discard",b"PRIVATE source error"),(receiver.stdout,"result",b""),(receiver.stderr,"discard",b""),(producer.stderr,"discard",b"")]
                if kind=="overflow":sequence=[(receiver.stderr,"discard",b"x"*65536)]*17
                position=[0]
                def select(timeout):
                    stream,tag,_=sequence[position[0]];position[0]+=1;return [(SimpleNamespace(fileobj=stream,data=tag),1)]
                selector.select.side_effect=select
                if kind=="success":selector.get_map.side_effect=[{1:True}]*6+[{}]
                if kind=="raw-error":selector.select.side_effect=RuntimeError("PRIVATE native exception")
                def read(fd,size):return b"" if fd==3 else sequence[position[0]-1][2]
                with patch.object(parent.subprocess,"Popen",side_effect=[receiver,producer]) as popen,patch.object(parent.selectors,"DefaultSelector",return_value=selector),patch.object(parent.os,"read",side_effect=read):
                    if kind=="success":
                        result=parent._deliver(a,inv,pipe);self.assertEqual(result["Outcome"],"SyntheticHandoffProven");self.assertNotIn("PRIVATE",json.dumps(result));self.assertEqual(popen.call_args_list[0].kwargs["pass_fds"],(3,));self.assertTrue(popen.call_args_list[0].kwargs["close_fds"]);self.assertEqual(popen.call_args_list[1].kwargs["stdout"],4)
                    else:
                        with self.assertRaises(parent.ParentFailure) as error:parent._deliver(a,inv,pipe)
                        self.assertNotIn("PRIVATE",str(error.exception))
                self.assertGreaterEqual(receiver.wait.call_count,1);self.assertGreaterEqual(producer.wait.call_count,1);pipe.close_write.assert_called_once()


    def test_success_receiver_requires_exact_nonempty_spec_digest(self):
        digest = "f"*64
        names = ("BootstrapIdentity", "BootstrapAllowMetadata", "BootstrapDenyMetadata", "SeedAllowV1", "SeedAllowV2", "SeedDenyV1")
        value = {"result": {"Contract": "registry-aws-exact-secret-permission-proof-v1", "SpecSHA256": digest, "Outcome": "SeededSyntheticVersions", "Steps": [{"Step": name, "Outcome": "Allowed", "Persistence": "Acknowledged"} for name in names]}}
        summary = parent._receiver_summary(json.dumps(value).encode(), digest)
        self.assertEqual(summary["HelperOutcome"], "SeededSyntheticVersions")
        self.assertEqual(summary["StepCount"], 6)
        self.assertTrue(summary["AllAcknowledged"])
        for spec in ("", "0"*64):
            value["result"]["SpecSHA256"] = spec
            with self.assertRaises(parent.ParentFailure) as error:
                parent._receiver_summary(json.dumps(value).encode(), digest)
            self.assertEqual(error.exception.code, "OutputInvalid")
        value = {"result": {"Contract": "registry-aws-exact-secret-permission-proof-v1", "SpecSHA256": "", "Outcome": "Unproven", "Steps": None}, "diagnostic": "AWSReadProofInvalidContract"}
        self.assertEqual(parent._receiver_summary(json.dumps(value).encode(), digest)["StepCount"], 0)


if __name__ == "__main__":
    unittest.main()
