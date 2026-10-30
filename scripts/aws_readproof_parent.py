#!/usr/bin/env python3
"""Finite reviewed host parent. No producer runs without exact private pins.

The synthetic mode sends malformed inert JSON to the real receiver through the
same anonymous pipe. Live export requires a separately reviewed exact SSO
configuration/tool implementation and root authorization; digests are equality
pins, not signatures or source/running-code attestation.
"""
from __future__ import annotations

import argparse
import calendar
import configparser
import errno
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import stat
import subprocess
import sys
import tempfile
import time

PROFILE = "SolutionsArchitecture/AWSAdministratorAccess"
ACCOUNT, REGION = "429214323166", "us-west-2"
PROOF = "fixture-20261030-01-aws-readproof"
MALFORMED = "synthetic-invalid-credential-json"
STEPS = {"BootstrapIdentity", "BootstrapAllowMetadata", "BootstrapDenyMetadata", "AllowMetadata", "SeedAllowV1", "SeedAllowV2", "SeedDenyV1", "AssumeReader", "ReaderIdentity", "ReadAllowedV1", "DenyOtherV2", "DenyOtherSecret", "DenyDirectKeyGeneration", "DenyTransitiveAssumption"}
CODES = {"AWSReadProof" + name for name in ("InvalidContract", "AuthorityDenied", "OutcomeUnproven", "UnexpectedPermit", "WrongIdentity", "MetadataChanged", "ProviderFailure", "PrivacyDenied", "AlreadyAttempted")}
FIELDS = {"Version", "Kind", "Until", "InvocationPath", "InvocationSHA256", "JournalRoot", "JournalPinPath", "JournalPinSHA256", "ReceiverPath", "ReceiverSHA256", "SourceFreezeSHA256", "BaseExpiresAt", "SourceRoleID", "PythonPath", "PythonSHA256", "AWSCLIPath", "AWSCLISHA256", "AWSConfigPath", "AWSConfigSHA256", "AWSHome", "ExportReviewSHA256"}


class ParentFailure(RuntimeError):
    ALLOWED = {"ArgumentsInvalid", "PinsInvalid", "PrivateFileDenied", "JournalNotFresh", "Expired", "FD3Busy", "PipeUnavailable", "ProducerFailed", "ProducerDeadline", "ReceiverFailed", "OutputOverflow", "OutputInvalid", "Deadline", "Unproven", "CleanupUnproven", "SyntheticUnproven"}

    def __init__(self, code):
        self.code = code if type(code) is str and code in self.ALLOWED else "Unproven"
        super().__init__("AWSReadProofParent" + self.code)


def _sha(value):
    return type(value) is str and re.fullmatch(r"[a-f0-9]{64}", value) is not None


def _object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ParentFailure("PinsInvalid")
        value[key] = item
    return value


def _json(raw):
    try:
        return json.loads(raw, object_pairs_hook=_object)
    except ParentFailure:
        raise
    except Exception:
        raise ParentFailure("PinsInvalid") from None


def _utc_ns(value):
    if type(value) is not str:
        raise ParentFailure("Expired")
    match = re.fullmatch(r"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2})(?:\.([0-9]{1,9}))?Z", value)
    if match is None:
        raise ParentFailure("Expired")
    try:
        base = time.strptime(match[1], "%Y-%m-%dT%H:%M:%S")
        if base.tm_sec > 59:
            raise ValueError()
        return calendar.timegm(base) * 10**9 + int((match[2] or "").ljust(9, "0"))
    except Exception:
        raise ParentFailure("Expired") from None


def _path(value, private_parent=False):
    if type(value) is not str or not os.path.isabs(value) or os.path.normpath(value) != value:
        raise ParentFailure("PinsInvalid")
    path = Path(value)
    for ancestor in reversed(path.parents):
        info = ancestor.lstat()
        if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
            raise ParentFailure("PrivateFileDenied")
    if private_parent:
        info = path.parent.stat()
        if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
            raise ParentFailure("PrivateFileDenied")
    return path


def _private(path, expected, limit=32768):
    if not _sha(expected):
        raise ParentFailure("PinsInvalid")
    path = _path(path, True)
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode) or stat.S_IMODE(before.st_mode) != 0o600 or before.st_uid != os.getuid() or before.st_nlink != 1 or not 0 < before.st_size <= limit:
        raise ParentFailure("PrivateFileDenied")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        opened = os.fstat(stream.fileno())
        if (before.st_dev, before.st_ino, before.st_uid, before.st_size) != (opened.st_dev, opened.st_ino, opened.st_uid, opened.st_size):
            raise ParentFailure("PrivateFileDenied")
        raw = stream.read(limit + 1)
    if len(raw) > limit or hashlib.sha256(raw).hexdigest() != expected:
        raise ParentFailure("PinsInvalid")
    return raw


def _binary(path, expected, private=False):
    if not _sha(expected):
        raise ParentFailure("PinsInvalid")
    path = _path(path, private)
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid not in {0, os.getuid()} or info.st_nlink != 1 or info.st_mode & 0o022 or not info.st_mode & 0o111 or not 0 < info.st_size <= 256*1024**2:
        raise ParentFailure("PrivateFileDenied")
    with path.open("rb") as stream:
        if hashlib.file_digest(stream, "sha256").hexdigest() != expected:
            raise ParentFailure("PinsInvalid")


def _fresh(a, invocation, pin):
    root = _path(a["JournalRoot"])
    info = root.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o700:
        raise ParentFailure("PrivateFileDenied")
    for path in (a["InvocationPath"], a["JournalPinPath"]):
        if Path(path) == root or root in Path(path).parents:
            raise ParentFailure("PinsInvalid")
    binding = pin["Binding"]
    if pin["Version"] != 1 or pin["Contract"] != "registry-aws-readproof-journal-pin-v1" or binding["Directory"] != {"Device": info.st_dev, "Inode": info.st_ino, "UID": info.st_uid} or binding["SpecSHA256"] != invocation["Config"]["ApprovalSHA256"]:
        raise ParentFailure("PinsInvalid")
    if sorted(item.name for item in root.iterdir()) != ["identity.json"]:
        raise ParentFailure("JournalNotFresh")
    raw = _private(str(root / "identity.json"), binding["ManifestSHA256"], 8192)
    manifest = _json(raw)
    if manifest != {"Contract": "registry-aws-readproof-local-attempts-v1", "BootstrapUID": binding["BootstrapUID"], "SpecSHA256": binding["SpecSHA256"], "Directory": binding["Directory"]}:
        raise ParentFailure("PinsInvalid")


def _expiry(a, invocation):
    c = invocation["Config"]
    now, until = time.time_ns(), _utc_ns(a["Until"])
    new, deny, base = (_utc_ns(value) for value in (c["FixtureExpiresAt"], c["DenyFixtureExpiresAt"], a["BaseExpiresAt"]))
    if not now + 5*10**9 < until <= now + 15*60*10**9 or a["Until"] != c["ApprovedUntil"] or until > min(new, deny, base) or new > min(deny, base):
        raise ParentFailure("Expired")


def _sso_config(raw):
    config = configparser.RawConfigParser(strict=True)
    config.read_string(raw.decode("utf8"))
    name = "profile " + PROFILE
    if name not in config:
        raise ParentFailure("PinsInvalid")
    profile = dict(config[name])
    if not set(profile) <= {"sso_session", "sso_account_id", "sso_role_name", "region", "output"} or profile.get("sso_account_id") != ACCOUNT or profile.get("sso_role_name") != "AWSAdministratorAccess" or not profile.get("sso_session"):
        raise ParentFailure("PinsInvalid")
    session = "sso-session " + profile["sso_session"]
    if set(config.sections()) != {name, session} or not set(config[session]) <= {"sso_start_url", "sso_region", "sso_registration_scopes"} or not config[session].get("sso_start_url", "").startswith("https://") or config[session].get("sso_region") != REGION:
        raise ParentFailure("PinsInvalid")


def _preflight(path, expected):
    a = _json(_private(path, expected))
    if type(a) is not dict or set(a) != FIELDS or type(a["Version"]) is not int or a["Version"] != 1 or a["Kind"] not in {"SyntheticHandoff", "Seed", "Probe"}:
        raise ParentFailure("PinsInvalid")
    invocation = _json(_private(a["InvocationPath"], a["InvocationSHA256"]))
    pin = _json(_private(a["JournalPinPath"], a["JournalPinSHA256"], 8192))
    if invocation["Version"] != 1 or invocation["Contract"] != "registry-aws-readproof-invocation-v1" or invocation["Account"] != ACCOUNT or invocation["Region"] != REGION or invocation["ProofID"] != PROOF or invocation["JournalPinSHA256"] != a["JournalPinSHA256"] or invocation["BinarySHA256"] != a["ReceiverSHA256"] or invocation["SourceFreezeSHA256"] != a["SourceFreezeSHA256"] or invocation["Config"]["SourceAdminRoleID"] != a["SourceRoleID"]:
        raise ParentFailure("PinsInvalid")
    expected_phase = "Seed" if a["Kind"] == "SyntheticHandoff" else a["Kind"]
    if invocation["Config"]["Phase"] != expected_phase or not _sha(a["SourceFreezeSHA256"]):
        raise ParentFailure("PinsInvalid")
    _expiry(a, invocation)
    _fresh(a, invocation, pin)
    _binary(a["ReceiverPath"], a["ReceiverSHA256"], True)
    live_fields = ("AWSCLIPath", "AWSCLISHA256", "AWSConfigPath", "AWSConfigSHA256", "AWSHome", "ExportReviewSHA256")
    if a["Kind"] == "SyntheticHandoff":
        if any(a[key] != "" for key in live_fields):
            raise ParentFailure("PinsInvalid")
        _binary(a["PythonPath"], a["PythonSHA256"])
    else:
        if a["PythonPath"] != "" or a["PythonSHA256"] != "" or not _sha(a["ExportReviewSHA256"]):
            raise ParentFailure("PinsInvalid")
        _binary(a["AWSCLIPath"], a["AWSCLISHA256"])
        _sso_config(_private(a["AWSConfigPath"], a["AWSConfigSHA256"], 1048576))
        home = _path(a["AWSHome"])
        info = home.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o022:
            raise ParentFailure("PrivateFileDenied")
    return a, invocation, pin


class _Pipe:
    def __init__(self, read, write):
        self.read, self.write = read, write

    def close_read(self):
        fd, self.read = self.read, None
        if fd is not None:
            os.close(fd)

    def close_write(self):
        fd, self.write = self.write, None
        if fd is not None:
            os.close(fd)


def _reserve_pipe():
    # Deliberately do not close/remap an unexpected inherited descriptor.
    try:
        os.fstat(3)
    except OSError as error:
        if error.errno != errno.EBADF:
            raise ParentFailure("PipeUnavailable") from None
    else:
        raise ParentFailure("FD3Busy")
    read, write = os.pipe()
    pipe = _Pipe(read, write)
    try:
        if read != 3:
            raise ParentFailure("PipeUnavailable")
        os.set_inheritable(read, False)
        os.set_inheritable(write, False)
        os.set_blocking(read, False)
        return pipe
    except BaseException:
        for close in (pipe.close_read, pipe.close_write):
            try: close()
            except Exception: pass
        raise ParentFailure("PipeUnavailable") from None


def _receiver_summary(raw, digest):
    value = _json(raw)
    if type(value) is not dict or not {"result"} <= set(value) <= {"result", "diagnostic"} or value.get("diagnostic", "") not in CODES | {""}:
        raise ParentFailure("OutputInvalid")
    r = value["result"]
    if type(r) is not dict or set(r) != {"Contract", "SpecSHA256", "Outcome", "Steps"} or r["Contract"] != "registry-aws-exact-secret-permission-proof-v1" or r["SpecSHA256"] not in {"", digest} or r["Outcome"] not in {"Unproven", "SeededSyntheticVersions", "ScopedEffectiveCallsMatched"} or r["Steps"] is not None and (type(r["Steps"]) is not list or len(r["Steps"]) > 11):
        raise ParentFailure("OutputInvalid")
    if r["Outcome"] != "Unproven" and (not _sha(digest) or r["SpecSHA256"] != digest):
        raise ParentFailure("OutputInvalid")
    steps = r["Steps"] or []
    for step in steps:
        if type(step) is not dict or step.get("Step") not in STEPS or step.get("Code", "") not in CODES | {"", "AccessDenied", "AccessDeniedException"} or step.get("Outcome") not in {"Allowed", "Denied", "Unproven", "UnexpectedAllowed"} or step.get("Persistence") not in {"Acknowledged", "Unknown", "NotAttempted"}:
            raise ParentFailure("OutputInvalid")
    last = steps[-1] if steps else {}
    return {"HelperDiagnostic": value.get("diagnostic", ""), "HelperOutcome": r["Outcome"], "StepCount": len(steps), "LastStep": last.get("Step", "none"), "LastProviderCode": last.get("Code", ""), "LastOutcome": last.get("Outcome", "none"), "LastPersistence": last.get("Persistence", "none"), "AllAcknowledged": bool(steps) and all(step["Persistence"] == "Acknowledged" for step in steps)}


def _producer(a, private_home):
    if a["Kind"] == "SyntheticHandoff":
        return [a["PythonPath"], "-I", "-S", "-c", "import os;os.write(1,b'" + MALFORMED + "')"], {"PATH": "/unavailable", "HOME": private_home}
    args = [a["AWSCLIPath"], "--profile", PROFILE, "--region", REGION, "--no-cli-pager", "--no-cli-auto-prompt", "--cli-connect-timeout", "3", "--cli-read-timeout", "5", "configure", "export-credentials", "--format", "process"]
    env = {"PATH": os.defpath, "HOME": a["AWSHome"], "AWS_CONFIG_FILE": a["AWSConfigPath"], "AWS_SHARED_CREDENTIALS_FILE": "/unavailable", "AWS_EC2_METADATA_DISABLED": "true", "AWS_CLI_AUTO_PROMPT": "off", "AWS_PAGER": "", "AWS_MAX_ATTEMPTS": "1", "AWS_RETRY_MODE": "standard"}
    return args, env


def _deliver(a, invocation, pipe):
    receiver = producer = selector = None
    primary = None
    run_dir = tempfile.TemporaryDirectory(prefix="aws-readproof-parent-")
    try:
        home, tmp = Path(run_dir.name) / "home", Path(run_dir.name) / "tmp"
        home.mkdir(mode=0o700); tmp.mkdir(mode=0o700)
        args = [a["ReceiverPath"], "--invocation", a["InvocationPath"], "--invocation-sha256", a["InvocationSHA256"], "--journal-root", a["JournalRoot"], "--journal-pin", a["JournalPinPath"], "--journal-pin-sha256", a["JournalPinSHA256"]]
        receiver = subprocess.Popen(args, env={"PATH": "/unavailable", "HOME": str(home), "TMPDIR": str(tmp)}, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True, pass_fds=(3,))
        if a["Kind"] != "SyntheticHandoff":
            pipe.close_read()
        source_args, source_env = _producer(a, str(home))
        producer = subprocess.Popen(source_args, env=source_env, stdin=subprocess.DEVNULL, stdout=pipe.write, stderr=subprocess.PIPE, close_fds=True)
        pipe.close_write()  # Only the producer owns a writer: EOF after export.
        started = time.monotonic(); producer_until = started + 8
        until = min(started + 310, started + (_utc_ns(a["Until"]) - time.time_ns()) / 10**9)
        selector = selectors.DefaultSelector()
        selector.register(receiver.stdout, selectors.EVENT_READ, "result")
        selector.register(receiver.stderr, selectors.EVENT_READ, "discard")
        selector.register(producer.stderr, selectors.EVENT_READ, "discard")
        raw, total = bytearray(), 0
        while selector.get_map():
            now = time.monotonic()
            if now >= until:
                raise ParentFailure("Deadline")
            code = producer.poll()
            if code is None and now >= producer_until:
                raise ParentFailure("ProducerDeadline")
            if code is not None and code != 0:
                raise ParentFailure("ProducerFailed")
            for key, _ in selector.select(min(0.2, max(0, until - now))):
                data = os.read(key.fileobj.fileno(), 65536)
                if not data:
                    selector.unregister(key.fileobj)
                    continue
                total += len(data)
                if total > 1048576:
                    raise ParentFailure("OutputOverflow")
                if key.data == "result":
                    raw.extend(data)
        producer.wait(timeout=max(0.1, producer_until - time.monotonic()))
        receiver.wait(timeout=max(0.1, until - time.monotonic()))
        if producer.returncode != 0:
            raise ParentFailure("ProducerFailed")
        summary = _receiver_summary(raw, invocation["Config"]["ApprovalSHA256"])
        if a["Kind"] == "SyntheticHandoff":
            if receiver.returncode != 1 or summary["HelperDiagnostic"] != "AWSReadProofInvalidContract" or summary["StepCount"] != 0 or os.read(pipe.read, 1) != b"":
                raise ParentFailure("SyntheticUnproven")
            _fresh(a, invocation, _json(_private(a["JournalPinPath"], a["JournalPinSHA256"], 8192)))
            return {"Outcome": "SyntheticHandoffProven", **summary}
        expected_outcome = "SeededSyntheticVersions" if a["Kind"] == "Seed" else "ScopedEffectiveCallsMatched"
        complete = receiver.returncode == 0 and summary["HelperDiagnostic"] == "" and summary["HelperOutcome"] == expected_outcome and summary["StepCount"] == (6 if a["Kind"] == "Seed" else 11) and summary["AllAcknowledged"]
        return {"Outcome": "Completed" if complete else "Unproven", **summary}
    except BaseException as error:
        primary = ParentFailure(error.code) if type(error) is ParentFailure else ParentFailure("Unproven")
        raise primary from None
    finally:
        failed = False
        for child in (producer, receiver):
            if child is not None:
                try:
                    if child.poll() is None:
                        try: child.kill()
                        except ProcessLookupError: pass
                except Exception:
                    failed = True
                try: child.wait(timeout=5)
                except Exception: failed = True
                for stream in (child.stdout, child.stderr):
                    if stream is not None:
                        try: stream.close()
                        except Exception: failed = True
        if selector is not None:
            try: selector.close()
            except Exception: failed = True
        try: run_dir.cleanup()
        except Exception: failed = True
        if failed:
            if primary is None:
                raise ParentFailure("CleanupUnproven") from None
            primary.add_note("AWSReadProofParentCleanupUnproven")


def run_parent(path, expected):
    pipe = None
    try:
        # Reserve FD3 before opening metadata, selectors, temporary files or CLI.
        pipe = _reserve_pipe()
        a, invocation, pin = _preflight(path, expected)
        _expiry(a, invocation)
        _fresh(a, invocation, pin)
        report = _deliver(a, invocation, pipe)
        return {"ApprovalSHA256": expected, "InvocationSHA256": a["InvocationSHA256"], **report}
    except BaseException as error:
        if type(error) is ParentFailure:
            raise ParentFailure(error.code) from None
        raise ParentFailure("Unproven") from None
    finally:
        if pipe is not None:
            failed = False
            for close in (pipe.close_write, pipe.close_read):
                try: close()
                except Exception: failed = True
            if failed: raise ParentFailure("CleanupUnproven") from None


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise ParentFailure("ArgumentsInvalid")


def main():
    try:
        parser = _Parser(description=__doc__, add_help=False)
        parser.add_argument("--approval", required=True)
        parser.add_argument("--approval-sha256", required=True)
        args = parser.parse_args()
        report = run_parent(args.approval, args.approval_sha256)
        print(json.dumps(report, sort_keys=True))
        return 0 if report["Outcome"] in {"SyntheticHandoffProven", "Completed"} else 1
    except ParentFailure as error:
        print(json.dumps({"Outcome": "Unproven", "Diagnostic": str(error)}, sort_keys=True))
        return 1
    except BaseException:
        print('{"Diagnostic":"AWSReadProofParentUnproven","Outcome":"Unproven"}')
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
