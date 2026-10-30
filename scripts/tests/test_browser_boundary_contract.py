"""Pure hostile-input tests only: no Docker, package acquisition or browser."""
import copy
import io
import json
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from operator_browser_contract import CASES, LABEL, TMP_OPTIONS, VERSION, admitted_driver_files, browser_seccomp, check_container, check_output, driver_files, existing_cache_matches, verify_seccomp
from run_operator_browser_boundary import admitted_image_path, stage_tar


def archive(extra=()):
    files = [("package/package.json", json.dumps({"name": "playwright-core", "version": VERSION}).encode(), tarfile.REGTYPE), ("package/index.mjs", b"synthetic", tarfile.REGTYPE), ("package/lib/server/chromium/chromium.js", b"synthetic", tarfile.REGTYPE), *extra]
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as stream:
        for name, data, kind in files:
            item = tarfile.TarInfo(name)
            item.type, item.size = kind, len(data)
            stream.addfile(item, io.BytesIO(data))
    return buffer.getvalue()


def container():
    return {"Image": "owned-image", "Config": {"Labels": {LABEL: "owned-run"}, "User": "1001:1001", "Env": [name + "=synthetic" for name in ("PATH", "LANG", "LC_ALL", "PLAYWRIGHT_BROWSERS_PATH", "HOME", "TMPDIR", "XDG_CACHE_HOME", "OPERATOR_UNIT_HOST_GATEWAY")]}, "HostConfig": {"NetworkMode": "none", "ReadonlyRootfs": True, "CapDrop": ["ALL"], "IpcMode": "private", "ShmSize": 256 * 1024**2, "Memory": 1024**3, "MemorySwap": 1024**3, "NanoCpus": 2 * 10**9, "PidsLimit": 192, "SecurityOpt": ["no-new-privileges", 'seccomp={"defaultAction":"SCMP_ACT_ERRNO"}'], "Tmpfs": {"/tmp": TMP_OPTIONS}}, "Mounts": [{"Type": "tmpfs", "Destination": "/tmp"}]}


class BrowserContractTests(unittest.TestCase):
    def test_derived_profile_adds_only_one_unconditional_chroot_rule(self):
        # Transform-only test mocks the preceding digest gate with a synthetic
        # profile; separate tests reject real mismatched digests. No cache,
        # acquisition, network or skips are needed for fresh pure guard tests.
        original = {"defaultAction": "SCMP_ACT_ERRNO", "syscalls": [{"names": ["read"], "action": "SCMP_ACT_ALLOW"}, {"names": ["chroot"], "action": "SCMP_ACT_ALLOW", "args": [], "comment": "", "includes": {"caps": ["CAP_SYS_CHROOT"]}, "excludes": {}}]}
        raw = json.dumps(original).encode()
        with patch("operator_browser_contract.verify_seccomp", return_value=raw):
            derived = browser_seccomp(raw)
            self.assertEqual(derived, browser_seccomp(raw))
        result = json.loads(derived)
        added = result["syscalls"].pop()
        self.assertEqual(result, original)
        self.assertEqual(added, {"names": ["chroot"], "action": "SCMP_ACT_ALLOW", "args": [], "includes": {}, "excludes": {}})
        with self.assertRaises(RuntimeError):
            browser_seccomp(raw)

    def test_only_exact_empty_public_ssh_directory_metadata_is_admitted(self):
        def directory():
            item = tarfile.TarInfo("root/.ssh")
            item.type, item.mode, item.uid, item.gid = tarfile.DIRTYPE, 0o700, 0, 0
            return item
        self.assertEqual(str(admitted_image_path(directory())), "root/.ssh")
        for key, value in (("type", tarfile.REGTYPE), ("type", tarfile.SYMTYPE), ("mode", 0o755), ("uid", 1001), ("gid", 1001), ("size", 1)):
            item = directory()
            setattr(item, key, value)
            with self.subTest(key=key), self.assertRaises(RuntimeError):
                admitted_image_path(item)
        for name in ("root/.ssh/id_rsa", "root/.ssh/config", "root/.ssh/nested", "home/pwuser/.ssh", "root/.aws", "run/docker.sock"):
            with self.subTest(name=name), self.assertRaises(RuntimeError):
                for item in (directory(), tarfile.TarInfo(name)):
                    admitted_image_path(item)

    def test_existing_cache_is_regular_bounded_and_identical_before_reuse(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture"
            path.write_bytes(b"public")
            self.assertTrue(existing_cache_matches(path, b"public"))
            self.assertFalse(existing_cache_matches(path, b"secret"))
            self.assertFalse(existing_cache_matches(path, b"tiny"))
            with path.open("wb") as stream:
                stream.truncate(1024**3)  # Sparse synthetic file; no huge read.
            self.assertFalse(existing_cache_matches(path, b"public"))
            path.unlink()
            path.symlink_to(Path(directory))
            self.assertFalse(existing_cache_matches(path, b"public"))
            self.assertFalse(existing_cache_matches(Path(directory), b"public"))

    def test_exact_digest_admission_cannot_be_replaced_by_valid_shape(self):
        with self.assertRaises(RuntimeError):
            driver_files(archive())
        with self.assertRaises(RuntimeError):
            verify_seccomp(b'{"defaultAction":"SCMP_ACT_ERRNO"}')

    def test_hostile_driver_paths_links_and_duplicates_are_refused(self):
        for name, kind in (("/absolute", tarfile.REGTYPE), ("package/../escape", tarfile.REGTYPE), ("package//alias", tarfile.REGTYPE), ("package/link", tarfile.SYMTYPE), ("package/link", tarfile.LNKTYPE), ("package/index.mjs", tarfile.REGTYPE)):
            with self.subTest(name=name, kind=kind), self.assertRaises(RuntimeError):
                admitted_driver_files(archive(((name, b"", kind),)))

    def test_driver_file_parent_collision_is_refused(self):
        with self.assertRaises(RuntimeError):
            admitted_driver_files(archive((("package/lib", b"synthetic", tarfile.REGTYPE),)))

    def test_driver_manifest_hook_dependency_and_wrong_version_are_refused(self):
        for mutation in ({"scripts": {"install": "forbidden"}}, {"dependencies": {"foreign": "1"}}, {"optionalDependencies": {"foreign": "1"}}, {"version": "1.57.0"}):
            files = admitted_driver_files(archive())
            manifest = {"name": "playwright-core", "version": VERSION, **mutation}
            files["package/package.json"] = json.dumps(manifest).encode()
            raw = stage_tar(files)
            buffer = io.BytesIO()
            with tarfile.open(fileobj=io.BytesIO(raw), mode="r:") as source, tarfile.open(fileobj=buffer, mode="w:gz") as target:
                for item in source:
                    if item.isfile():
                        target.addfile(item, source.extractfile(item))
            with self.subTest(mutation=mutation), self.assertRaises(RuntimeError):
                admitted_driver_files(buffer.getvalue())

    def test_safe_stage_tar_has_only_owned_regular_readonly_files(self):
        with tarfile.open(fileobj=io.BytesIO(stage_tar({"reviewed/package/index.mjs": b"public"}))) as stream:
            entries = stream.getmembers()
            self.assertEqual([item.name for item in entries], ["reviewed", "reviewed/package", "reviewed/package/index.mjs"])
            self.assertTrue(all(item.mode == (0o555 if item.isdir() else 0o444) for item in entries))

    def test_container_boundary_rejects_network_privilege_mount_and_profile_changes(self):
        profile = b'{"defaultAction":"SCMP_ACT_ERRNO"}'
        check_container(container(), "owned-run", "owned-image", profile)
        for key, value in (("NetworkMode", "host"), ("ReadonlyRootfs", False), ("CapAdd", ["SYS_ADMIN"]), ("CapDrop", []), ("Binds", ["host:/secret"]), ("IpcMode", "host"), ("PidMode", "host"), ("MemorySwap", -1), ("SecurityOpt", ["no-new-privileges", "seccomp=unconfined"])):
            candidate = container()
            candidate["HostConfig"][key] = value
            with self.subTest(key=key), self.assertRaises((RuntimeError, ValueError)):
                check_container(candidate, "owned-run", "owned-image", profile)

    def test_container_identity_and_ambient_environment_are_refused(self):
        for change in (lambda v: v["Config"].update(User="0:0"), lambda v: v["Config"]["Env"].append("AWS_PROFILE=ambient"), lambda v: v.update(Image="foreign-image"), lambda v: v["Config"]["Labels"].update({LABEL: "foreign-run"})):
            value = copy.deepcopy(container())
            change(value)
            with self.assertRaises(RuntimeError):
                check_container(value, "owned-run", "owned-image", b'{"defaultAction":"SCMP_ACT_ERRNO"}')

    def test_tmpfs_wrong_size_uid_and_executable_modes_are_refused(self):
        for options in (TMP_OPTIONS.replace("128m", "1g"), TMP_OPTIONS.replace("uid=1001", "uid=0"), TMP_OPTIONS.replace("gid=1001", "gid=0"), TMP_OPTIONS.replace("noexec", "exec"), TMP_OPTIONS.replace("0700", "0777")):
            value = container()
            value["HostConfig"]["Tmpfs"]["/tmp"] = options
            with self.subTest(options=options), self.assertRaises(RuntimeError):
                check_container(value, "owned-run", "owned-image", b'{"defaultAction":"SCMP_ACT_ERRNO"}')

    def test_finite_exact_case_output_rejects_skips_omissions_duplicates(self):
        good = "\n".join("PASS browser " + name for name in CASES)
        check_output(good)
        for output in (good + "\nSKIP unsupported", good.split("\n", 1)[1], good + "\nPASS browser browser-closed", good.replace("renderer-kernel-sandbox", "configured-sandbox")):
            with self.assertRaises(RuntimeError):
                check_output(output)


if __name__ == "__main__":
    unittest.main()
