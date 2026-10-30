"""Local helper contracts; real subprocess and socket creation are denied.

Run: python3 -I -B -S -m unittest discover -s internal/buildspec -p runner_test.py
No authored Dockerfile, pack process or generated build command is executed.
"""
import hashlib
import importlib.util
import io
import json
import pathlib
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("registry_build_helper", pathlib.Path(__file__).with_name("runner.py"))
helper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helper)


def guard(event, args):
    if event in {"subprocess.Popen", "os.system", "socket.__new__", "os.exec", "os.posix_spawn"}:
        raise AssertionError("real process/network call forbidden in helper tests")


sys.addaudithook(guard)


class HelperContracts(unittest.TestCase):
    def test_guard_rejects_real_process_and_socket_before_execution(self):
        import socket
        with self.assertRaises(AssertionError):
            subprocess.run([sys.executable, "-c", "raise SystemExit(0)"], check=True)
        with self.assertRaises(AssertionError):
            socket.socket()

    def archive(self, root, members=None):
        filename = root / "source.tar"
        with tarfile.open(filename, "w") as archive:
            for info, data in members or [(tarfile.TarInfo("Dockerfile"), b"FROM scratch\n")]:
                if info.isfile():
                    info.size = len(data)
                archive.addfile(info, io.BytesIO(data))
        return filename, "sha256:" + hashlib.sha256(filename.read_bytes()).hexdigest()

    def config(self, root):
        archive, digest = self.archive(root)
        tools = {}
        for name in ("Docker", "Python", "Pack", "amd64", "arm64"):
            binary = root / name
            binary.write_bytes(b"not executed")
            tools[name] = {"Path": str(binary), "SHA256": hashlib.sha256(binary.read_bytes()).hexdigest()}
        tools["Wrappers"] = {name: tools.pop(name) for name in ("amd64", "arm64")}
        tools["RunnerPlatform"] = "linux/amd64"
        auth = root / "docker-auth"
        auth.mkdir()
        (auth / "config.json").write_text(json.dumps({"auths": {"registry.test": {"auth": "c2NvcGVkOnRva2Vu"}}}))
        return {"Tools": tools, "Root": str(root), "SourceArchive": str(archive), "SourceDigest": digest,
                "MaxFiles": 10, "MaxExpandedBytes": 1024,
                "RegistryAuthPath": str(auth), "RegistryEndpoints": ["registry.test"], "Builds": []}

    def test_source_digest_bounds_traversal_and_links(self):
        for kind in ("digest", "traversal", "symlink", "hardlink", "device", "size", "files"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory)
                config = self.config(root)
                if kind == "digest":
                    config["SourceDigest"] = "sha256:" + "0" * 64
                elif kind in ("traversal", "symlink", "hardlink", "device"):
                    member = tarfile.TarInfo("../../escape" if kind == "traversal" else "link")
                    if kind != "traversal":
                        member.type = {"symlink": tarfile.SYMTYPE, "hardlink": tarfile.LNKTYPE, "device": tarfile.CHRTYPE}[kind]
                        member.linkname = "/etc/passwd"
                    filename, digest = self.archive(root, [(member, b"payload")])
                    config.update(SourceArchive=str(filename), SourceDigest=digest)
                elif kind == "size":
                    config["MaxExpandedBytes"] = 1
                else:
                    config["MaxFiles"] = 0
                with self.assertRaises(ValueError):
                    helper.extract_source(config, root / "source")
                self.assertFalse((root.parent / "escape").exists())

    def test_source_extract_is_exclusive_and_preserves_exec_mode_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            config = self.config(root)
            source = root / "source"
            helper.extract_source(config, source)
            self.assertEqual((source / "Dockerfile").read_bytes(), b"FROM scratch\n")
            self.assertEqual((source / "Dockerfile").stat().st_mode & 0o7777, 0o600)
            with self.assertRaises(FileExistsError):
                helper.extract_source(config, source)

    def test_process_toml_roundtrip_without_running_authored_command(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory) / "processes"
            command = "echo '\"'; $(must_not_run)\n😀\x7f"
            helper.process_buildpack(root, {"web": command})
            parsed = tomllib.loads((root / "launch.toml").read_text())
            self.assertEqual(parsed["processes"][0]["command"], ["/bin/sh", "-c", command])
            self.assertNotIn("must_not_run", (root / "bin" / "build").read_text())
            self.assertIn("cp --", (root / "bin" / "build").read_text())

    def test_pack_argv_keeps_private_values_data_and_scrubs_ambient_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            config = self.config(root)
            private = "PRIVATE-'\n$(must_not_run)"
            config["Builds"] = [{"Name": "app", "Image": "registry.test/app:build-key", "Goarch": "",
                                 "Builder": "registry.test/builder@sha256:" + "a" * 64,
                                 "Definition": {"strategy": "buildpack", "context": ".", "env": {"PRIVATE": private}, "processes": {"web": private}}}]
            calls = []
            with patch.dict(helper.os.environ, {"AWS_SECRET_ACCESS_KEY": "ambient-secret", "DOCKER_HOST": "tcp://foreign"}), patch.object(helper.subprocess, "run", side_effect=lambda argv, **kwargs: calls.append((argv, kwargs))):
                helper.main(config)
            pack, push = calls
            self.assertIn("PRIVATE=" + private, pack[0])
            self.assertEqual(push[0][-2:], ["push", "registry.test/app:build-key"])
            self.assertEqual(pack[1]["env"]["DOCKER_HOST"], "unix:///var/run/docker.sock")
            self.assertNotIn("AWS_SECRET_ACCESS_KEY", pack[1]["env"])
            self.assertTrue(pack[1]["check"])

    def test_wrapper_preserves_entrypoint_cmd_and_architecture_without_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            binary = root / "wrapper"
            binary.write_bytes(b"pinned wrapper")
            command = ["PRIVATE-ARG", "argument with ' and $()"]
            image_config = [{"Config": {"Entrypoint": ["/bin/start"], "Cmd": command}}]
            with patch.object(helper.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, stdout=json.dumps(image_config).encode())) as fake:
                helper.wrap_lambda("/pinned/docker", str(binary), "registry-local/app:unwrapped", "registry.test/app:build-key", "arm64", root / "output")
            dockerfile = (root / "output" / "Dockerfile").read_text()
            self.assertIn("TMPRL_ORIGINAL_ENTRYPOINT_B64=", dockerfile)
            self.assertIn("CMD " + json.dumps(command, separators=(",", ":")), dockerfile)
            self.assertIn("linux/arm64", fake.call_args_list[-1].args[0])
            self.assertEqual((root / "output" / "secret-wrapper").read_bytes(), binary.read_bytes())

    def test_registry_helpers_foreign_hosts_and_tool_tampering_fail_before_subprocess(self):
        for change in ("helper", "foreign", "tool"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                root = pathlib.Path(directory)
                config = self.config(root)
                if change == "tool":
                    pathlib.Path(config["Tools"]["Docker"]["Path"]).write_bytes(b"tampered")
                else:
                    auth = {"auths": {"foreign.test" if change == "foreign" else "registry.test": {"auth": "private"}}}
                    if change == "helper":
                        auth["credsStore"] = "ambient-helper"
                    (pathlib.Path(config["RegistryAuthPath"]) / "config.json").write_text(json.dumps(auth))
                with patch.object(helper.subprocess, "run") as fake, self.assertRaises(ValueError):
                    helper.main(config)
                fake.assert_not_called()


if __name__ == "__main__":
    unittest.main()
