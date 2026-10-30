"""Negative contracts for compile-only public artifacts; no Docker/network."""
import hashlib
import io
import json
from pathlib import Path
import struct
import sys
import tarfile
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from operator_go_compiler_inputs import hash1, module_escape, snapshot
from operator_go_linux_compiler import extract_elf


def elf(machine=183, segment=1):
    value = bytearray(120)
    value[:7] = b"\x7fELF\x02\x01\x01"
    struct.pack_into("<HH", value, 16, 2, machine)
    struct.pack_into("<Q", value, 32, 64)
    struct.pack_into("<HH", value, 54, 56, 1)
    struct.pack_into("<I", value, 64, segment)
    return bytes(value)


def archive(data, name="package-0", kind=tarfile.REGTYPE, extra=False):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as stream:
        entry = tarfile.TarInfo(name)
        entry.type = kind
        entry.size = len(data) if kind == tarfile.REGTYPE else 0
        stream.addfile(entry, io.BytesIO(data) if entry.size else None)
        if extra:
            stream.addfile(tarfile.TarInfo("unexpected"))
    return output.getvalue()


class CompilerContract(unittest.TestCase):
    def public_snapshot(self, root, *, symlink=False, embed=False):
        project = root / "operator-v2"
        package = project / "internal/example"
        package.mkdir(parents=True)
        (root / ".venv/operator-go-modules").mkdir(parents=True)
        (root / "scripts").mkdir()
        (root / "scripts/operator_unit_boundary.go").write_text("package main\n")
        (project / "go.mod").write_text("module example.com/public\n")
        (project / "go.sum").write_text("")
        (package / "example_test.go").write_text("package example\n")
        if symlink:
            outside = root / "synthetic-private.go"
            outside.write_text("synthetic marker, never staged")
            (package / "example.go").symlink_to(outside)
        else:
            (package / "example.go").write_text("package example\n")
        module = {"Path": "example.com/public", "Dir": str(project), "Main": True}
        info = {"ImportPath": "example.com/public/internal/example", "Dir": str(package), "Module": module, "GoFiles": ["example.go"], "TestGoFiles": ["example_test.go"]}
        if embed:
            (package / "private.txt").write_text("synthetic unreviewed embed")
            info["EmbedFiles"] = ["private.txt"]

        def metadata(arguments, **kwargs):
            if arguments[1:3] == ["mod", "verify"]:
                output = "all modules verified"
            elif "-m" in arguments:
                output = json.dumps(module)
            else:
                output = json.dumps(info)
                if "-deps" in arguments:
                    # Dependencies may expose TestGoFiles metadata but they
                    # must never silently become requested test targets.
                    dependency = {**info, "ImportPath": "example.com/public/internal/dependency"}
                    output += "\n" + json.dumps(dependency)
            return SimpleNamespace(returncode=0, stdout=output)

        with patch("operator_go_compiler_inputs.subprocess.run", side_effect=metadata):
            return snapshot(root, "/synthetic/go", {}, "./internal/example", "", root / "inputs.tar")

    def test_only_requested_package_becomes_test_target(self):
        with tempfile.TemporaryDirectory() as directory:
            result = self.public_snapshot(Path(directory).resolve())
            self.assertEqual([item["path"] for item in result["packages"]], ["example.com/public/internal/example"])

    def test_symlink_source_is_rejected_before_staging(self):
        with tempfile.TemporaryDirectory() as directory, self.assertRaisesRegex(RuntimeError, "non-symlink"):
            self.public_snapshot(Path(directory).resolve(), symlink=True)

    def test_unreviewed_repository_embed_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory, self.assertRaisesRegex(RuntimeError, "embed requires"):
            self.public_snapshot(Path(directory).resolve(), embed=True)

    def test_exact_native_regular_static_artifact(self):
        for arch, machine in (("arm64", 183), ("amd64", 62)):
            value = elf(machine)
            self.assertEqual(extract_elf(archive(value), "package-0", arch), value)

    def test_cross_platform_dynamic_and_corrupt_artifacts_rejected(self):
        for value in (elf(62), elf(segment=2), elf(segment=3), b"not ELF", elf()[:100]):
            with self.subTest(value=value[:20]), self.assertRaises(RuntimeError):
                extract_elf(archive(value), "package-0", "arm64")

    def test_unexpected_names_links_and_extra_entries_rejected(self):
        for raw in (archive(elf(), name="../package-0"), archive(elf(), kind=tarfile.SYMTYPE), archive(elf(), kind=tarfile.LNKTYPE), archive(elf(), extra=True)):
            with self.assertRaises(RuntimeError):
                extract_elf(raw, "package-0", "arm64")

    def test_module_cache_paths_are_bounded_and_escaped(self):
        self.assertEqual(module_escape("github.com/BurntSushi/toml"), "github.com/!burnt!sushi/toml")
        for name in ("../secret", "/absolute", "a//b", "a/../b", "a b", "a\\b"):
            with self.subTest(name=name), self.assertRaises(RuntimeError):
                module_escape(name)

    def test_go_hash1_known_mod_pin_and_order(self):
        records = [("go.mod", hashlib.sha256(b"module example.com/public\n").hexdigest())]
        self.assertEqual(hash1(records), "h1:AqOC1BNjmnfhWIfZs1PLbqgVsMgHi37bn5dzCofkh5E=")
        self.assertEqual(hash1([("z", "2"), ("a", "1")]), hash1([("a", "1"), ("z", "2")]))


if __name__ == "__main__":
    unittest.main()
