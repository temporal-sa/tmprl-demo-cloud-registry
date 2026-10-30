#!/usr/bin/env python3
"""Build ONLY the maintained wrapper for linux/amd64 and linux/arm64 offline.

Requires an explicit, checksum-pinned existing Go executable and expected exact
version, plus an already acquired module cache. Does not download tools/modules,
execute authored build commands, contact AWS or authenticate the supplied Go
toolchain. Review that artifact's provenance independently before accepting its
hash. Output is a NEW private directory containing binaries and a safe manifest;
its ToolsWrappers can be copied into a reviewed buildspec.Tools manifest only
after independent runner/tool installation and checksum verification.

The bootstrap producer/delivery, Lambda image integration and IAM are separate.
This is local repeat-build evidence, not a signed/reproducible supply-chain
attestation or proof that an arbitrary configured compiler is trustworthy.
The executable hash alone does not bind the complete GOROOT compiler/linker/
standard-library tree; full toolchain provenance/attestation is separate work.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
MODULE = ROOT / "operator-v2"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sources() -> dict[str, str]:
    paths = [MODULE / "go.mod", MODULE / "go.sum", Path(__file__).resolve()]
    for directory in (MODULE / "internal/lambdawrapper", MODULE / "cmd/registry-lambda-wrapper"):
        paths.extend(sorted(directory.glob("*.go")))
    if len(paths) < 5 or any(path.is_symlink() or not path.is_file() for path in paths):
        raise RuntimeError("Wrapper source files must be regular, explicit local files")
    return {str(path.relative_to(ROOT)): digest(path) for path in paths}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--go", required=True, help="Explicit existing Go executable (symlink is resolved and target pinned)")
    parser.add_argument("--go-sha256", required=True, help="Independently approved lowercase SHA-256 of that executable")
    parser.add_argument("--go-version", required=True, help="Exact approved compiler version, e.g. go1.26.4")
    parser.add_argument("--module-cache", required=True, help="Already acquired local module cache; acquisition is separate")
    parser.add_argument("--output", required=True, help="New directory; never overwrite an existing output")
    args = parser.parse_args()
    compiler = Path(args.go).resolve(strict=True)
    cache = Path(args.module_cache).resolve(strict=True)
    output = Path(args.output).absolute()
    if not compiler.is_file() or not os.access(compiler, os.X_OK) or not cache.is_dir():
        parser.error("Existing executable/compiler and module cache directory required")
    if len(args.go_sha256) != 64 or any(c not in "0123456789abcdef" for c in args.go_sha256) or digest(compiler) != args.go_sha256:
        parser.error("Go executable checksum does not match the exact approved binding")
    if output.exists() or output.is_symlink() or output.parent.resolve(strict=True) != output.parent:
        parser.error("Output must be a new path under an existing canonical parent")
    initial = sources()
    output.mkdir(mode=0o700)
    # An incomplete build is retained, explicitly marked, never mistaken for a
    # complete manifest. No rewrite/cleanup of caller-owned paths is performed.
    (output / "INCOMPLETE").write_text("No usable tool manifest until manifest.json exists and INCOMPLETE is absent.\n")
    with tempfile.TemporaryDirectory(prefix="lambda-wrapper-build-") as temp:
        temporary = Path(temp)
        environment = {
            "PATH": os.defpath, "HOME": str(temporary), "GOENV": "off", "GOTOOLCHAIN": "local",
            "GOPROXY": "off", "GOSUMDB": "off", "GOMODCACHE": str(cache), "GOCACHE": str(temporary / "cache"),
            "CGO_ENABLED": "0", "GOOS": "linux", "GOFLAGS": "", "GOWORK": "off", "GOTELEMETRY": "off",
        }

        def command(arguments: list[str]) -> str:
            result = subprocess.run([str(compiler), *arguments], cwd=MODULE, env=environment, capture_output=True, text=True, timeout=240, check=False)
            if result.returncode:
                # Compiler diagnostics are not a secret transport; nevertheless
                # do not dump arbitrary tool output or execution source here.
                raise RuntimeError("Explicit wrapper compiler command failed (output withheld)")
            return result.stdout

        version = command(["version"]).strip()
        fields = version.split()
        if len(fields) != 4 or fields[0:2] != ["go", "version"] or fields[2] != args.go_version:
            raise RuntimeError("Go version does not match the approved exact toolchain")
        command(["mod", "verify"])
        wrappers = {}
        for arch in ("amd64", "arm64"):
            environment["GOARCH"] = arch
            directory = output / arch
            directory.mkdir(mode=0o700)
            binary = directory / "secret-wrapper"
            repeat = temporary / ("repeat-" + arch)
            for destination in (binary, repeat):
                command(["build", "-mod=readonly", "-trimpath", "-buildvcs=false", "-ldflags=-buildid=", "-o", str(destination), "./cmd/registry-lambda-wrapper"])
            checksum = digest(binary)
            if digest(repeat) != checksum:
                raise RuntimeError("Wrapper repeat builds differ; refusing a tool binding")
            binary.chmod(0o555)
            wrappers[arch] = {"Path": "/opt/registry-tools/lambda-wrapper/" + arch + "/secret-wrapper", "SHA256": checksum}
        if digest(compiler) != args.go_sha256 or sources() != initial:
            raise RuntimeError("Compiler or wrapper sources changed during builds")
        source_digest = hashlib.sha256(json.dumps(initial, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        manifest = {
            "Version": "registry-lambda-wrapper-artifacts-v1", "RuntimeVersion": "registry-lambda-wrapper-v1",
            "GoVersion": version, "GoExecutableSHA256": args.go_sha256,
            "SourceDigest": "sha256:" + source_digest, "SourceFiles": initial,
            "ToolsWrappers": wrappers, "RepeatedBuildsEqual": True,
            "Provenance": "Local explicit compiler/checksum plus offline verified module cache; not signed attestation",
            "BootstrapDeliveryVerified": False, "LambdaIntegrationVerified": False,
            "FullGoToolchainTreeVerified": False, "SignedAttestationVerified": False,
        }
        (output / "manifest.json").write_text(json.dumps(manifest, sort_keys=True, indent=2) + "\n")
        (output / "INCOMPLETE").unlink()
    print("Built checksum-bound linux/amd64 and linux/arm64 wrapper artifacts (local evidence only).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
