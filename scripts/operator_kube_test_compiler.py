"""Two closed native integration compiler profiles; no CLI/target passthrough."""
from __future__ import annotations

import hashlib
from pathlib import Path
import re
import shutil
from types import MappingProxyType

TARGET = "./integration"
PACKAGE = "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/integration"
PROFILES = MappingProxyType({
    "diagnostic-browser": "integration,browser_integration",
    "candidate-preparation": "integration,preparation_integration",
})


class CompilerContractFailure(RuntimeError):
    pass


def compile_local_diagnostics(socket, directory, test_destination, command_destination):
    from operator_go_linux_compiler import Compiler
    directory.mkdir(mode=0o700)
    compiler = Compiler(socket, directory)
    primary = None
    try:
        artifacts, _, manifest = compiler.compile_local_diagnostics()
        expected = [(PACKAGE, Path("integration"), test_destination),
                    ("github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/cmd/registry-diagnostics", Path("cmd/registry-diagnostics"), command_destination)]
        if len(artifacts) != 2 or manifest["target"] != "./..." or manifest["tags"] != "integration" or re.fullmatch(r"[a-f0-9]{64}", manifest.get("digest", "")) is None:
            raise CompilerContractFailure("LocalDiagnosticSelectionInvalid")
        hashes = []
        for artifact, (package, workdir, destination) in zip(artifacts, expected):
            if artifact[:2] != (package, workdir):
                raise CompilerContractFailure("LocalDiagnosticSelectionInvalid")
            shutil.copyfile(artifact[2], destination)
            destination.chmod(0o555)
            with destination.open("rb") as stream:
                hashes.append(hashlib.file_digest(stream, "sha256").hexdigest())
        return {"compilerInputSHA256": manifest["digest"], "staticELFSHA256": hashes[0], "commandELFSHA256": hashes[1]}
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            compiler.close()
        except Exception as error:
            if primary is None:
                raise
            primary.add_note(str(error))


def compile_profile(profile, socket, directory, destination):
    if type(profile) is not str or profile not in PROFILES:
        raise CompilerContractFailure("ProfileInvalid")
    from operator_go_linux_compiler import Compiler
    directory.mkdir(mode=0o700)
    compiler = Compiler(socket, directory)
    primary = None
    try:
        tags = PROFILES[profile]
        artifacts, _, manifest = compiler.compile(TARGET, tags)
        if len(artifacts) != 1 or artifacts[0][0] != PACKAGE or artifacts[0][1] != Path("integration") or manifest["target"] != TARGET or manifest["tags"] != tags:
            raise CompilerContractFailure("SelectionInvalid")
        if type(manifest.get("digest")) is not str or re.fullmatch(r"[a-f0-9]{64}", manifest["digest"]) is None:
            raise CompilerContractFailure("ManifestDigestInvalid")
        # The reviewed Compiler.extract has already required one bounded native
        # regular static ELF, before any package initialization can execute.
        shutil.copyfile(artifacts[0][2], destination)
        destination.chmod(0o555)
        with destination.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        return {"compilerInputSHA256": manifest["digest"], "staticELFSHA256": digest}
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            compiler.close()
        except Exception as error:
            if primary is None:
                raise
            primary.add_note(str(error))
