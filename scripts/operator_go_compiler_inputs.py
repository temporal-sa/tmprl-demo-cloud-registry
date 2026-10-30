"""Build an explicit public-source/module artifact for isolated Go compilation.

Only metadata listing and go.sum module verification occur on the host. No test
binary/package init, generated command or project build command is executed.
"""
import base64
import hashlib
import io
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import tarfile

IMPORT_PATH = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._!/+~-]*$")
MAX_INPUT_BYTES = 512 * 1024 * 1024
MAX_INPUT_FILES = 50000
PUBLIC_EMBEDS = {
    "internal/buildspec/runner.py",
    "internal/project/schemas/project.schema.json",
    "testinfra/sandbox-plan.template.json",
    "testinfra/backend.hcl.example",
    "testinfra/action-resource-review.json",
}


def public_test_fixtures(root):
    paths = [root / "schemas/project.schema.json", *sorted((root / "projects/demo").glob("*.yaml")), *sorted((root / "projects/operator").glob("*.yaml"))]
    for directory in ("fixture-20261030-01", "fixture-20261030-01-deny"):
        paths.extend(root / "operator-v2/testinfra/fixtures" / directory / name for name in ("main.tf.json", "review.json", "temporal.review.json"))
    paths.extend(root / "operator-v2/testinfra/fixtures/fixture-20261030-01" / name for name in ("temporal-project-create.request.json", "temporal-project-create.review.json"))
    return paths


def module_escape(value):
    if not IMPORT_PATH.fullmatch(value) or any(part in {"", ".", ".."} for part in value.split("/")):
        raise RuntimeError("Unexpected public module path/version")
    return "".join("!" + char.lower() if char.isupper() else char for char in value)


def hash1(records):
    digest = hashlib.sha256()
    for name, checksum in sorted(records):
        digest.update((checksum + "  " + name + "\n").encode())
    return "h1:" + base64.b64encode(digest.digest()).decode()


def json_stream(value):
    decoder = json.JSONDecoder()
    while value.strip():
        value = value.lstrip()
        item, end = decoder.raw_decode(value)
        yield item
        value = value[end:]


def snapshot(root, go, environment, target, tags, archive):
    project = root / "operator-v2"
    modules = (root / ".venv/operator-go-modules").resolve(strict=True)
    env = {**environment, "GOWORK": "off", "GOENV": "off", "GOFLAGS": "-mod=readonly -p=1", "GOMAXPROCS": "2"}

    def metadata(arguments):
        result = subprocess.run([go, *arguments], cwd=project, env=env, capture_output=True, text=True, timeout=180)
        if result.returncode or len(result.stdout) > 32 * 1024 * 1024:
            raise RuntimeError("Public Go metadata/module verification failed; restore compile-ready sources and public prerequisite cache")
        return result.stdout

    if metadata(["mod", "verify"]).strip() != "all modules verified":
        raise RuntimeError("Public Go module checksums were not verified")
    # -e permits absent .info for unused lazy-graph modules. No source error is
    # accepted: selected packages below must list successfully, and every
    # staged graph .mod and source tree is independently checked against sum.
    graph = list(json_stream(metadata(["list", "-m", "-json", "-e", "all"])))
    main = [module for module in graph if module.get("Main")]
    if len(main) != 1 or Path(main[0]["Dir"]).resolve() != project or any(module.get("Replace") for module in graph):
        raise RuntimeError("Explicit module graph must have one owned main module and no replacement paths")
    tag_flags = ["-tags", tags] if tags else []
    selected = {package["ImportPath"] for package in json_stream(metadata(["list", "-json", *tag_flags, target]))}
    packages = list(json_stream(metadata(["list", "-deps", "-test", "-json", *tag_flags, target])))
    source = {}
    total = 0
    module_pins = []
    sums = {}
    for line in (project / "go.sum").read_text().splitlines():
        path, version, checksum = line.split()
        if (path, version) in sums and sums[path, version] != checksum:
            raise RuntimeError("Conflicting public module checksum pins")
        sums[path, version] = checksum

    def add(path, destination):
        nonlocal total
        path = Path(path).absolute()
        if destination in source:
            if source[destination][0] != path:
                raise RuntimeError("Public artifact destination collision")
            return source[destination][1]
        info = path.lstat()
        if path.is_symlink() or path.resolve() != path or not stat.S_ISREG(info.st_mode) or info.st_size > 16 * 1024 * 1024:
            raise RuntimeError("Public artifact input must be a bounded regular non-symlink file")
        total += info.st_size
        if total > MAX_INPUT_BYTES or len(source) >= MAX_INPUT_FILES:
            raise RuntimeError("Explicit public compiler artifact exceeds its bound")
        data = path.read_bytes()
        source[destination] = (path, hashlib.sha256(data).hexdigest(), len(data))
        return source[destination][1]

    for name in ("go.mod", "go.sum"):
        add(project / name, "workspace/operator-v2/" + name)
    main_module = main[0]["Path"]
    needed_modules = {}
    tests = {}
    for package in packages:
        module = package.get("Module")
        if not module:
            continue  # Standard library belongs to the pinned compiler image.
        if not module.get("Main"):
            needed_modules[(module["Path"], module["Version"])] = module
            continue
        import_path = package["ImportPath"]
        if import_path.endswith(".test") and package.get("Name") == "main":
            continue  # Go regenerates its own test main inside isolation.
        directory = Path(package["Dir"]).resolve(strict=True)
        relative = directory.relative_to(project)
        if not relative.parts or relative.parts[0] not in {"cmd", "internal", "integration", "testinfra"} or "fixtures" in relative.parts or any(part.startswith(".") for part in relative.parts):
            raise RuntimeError("Go source package is outside explicit public code directories")
        for field in ("GoFiles", "TestGoFiles", "XTestGoFiles", "SFiles", "HFiles", "SysoFiles", "EmbedFiles", "TestEmbedFiles", "XTestEmbedFiles"):
            for filename in package.get(field, []):
                path = directory / filename
                if field.endswith("EmbedFiles") and path.relative_to(project).as_posix() not in PUBLIC_EMBEDS:
                    raise RuntimeError("New repository embed requires explicit public artifact review")
                add(path, "workspace/operator-v2/" + str(path.relative_to(project)))
        if import_path in selected and not package.get("ForTest") and (package.get("TestGoFiles") or package.get("XTestGoFiles")):
            if not IMPORT_PATH.fullmatch(import_path) or not import_path.startswith(main_module + "/"):
                raise RuntimeError("Unexpected test package import path")
            tests[import_path] = str(relative)
    for module in graph:
        if module.get("Main"):
            continue
        path, version = module["Path"], module["Version"]
        expected = sums.get((path, version + "/go.mod"))
        mod_file = modules / "cache/download" / module_escape(path) / "@v" / (module_escape(version) + ".mod")
        if (not expected or not mod_file.exists()) and (path, version) not in needed_modules:
            continue  # Unused lazy-graph metadata is neither trusted nor copied.
        if not expected or hash1([("go.mod", hashlib.sha256(mod_file.read_bytes()).hexdigest())]) != expected:
            raise RuntimeError("Public module graph metadata is missing or differs from go.sum: " + path + "@" + version)
        if module.get("GoMod") and Path(module["GoMod"]).absolute() != mod_file:
            raise RuntimeError("Module metadata escaped the exact public cache path")
        for suffix in (".mod", ".info", ".ziphash"):
            path = mod_file.with_suffix(suffix)
            if path.exists():
                add(path, "module-metadata/" + str(path.relative_to(modules / "cache")))
    for key, module in sorted(needed_modules.items()):
        if not module.get("Sum") or not module.get("Dir") or module.get("Replace") or module.get("Error") or sums.get(key) != module["Sum"]:
            raise RuntimeError("Required public module source is not checksum pinned")
        directory = modules / (module_escape(key[0]) + "@" + module_escape(key[1]))
        if Path(module["Dir"]).absolute() != directory or directory.resolve(strict=True) != directory:
            raise RuntimeError("Module source escaped the exact public cache path")
        module_pins.append({"path": key[0], "version": key[1], "sum": module["Sum"]})
        records = []
        for path in sorted(directory.rglob("*")):
            if path.is_dir() and not path.is_symlink():
                continue
            checksum = add(path, "modules/" + str(path.relative_to(modules)))
            records.append((key[0] + "@" + key[1] + "/" + path.relative_to(directory).as_posix(), checksum))
        if hash1(records) != module["Sum"]:
            raise RuntimeError("Staged public module tree differs from its exact go.sum checksum")
    if not tests:
        raise RuntimeError("No exact Go test packages selected")
    fixtures = []
    if target == "./...":
        for path in public_test_fixtures(root):
            name = "workspace/" + path.relative_to(root).as_posix()
            add(path, name)
            fixtures.append(name)
    add(root / "scripts/operator_unit_boundary.go", "inputs/proof.go")
    records = [{"path": path, "sha256": item[1], "bytes": item[2]} for path, item in sorted(source.items())]
    manifest = {"schema": 1, "target": target, "tags": tags, "modules": module_pins, "packages": [{"path": key, "workdir": value, "binary": "package-" + str(index)} for index, (key, value) in enumerate(sorted(tests.items()))], "fixtures": fixtures, "files": records, "bytes": total}
    manifest_bytes = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
    manifest["digest"] = hashlib.sha256(manifest_bytes).hexdigest()
    with tarfile.open(archive, "w") as stream:
        directories = set()
        for name in [*source, "inputs/manifest.json"]:
            current = Path(name).parent
            while str(current) != ".":
                directories.add(str(current))
                current = current.parent
        for name in sorted(directories, key=lambda item: (item.count("/"), item)):
            info = tarfile.TarInfo(name)
            info.type, info.mode = tarfile.DIRTYPE, 0o555
            stream.addfile(info)
        for name, (path, digest, size) in sorted(source.items()):
            data = path.read_bytes()
            if len(data) != size or hashlib.sha256(data).hexdigest() != digest:
                raise RuntimeError("Public source changed during compiler snapshot")
            info = tarfile.TarInfo(name)
            info.mode, info.size = 0o444, size
            stream.addfile(info, io.BytesIO(data))
        data = json.dumps(manifest, sort_keys=True, separators=(",", ":")).encode()
        info = tarfile.TarInfo("inputs/manifest.json")
        info.mode, info.size = 0o444, len(data)
        stream.addfile(info, io.BytesIO(data))
    return manifest
