"""Compile static Go test artifacts inside a pinned owned offline Linux container.

This helper never executes project test/package initialization. Scratch test
execution is a separate caller-owned phase with the existing strict proof.
"""
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import selectors
import struct
import subprocess
import tarfile
import tempfile
import time
import uuid

from operator_go_compiler_inputs import snapshot
from prepare_operator_go_compiler import VERSION, reference, verify_image

ROOT = Path(__file__).resolve().parents[1]
LABEL = "operator.local/run-id"
MAX_BINARY_BYTES = 128 * 1024 * 1024
MAX_OUTPUT_BYTES = 768 * 1024 * 1024


def extract_elf(raw, filename, architecture):
    if len(raw) > MAX_BINARY_BYTES + 2 * 1024 * 1024:
        raise RuntimeError("Compiled artifact exceeds its extraction bound")
    with tarfile.open(fileobj=io.BytesIO(raw), mode="r:*") as stream:
        entries = stream.getmembers()
        if len(entries) != 1 or not entries[0].isfile() or entries[0].name != filename or entries[0].size > MAX_BINARY_BYTES:
            raise RuntimeError("Compiled output is not the exact bounded regular file")
        data = stream.extractfile(entries[0]).read()
    machine = {"arm64": 183, "amd64": 62}.get(architecture)
    if machine is None or len(data) < 64 or data[:7] != b"\x7fELF\x02\x01\x01" or struct.unpack_from("<HH", data, 16) != (2, machine):
        raise RuntimeError("Compiled artifact is not the native static ELF class")
    offset = struct.unpack_from("<Q", data, 32)[0]
    width, count = struct.unpack_from("<HH", data, 54)
    if width < 56 or count > 128 or offset + width * count > len(data) or any(struct.unpack_from("<I", data, offset + index * width)[0] in {2, 3} for index in range(count)):
        raise RuntimeError("Compiled artifact requests a dynamic loader/shared runtime")
    return data


class Compiler:
    def __init__(self, docker_socket, directory):
        self.directory = Path(directory)
        self.docker, self.go = shutil.which("docker"), shutil.which("go")
        endpoint = Path(docker_socket).resolve(strict=True)
        if not self.docker or not self.go or not endpoint.is_socket():
            raise RuntimeError("Existing metadata-only Go tool and explicit local Docker Unix socket required")
        home = self.directory / "compiler-home"
        home.mkdir(mode=0o700)
        self.env = {"PATH": os.defpath, "HOME": str(home), "DOCKER_CONFIG": str(home), "DOCKER_HOST": "unix://" + str(endpoint), "GOTOOLCHAIN": "local", "GOPROXY": "off", "GOSUMDB": "off", "CGO_ENABLED": "0", "GOOS": "linux", "GOWORK": "off", "GOENV": "off", "GOCACHE": str(ROOT / ".venv/operator-go-cache"), "GOMODCACHE": str(ROOT / ".venv/operator-go-modules")}
        server = json.loads(self.command(["version", "--format", "{{json .Server}} "]))
        if server.get("Os") != "linux" or server.get("Arch") not in {"arm64", "amd64"}:
            raise RuntimeError("Only native Linux amd64/arm64 Docker compiler targets are supported")
        self.arch = server["Arch"]
        self.env["GOARCH"] = self.arch
        self.base = reference(self.arch)
        self.base_id = verify_image(self.docker, self.env, self.arch)
        self.run = "operator-compiler-" + uuid.uuid4().hex[:12]
        self.staging, self.container = self.run + "-stage", self.run + "-compile"
        self.image, self.image_id, self.container_id = self.run + ":owned", None, None

    def command(self, arguments, *, timeout=180, binary=False, data=None):
        result = subprocess.run([self.docker, *map(str, arguments)], env=self.env, input=data, capture_output=True, timeout=timeout)
        if result.returncode:
            # Inputs are only public source/metadata; no user credentials or
            # raw private runtime payloads exist in this compilation tier.
            detail = (result.stdout + result.stderr).decode(errors="replace")[-12000:]
            raise RuntimeError("Owned Linux compiler command failed: " + str(arguments[0]) + "\n" + detail)
        return result.stdout if binary else result.stdout.decode()

    def inspect(self, kind, name):
        result = subprocess.run([self.docker, "inspect", "--type=" + kind, name], env=self.env, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            return json.loads(result.stdout)[0]
        if "No such" in result.stderr:
            return None
        raise RuntimeError("Cannot establish exact compiler resource identity")

    def owned(self, name, image_id, container_id=None):
        value = self.inspect("container", name)
        if value is None:
            return None
        if value["Name"] != "/" + name or value["Config"].get("Labels", {}).get(LABEL) != self.run or value["Image"] != image_id or container_id is not None and value["Id"] != container_id:
            raise RuntimeError("Refusing compiler resource access/removal: ownership changed")
        return value

    def execution_boundary(self):
        value = self.owned(self.container, self.image_id, self.container_id)
        host = value["HostConfig"] if value else {}
        if not value or not value["State"]["Running"] or host.get("NetworkMode") != "none" or not host.get("ReadonlyRootfs") or value["Config"].get("User") != "65532:65532" or set(host.get("CapDrop", [])) != {"ALL"} or not any(item.startswith("no-new-privileges") for item in host.get("SecurityOpt", [])) or host.get("Binds") or host.get("Memory") != 3*1024**3 or host.get("MemorySwap") != 3*1024**3 or host.get("NanoCpus") != 2*10**9 or host.get("PidsLimit") != 128 or set(host.get("Tmpfs", {})) != {"/tmp", "/probe", "/modules/cache"} or any(item.get("Type") != "tmpfs" for item in value.get("Mounts", [])):
            raise RuntimeError("Pinned compiler execution boundary differs; no weaker fallback")

    def extract(self, source, destination):
        self.execution_boundary()
        # Bound the stream before accumulating it on the host, not just the
        # declared tar entry size after extraction. No tar paths are extracted.
        # docker cp excludes tmpfs (Docker CLI's documented corner case).
        # Stream one exact file with the pinned base's tar inside the already
        # verified live namespace instead; never extract archive paths on host.
        process = subprocess.Popen([self.docker, "exec", self.container, "/usr/bin/tar", "--format=ustar", "--no-recursion", "-cf", "-", "--directory", str(Path(source).parent), Path(source).name], env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        chunks, size, detail = [], 0, b""
        deadline = time.monotonic() + 60
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ, "artifact")
                selector.register(process.stderr, selectors.EVENT_READ, "error")
                while selector.get_map():
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise RuntimeError("Compiled artifact extraction exceeded its time bound")
                    for key, _ in selector.select(remaining):
                        value = os.read(key.fileobj.fileno(), 65536)
                        if not value:
                            selector.unregister(key.fileobj)
                        elif key.data == "artifact":
                            size += len(value)
                            if size > MAX_BINARY_BYTES + 2 * 1024 * 1024:
                                raise RuntimeError("Compiled artifact stream exceeds its bound")
                            chunks.append(value)
                        else:
                            detail = (detail + value)[-12000:]
            if process.wait(timeout=max(0.1, deadline - time.monotonic())):
                raise RuntimeError("Owned compiler artifact copy failed: " + detail.decode(errors="replace"))
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=10)
            process.stdout.close()
            process.stderr.close()
        raw = b"".join(chunks)
        data = extract_elf(raw, Path(source).name, self.arch)
        destination.write_bytes(data)
        destination.chmod(0o555)
        return len(data), hashlib.sha256(data).hexdigest()

    def compile(self, target="./...", tags=""):
        return self._compile(target, tags, False)

    def compile_local_diagnostics(self):
        # One reviewed OS entrypoint and integration test from the SAME public
        # workspace. No caller can select command, tags or output path here.
        return self._compile("./...", "integration", True)

    def _compile(self, target, tags, local_diagnostics):
        if type(local_diagnostics) is not bool or local_diagnostics and (target, tags) != ("./...", "integration"):
            raise RuntimeError("Fixed local diagnostic compiler selection invalid")
        archive = self.directory / "compiler-inputs.tar"
        manifest = snapshot(ROOT, self.go, self.env, target, tags, archive)
        # Root here is synthetic content in this owned public image, never the
        # host home. Traversal lets the unchanged strict absence probe verify
        # that the compiler base contains no credential files at these paths.
        with tarfile.open(archive, "a") as stream:
            info = tarfile.TarInfo("root")
            info.type, info.mode = tarfile.DIRTYPE, 0o555
            stream.addfile(info)
        self.command(["create", "--name", self.staging, "--label", LABEL + "=" + self.run, "--entrypoint", "/usr/bin/true", self.base])
        with archive.open("rb") as source:
            result = subprocess.run([self.docker, "cp", "-", self.staging + ":/"], env=self.env, stdin=source, capture_output=True, timeout=180)
        if result.returncode:
            raise RuntimeError("Owned public compiler artifact staging failed")
        self.owned(self.staging, self.base_id)
        print("Frozen public compiler snapshot copied: sha256:" + manifest["digest"] + "; " + str(manifest["bytes"]) + " bytes; " + str(len(manifest["packages"])) + " requested test package(s).", flush=True)
        self.image_id = self.command(["commit", "--change", "USER 65532:65532", "--change", "LABEL " + LABEL + "=" + self.run, "--change", "LABEL operator.local/source-digest=" + manifest["digest"], self.staging, self.image], timeout=180).strip()
        self.command(["rm", self.staging])
        bridge = json.loads(self.command(["network", "inspect", "bridge"]))[0]
        gateway = bridge["IPAM"]["Config"][0]["Gateway"]
        environment = {"HOME": "/tmp/home", "TMPDIR": "/tmp/work", "GOCACHE": "/tmp/cache", "GOPATH": "/tmp/gopath", "GOMODCACHE": "/modules", "GOENV": "off", "GOWORK": "off", "GOTOOLCHAIN": "local", "GOPROXY": "off", "GOSUMDB": "off", "CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": self.arch, "GOMAXPROCS": "2", "GOFLAGS": "-mod=readonly -p=1", "OPERATOR_UNIT_HOST_GATEWAY": gateway}
        arguments = ["run", "-d", "--name", self.container, "--label", LABEL + "=" + self.run, "--network=none", "--read-only", "--cap-drop=ALL", "--security-opt", "no-new-privileges", "--cpus=2", "--memory=3g", "--memory-swap=3g", "--pids-limit=128", "--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=2g,uid=65532,gid=65532,mode=0700", "--tmpfs", "/probe:rw,exec,nosuid,nodev,size=16m,uid=65532,gid=65532,mode=0700", "--tmpfs", "/modules/cache:rw,noexec,nosuid,nodev,size=64m,uid=65532,gid=65532,mode=0700"]
        for key, value in environment.items():
            arguments.extend(["--env", key + "=" + value])
        arguments.extend(["--entrypoint", "/usr/bin/sleep", self.image_id, "infinity"])
        self.container_id = self.command(arguments).strip()
        self.execution_boundary()
        self.command(["exec", self.container, "/usr/bin/mkdir", "-m", "0700", "/tmp/home", "/tmp/work", "/tmp/cache", "/tmp/out", "/tmp/gopath"])
        self.command(["exec", self.container, "/usr/bin/cp", "-R", "/module-metadata/.", "/modules/cache/"])
        self.command(["exec",self.container,"/usr/bin/chmod","-R","u+w","/modules/cache"])
        version = self.command(["exec", self.container, "/usr/local/go/bin/go", "version"]).strip()
        if version != "go version " + VERSION + " linux/" + self.arch:
            raise RuntimeError("Native compiler executable version changed")
        self.command(["exec", "--workdir", "/tmp", self.container, "/usr/local/go/bin/go", "build", "-o", "/probe/proof", "/inputs/proof.go"], timeout=240)
        print(self.command(["exec", self.container, "/probe/proof"]).strip(), flush=True)
        total = 0
        artifacts = []
        packages = manifest["packages"]
        if local_diagnostics:
            packages = [p for p in packages if p["path"] == "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/integration"]
            commands = [p for p in manifest["packages"] if p["path"] == "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/cmd/registry-diagnostics"]
            if len(packages) != 1 or len(commands) != 1:
                raise RuntimeError("Fixed local diagnostic command/test source selection missing")
        for package in packages:
            self.execution_boundary()
            arguments = ["exec", "--workdir", "/workspace/operator-v2", self.container, "/usr/local/go/bin/go", "test", "-c"]
            if tags:
                arguments.extend(["-tags", tags])
            arguments.extend(["-o", "/tmp/out/" + package["binary"], package["path"]])
            self.command(arguments, timeout=300)
            destination = self.directory / package["binary"]
            size, digest = self.extract("/tmp/out/" + package["binary"], destination)
            total += size
            if total > MAX_OUTPUT_BYTES:
                raise RuntimeError("Total compiled artifacts exceed their bound")
            artifacts.append((package["path"], Path(package["workdir"]), destination))
            print("Compiled only (no package init): " + package["path"] + "; ELF sha256:" + digest, flush=True)
        if local_diagnostics:
            self.execution_boundary()
            self.command(["exec", "--workdir", "/workspace/operator-v2", self.container, "/usr/local/go/bin/go", "build", "-o", "/tmp/out/registry-diagnostics", "./cmd/registry-diagnostics"], timeout=300)
            destination = self.directory / "registry-diagnostics"
            size, digest = self.extract("/tmp/out/registry-diagnostics", destination)
            if total + size > MAX_OUTPUT_BYTES:
                raise RuntimeError("Total compiled artifacts exceed their bound")
            artifacts.append(("github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/cmd/registry-diagnostics", Path("cmd/registry-diagnostics"), destination))
            print("Compiled fixed local diagnostic OS entrypoint; ELF sha256:" + digest, flush=True)
        proof = self.directory / "proof"
        self.extract("/probe/proof", proof)
        return artifacts, proof, manifest

    def close(self):
        errors = []
        for name, image_id, container_id in ((self.container, self.image_id, self.container_id), (self.staging, self.base_id, None)):
            try:
                if image_id and self.owned(name, image_id, container_id):
                    self.command(["rm", "-f", name])
            except Exception as error:
                errors.append(str(error))
        try:
            value = self.inspect("image", self.image)
            if value is not None:
                if value["Id"] != self.image_id or value["Config"].get("Labels", {}).get(LABEL) != self.run:
                    raise RuntimeError("Refusing derived compiler image removal: ownership changed")
                self.command(["image", "rm", self.image])
        except Exception as error:
            errors.append(str(error))
        if errors:
            raise RuntimeError("Owned compiler cleanup incomplete: " + "; ".join(errors))


def main():
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docker-socket", default="/var/run/docker.sock")
    parser.add_argument("--focus-catalog", action="store_true", required=True, help="Compile only the catalogdiagnostics cross-link reproducer; execute no project tests")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="operator-linux-compiler-") as directory:
        compiler = Compiler(args.docker_socket, directory)
        failure = None
        try:
            artifacts, _, manifest = compiler.compile("./internal/catalogdiagnostics")
            if len(artifacts) != 1:
                raise RuntimeError("Focused compiler proof selected an unexpected package")
            print(f"Focused original-flag native compilation passed: {len(artifacts)} package, {manifest['bytes']} explicit public input bytes; no project package initialization/test ran.")
        except Exception as error:
            failure = error
            raise
        finally:
            try:
                compiler.close()
            except Exception as error:
                if failure is None:
                    raise
                failure.add_note(str(error))
                print(str(error))


if __name__ == "__main__":
    main()
