"""Static execution helper. Private config is not a diagnostic/history DTO.

Only the compiler stages this file. Source/archive and executor provenance must
be independently verified by the adapter before invocation. This helper checks
bytes and file containment; it does not authenticate DTO claims or IAM policy.
"""
import base64
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import sys
import tarfile

CHILD_ENV = {}


def verify_file(filename, expected):
    digest = hashlib.sha256()
    with open(filename, "rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != expected:
        raise ValueError("input checksum mismatch")


def extract_source(config, destination):
    if pathlib.Path(config["SourceArchive"]).stat().st_size > 256 * 1024 * 1024:
        raise ValueError("source archive too large")
    verify_file(config["SourceArchive"], config["SourceDigest"].removeprefix("sha256:"))
    destination.mkdir(mode=0o700)  # never reuse an earlier source directory
    count, expanded = 0, 0
    with tarfile.open(config["SourceArchive"], "r:*") as archive:
        for member in archive:
            count += 1
            expanded += member.size
            parts = pathlib.PurePosixPath(member.name)
            if (count > config["MaxFiles"] or expanded > config["MaxExpandedBytes"]
                    or parts.is_absolute() or ".." in parts.parts
                    or "\\" in member.name or member.size < 0
                    or not (member.isdir() or member.isfile())):
                raise ValueError("unsupported or unsafe source archive")
            target = destination.joinpath(*parts.parts)
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True, mode=0o700)
                continue
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            # Exclusive create prevents duplicate members and directory overwrite.
            with target.open("xb") as output, archive.extractfile(member) as source:
                shutil.copyfileobj(source, output, 1024 * 1024)
            target.chmod(0o700 if member.mode & 0o111 else 0o600)


def run(argv, **kwargs):
    # Do not print argv: pack environment values and process config are private.
    subprocess.run(argv, check=True, env=CHILD_ENV, **kwargs)


def process_buildpack(root, processes):
    root.mkdir(mode=0o700)
    (root / "bin").mkdir(mode=0o700)
    (root / "buildpack.toml").write_text(
        'api = "0.10"\n[buildpack]\nid = "tmprl-demo/generated-processes"\n'
        'name = "registry generated process types"\nversion = "0.0.1"\n',
        encoding="utf-8")
    # Keep Unicode scalar characters literal; JSON surrogate-pair escapes are
    # invalid TOML. DEL must be escaped even though JSON permits it literally.
    toml_string = lambda value: json.dumps(value, ensure_ascii=False).replace("\x7f", "\\u007f")
    launch = ""
    for name, command in sorted(processes.items()):
        launch += ('[[processes]]\ntype = ' + toml_string(name)
                   + '\ncommand = ["/bin/sh", "-c", '
                   + toml_string(command)
                   + ']\ndefault = false\n\n')
    (root / "launch.toml").write_text(launch, encoding="utf-8")
    (root / "bin" / "detect").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (root / "bin" / "build").write_text(
        '#!/bin/sh\nset -eu\ncp -- "$(dirname "$0")/../launch.toml" "$1/launch.toml"\n',
        encoding="utf-8")
    (root / "bin" / "detect").chmod(0o700)
    (root / "bin" / "build").chmod(0o700)


def wrap_lambda(docker, binary, base, image, goarch, root):
    root.mkdir(mode=0o700)
    shutil.copyfile(binary, root / "secret-wrapper")
    (root / "secret-wrapper").chmod(0o755)
    # Capture instead of logging image config, which may contain sensitive env.
    output = subprocess.run([docker, "image", "inspect", base], check=True,env=CHILD_ENV,
                            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL).stdout
    config = json.loads(output)[0].get("Config") or {}
    entrypoint, command = config.get("Entrypoint") or [], config.get("Cmd") or []
    if not all(isinstance(value, str) for value in entrypoint + command):
        raise ValueError("unsupported image command metadata")
    encode = lambda value: base64.b64encode(json.dumps(value, separators=(",", ":")).encode()).decode()
    dockerfile = [
        "FROM " + base,
        "COPY secret-wrapper /opt/tmprl-demo-cloud/bin/secret-wrapper",
        "ENV TMPRL_ORIGINAL_ENTRYPOINT_B64=" + encode(entrypoint),
        "ENV TMPRL_ORIGINAL_CMD_B64=" + encode(command),
        'ENTRYPOINT ["/opt/tmprl-demo-cloud/bin/secret-wrapper"]',
    ]
    if command:
        dockerfile.append("CMD " + json.dumps(command, separators=(",", ":")))
    (root / "Dockerfile").write_text("\n".join(dockerfile) + "\n", encoding="utf-8")
    run([docker, "build", "--platform", "linux/" + goarch, "-f",
         str(root / "Dockerfile"), "-t", image, str(root)])


def main(config):
    tools = config["Tools"]
    for binary in [tools["Docker"], tools["Python"], tools["Pack"]] + list(tools["Wrappers"].values()):
        if binary["Path"]:
            verify_file(binary["Path"], binary["SHA256"])
    root = pathlib.Path(config["Root"])
    # The adapter stages scoped ECR auth privately. Never use ambient HOME,
    # credential helpers, Docker contexts or inherited AWS/source credentials.
    auth_root = pathlib.Path(config["RegistryAuthPath"])
    with (auth_root / "config.json").open(encoding="utf-8") as stream:
        auth = json.load(stream)
    if set(auth) != {"auths"} or set(auth["auths"]) != set(config["RegistryEndpoints"]):
        raise ValueError("registry authentication scope mismatch")
    for entry in auth["auths"].values():
        if set(entry) != {"auth"} or not isinstance(entry["auth"], str):
            raise ValueError("unsupported registry authentication")
    (root / "home").mkdir(mode=0o700)
    CHILD_ENV.update({"PATH":"/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                      "HOME":str(root / "home"),"DOCKER_CONFIG":str(auth_root),
                      "DOCKER_HOST":"unix:///var/run/docker.sock"})
    source = root / "source"
    extract_source(config, source)
    docker = tools["Docker"]["Path"]
    for build in config["Builds"]:
        definition = build["Definition"]
        image, goarch = build["Image"], build["Goarch"]
        if definition.get("strategy", "dockerfile") == "dockerfile":
            base = "registry-local/" + build["Name"] + ":unwrapped" if goarch else image
            argv = [docker, "build"]
            if goarch:
                argv += ["--platform", "linux/" + goarch]
            argv += ["-f", str(source / definition.get("dockerfile", "Dockerfile")),
                     "-t", base, str(source / definition["context"])]
            run(argv)
            if goarch:
                wrap_lambda(docker, tools["Wrappers"][goarch]["Path"], base,
                            image, goarch, root / ("wrapper-" + build["Name"]))
        else:
            argv = [tools["Pack"]["Path"], "build", image, "--builder",
                    build["Builder"], "--path", str(source / definition["context"])]
            for name, value in sorted(definition.get("env", {}).items()):
                argv += ["--env", name + "=" + value]
            if definition.get("processes"):
                processes = root / ("processes-" + build["Name"])
                process_buildpack(processes, definition["processes"])
                argv += ["--post-buildpack", str(processes)]
            run(argv)
        run([docker, "push", image])


if __name__ == "__main__":
    try:
        with open(sys.argv[1], encoding="utf-8") as source:
            main(json.load(source))
    except Exception:
        # No exception text, argv, source path or private config in diagnostics.
        print("registry build execution failed; inspect authorized build evidence", file=sys.stderr)
        sys.exit(1)
