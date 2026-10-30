"""Verify one owned kind-node fixture's actual local manifest/config identity.

Only called by run_operator_kube_tests.py with its explicit scrubbed environment
and ownership-checked Docker node. No registry commands, host config, arbitrary
image selection or force-overwrite path exists. OCI manifest identity is distinct
from the image configuration digest/opaque Docker image ID.
"""
import hashlib
import io
import json
import re
import subprocess
import tarfile
import time

DIGEST = re.compile(r"^sha256:[a-f0-9]{64}$")
MANIFEST_TYPES = {"application/vnd.oci.image.manifest.v1+json", "application/vnd.docker.distribution.manifest.v2+json"}
CONFIG_TYPES = {"application/vnd.oci.image.config.v1+json", "application/vnd.docker.container.image.v1+json"}
LABEL = "operator.local/run-id"


def verify_fixture_image(docker, environment, node, network, run_id, architecture, source, expected_image_id, expected_node_image_id, expected_node_id):
    def command(arguments, *, allow_failure=False):
        result = subprocess.run([docker, *arguments], env=environment, capture_output=True, timeout=60, check=False)
        if result.returncode and not allow_failure:
            raise RuntimeError("Owned fixture image inspection failed: " + result.stderr.decode(errors="replace")[-1000:])
        return result

    def node_identity():
        owned = json.loads(command(["inspect", "--type=container", node]).stdout)[0]
        if owned["Id"] != expected_node_id or owned["Name"] != "/" + node or owned["Image"] != expected_node_image_id or owned["Config"].get("Labels", {}).get("io.x-k8s.kind.cluster") != run_id or set(owned["NetworkSettings"]["Networks"]) != {network} or any(owned["NetworkSettings"].get("Ports", {}).values()):
            raise RuntimeError("Refusing fixture metadata access: node/run/network/image identity changed")

    def image_identity():
        owned = json.loads(command(["image", "inspect", source]).stdout)[0]
        if owned["Id"] != expected_image_id or owned["Config"].get("Labels", {}).get(LABEL) != run_id or owned["Os"] != "linux" or owned["Architecture"] != architecture:
            raise RuntimeError("Generated fixture tag/config/platform identity changed")

    node_identity()
    image_identity()
    # A Docker image ID is opaque here. Obtain raw config bytes independently
    # from this exact owned local image archive, never reinterpret ID as manifest.
    archive = command(["image", "save", source]).stdout
    if len(archive) > 64 * 1024 * 1024:
        raise RuntimeError("Owned fixture archive unexpectedly large")
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:*") as stream:
        manifests = json.load(stream.extractfile("manifest.json"))
        if len(manifests) != 1 or source not in manifests[0].get("RepoTags", []):
            raise RuntimeError("Owned fixture archive does not describe exact source tag")
        member = stream.getmember(manifests[0]["Config"])
        if not member.isfile() or member.size > 1024 * 1024:
            raise RuntimeError("Owned fixture configuration is not bounded regular data")
        config_bytes = stream.extractfile(member).read()
    config_digest = "sha256:" + hashlib.sha256(config_bytes).hexdigest()
    config = json.loads(config_bytes)
    if config.get("architecture") != architecture or config.get("os") != "linux" or config.get("config", {}).get("Labels", {}).get(LABEL) != run_id:
        raise RuntimeError("Owned fixture archive config provenance mismatch")
    source_ref = "docker.io/library/" + source
    ctr = ["exec", node, "ctr", "--address", "/run/containerd/containerd.sock", "--namespace", "k8s.io"]

    def image_rows():
        listing = command([*ctr, "images", "list"]).stdout.decode()
        return {fields[0]: fields for line in listing.splitlines()[1:] if len(fields := line.split()) >= 3}

    rows = image_rows()
    row = rows.get(source_ref)
    if row is None or row[1] not in MANIFEST_TYPES or not DIGEST.fullmatch(row[2]):
        raise RuntimeError("Owned fixture must have one supported native image manifest; no index/config-digest fallback")
    manifest_digest = row[2]
    manifest_bytes = command([*ctr, "content", "get", manifest_digest]).stdout
    if "sha256:" + hashlib.sha256(manifest_bytes).hexdigest() != manifest_digest:
        raise RuntimeError("Local manifest bytes do not match manifest digest")
    manifest = json.loads(manifest_bytes)
    descriptor = manifest.get("config", {})
    if manifest.get("schemaVersion") != 2 or manifest.get("mediaType") not in MANIFEST_TYPES or descriptor.get("mediaType") not in CONFIG_TYPES or descriptor.get("digest") != config_digest or descriptor.get("size") != len(config_bytes) or manifest_digest == config_digest:
        raise RuntimeError("Manifest does not bind the exact independently verified fixture config")
    local_config = command([*ctr, "content", "get", config_digest]).stdout
    if local_config != config_bytes or "sha256:" + hashlib.sha256(local_config).hexdigest() != config_digest:
        raise RuntimeError("Node config bytes differ from exact owned Docker fixture")
    immutable = "docker.io/library/operator-local-fixture@" + manifest_digest
    existing = rows.get(immutable)
    if existing is not None:
        if existing[1:3] != row[1:3]:
            raise RuntimeError("Refusing an existing fixture alias with another target")
    else:
        # No --force: concurrent/preexisting unproven alias is an error.
        command([*ctr, "images", "tag", source_ref, immutable])
    alias = image_rows().get(immutable)
    if alias is None or alias[1:3] != row[1:3]:
        raise RuntimeError("Immutable local alias did not retain verified manifest target")
    cri = ["exec", node, "crictl", "--runtime-endpoint", "unix:///run/containerd/containerd.sock", "--image-endpoint", "unix:///run/containerd/containerd.sock", "inspecti", immutable]
    deadline = time.monotonic() + 5
    while True:
        result = command(cri, allow_failure=True)
        if result.returncode == 0:
            status = json.loads(result.stdout).get("status", {})
            if status.get("id") != config_digest or immutable not in status.get("repoDigests", []):
                raise RuntimeError("CRI alias/config/manifest identity differs from owned proof")
            break
        if time.monotonic() >= deadline:
            raise RuntimeError("CRI did not resolve verified local immutable fixture alias")
        time.sleep(0.1)
    node_identity()
    image_identity()
    return {"imageDigest": immutable, "imageManifestDigest": manifest_digest, "imageConfigDigest": config_digest}
