from pathlib import Path

import pytest

from scripts.promote_image import IMAGE_REPOSITORY, promote


def test_promote_pins_the_single_broker_image(tmp_path: Path) -> None:
    manifest = tmp_path / "broker.yaml"
    manifest.write_text(
        "containers:\n"
        "  - name: broker\n"
        f"    image: {IMAGE_REPOSITORY}:old\n"
    )
    image = f"{IMAGE_REPOSITORY}:main@sha256:{'a' * 64}"

    assert promote(manifest, image) is True
    assert f"    image: {image}\n" == manifest.read_text().split("broker\n", 1)[1]
    assert promote(manifest, image) is False


def test_promote_rejects_a_non_digest_image(tmp_path: Path) -> None:
    manifest = tmp_path / "broker.yaml"
    manifest.write_text(f"image: {IMAGE_REPOSITORY}:old\n")

    with pytest.raises(ValueError, match="pinned"):
        promote(manifest, f"{IMAGE_REPOSITORY}:main")


def test_promote_requires_exactly_one_broker_image(tmp_path: Path) -> None:
    manifest = tmp_path / "broker.yaml"
    manifest.write_text("image: example.invalid/other:latest\n")
    image = f"{IMAGE_REPOSITORY}:main@sha256:{'b' * 64}"

    with pytest.raises(ValueError, match="exactly one"):
        promote(manifest, image)
