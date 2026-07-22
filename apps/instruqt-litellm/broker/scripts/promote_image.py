#!/usr/bin/env python3
"""Pin the broker Deployment to a validated ECR image digest."""

from __future__ import annotations

import argparse
import re
from pathlib import Path


IMAGE_REPOSITORY = (
    "429214323166.dkr.ecr.us-west-1.amazonaws.com/"
    "temporal-instruqt-litellm-broker"
)
IMAGE_PATTERN = re.compile(
    rf"^(?P<prefix>\s*image:\s+){re.escape(IMAGE_REPOSITORY)}\S*(?P<suffix>\s*)$",
    re.MULTILINE,
)
EXPECTED_IMAGE = re.compile(
    rf"^{re.escape(IMAGE_REPOSITORY)}:main@sha256:[0-9a-f]{{64}}$"
)


def promote(manifest: Path, image: str) -> bool:
    if not EXPECTED_IMAGE.fullmatch(image):
        raise ValueError(
            "image must be the broker ECR repository pinned as main@sha256:<64 hex>"
        )

    original = manifest.read_text()
    updated, replacements = IMAGE_PATTERN.subn(rf"\g<prefix>{image}\g<suffix>", original)
    if replacements != 1:
        raise ValueError(
            f"expected exactly one broker image in {manifest}, found {replacements}"
        )

    if updated == original:
        return False

    manifest.write_text(updated)
    return True


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--image", required=True)
    args = parser.parse_args()

    changed = promote(args.manifest, args.image)
    print("updated" if changed else "already current")


if __name__ == "__main__":
    main()
