from __future__ import annotations

import hashlib


def lambda_function_name(project: str, component: str) -> str:
    return aws_name("tmprl-dem-cld", project, component, max_length=64)


def lambda_execution_role_name(project: str, component: str) -> str:
    return typed_aws_name("tmprl-dem-cld", project, component, "lambda", max_length=64)


def temporal_lambda_invoke_role_name(project: str, component: str) -> str:
    return typed_aws_name(
        "tmprl-dem-cld",
        project,
        component,
        "temporal",
        max_length=64,
    )


def temporal_invoke_external_id(project: str, component: str) -> str:
    raw = f"tmprl-demo-cloud:{project}:{component}"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
    return f"tdc-{digest}"


def aws_name(*parts: str, max_length: int) -> str:
    raw = "-".join(part.strip("-") for part in parts if part.strip("-"))
    if len(raw) <= max_length:
        return raw
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:10]
    stem = raw[: max_length - len(digest) - 1].rstrip("-")
    return f"{stem}-{digest}"


def typed_aws_name(*parts: str, max_length: int) -> str:
    if len(parts) < 2:
        return aws_name(*parts, max_length=max_length)
    suffix = parts[-1].strip("-")
    prefix_parts = parts[:-1]
    raw = "-".join(part.strip("-") for part in (*prefix_parts, suffix) if part.strip("-"))
    if len(raw) <= max_length:
        return raw
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:10]
    suffix_part = f"-{digest}-{suffix}"
    stem = "-".join(part.strip("-") for part in prefix_parts if part.strip("-"))
    stem = stem[: max_length - len(suffix_part)].rstrip("-")
    return f"{stem}{suffix_part}"
