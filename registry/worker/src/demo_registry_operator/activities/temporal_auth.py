"""Runtime authentication reconciliation for Temporal Cloud namespaces."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from demo_registry_operator.kube import read_namespace, upsert_secret_data
from demo_registry_operator.resources import (
    STANDARD_TEMPORAL_ENV,
    TEMPORAL_AUTH_MODE_MTLS,
    TEMPORAL_MTLS_CA_CERT_ROTATE_EVERY_DAYS,
    TEMPORAL_MTLS_CA_SECRET_PATH,
    TEMPORAL_MTLS_CLIENT_CERT_ROTATE_EVERY_DAYS,
    TEMPORAL_MTLS_INPUT_REF_ENV,
    TEMPORAL_SECRET_NAME,
    TEMPORAL_WORKER_RUNTIME_SECRET_NAME,
    common_labels,
    project_name,
    project_namespace,
    project_temporal_namespace,
    temporal_components,
    temporal_credentials_secret_needed,
    temporal_mtls_status,
    temporal_secret_remote_path,
    temporal_worker_runtime_secret_needed,
)


MTLS_CA_CERT_VALIDITY_DAYS = 180
MTLS_CLIENT_CERT_VALIDITY_DAYS = 14
MTLS_CA_OVERLAP_DAYS = MTLS_CLIENT_CERT_VALIDITY_DAYS
MANUAL_ROTATION_REASON = "ManualRotation"


@dataclass
class ApiKeyResult:
    key_id: str
    token: str
    expiry_time: str | None = None


class RuntimeNamespace(Protocol):
    qualified_name: str
    grpc_address: str


class ApiKeyAuthReconciler(Protocol):
    async def find_service_account(self, name: str) -> str | None: ...

    async def create_service_account(
        self, name: str, qualified_namespace: str, description: str
    ) -> str: ...

    async def create_api_key(
        self,
        service_account_id: str,
        display_name: str,
        description: str,
        ttl_days: int,
    ) -> ApiKeyResult: ...

    async def delete_api_key(self, key_id: str) -> str: ...


class MtlsAuthReconciler(Protocol):
    async def configure_namespace_mtls(
        self,
        qualified_name: str,
        accepted_client_ca: str,
        certificate_common_name: str,
    ) -> str: ...


class SecretStore(Protocol):
    def read_json(self, path: str) -> dict[str, Any] | None: ...

    def write_json(
        self, path: str, value: dict[str, Any], description: str | None = None
    ) -> None: ...


class RuntimeCanary(Protocol):
    async def __call__(self, credential: dict[str, Any]) -> dict[str, Any]: ...


async def reconcile_api_key_auth(
    demo_project: dict[str, Any],
    *,
    namespace: RuntimeNamespace,
    reconciler: ApiKeyAuthReconciler,
    secret_store: SecretStore,
    checked_at: str,
    api_key_ttl_days: int,
    rotate_before_days: int,
    mirror_kubernetes_secrets: bool = True,
    force_rotation: bool = False,
    runtime_canary: RuntimeCanary | None = None,
) -> dict[str, Any]:
    """Create-or-verify API-key runtime auth for one Temporal Cloud namespace.

    Namespace lifecycle is intentionally owned by ``temporal_cloud``. This
    function owns the auth-side resources that depend on the namespace: the
    namespace-scoped service account, its short-lived API key, and the runtime
    secret projected into project workloads.
    """

    project = project_name(demo_project)
    service_account_name = f"{project}-runtime"
    service_account_id = await reconciler.find_service_account(service_account_name)
    service_account_created = False
    if service_account_id is None:
        service_account_id = await reconciler.create_service_account(
            service_account_name,
            namespace.qualified_name,
            f"tmprl-demo.cloud runtime service account for {project}",
        )
        service_account_created = True

    runtime_path = temporal_secret_remote_path(demo_project)
    existing = secret_store.read_json(runtime_path)
    rotation = temporal_api_key_rotation_decision(
        existing,
        checked_at=checked_at,
        rotate_before_days=rotate_before_days,
    )
    if (
        force_rotation
        and isinstance(existing, dict)
        and existing.get("apiKey")
    ):
        rotation = {"rotate": True, "reason": MANUAL_ROTATION_REASON}

    previous_key_id = None
    if isinstance(existing, dict) and existing.get("apiKey") and not rotation["rotate"]:
        key_id = existing.get("keyId")
        expiry_time = existing.get("expiryTime")
        key_action = "verified"
        created_at = existing.get("createdAt")
        canary_status = existing.get("canary") if isinstance(existing, dict) else None
    else:
        result = await reconciler.create_api_key(
            service_account_id,
            service_account_name,
            f"tmprl-demo.cloud runtime key for {project}",
            api_key_ttl_days,
        )
        key_id = result.key_id
        expiry_time = result.expiry_time
        previous_key_id = existing.get("keyId") if isinstance(existing, dict) else None
        previous_expiry_time = (
            existing.get("expiryTime") if isinstance(existing, dict) else None
        )
        key_action = (
            "rotated"
            if isinstance(existing, dict) and existing.get("apiKey")
            else "created"
        )
        created_at = checked_at
        secret_value: dict[str, Any] = {
            "address": namespace.grpc_address,
            "namespace": namespace.qualified_name,
            "apiKey": result.token,
            "tls": "true",
            "keyId": result.key_id,
            "serviceAccountId": service_account_id,
            "createdAt": checked_at,
            "rotationReason": rotation["reason"],
        }
        if expiry_time:
            secret_value["expiryTime"] = expiry_time
        if previous_key_id:
            secret_value["previousKeyId"] = previous_key_id
        if previous_expiry_time:
            secret_value["previousExpiryTime"] = previous_expiry_time

        try:
            canary_status = await run_runtime_canary(secret_value, runtime_canary)
        except Exception:
            await cleanup_created_api_key(reconciler, result.key_id)
            raise
        secret_value["canary"] = canary_status

        secret_store.write_json(
            runtime_path,
            secret_value,
            f"Temporal Cloud runtime credentials for {project}",
        )
        if mirror_kubernetes_secrets:
            mirror_runtime_kubernetes_secrets(demo_project, secret_value)

    api_key_status: dict[str, Any] = {
        "keyId": key_id,
        "action": key_action,
        "scope": "namespace",
        "namespace": namespace.qualified_name,
        "ttlDays": api_key_ttl_days,
        "rotateBeforeExpiryDays": rotate_before_days,
        "secretRemotePath": runtime_path,
        "expiryTime": expiry_time,
        "createdAt": created_at,
        "components": sorted(temporal_components(demo_project)),
    }
    if previous_key_id:
        api_key_status["previousKeyId"] = previous_key_id
    if rotation.get("reason"):
        api_key_status["rotationReason"] = rotation["reason"]
    if isinstance(canary_status, dict):
        api_key_status["canary"] = canary_status

    return {
        "serviceAccount": {
            "id": service_account_id,
            "name": service_account_name,
            "action": "created" if service_account_created else "verified",
        },
        "apiKey": api_key_status,
    }


async def run_runtime_canary(
    credential: dict[str, Any],
    runtime_canary: RuntimeCanary | None,
) -> dict[str, Any]:
    if runtime_canary is None:
        from demo_registry_operator.activities.temporal_canary import (
            run_temporal_runtime_canary,
        )

        runtime_canary = run_temporal_runtime_canary

    status = await runtime_canary(credential)
    if not isinstance(status, dict) or not status.get("ok"):
        raise RuntimeError(f"Temporal runtime credential canary failed: {status!r}")
    return status


async def cleanup_created_api_key(
    reconciler: ApiKeyAuthReconciler,
    key_id: str,
) -> None:
    delete_api_key = getattr(reconciler, "delete_api_key", None)
    if not callable(delete_api_key):
        return
    try:
        await delete_api_key(key_id)
    except Exception:
        # The canary failure is the actionable error. Cleanup is best-effort and
        # should not hide the failure that blocked promotion.
        pass


def temporal_api_key_rotation_decision(
    existing: Any,
    *,
    checked_at: str,
    rotate_before_days: int,
) -> dict[str, Any]:
    if not isinstance(existing, dict) or not existing.get("apiKey"):
        return {"rotate": True, "reason": "MissingRuntimeSecret"}
    if not existing.get("keyId"):
        return {"rotate": True, "reason": "MissingKeyId"}

    expiry_time = existing.get("expiryTime")
    if not isinstance(expiry_time, str) or not expiry_time:
        return {"rotate": True, "reason": "MissingExpiryTime"}

    expiry = parse_datetime(expiry_time)
    checked = parse_datetime(checked_at)
    if expiry is None:
        return {"rotate": True, "reason": "InvalidExpiryTime"}
    if checked is None:
        checked = datetime.now(UTC)

    rotate_at = expiry - timedelta(days=rotate_before_days)
    if checked >= rotate_at:
        return {
            "rotate": True,
            "reason": "RotateBeforeExpiry",
            "rotateAt": rotate_at.isoformat(),
        }

    return {
        "rotate": False,
        "reason": "ExistingKeyStillFresh",
        "rotateAt": rotate_at.isoformat(),
    }


def prepare_mtls_ca_material(
    *,
    secret_store: SecretStore,
    checked_at: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    existing = secret_store.read_json(TEMPORAL_MTLS_CA_SECRET_PATH)
    rotation = temporal_mtls_ca_rotation_decision(
        existing,
        checked_at=checked_at,
        rotate_every_days=TEMPORAL_MTLS_CA_CERT_ROTATE_EVERY_DAYS,
    )
    if isinstance(existing, dict) and existing.get("caCert") and not rotation["rotate"]:
        cleanup = temporal_mtls_ca_cleanup_decision(existing, checked_at=checked_at)
        if cleanup["cleanup"]:
            material = {
                key: value
                for key, value in existing.items()
                if key
                not in {
                    "previousCaCert",
                    "previousCaSerialNumber",
                    "previousCaRemoveAt",
                }
            }
            return material, cleanup
        return existing, rotation
    generated = generate_mtls_ca_certificate(checked_at=checked_at)
    if not isinstance(existing, dict) or not existing.get("caCert"):
        claimed = claim_initial_mtls_ca_material(secret_store, generated)
        if claimed is not None:
            return claimed
    if isinstance(existing, dict) and existing.get("caCert"):
        checked = parse_datetime(checked_at) or datetime.now(UTC)
        generated.update(
            {
                "previousCaCert": existing["caCert"],
                "previousCaSerialNumber": existing.get("serialNumber"),
                "previousCaRemoveAt": (
                    checked + timedelta(days=MTLS_CA_OVERLAP_DAYS)
                ).isoformat(),
            }
        )
    return generated, rotation


def claim_initial_mtls_ca_material(
    secret_store: SecretStore,
    generated: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    create_if_missing = getattr(secret_store, "create_json_if_missing", None)
    if not callable(create_if_missing):
        return None
    created = create_if_missing(
        TEMPORAL_MTLS_CA_SECRET_PATH,
        generated,
        "Temporal Cloud mTLS platform client CA",
    )
    if created is True:
        return generated, {
            "rotate": True,
            "reason": "MissingCertificateAuthority",
            "persisted": True,
        }
    existing = secret_store.read_json(TEMPORAL_MTLS_CA_SECRET_PATH)
    if isinstance(existing, dict) and existing.get("caCert"):
        return existing, {
            "rotate": False,
            "reason": "ExistingCertificateAuthorityClaimedByPeer",
            "rotateAt": mtls_rotate_at(
                str(existing.get("notBefore", "")),
                TEMPORAL_MTLS_CA_CERT_ROTATE_EVERY_DAYS,
            ),
        }
    return None


async def reconcile_mtls_auth(
    demo_project: dict[str, Any],
    *,
    namespace: RuntimeNamespace,
    reconciler: MtlsAuthReconciler,
    secret_store: SecretStore,
    checked_at: str,
    mirror_kubernetes_secrets: bool = True,
    force_client_rotation: bool = False,
    runtime_canary: RuntimeCanary | None = None,
    ca_material: dict[str, Any] | None = None,
    ca_rotation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create-or-verify mTLS runtime auth for one Temporal Cloud namespace."""

    project = project_name(demo_project)
    runtime_path = temporal_secret_remote_path(demo_project)
    existing = secret_store.read_json(runtime_path)
    previous_ca = secret_store.read_json(TEMPORAL_MTLS_CA_SECRET_PATH)
    if ca_material is None or ca_rotation is None:
        ca_material, ca_rotation = prepare_mtls_ca_material(
            secret_store=secret_store,
            checked_at=checked_at,
        )
    common_name = temporal_mtls_client_common_name(demo_project)
    namespace_config_action = await reconciler.configure_namespace_mtls(
        namespace.qualified_name,
        accepted_client_ca_bundle(ca_material),
        common_name,
    )

    ca_material_changed = bool(ca_rotation.get("rotate") or ca_rotation.get("cleanup"))
    client_rotation = temporal_mtls_client_rotation_decision(
        existing,
        checked_at=checked_at,
        ca_serial_number=str(ca_material["serialNumber"]),
        rotate_every_days=TEMPORAL_MTLS_CLIENT_CERT_ROTATE_EVERY_DAYS,
    )
    if (
        force_client_rotation
        and isinstance(existing, dict)
        and existing.get("clientCert")
    ):
        client_rotation = {"rotate": True, "reason": MANUAL_ROTATION_REASON}

    client_changed = False
    if (
        isinstance(existing, dict)
        and existing.get("clientCert")
        and existing.get("clientKey")
        and not client_rotation["rotate"]
    ):
        client_action = "verified"
        created_at = existing.get("createdAt") or existing.get("clientCertNotBefore")
        client_cert = {
            "serialNumber": existing.get("clientCertSerialNumber"),
            "notBefore": existing.get("clientCertNotBefore"),
            "notAfter": existing.get("clientCertNotAfter"),
            "rotateAt": client_rotation.get("rotateAt"),
        }
        canary_status = existing.get("canary")
    else:
        client_changed = True
        client_cert = generate_mtls_client_certificate(
            ca_material,
            common_name=common_name,
            checked_at=checked_at,
        )
        client_action = (
            "rotated"
            if isinstance(existing, dict) and existing.get("clientCert")
            else "created"
        )
        created_at = checked_at
        previous_client_serial = (
            existing.get("clientCertSerialNumber")
            if isinstance(existing, dict)
            else None
        )
        secret_value: dict[str, Any] = {
            "authMode": TEMPORAL_AUTH_MODE_MTLS,
            "address": namespace.grpc_address,
            "namespace": namespace.qualified_name,
            "tls": "true",
            "caCert": ca_material["caCert"],
            "clientCert": client_cert["clientCert"],
            "clientKey": client_cert["clientKey"],
            "caSerialNumber": ca_material["serialNumber"],
            "clientCertSerialNumber": client_cert["serialNumber"],
            "clientCertCommonName": common_name,
            "clientCertNotBefore": client_cert["notBefore"],
            "clientCertNotAfter": client_cert["notAfter"],
            "clientCertRotateAt": mtls_rotate_at(
                client_cert["notBefore"],
                TEMPORAL_MTLS_CLIENT_CERT_ROTATE_EVERY_DAYS,
            ),
            "createdAt": checked_at,
            "rotationReason": client_rotation["reason"],
        }
        if previous_client_serial:
            secret_value["previousClientCertSerialNumber"] = previous_client_serial

        try:
            canary_status = await run_runtime_canary(secret_value, runtime_canary)
        except Exception:
            await rollback_mtls_namespace_ca(
                reconciler,
                namespace.qualified_name,
                common_name,
                previous_ca,
                ca_rotation,
            )
            raise
        secret_value["canary"] = canary_status

        if ca_material_changed:
            secret_store.write_json(
                TEMPORAL_MTLS_CA_SECRET_PATH,
                ca_material,
                "Temporal Cloud mTLS platform client CA",
            )
        secret_store.write_json(
            runtime_path,
            secret_value,
            f"Temporal Cloud mTLS runtime credentials for {project}",
        )
        if mirror_kubernetes_secrets:
            mirror_runtime_kubernetes_secrets(demo_project, secret_value)
    if ca_material_changed and not client_changed:
        try:
            canary_status = await run_runtime_canary(existing, runtime_canary)
        except Exception:
            await rollback_mtls_namespace_ca(
                reconciler,
                namespace.qualified_name,
                common_name,
                previous_ca,
                ca_rotation,
            )
            raise
        secret_store.write_json(
            TEMPORAL_MTLS_CA_SECRET_PATH,
            ca_material,
            "Temporal Cloud mTLS platform client CA",
        )

    ca_status = {
        "action": (
            "cleaned-up"
            if ca_rotation.get("cleanup")
            else
            "rotated"
            if ca_rotation["rotate"]
            and isinstance(previous_ca, dict)
            and previous_ca.get("caCert")
            else "created" if ca_rotation["rotate"] else "verified"
        ),
        "scope": "platform",
        "serialNumber": str(ca_material.get("serialNumber", "")),
        "secretRemotePath": TEMPORAL_MTLS_CA_SECRET_PATH,
        "notBefore": ca_material.get("notBefore"),
        "notAfter": ca_material.get("notAfter"),
        "rotateAt": ca_rotation.get("rotateAt"),
        "rotation": {
            "strategy": "staggered",
            "rotateEveryDays": TEMPORAL_MTLS_CA_CERT_ROTATE_EVERY_DAYS,
        },
    }
    if ca_rotation.get("reason"):
        ca_status["rotationReason"] = ca_rotation["reason"]
    previous_ca_remove_at = ca_material.get("previousCaRemoveAt")
    if isinstance(previous_ca_remove_at, str) and previous_ca_remove_at:
        ca_status["previousCaRemoveAt"] = previous_ca_remove_at
        ca_status["previousCaSerialNumber"] = ca_material.get(
            "previousCaSerialNumber"
        )
        ca_status["overlapDays"] = MTLS_CA_OVERLAP_DAYS

    client_status = {
        "action": client_action,
        "identityScope": "namespace",
        "namespace": namespace.qualified_name,
        "commonName": common_name,
        "serialNumber": client_cert.get("serialNumber"),
        "notBefore": client_cert.get("notBefore"),
        "notAfter": client_cert.get("notAfter"),
        "createdAt": created_at,
        "rotateAt": (
            client_cert.get("rotateAt")
            or mtls_rotate_at(
                str(client_cert.get("notBefore", checked_at)),
                TEMPORAL_MTLS_CLIENT_CERT_ROTATE_EVERY_DAYS,
            )
        ),
        "rotation": {
            "strategy": "staggered",
            "rotateEveryDays": TEMPORAL_MTLS_CLIENT_CERT_ROTATE_EVERY_DAYS,
        },
    }
    if client_rotation.get("reason"):
        client_status["rotationReason"] = client_rotation["reason"]
    if isinstance(canary_status, dict):
        client_status["canary"] = canary_status

    mtls_status = {
        **temporal_mtls_status(demo_project),
        "action": (
            "rotated"
            if ca_status["action"] == "rotated" or client_action == "rotated"
            else "created" if client_action == "created" else "verified"
        ),
        "namespace": namespace.qualified_name,
        "namespaceConfigAction": namespace_config_action,
        "caCertificate": ca_status,
        "clientCertificate": client_status,
    }
    return {"mtls": mtls_status}


def temporal_mtls_ca_rotation_decision(
    existing: Any,
    *,
    checked_at: str,
    rotate_every_days: int,
) -> dict[str, Any]:
    if not isinstance(existing, dict) or not existing.get("caCert"):
        return {"rotate": True, "reason": "MissingCertificateAuthority"}
    if not existing.get("caKey"):
        return {"rotate": True, "reason": "MissingCertificateAuthorityKey"}
    if not existing.get("serialNumber"):
        return {"rotate": True, "reason": "MissingCertificateAuthoritySerial"}

    not_before = parse_datetime(str(existing.get("notBefore", "")))
    checked = parse_datetime(checked_at) or datetime.now(UTC)
    if not_before is None:
        return {"rotate": True, "reason": "InvalidCertificateAuthorityNotBefore"}

    rotate_at = not_before + timedelta(days=rotate_every_days)
    if checked >= rotate_at:
        return {
            "rotate": True,
            "reason": "RotateCertificateAuthority",
            "rotateAt": rotate_at.isoformat(),
        }
    return {
        "rotate": False,
        "reason": "ExistingCertificateAuthorityStillFresh",
        "rotateAt": rotate_at.isoformat(),
    }


def temporal_mtls_ca_cleanup_decision(
    existing: dict[str, Any],
    *,
    checked_at: str,
) -> dict[str, Any]:
    if not existing.get("previousCaCert"):
        return {
            "rotate": False,
            "cleanup": False,
            "reason": "NoPreviousCertificateAuthority",
        }

    remove_at = existing.get("previousCaRemoveAt")
    if not isinstance(remove_at, str) or not remove_at:
        return {
            "rotate": False,
            "cleanup": True,
            "reason": "MissingPreviousCertificateAuthorityRemoveAt",
        }

    parsed_remove_at = parse_datetime(remove_at)
    checked = parse_datetime(checked_at) or datetime.now(UTC)
    if parsed_remove_at is None:
        return {
            "rotate": False,
            "cleanup": True,
            "reason": "InvalidPreviousCertificateAuthorityRemoveAt",
        }
    if checked >= parsed_remove_at:
        return {
            "rotate": False,
            "cleanup": True,
            "reason": "RemovePreviousCertificateAuthority",
            "rotateAt": mtls_rotate_at(
                str(existing.get("notBefore", checked_at)),
                TEMPORAL_MTLS_CA_CERT_ROTATE_EVERY_DAYS,
            ),
            "removeAt": parsed_remove_at.isoformat(),
        }
    return {
        "rotate": False,
        "cleanup": False,
        "reason": "PreviousCertificateAuthorityOverlapActive",
        "rotateAt": mtls_rotate_at(
            str(existing.get("notBefore", checked_at)),
            TEMPORAL_MTLS_CA_CERT_ROTATE_EVERY_DAYS,
        ),
        "removeAt": parsed_remove_at.isoformat(),
    }


def temporal_mtls_client_rotation_decision(
    existing: Any,
    *,
    checked_at: str,
    ca_serial_number: str,
    rotate_every_days: int,
) -> dict[str, Any]:
    if not isinstance(existing, dict) or not existing.get("clientCert"):
        return {"rotate": True, "reason": "MissingRuntimeSecret"}
    if not existing.get("clientKey"):
        return {"rotate": True, "reason": "MissingClientPrivateKey"}
    if str(existing.get("caSerialNumber", "")) != ca_serial_number:
        return {"rotate": True, "reason": "CertificateAuthorityRotated"}

    not_before = parse_datetime(str(existing.get("clientCertNotBefore", "")))
    checked = parse_datetime(checked_at) or datetime.now(UTC)
    if not_before is None:
        return {"rotate": True, "reason": "InvalidClientCertificateNotBefore"}

    rotate_at = not_before + timedelta(days=rotate_every_days)
    if checked >= rotate_at:
        return {
            "rotate": True,
            "reason": "RotateClientCertificate",
            "rotateAt": rotate_at.isoformat(),
        }
    return {
        "rotate": False,
        "reason": "ExistingClientCertificateStillFresh",
        "rotateAt": rotate_at.isoformat(),
    }


def generate_mtls_ca_certificate(*, checked_at: str) -> dict[str, Any]:
    x509, hashes, serialization, rsa, NameOID = cryptography_primitives()
    checked = parse_datetime(checked_at) or datetime.now(UTC)
    not_before = checked - timedelta(minutes=5)
    not_after = checked + timedelta(days=MTLS_CA_CERT_VALIDITY_DAYS)
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name(
        [
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Temporal Demo Cloud"),
            x509.NameAttribute(
                NameOID.COMMON_NAME,
                "tmprl-demo.cloud Temporal Cloud client CA",
            ),
        ]
    )
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_encipherment=False,
                content_commitment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(private_key, hashes.SHA256())
    )
    return {
        "caCert": pem_certificate(certificate, serialization),
        "caKey": pem_private_key(private_key, serialization),
        "serialNumber": str(certificate.serial_number),
        "notBefore": not_before.isoformat(),
        "notAfter": not_after.isoformat(),
        "createdAt": checked_at,
        "rotateEveryDays": TEMPORAL_MTLS_CA_CERT_ROTATE_EVERY_DAYS,
    }


def generate_mtls_client_certificate(
    ca_material: dict[str, Any],
    *,
    common_name: str,
    checked_at: str,
) -> dict[str, Any]:
    (
        x509,
        hashes,
        serialization,
        rsa,
        NameOID,
        ExtendedKeyUsageOID,
    ) = cryptography_primitives(include_extended_key_usage=True)
    checked = parse_datetime(checked_at) or datetime.now(UTC)
    not_before = checked - timedelta(minutes=5)
    not_after = checked + timedelta(days=MTLS_CLIENT_CERT_VALIDITY_DAYS)
    ca_cert = x509.load_pem_x509_certificate(
        str(ca_material["caCert"]).encode("utf-8")
    )
    ca_key = serialization.load_pem_private_key(
        str(ca_material["caKey"]).encode("utf-8"),
        password=None,
    )
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name(
        [
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "Temporal Demo Cloud"),
            x509.NameAttribute(NameOID.COMMON_NAME, common_name),
        ]
    )
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(ca_cert.subject)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_encipherment=True,
                content_commitment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )
    return {
        "clientCert": pem_certificate(certificate, serialization),
        "clientKey": pem_private_key(private_key, serialization),
        "serialNumber": str(certificate.serial_number),
        "notBefore": not_before.isoformat(),
        "notAfter": not_after.isoformat(),
        "commonName": common_name,
    }


def temporal_mtls_client_common_name(demo_project: dict[str, Any]) -> str:
    return f"temporal-namespace:{project_temporal_namespace(demo_project)}"


def accepted_client_ca_bundle(ca_material: dict[str, Any]) -> str:
    certs = [str(ca_material["caCert"]).strip()]
    previous = ca_material.get("previousCaCert")
    if isinstance(previous, str) and previous.strip():
        certs.append(previous.strip())
    return "\n".join(certs) + "\n"


def mtls_rotate_at(not_before: str, rotate_every_days: int) -> str | None:
    parsed = parse_datetime(not_before)
    if parsed is None:
        return None
    return (parsed + timedelta(days=rotate_every_days)).isoformat()


async def rollback_mtls_namespace_ca(
    reconciler: MtlsAuthReconciler,
    qualified_name: str,
    common_name: str,
    previous_ca: Any,
    ca_rotation: dict[str, Any],
) -> None:
    if not (ca_rotation.get("rotate") or ca_rotation.get("cleanup")):
        return
    if not isinstance(previous_ca, dict) or not previous_ca.get("caCert"):
        return
    try:
        await reconciler.configure_namespace_mtls(
            qualified_name,
            accepted_client_ca_bundle(previous_ca),
            common_name,
        )
    except Exception:
        pass


def cryptography_primitives(*, include_extended_key_usage: bool = False) -> tuple[Any, ...]:
    try:
        from cryptography import x509
        from cryptography.hazmat.primitives import hashes, serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
    except ImportError as error:
        raise RuntimeError(
            "cryptography is required for Temporal Cloud mTLS certificate generation"
        ) from error

    if include_extended_key_usage:
        return x509, hashes, serialization, rsa, NameOID, ExtendedKeyUsageOID
    return x509, hashes, serialization, rsa, NameOID


def pem_certificate(certificate: Any, serialization: Any) -> str:
    return certificate.public_bytes(serialization.Encoding.PEM).decode("utf-8")


def pem_private_key(private_key: Any, serialization: Any) -> str:
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode("utf-8")


def parse_datetime(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def mirror_runtime_kubernetes_secrets(
    demo_project: dict[str, Any],
    secret_value: dict[str, Any],
) -> dict[str, str]:
    namespace = project_namespace(demo_project)
    if read_namespace(namespace) is None:
        return {"phase": "Skipped", "reason": "NamespaceMissing"}

    labels = common_labels(demo_project)
    annotations = {
        "platform.temporal.io/secret-source": temporal_secret_remote_path(demo_project),
        "platform.temporal.io/temporal-auth-mode": (
            "apiKey" if secret_value.get("apiKey") else "mtls"
        ),
    }
    if secret_value.get("keyId"):
        annotations["platform.temporal.io/temporal-api-key-id"] = str(
            secret_value["keyId"]
        )
    if secret_value.get("clientCertSerialNumber"):
        annotations["platform.temporal.io/temporal-client-cert-serial"] = str(
            secret_value["clientCertSerialNumber"]
        )
    credential_data = runtime_secret_data(secret_value)
    worker_runtime_data = worker_runtime_secret_data(secret_value)
    actions: dict[str, str] = {}
    if temporal_credentials_secret_needed(demo_project):
        actions[TEMPORAL_SECRET_NAME] = upsert_secret_data(
            namespace,
            TEMPORAL_SECRET_NAME,
            credential_data,
            labels=labels,
            annotations=annotations,
        )
    if temporal_worker_runtime_secret_needed(demo_project):
        actions[TEMPORAL_WORKER_RUNTIME_SECRET_NAME] = upsert_secret_data(
            namespace,
            TEMPORAL_WORKER_RUNTIME_SECRET_NAME,
            worker_runtime_data,
            labels=labels,
            annotations=annotations,
        )
    return actions


def runtime_secret_data(secret_value: dict[str, Any]) -> dict[str, str]:
    base = {
        STANDARD_TEMPORAL_ENV["endpoint"]: str(secret_value["address"]),
        STANDARD_TEMPORAL_ENV["namespace"]: str(secret_value["namespace"]),
        STANDARD_TEMPORAL_ENV["tls"]: str(secret_value["tls"]),
    }
    if secret_value.get("apiKey"):
        return {
            **base,
            STANDARD_TEMPORAL_ENV["api_key"]: str(secret_value["apiKey"]),
        }
    return {
        **base,
        TEMPORAL_MTLS_INPUT_REF_ENV["temporal.caCert"]: str(secret_value["caCert"]),
        TEMPORAL_MTLS_INPUT_REF_ENV["temporal.clientCert"]: str(
            secret_value["clientCert"]
        ),
        TEMPORAL_MTLS_INPUT_REF_ENV["temporal.clientKey"]: str(secret_value["clientKey"]),
    }


def worker_runtime_secret_data(secret_value: dict[str, Any]) -> dict[str, str]:
    if secret_value.get("apiKey"):
        return {
            STANDARD_TEMPORAL_ENV["api_key"]: str(secret_value["apiKey"]),
            STANDARD_TEMPORAL_ENV["tls"]: str(secret_value["tls"]),
        }
    return {
        STANDARD_TEMPORAL_ENV["tls"]: str(secret_value["tls"]),
        TEMPORAL_MTLS_INPUT_REF_ENV["temporal.clientCert"]: str(
            secret_value["clientCert"]
        ),
        TEMPORAL_MTLS_INPUT_REF_ENV["temporal.clientKey"]: str(secret_value["clientKey"]),
    }
