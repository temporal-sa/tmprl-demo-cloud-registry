from __future__ import annotations

import json
import mimetypes
import os
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse


ROOT = Path(__file__).resolve().parent
STATIC_DIR = ROOT / "static"
DEFAULT_REGISTRY_PATH = ROOT / "data" / "registry.json"
DEFAULT_STATUS_DIR = ROOT / "status"


def configured_path(env_name: str, default: Path) -> Path:
    return Path(os.environ.get(env_name, str(default))).resolve()


def load_json_file(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        payload = json.load(file)
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return payload


def load_registry(path: Path) -> dict[str, Any]:
    registry = load_json_file(path)
    projects = registry.get("projects")
    if not isinstance(projects, list):
        raise ValueError(f"{path} must contain a projects list")
    return registry


def status_key(project: dict[str, Any]) -> str | None:
    catalog_status = project.get("catalogStatus")
    if not isinstance(catalog_status, dict):
        return None
    config_map = catalog_status.get("configMap")
    if not isinstance(config_map, dict):
        return None
    key = config_map.get("key")
    if not isinstance(key, str) or not key:
        return None
    if Path(key).name != key:
        return None
    return key


def load_project_status(project: dict[str, Any], status_dir: Path) -> dict[str, Any] | None:
    key = status_key(project)
    if key is None:
        return None
    path = (status_dir / key).resolve()
    if status_dir not in path.parents:
        return None
    if not path.exists():
        return None
    try:
        return load_json_file(path)
    except (OSError, ValueError, json.JSONDecodeError) as error:
        return {
            "phase": "StatusUnavailable",
            "message": f"Could not read {key}: {error}",
        }


def catalog_payload(registry_path: Path, status_dir: Path) -> dict[str, Any]:
    registry = load_registry(registry_path)
    projects: list[dict[str, Any]] = []
    live_status_count = 0

    for project in registry.get("projects", []):
        if not isinstance(project, dict):
            continue
        enriched = dict(project)
        project_status = load_project_status(project, status_dir)
        if project_status is not None:
            enriched["status"] = project_status
            live_status_count += 1
        projects.append(enriched)

    return {
        **registry,
        "projects": projects,
        "catalog": {
            "projectCount": len(projects),
            "liveStatusCount": live_status_count,
        },
    }


class RegistryRequestHandler(BaseHTTPRequestHandler):
    registry_path: Path
    status_dir: Path
    server_version = "DemoRegistryWeb/0.1"

    def do_GET(self) -> None:
        path = urlparse(self.path).path
        try:
            if path == "/health":
                self.send_json({"ok": True})
            elif path in {"/registry.json", "/api/registry"}:
                self.send_json(load_registry(self.registry_path))
            elif path == "/api/catalog":
                self.send_json(catalog_payload(self.registry_path, self.status_dir))
            elif path in {"/", "/index.html"}:
                self.send_static("index.html")
            elif path.startswith("/static/"):
                self.send_static(path.removeprefix("/static/"))
            else:
                self.send_json(
                    {"error": "not_found", "path": path},
                    status=HTTPStatus.NOT_FOUND,
                )
        except (OSError, ValueError, json.JSONDecodeError) as error:
            self.send_json(
                {"error": "registry_unavailable", "message": str(error)},
                status=HTTPStatus.INTERNAL_SERVER_ERROR,
            )

    def send_json(
        self,
        payload: dict[str, Any],
        *,
        status: HTTPStatus = HTTPStatus.OK,
    ) -> None:
        encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(encoded)

    def send_static(self, relative_name: str) -> None:
        path = safe_static_path(relative_name)
        if path is None or not path.is_file():
            self.send_json(
                {"error": "not_found", "path": relative_name},
                status=HTTPStatus.NOT_FOUND,
            )
            return

        encoded = path.read_bytes()
        content_type = mimetypes.guess_type(path.name)[0] or "application/octet-stream"
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(encoded)))
        self.send_header("Cache-Control", "public, max-age=60")
        self.end_headers()
        self.wfile.write(encoded)


def safe_static_path(relative_name: str) -> Path | None:
    decoded = unquote(relative_name).lstrip("/")
    path = (STATIC_DIR / decoded).resolve()
    if path == STATIC_DIR or STATIC_DIR not in path.parents:
        return None
    return path


def make_handler(
    registry_path: Path,
    status_dir: Path,
) -> type[RegistryRequestHandler]:
    class ConfiguredRegistryRequestHandler(RegistryRequestHandler):
        pass

    ConfiguredRegistryRequestHandler.registry_path = registry_path
    ConfiguredRegistryRequestHandler.status_dir = status_dir
    return ConfiguredRegistryRequestHandler


def main() -> None:
    registry_path = configured_path("REGISTRY_DATA_PATH", DEFAULT_REGISTRY_PATH)
    status_dir = configured_path("CATALOG_STATUS_DIR", DEFAULT_STATUS_DIR)
    port = int(os.environ.get("PORT", "8080"))
    handler = make_handler(registry_path, status_dir)
    server = ThreadingHTTPServer(("0.0.0.0", port), handler)
    print(f"Serving registry catalog on :{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
