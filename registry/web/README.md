# Demo Registry Web

Dependency-free web service for the registry catalog.

The image serves the catalog from `REGISTRY_DATA_PATH`
(`/app/data/registry.json`) and exposes:

- `/health`
- `/registry.json`
- `/api/registry`
- `/api/catalog`
- `/`

The catalog is published at runtime by the registry operator: it lists every
`DemoProject` CR and writes a `registry-catalog` ConfigMap, which the operator
mounts into this Deployment at `/app/data`. The image ships an empty fallback so
it builds and serves before the first publish (and the server tolerates a
missing file by serving an empty catalog).

`/api/catalog` also merges optional per-project status JSON files from
`CATALOG_STATUS_DIR`. File names come from each project's
`catalogStatus.configMap.key` entry, such as `banking-money-transfer.json`. The
operator mounts the mirrored status ConfigMap at `/app/status`, matching the
image default.
