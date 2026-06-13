# Demo Registry Web

Dependency-free web service for the registry catalog.

The image serves the reviewed generated catalog from
`clusters/sa-demo/generated/registry.json` and exposes:

- `/health`
- `/registry.json`
- `/api/registry`
- `/api/catalog`
- `/`

`/api/catalog` also merges optional per-project status JSON files from
`CATALOG_STATUS_DIR`. File names come from each project's
`catalogStatus.configMap.key` entry, such as `dejavu-tacos.json`. The generated
`demo-registry` web Deployment mounts the mirrored status ConfigMap at
`/app/status`, matching the image default.
