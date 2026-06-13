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
`catalogStatus.configMap.key` entry, such as `dejavu-tacos.json`. The current
generated deployment does not mount the live Kubernetes ConfigMap yet, so the
first image provides a buildable static catalog with an extension point for live
status wiring.
