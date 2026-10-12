# Demo Registry Web

Dependency-free web service for the registry catalog.

The image serves the catalog from `REGISTRY_DATA_PATH`
(`/app/data/registry.json`) and exposes:

- `/health`
- `/registry.json`
- `/api/registry`
- `/api/catalog`
- `/_auth/verify`
- `POST /api/namespace-access`
- `/auth/login`
- `/oauth/google/start`
- `/oauth/google/callback`
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

Canonical demos are featured with generated artwork. To show a screenshot of the
demo instead, add `static/screenshots/<demo name>.jpg` (the `DemoProject` name,
e.g. `canonical-ai-demo.jpg`; ~1600px wide works well). The UI picks it up on
the next image build and falls back to the artwork when the file is missing.

`POST /api/namespace-access` backs the header's "Request Namespace Access" button.
It requires a signed-in session and runs the operator's
`GrantNamespaceReadAccessWorkflow` for the session's email (never an address
from the request body), which adds that Temporal Cloud user to the group that
can read every demo namespace. It returns the workflow's result, e.g.
`{"status": "added" | "already-member" | "user-not-found", ...}`.

`/_auth/verify` is the Traefik forward-auth endpoint used by protected demo
routes. With `AUTH_MODE=google`, missing or invalid sessions redirect to
`/auth/login`, which starts Google OAuth and sets the shared
`.tmprl-demo.cloud` session cookie before returning to the original demo URL.
The verifier injects `X-Temporal-Auth-JWT`, `X-Temporal-Auth-Subject`,
`X-Temporal-Auth-Email`, and `X-Temporal-Auth-Name` for downstream apps.

`/_auth/verify-workspace` is the forward-auth endpoint for crashable and
isolated workspace routes only; the operator rewrites each workspace's auth
middleware to point here, and never points an always-on demo at it. Employees
pass exactly as on `/_auth/verify`. Anyone else needs a share link minted by the
workspace owner (`POST /api/crashable-workspace/share`, auth-gated demos only,
default 4h, max 24h). The link carries a `tmprl_share` query param that is exchanged
for a host-only `temporal_workspace_share` cookie and stripped from the URL.
Links are signed with a key derived from the session key and bound to the
workspace host, its incarnation (`created_at`), and its share epoch, so they
stop working when they expire, when the owner calls
`POST /api/crashable-workspace/revoke-share-links`, or when the workspace is
deleted or recreated. Guests get `X-Temporal-Auth-Subject: share:<id>`,
`X-Temporal-Auth-Name: Guest`, a synthetic per-browser
`X-Temporal-Auth-Email: guest-<id>@guest.tmprl-demo.cloud`, and a downstream JWT with
`guest: true`. `/_auth/verify` never reads share links or cookies.

`AUTH_MODE=deny` fails closed with `401 unauthorized`. `AUTH_MODE=bootstrap-allow`
is an explicit insecure bootstrap bypass and should not be used for protected
routes.
