# 12 · Demo catalog readiness

Every demo should be designed for eventual inclusion in the shared [tmprl-demo.cloud registry](https://github.com/temporal-sa/tmprl-demo-cloud-registry), even when its first run is local. That includes concurrent browser sessions and a per-user crashable workspace. Catalog readiness is an architectural constraint, not a request to deploy or publish the demo. The owner still decides when onboarding happens.

The live platform contract is the registry repository, available locally at `~/Source/tmprl-demo-cloud-registry`. Before settling the deployment design, inspect:

- `projects/README.md`
- `schemas/project.schema.json`
- `scripts/validate_projects.py`
- comparable resources under `projects/demo/`

Do not rely on a copied schema or remembered fields. The contract evolves.

## Isolate every browser session

A catalog demo can have multiple people using the same deployment at the same time. Interactive state must therefore be isolated by session or authenticated subject.

- Give each browser a stable, opaque session ID. For an authenticated demo, the platform SSO gate can supply `X-Temporal-Auth-Subject`, `X-Temporal-Auth-Email`, and related headers; for a public demo, use an unguessable, integrity-protected cookie or token. Do not accept a user identity from an unrestricted request field.
- Scope workflow IDs, entity keys, API reads, updates, signals, WebSocket/SSE streams, reset actions, and any persisted records to that session. A useful shape is `<demo>-<session>-<business-key>`.
- Never use a process-global “current run,” singleton browser state, shared fixed workflow ID, or unscoped latest-result endpoint for participant activity. Presenter-wide scenarios may be shared only when that behavior is intentional and clearly separate from participant sessions.
- A reset, replay, failure injection, or destructive control must affect only the caller's session unless it is a protected presenter control.
- Keep browser-local state namespaced as well. Avoid fixed `localStorage` keys that make unrelated tabs or personas overwrite each other; use session-scoped storage when a fresh tab should be independent.
- Expire or otherwise bound abandoned sessions and long-running Workflow histories. Cleanup must not delete another active user's state.

The registry's ingress authentication establishes identity and access to a route; it does **not** isolate application state. The application remains responsible for authorization and session scoping. Treat forwarded identity headers as trusted only when requests must pass through the platform ingress; provide an explicit local-development identity mechanism rather than silently trusting arbitrary headers everywhere.

## Support a crashable workspace

Every demo must be able to run as a catalog crashable workspace. That workspace is a time-limited, per-user Kubernetes clone with its own namespace and `crash-<id>.tmprl-demo.cloud` hostname. It reuses the demo's Temporal Cloud namespace and secrets. The platform isolates its work by assigning the task queue `<baseTaskQueue>-<workspace id>` and injecting that value through one environment variable on every worker and every `temporalAccess` component. Catalog controls can then force-delete those worker pods or scale them between zero and one while the UI and API remain running.

Confirm the current behavior in the registry's `registry.crashable` schema and crashable-workspace implementation before relying on a detail. Design the application for the contract above:

- Read the task queue from one environment variable in every worker and every process that starts, signals, queries, or lists work. Declare that variable as `taskQueueEnv` and the shared demo's normal queue name as `baseTaskQueue`.
- Carry that queue through workflow code in a replay-safe way. Start workflows on the configured queue and let activities and child workflows inherit it, or pass the queue in workflow input. Keep queue names out of workflow literals and out of direct environment reads inside workflow code.
- Derive any additional queue names from the injected value, so a clone's starters and workers remain paired.
- Include the active task-queue value in Workflow IDs and in keys written to shared external stores. Crashable clones share a Temporal namespace, so listings, links, signals, and queries need to select the current queue's work.
- Keep the experience correct with any number of worker replicas, from zero to many. Shared demos autoscale their workers on task-queue backlog (one to ten replicas by default, optionally down to zero). In a crashable clone, a crash deletes worker pods immediately and scale controls set worker replicas to zero or one.
- Keep the UI and API available while workers are absent. They should accept new work for Temporal to queue and read durable execution state.
- Serve the workspace correctly from its own hostname. Derive absolute URLs, cookie scope, redirects, and callbacks from the incoming request host.
- Run crash-relevant work in Kubernetes worker components. The platform's crash and scale controls target those worker pods and Worker Deployments.
- Treat in-cluster components as workspace-local. Shared external databases, buckets, and other stores need task-queue or workspace scoping because the clone reuses the demo's external credentials and resources.

## Use a registry-compatible application shape

Design the local stack so it maps directly to one `DemoProject` resource:

- Keep source in, or make it movable to, an HTTPS repository in the `temporal-sa` GitHub organization. Use a concise DNS-label-safe project name, ideally no more than 25 characters.
- Make each long-running web app, API, worker, and supporting service an explicit process. Give each process a reproducible Dockerfile build, or use a conventional buildpack-compatible process when the current contract supports it.
- Keep application images self-contained. Do not make production startup depend on source-tree bind mounts, a developer's home directory, manually installed host tools, or host-only URLs.
- Read runtime addresses and settings from environment variables. Local defaults may be convenient, but Temporal address, namespace, auth, TLS, service URLs, ports, task queues, and public base URLs must be overridable without code changes.
- Separate browser-facing services, Temporal clients, and workers clearly. A non-worker service that starts, signals, queries, or lists Workflows needs `temporalAccess`; a normal worker maps to `worker: true`.
- Use service boundaries that work with Kubernetes DNS. Internal callers should be configurable to use names such as `http://backend:8000`, never require `localhost`, `host.docker.internal`, or Compose-only aliases in hosted mode.
- Serve the public experience through one ingress hostname. Proxy `/api` or similar paths in the frontend, or model explicit registry ingress routes. Do not require arbitrary public ports.
- Provide a cheap, stable HTTP health endpoint suitable for a candidate `smokeCheck`. It should prove required application wiring without mutating demo state or depending on a fragile external operation.
- Keep ordinary components stateless and replaceable. Do not use an in-process map or local filesystem as the source of truth for user state. Put durable orchestration state in Temporal and use an explicitly supported external store when additional persistence is necessary.
- Store no secret values in source or project YAML. Accept secrets through environment variables compatible with the registry's AWS Secrets Manager mappings. Grant credentials only to the components that need them.
- Assume service-account token automount is off. Any Kubernetes API access must be explicit and least-privileged.

Keep `docker-compose.yml`, local scripts, and local Temporal defaults as developer conveniences. They should exercise the same process boundaries and configuration inputs that the hosted deployment will use.

## Design within the automation boundary

The supported onboarding path is intentionally thin: commit one `DemoProject` resource and let Flux and the registry operator derive namespaces, image repositories and tags, credentials, Services, ingress, and rollout resources.

Do not make hand-written Kubernetes overlays, Helm releases, fixed ECR tags, resolved Temporal Cloud namespace names, per-project Flux configuration, or manually distributed Temporal credentials part of the core demo architecture. A demo whose story specifically needs custom Kubernetes resources can retain a local or specialized mode, but its core audience experience should have a catalog-compatible deployment path or explicitly identify the current platform gap.

Prefer platform features before adding adapters:

- component env values and `valueFrom.inputRef` remapping;
- stable component Service DNS and ingress routes;
- platform-managed Temporal API-key or mTLS injection;
- `temporalProxy` for applications limited to local plaintext Temporal connectivity;
- project or shared Secrets Manager inputs;
- Dockerfile or supported buildpack builds;
- Worker Versioning, additional namespaces, Nexus Endpoints, or Lambda workers only when the demo actually needs them and the current schema supports the combination.

Include catalog metadata—title, summary, tags, SDKs, readiness, and useful links or assets—as part of the design, not as a last-minute publishing task. Recordings, slide decks, and other shared material for a catalog demo go in its folder under [Demo Catalog Material](https://drive.google.com/drive/folders/119d2IuE3lWqCs3ZRApH342i-a7_KomNQ); see [guide 13](13-shared-materials.md).

## Planning and acceptance criteria

During planning, identify:

1. the session boundary and identity source;
2. how session identity scopes Workflow IDs, APIs, streams, controls, and cleanup;
3. the task-queue environment variable, base queue, and how workflow code inherits that queue;
4. how Workflow IDs and external records stay distinct across crashable workspaces that share a Temporal namespace;
5. the build and component mapping;
6. public and internal routes;
7. Temporal configuration and credential flow;
8. required secrets and external state;
9. the smoke-check endpoint; and
10. any known mismatch with the current registry contract.

Before calling a demo catalog-ready:

- run two independent browser contexts concurrently and verify that neither can view, mutate, reset, or subscribe to the other's state;
- run with a non-default task-queue value, kill or scale the worker to zero, and verify that the UI stays available and the work resumes on a replacement worker;
- verify that another task queue cannot list or signal that workspace's executions;
- restart or replace each stateless service and verify active durable work remains usable;
- build every hosted image from a clean checkout;
- run the app with hosted-style environment values and no developer config files;
- verify the ingress route and smoke endpoint;
- draft the corresponding `DemoProject` resource and validate it from the registry repository with `python3 scripts/validate_projects.py`.

Drafting or validating a resource does not authorize committing it to the registry or deploying it.
