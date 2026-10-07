# green (tmprl-dem-cld-green)

Flux bootstrap configuration for the Tofu-managed `tmprl-dem-cld-green` EKS
cluster — the green side of the blue/green pair and the standby cutover target.

This tree is a **self-contained duplication** of `clusters/blue/`, not an
overlay: every manifest reads exactly as deployed, with no base to peel back.
Per-color/per-cluster deltas versus blue are only:

- **cluster-autoscaler** `autoDiscovery.clusterName: tmprl-dem-cld-green`.
- **registry operator** `DEMO_REGISTRY_CLUSTER=green` (derives task queue
  `registry-management-green` and workflow ids `demo-project/green/<name>`).
- **Flux paths** are `./clusters/green/...`.

Everything else is byte-identical to blue, including the shared `tmprl-dem-cld-*`
IRSA role ARNs (color-union trust covers green), the operator image, the wildcard
cert, and the operator's `ndots:2` dnsConfig. Versions match blue exactly:
latest-GA had not moved between the blue build and the green build (Flux 2.9.1,
cert-manager v1.21.0, external-secrets 2.7.0, Traefik **v3** chart 41,
cluster-autoscaler chart 9.58.0 / image v1.36.0, temporal-worker-controller
0.27.0, metrics-server 3.13.1, k8s-dns-node-cache 1.26.8, EKS 1.36).

## Layout

- `flux-system/` — the root sync: the `GitRepository` (HTTPS, reusing the
  read-only PAT; branch `feat/full-e2e-iac`, flip to `main` at merge) and the
  self-managed `Kustomization`s. Tofu installs Flux and applies the initial
  `GitRepository` + root `Kustomization`; everything below self-manages.
- `addons/` — platform layer: node-local-dns, metrics-server, cluster-autoscaler,
  external-secrets (+ ClusterSecretStore), cert-manager (+ DNS-01 ClusterIssuers),
  temporal-worker-controller, traefik, wildcard-cert. (No external-dns: the
  `*.tmprl-demo.cloud` wildcard CNAMEs to `live`; there are no per-demo records.)
- `bootstrap/` — registry CRDs + the operator (one worker+watch Deployment on
  `registry-management-green`). The operator reconciles DemoProject CRs applied to
  green onto green.

## Not wired (by design)

- **instruqt-litellm** (app layer) — not needed for the cutover gate.
- **The real `projects/` catalog** — green is warmed with a throwaway `p6-smoke`
  DemoProject applied directly, so green never co-manages blue's active demos.
  Real-demo migration is P7.

## Cross-phase gotchas (carried from the blue tree)

- **Traefik v3 CRD apiGroup** is `traefik.io`. The operator emits
  `traefik.io/v1alpha1` IngressRoute/Middleware when colored, and
  the RBAC grant matches; v3 silently ignores v2-group objects.
- **worker-controller `certmanager.install=false`** — `install: true` deploys a
  whole second cert-manager subchart. Green relies on its own cert-manager addon,
  so the release `dependsOn` cert-manager.
- **node-local-dns** takes effect only if the kubelet `--cluster-dns` points at the
  link-local `169.254.20.10`; verify the cache is actually in-path on green.
- **Let's Encrypt** prod rate limits: the wildcard-cert addon means demos are served
  a shared `*.tmprl-demo.cloud` cert (no per-host minting). Green mints its own
  wildcard cert independently of blue via DNS-01, so it can pre-warm TLS with zero
  inbound traffic before the cutover.
