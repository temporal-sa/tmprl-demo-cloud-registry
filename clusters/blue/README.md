# blue (tmprl-dem-cld-blue)

Flux bootstrap configuration for the Tofu-managed `tmprl-dem-cld-blue` EKS
cluster — the blue side of the blue/green pair. Authored in P3 of the IaC
migration (see `docs/iac-migration-plan.md`).

This tree is a **self-contained duplication** of `clusters/sa-demo/` (the hand-
built pet), not an overlay: every manifest reads exactly as deployed, with no
base to peel back. Per-color/per-cluster deltas versus the pet are only:

- **IRSA annotations** point at the P2 `tmprl-dem-cld-*` roles (shared blue/green)
  instead of the pet's legacy role names.
- **cluster-autoscaler** `autoDiscovery.clusterName: tmprl-dem-cld-blue`.
- **Flux paths** are `./clusters/blue/...`.
- **Versions are latest-GA** ("fresh stack is the upgrade vehicle" doctrine), so
  they intentionally differ from the pet: Flux 2.9.1, cert-manager v1.21.0,
  external-secrets 2.7.0, Traefik **v3** (chart 41), cluster-autoscaler chart
  9.58.0 / image v1.36.0, temporal-worker-controller 0.27.0, metrics-server
  3.13.1, k8s-dns-node-cache 1.26.8.
- **cert-manager + the DNS-01 ClusterIssuers are new here** — on the pet they are
  installed out-of-band; blue brings them into Flux (role `tmprl-dem-cld-cert-manager`).

## Layout

- `flux-system/` — the root sync: the `GitRepository` (HTTPS, reusing the
  read-only PAT; branch `feat/full-e2e-iac` during P3, flip to `main` at merge)
  and the self-managed `Kustomization`s. Tofu installs Flux and applies the
  initial `GitRepository` + root `Kustomization`; everything below self-manages.
- `addons/` — platform layer wired for P3: node-local-dns, metrics-server,
  cluster-autoscaler, external-secrets (+ ClusterSecretStore), cert-manager
  (+ ClusterIssuers), temporal-worker-controller, traefik.

## Not yet wired (later phases)

- **instruqt-litellm** (app layer) — depends on external-secrets, cert-manager,
  traefik; deferred until the platform is green.
- **registry** (CRDs, operator, projects, instruqt courses) — the operator runs
  scaled-to-zero until the P6 cutover; its clean IRSA role is held by the pet
  until then.

## Cross-phase gotchas (from the P3 latest-GA validation)

- **Traefik v3 CRD apiGroup** changed `traefik.containo.us` → `traefik.io`. When
  instruqt-litellm and the operator-generated `IngressRoute`/`Middleware` objects
  are ported here they MUST use `traefik.io/v1alpha1` and v3 matcher syntax, or
  Traefik v3 silently ignores them.
- **Flux image-automation** CRDs serve only `image.toolkit.fluxcd.io/v1` in Flux
  2.9. The pet's `instruqt-litellm/install/image-automation.yaml` uses
  `v1beta2` — rewrite to `v1` when porting.
- **node-local-dns** takes effect only if the kubelet `--cluster-dns` points at
  the link-local `169.254.20.10`; the DaemonSet also binds `172.20.0.10` and
  installs intercept rules. Verify the cache is actually in-path on blue.
- **Let's Encrypt** prod rate limits: don't pre-warm blue against the same
  hostnames the pet already serves; use `letsencrypt-staging` or distinct hosts
  until cutover.
