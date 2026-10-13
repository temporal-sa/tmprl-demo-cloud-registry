# blue (tmprl-dem-cld-blue)

Flux desired state for the active, Tofu-managed `tmprl-dem-cld-blue` EKS
cluster. This is a self-contained tree rather than an overlay: every manifest
reads exactly as deployed.

Cluster-specific values include:

- **IRSA annotations** point at the `tmprl-dem-cld-*` roles shared by blue and
  green.
- **cluster-autoscaler** `autoDiscovery.clusterName: tmprl-dem-cld-blue`.
- **Flux paths** are `./clusters/blue/...`.
- **Versions are explicitly pinned**: Flux 2.9.1, cert-manager v1.21.0,
  external-secrets 2.7.0, Traefik **v3** (chart 41), cluster-autoscaler chart
  9.58.0 / image v1.36.0, temporal-worker-controller 0.31.0, KEDA 2.21.0, metrics-server
  3.13.1, k8s-dns-node-cache 1.26.8.
- **cert-manager + the DNS-01 ClusterIssuers** are managed by Flux (role
  `tmprl-dem-cld-cert-manager`).

## Layout

- `flux-system/` — the root sync: the `GitRepository` (HTTPS, reusing the
  read-only PAT; branch `main`) and the self-managed `Kustomization`s. Tofu
  installs Flux and applies the initial `GitRepository` + root `Kustomization`;
  everything below self-manages.
- `addons/` — cluster-local platform layer: node-local-dns, metrics-server,
  cluster-autoscaler, external-secrets (+ ClusterSecretStore), cert-manager
  (+ ClusterIssuers), temporal-worker-controller, traefik, and wildcard TLS.
  There is no external-dns: `*.tmprl-demo.cloud` (Tofu, `infra/dns.tf`) CNAMEs
  to `live`, and Traefik routes every host by name. The Instruqt LiteLLM app lives at the repo-level
  `apps/instruqt-litellm/deploy` path and is referenced by the root sync.
- `bootstrap/` — registry CRDs and the scaled-to-zero bootstrap watch handoff.
  The active operator worker is self-rendered through the registry project.

## Cross-phase gotchas (from the P3 latest-GA validation)

- **Traefik v3 CRD apiGroup** is `traefik.io`. LiteLLM and operator-generated
  `IngressRoute`/`Middleware` objects MUST use `traefik.io/v1alpha1` and v3
  matcher syntax, or Traefik v3 silently ignores them.
- **node-local-dns** takes effect only if the kubelet `--cluster-dns` points at
  the link-local `169.254.20.10`; the DaemonSet also binds `172.20.0.10` and
  installs intercept rules. Verify the cache is actually in-path on blue.
- **Let's Encrypt** prod rate limits still apply when warming another color;
  use `letsencrypt-staging` or distinct hosts until cutover.
