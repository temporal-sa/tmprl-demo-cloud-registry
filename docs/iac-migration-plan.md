# IaC + Blue/Green Migration Plan

Plan for making the `tmprl-demo.cloud` demo environment fully deployable with
OpenTofu, and for running blue/green clusters where cutover is a single DNS
change. Pairs with [architecture.md](architecture.md) (what the platform is) and
[operations.md](operations.md) (runbook). This doc is the phase tracker for the
infrastructure-as-code work specifically.

## Goal

`tofu apply` stands up a complete demo environment from nothing: a new EKS
cluster, its networking, load balancing, IAM/IRSA, Flux, and the wildcard DNS
routing — with the existing registry operator taking over demo provisioning from
there. A second cluster can be stood up alongside the first, and the "base" DNS
entry (`*.tmprl-demo.cloud`) can be swung from one to the other for a blue/green
cutover.

## Working conventions

How this feature is built. These are hard constraints, not preferences.

### Coding

- **Abstraction is the enemy of human understanding.** Helper code is fine, but
  do not stack multiple layers of abstraction. Someone reading the stack cold
  should follow it without peeling back indirection.
- **No stringified/piped code.** Never write a bash script that pipes stringified
  Python (or any language) into an interpreter. If Python is the right fit, write
  a proper Python program; if Go is, write proper Go. The language choice is open
  — the requirement is that it is a real, readable program, not glue-stringified
  source.
- **Optimize for low cognitive load.** Prefer a simple, obvious stack over a
  clever one. OpenTofu HCL is the primary tool; reach for a real program only
  when HCL genuinely cannot express the step.

### Commits & branch

- Work happens on the `feat/full-e2e-iac` feature branch.
- **Commit concisely as we go.** Small, self-contained commits that each make one
  reviewable change. **No huge one-shot commits** that land a whole phase at once
  — the change history must stay cleanly trackable.

### Progress tracking (context is cleared between phases)

Context is wiped between phases, so **this doc is the durable memory.** Keep the
Phase status table and the Progress log below current as work lands — record what
is done, what is in flight, and any decision or gotcha the next session needs.
Update them in the same commits as the work.

## Explicitly NOT automated (configured at deploy time)

- **Domain registration.** `tmprl-demo.cloud` and its Route53 hosted zone
  (`Z08792403SH7ADUAH7X5X`) already exist; Tofu takes the zone id as an input,
  it does not register domains.
- **Secret values.** Tofu creates the AWS Secrets Manager *containers* it needs
  (bootstrap Temporal Cloud creds, GitHub read-only PAT, Instruqt CLI token),
  but the sensitive values are populated out-of-band, exactly as today.

## Decisions (locked)

| Decision | Choice |
| --- | --- |
| Region | **us-west-1** (reuse existing ACM wildcard cert, Route53 zone, Secrets Manager, CodeBuild) |
| Cluster provisioning | **Greenfield + migrate** — build a new Tofu-managed cluster alongside the hand-built `sa-demo` pet, never import the pet |
| VPC | **New Tofu-managed VPC**, shared by blue + green |
| Blue/green operator model | **Standby** — only the active color's operator polls Temporal; the other runs scaled to zero until cutover |
| Cutover mechanism | **Declarative base-record swing** — a Tofu `active_color` variable repoints one `live.tmprl-demo.cloud` record; managed demos CNAME `live` (created by external-dns), while the pet keeps the `*` wildcard as its manual catch-all. See [DNS + cutover design](#dns--cutover-design-p4p6p7). No imperative scripts. |
| Operator multi-cluster safety | **Add a cluster/color dimension** to the operator (task queue + workflow id + reconcile key) |
| State backend | **Dedicated S3 bucket** `tmprl-dem-cld-tofu-state-429214323166-us-west-1` (versioning, SSE-S3, public access blocked, TLS-only policy) |
| State locking | **Native S3 lockfile** (`use_lockfile`, OpenTofu ≥ 1.10) — no DynamoDB table |

## Current state (as inspected)

- `sa-demo` EKS in **us-west-1**, control plane 1.30 / nodes 1.29.15, in
  `vpc-00bb9a602ae4bad2c` (4 subnets). Hand-built 2023, no IaC.
- Ingress: **Traefik → a Classic ELB** (`a7372c66…elb.amazonaws.com`),
  provisioned by the in-tree cloud provider (no AWS LB Controller installed).
  Traefik terminates TLS.
- **`*.tmprl-demo.cloud` is a single wildcard CNAME → the Traefik ELB, TTL 300.**
  This one record is the entire blue/green swing point. `registry.tmprl-demo.cloud`
  is a separate CloudFront alias and does **not** move on cutover.
- ACM `*.tmprl-demo.cloud` exists (us-west-1, ISSUED).
- Everything above Flux is GitOps (`clusters/sa-demo/`). The IaC seam was always
  intended — see architecture.md §"Flux Kustomizations are self-managed".

## Tofu / Flux boundary

Tofu owns AWS primitives + the Flux install + the first root sync. Flux keeps
owning in-cluster addons, the operator, and projects exactly as today.

Tofu owns: VPC, EKS control plane, managed node group, OIDC provider, IRSA roles
(`registry-operator`, `traefik-ingress-role`, cluster-autoscaler,
external-secrets, CodeBuild service role), ECR, CodeBuild project, Secrets
Manager containers, ACM wildcard (data-source the existing cert), the Route53
wildcard record, and `flux install` + the root `GitRepository`/`Kustomization`.

## The operator multi-cluster problem (root of the design)

The registry operator is a Temporal worker whose entity workflows live in
Temporal Cloud, and it has **zero cluster-awareness**:

- Task queue is a single fixed default `registry-management` (`config.py:79`).
- Entity workflow id is `demo-project/<name>` — no cluster
  (`watch.py:662`); reconcile identity is a pure spec/source hash with no cluster
  component (`resources.py:601`).
- K8s client is in-cluster only (`kube.py:1223`).

So two clusters on the same Temporal namespace would (a) cross-route k8s-applying
activities to whichever worker polls first, and (b) treat green's demos as
"already synced" from blue's completed workflows, leaving green empty.

**Fix (Phase 5):** a single `DEMO_REGISTRY_CLUSTER` (color) env var, injected
per-cluster by the Flux overlay, folded into:

- **task queue** → `registry-management-<color>` (green's workers only poll
  green's queue — no cross-routing);
- **workflow id** → `demo-project/<color>/<name>` (green starts fresh workflows
  that reconcile onto green, idempotently re-adopting shared Temporal Cloud
  namespaces / ECR);
- **reconcile key** (belt-and-suspenders).

This is one new dimension, not a new abstraction layer, and fits the existing
level-triggered / derive-from-name design. Shared global resources stay safe
because in standby only one color's operator is ever running.

## DNS + cutover design (P4/P6/P7)

How managed demos get stable per-demo DNS that swings atomically between colors
**without disrupting the pet's manual traffic**. Supersedes the earlier
raw-wildcard-swing and per-color-prefix ideas.

### The records

| Record | Points at | Owner | Notes |
| --- | --- | --- | --- |
| `*.tmprl-demo.cloud` | pet Traefik ELB | Tofu (leave as-is) | The **manual/legacy catch-all** — never moved. Everything users deploy by hand on the pet keeps resolving here. |
| `live.tmprl-demo.cloud` | **active color's** Traefik ELB (ALIAS A) | Tofu, via `active_color` | The **single swing point.** Low TTL (~60s). This is the only thing that moves on cutover. |
| `<demo>.tmprl-demo.cloud` | CNAME → `live` | **external-dns** (per cluster) | One per managed demo. An exact record **overrides the wildcard** (verified live: `registry` beats `*`), so managed demos route to the active color while everything else falls through to the pet. |
| `blue.` / `green.tmprl-demo.cloud` | that color's ELB | Tofu | Stable per-color names for pre-cutover validation (`curl`, browser). cert-manager can pre-mint their certs via DNS-01 with zero inbound traffic. |

### Why it's multi-cluster-safe (the key property)

Per-demo records are **cluster-agnostic** — they always target `live`, never a
specific ELB. So blue's and green's external-dns write the *identical* record;
there is no Route53 ownership fight. The color lives in exactly one place: the
`live` record (Tofu-managed). This is what makes running external-dns on two
clusters against one zone safe.

### Ownership split

- **Tofu:** `live` (the swing), the `blue.`/`green.` names, and it leaves the `*`
  wildcard on the pet. Data-sources each Traefik ELB hostname.
- **external-dns** (new addon, per cluster): the per-demo `<demo> → live` CNAMEs
  + ownership TXTs, generated from the operator's IngressRoutes. Config:
  `--source=traefik-proxy` (reads Traefik **v3** `traefik.io` natively),
  `--provider=aws`, `--domain-filter=tmprl-demo.cloud`, `--registry=txt`,
  `--txt-prefix=extdns-`, a per-color `--txt-owner-id`, `--policy=sync`. It can
  **never** touch `*`/`live`/`registry` (it owns only records carrying its TXT).
  New IRSA role `tmprl-dem-cld-external-dns` (route53 Change/List on the zone;
  model on the cert-manager role).
- **Operator [P5]:** stamps `external-dns.alpha.kubernetes.io/target:
  live.tmprl-demo.cloud` on every IngressRoute it generates. This is the **only**
  operator DNS change and it needs **no** Route53 permissions (external-dns holds
  the creds).

### The flip (blue → green → blue)

1. Warm the target color fully — its operator reconciles all demos (IngressRoutes
   + certs; cert-manager pre-mints via DNS-01, no traffic needed). Validate via
   `green.tmprl-demo.cloud` while prod still flows to blue.
2. `active_color=green` → `tofu apply` repoints `live` → green's ELB. All managed
   demos follow within the TTL (they all CNAME `live`).
3. Rollback = `active_color=blue`, apply again. Instant — blue never moved.

The pet wildcard is untouched throughout, so manual users are never disrupted.
Because every managed demo CNAMEs `live`, they swing **together** — the target
color must have *all* demos before the flip. Gradual per-demo migration is
possible instead: a demo shifts to blue the moment blue's external-dns creates
its explicit record (exact record beats the pet wildcard).

### Spike findings (external-dns v0.21.0, dry-run against blue's live Traefik v3)

- external-dns reads Traefik **v3** IngressRoutes natively (`traefik.io/v1alpha1`;
  `TraefikEnableLegacy:false` by default). Confirmed: it discovered a real v3
  IngressRoute on blue.
- The traefik source needs a **target** on the IngressRoute — it emits *no*
  endpoint without one. `--default-targets` and `--force-default-targets` do
  **not** help the traefik source (proven: still "No endpoints could be
  generated"). The working mechanism is the per-IngressRoute
  `external-dns.alpha.kubernetes.io/target` annotation → hence the operator change.
- With the annotation it produced exactly `spike-demo → CNAME → live` + an
  ownership TXT at `extdns-cname-spike-demo` (different name → no CNAME/TXT clash).

### Gotchas to honour

- **Bootstrap order:** `live` (Tofu) must exist before demo CNAMEs resolve.
- **LE rate limits:** both colors issuing certs for the same demo hosts during
  coexistence can hit Let's Encrypt duplicate-cert limits — use staging while
  iterating / mind repeated flips.
- **Name shadowing:** an explicit managed-demo record shadows the wildcard for
  that exact name; add a naming guard so a managed demo can't collide with a
  manual pet host.
- external-dns runs on the active color (standby); during a transition both may
  run, but records are identical (→ `live`) so it's benign.

## Phases (test as we go)

Each phase has an explicit test gate before moving on.

- **P0 — Foundations.** `infra/` Tofu layout, S3 state backend with native
  lockfile locking (`use_lockfile`, no DynamoDB), provider config, input
  variables (domain, zone id, region, account, bootstrap-cred references).
  *Gate: clean `init`/`plan`, only the state bucket exists.*
- **P1 — Network + cluster.** New VPC, EKS control plane, managed node group,
  OIDC provider. *Gate: nodes Ready via Tofu-emitted kubeconfig; cluster
  destroys cleanly.*
- **P2 — Identity + AWS platform.** IRSA roles, ECR, CodeBuild project, Secrets
  Manager containers, ACM wildcard (data source). *Gate: roles assumable, cert
  validated.*
- **P3 — Flux bootstrap.** Tofu installs Flux + the root sync against a
  **per-color cluster path** (`clusters/<color>/`, parametrizing today's
  hardcoded `sa-demo`). *Gate: addons reconcile, Traefik LB provisioned.*
- **P4 — Routing + DNS.** Tofu data-sources each color's Traefik LB hostname and
  creates the `live.tmprl-demo.cloud` swing record (ALIAS → active color) plus the
  per-color `blue.`/`green.` validation names; adds the **external-dns** addon
  (+ IRSA role). The pet keeps the `*` wildcard untouched (manual catch-all).
  Classic ELB already confirmed in P3. See
  [DNS + cutover design](#dns--cutover-design-p4p6p7). *Gate: a per-demo record
  resolves to blue and a demo is reachable there, with the pet wildcard undisturbed.*
- **P5 — Operator cluster dimension.** Implement the color dimension above, AND
  have the operator stamp the `external-dns.alpha.kubernetes.io/target:
  live.tmprl-demo.cloud` annotation on every generated IngressRoute (the only
  operator DNS change; no Route53 perms on the operator). *Gate: two clusters, no
  activity cross-routing, green reconciles its demos independently of blue.*
- **P6 — Blue/green cutover.** `active_color` swings the single `live` record; all
  managed demos follow (they CNAME `live`); the pet wildcard never moves.
  *Gate: stand up green, flip, verify, roll back.*
- **P7 — Migrate + retire the pet.** Move managed demos onto the new stack —
  explicit `live`-CNAME records override the pet wildcard per demo, so migration
  is gradual and reversible. The pet can then persist as the `*` wildcard/manual
  catch-all or be decommissioned (a separate, unforced decision).

## Open items to resolve early

- **Execution model: local SSO now → GitHub Actions later.** Today `tofu` runs
  locally under the operator's SSO admin session (hence the credential-bridge
  gotcha below). The intended end state is Tofu running in a **GitHub Action
  that assumes a cross-account role** via GitHub OIDC federation
  (`token.actions.githubusercontent.com` — the account already federates it,
  e.g. `tmprl-dem-cld-instruqt-broker-github-actions`). Design P2+ so this lands
  cleanly:
  - Create the CI **assume-role** (GitHub OIDC trust scoped to this repo/branch;
    permissions for EKS/EC2/IAM/ECR/etc. + the state bucket) as part of P2's
    identity work. It becomes the standing Tofu apply identity.
  - That same CI role ARN is the **durable `cluster_admin_principal_arns`
    entry** — it is stable (does not churn like the SSO role), so it resolves the
    P1 review finding properly rather than needing a separate break-glass role.
  - The S3 state backend (`…-tofu-state-…`) and `allowed_account_ids` guard must
    both admit the CI role (it assumes into 429214323166); confirm the bucket
    policy allows it.
  - The local credential-bridge stays only as a break-glass / dev convenience.
- **cert-manager challenge type. ✅ RESOLVED (P2 discovery).** The live
  `letsencrypt`/`letsencrypt-staging` ClusterIssuers already use **DNS-01 via
  Route53** (`hostedZoneID Z08792403SH7ADUAH7X5X`, region us-west-1, empty
  `secretAccessKeySecretRef` → ambient IRSA on `cert-manager/cert-manager`). No
  HTTP-01→DNS-01 migration needed — a standby green cluster *can* pre-warm valid
  TLS before the DNS swing. P2 creates the per-color cert-manager IRSA role;
  bringing cert-manager + the issuers into Flux is P3 (they are installed
  out-of-band today, not in the repo).
- **Classic ELB vs NLB. ✅ RESOLVED (P4) — kept the Classic ELB.** Tofu
  data-sources the live Traefik Service via the per-color kubernetes provider and
  builds the `live`/`<color>` ALIAS A records against the Classic-ELB canonical
  zone (`data.aws_elb_hosted_zone_id` → `Z368ELLRRE2KJ0` in us-west-1). No AWS LB
  Controller, no NLB migration — the in-tree provider's ELB is sufficient for the
  swing design and an NLB would only add churn. Revisit only if the classic ELB is
  actually retired by AWS.
- **Node group sizing.** Match current capacity (4 nodes) as a starting point;
  cluster-autoscaler tuning carries over from the existing addon.

## Phase status

| Phase | Status |
| --- | --- |
| P0 — Foundations | ✅ Done |
| P1 — Network + cluster | ✅ Done (gate passed: nodes Ready + clean destroy + clean re-create) |
| P2 — Identity + AWS platform | ✅ Done (gate passed: 5 IRSA roles assumable on blue + cert validated; shared substrate adopted, 0 destroys) |
| P3 — Flux bootstrap | ✅ Done (gate passed: Flux on blue via Tofu; 10 platform Kustomizations Ready; Traefik Classic ELB live + serving; external-secrets store Valid + cert-manager issuers Ready = IRSA proven; `tofu plan` idempotent) |
| P4 — Routing + DNS | ✅ Done (gate passed: managed-demo record resolves to blue + reachable via public DNS; pet wildcard undisturbed; external-dns writes CNAME→live + owner=blue TXT and retracts on delete; `tofu plan` idempotent) |
| P5 — Operator cluster dimension | ✅ Done (gate passed: blue operator on `registry-management-blue` + `demo-project/blue/*` coexists with the pet's uncolored ids in ONE Temporal namespace, no cross-routing; throwaway demo reconciled on blue end-to-end — v3 IngressRoute + external-dns CNAME→live + wildcard TLS, HTTP 200 via public DNS; pet wildcard undisturbed; clean teardown) |
| P6 — Blue/green cutover | Not started |
| P7 — Migrate + retire the pet | Not started |

## Progress log

Newest first. One entry per meaningful step so a fresh session can resume.

- **P5 GATE PASSED — operator cluster dimension live + validated on blue.** The
  registry operator is now color-safe and was proven end-to-end on blue against a
  throwaway demo, with the pet undisturbed. **The killer evidence:** in the ONE
  shared Temporal Cloud control-plane namespace (`tmprl-dem-cld-registry-operator`),
  blue's `demo-project/blue/p5-smoke` (task queue `registry-management-blue`) ran
  alongside the pet's uncolored `demo-project/<name>` (task queue
  `registry-management`) with zero cross-routing — the pet IS the "second cluster"
  that proves the gate. Every changed behavior is a byte-identical no-op when
  `DEMO_REGISTRY_CLUSTER` is unset, so the live pet is untouched.
  - **Operator code (all color-gated on `DEMO_REGISTRY_CLUSTER`):** commit `799f7ec`
    — one `DEMO_REGISTRY_CLUSTER` env folded into the task queue
    (`registry-management-<color>`), entity workflow ids
    (`demo-project/<color>/<name>`, `instruqt-course/<color>/<name>` via new
    single-source `derive` helpers used by every producer/consumer incl. the 4
    inline canary sites), and the `worker.py` worker-update **batch-signal** query
    (color-scoped `WorkflowId STARTS_WITH`, so a colored operator can't signal the
    pet's running workflows in the shared namespace — a hazard the pre-impl mapping
    caught). Traefik v2→v3: emits `traefik.io/v1alpha1` when colored (pet stays
    `traefik.containo.us`); `kube.py` plural map carries both. external-dns target
    annotation (`live.tmprl-demo.cloud`) stamped on generated IngressRoutes when
    colored. `c9ed297` — Temporal Cloud namespace tags `owner=tmprl-demo.cloud` +
    `repo=<owner/repo>` (own idempotent `UpdateNamespaceTags` activity, per the
    one-side-effect rule). `55abf3b` — colored clusters serve demos from a single
    per-cluster **wildcard cert** via Traefik's default `TLSStore` (`tls: {}`, no
    per-host `Certificate`) so rapid rollouts stop minting per-host LE certs.
  - **Two review-driven fixes (both surfaced by adversarial review / live run):**
    `56b488e` — namespace tagging is **best-effort** (a tag failure must never
    propagate into the reconcile's compensating try and tear down the just-created
    namespace); `509031d` — coerce tag values to TC's charset `[a-z0-9._-@]`
    (lowercase, `/`→`_`; TC rejects `/`, so `temporal-sa/x` → `temporal-sa_x`).
  - **Deploy (blue):** `039388a` un-defers the operator IRSA role as
    `tmprl-dem-cld-registry-operator-bg` (greenfield, color-union trust; clean name
    held by the pet until P7; policy verbatim from the pet). `3962054` the blue
    registry layer — CRDs + a **single worker+watch** operator Deployment on
    `registry-management-blue` (NOT the pet's split watch/self-render model);
    `DEMO_REGISTRY_CLUSTER=blue` in its configMap; traefik.io RBAC. `7fe0dc2` the
    wildcard cert addon (`*.tmprl-demo.cloud` cert-manager Certificate DNS-01 +
    `TLSStore/default` in the traefik ns). The real `projects/` catalog is
    deliberately NOT wired to blue — only a throwaway `p5-smoke` demo was applied.
  - **e2e evidence (throwaway `p5-smoke`, now torn down):** operator logged
    `task_queue=registry-management-blue watch_mode=always`; reconciled
    `p5-smoke` → v3 IngressRoute (`tls: {}`, 0 per-host certs) + external-dns
    `p5-smoke → CNAME → live` + `owner=blue` TXT + Temporal namespace
    `tmprl-dem-cld-p5-smoke` (tagged owner+repo); **`https://p5-smoke.tmprl-demo.cloud`
    → HTTP 200** with the `CN=*.tmprl-demo.cloud` wildcard cert (verified on both
    blue ELB IPs + public resolvers 1.1.1.1/8.8.8.8). An unmanaged host still
    resolved to the pet ELB (wildcard undisturbed). **Delete → clean finalize:**
    namespace terminated + external-dns retracted the CNAME/TXT (proves the P7
    teardown/retract path). **459 tests pass** (+ new `test_cluster_dimension.py`,
    `test_namespace_tags.py`; 2 pre-existing env-only failures unrelated).
  - **Decisions (locked with the user):** (1) **reconcile-key color-fold SKIPPED**
    — per-color workflow ids already isolate each color's state; folding color into
    the key is redundant AND risks a permanent reconcile hot-loop (it's recomputed
    in deterministic workflow + activity code with no color in scope). (2) tag
    separator **`_`** (TC forbids `/`). (3) **wildcard default cert** for demo TLS
    (kills the LE rate-limit class + gives blue/green cert independence) over
    per-host-cert reuse.
  - **CROSS-PHASE flags for P6/P7:**
    - **Blue's operator is currently RUNNING (replicas 1), scaled up for the P5
      e2e** — it is a standalone worker+watch, not the pet's split model. The locked
      standby model wants the non-active color scaled to zero until cutover; whether
      to return blue to standby now (set the Deployment replicas to 0) or leave it
      running is an open P6 operational decision.
    - **Merge-apply gotcha:** an existing demo that already carries a per-host cert
      keeps it (an empty `tls: {}` merge doesn't clear a set `secretName`); only
      newly-created/re-created demos pick up the wildcard. Harmless (old certs just
      renew), but the P7 migration re-creates demos so they land on the wildcard.
    - **`-bg` IRSA role name** is interim; at P7 (pet retired) the clean
      `tmprl-dem-cld-registry-operator` name frees up (rename/adopt is optional).
    - The operator self-canary inherits color via `platform_env_vars` injecting
      `DEMO_REGISTRY_CLUSTER` into the rendered WorkerDeployment (the per-cluster
      configMap only feeds the bootstrap pod). Live blue facts (read from `tofu
      output` / the cluster — do not hardcode): IRSA role
      `tmprl-dem-cld-registry-operator-bg`; operator image tag is pinned in
      `clusters/blue/bootstrap/registry-operator/deployment.yaml`.

- **P4 GATE PASSED — routing + DNS live on blue.** Tofu now owns the swing +
  per-color records; external-dns turns Traefik IngressRoutes into per-demo
  Route53 records. `tofu apply`: **4 added, 0 changed, 0 destroyed**; re-`plan` =
  "No changes" (idempotent; pet + all P1–P3 resources untouched). Commits
  `cf92a0a` (IRSA role) + `16d85c1` (DNS records) + `a5b0ada` (external-dns addon)
  + `5e3529a` (review follow-up), pushed; Flux reconciled external-dns on blue.
  - **Live P4 records** (read from `tofu output dns` / the zone — ELB rotates on
    rebuild, do not hardcode): `live.tmprl-demo.cloud` **A ALIAS →** active color's
    Traefik ELB (blue: `a65d2de6…`), `blue.tmprl-demo.cloud` A ALIAS → same. Pet
    `*.tmprl-demo.cloud` **untouched** (still CNAME → pet ELB `a7372c66…`, TTL 300);
    `registry` untouched. Classic-ELB alias zone `Z368ELLRRE2KJ0`.
  - **external-dns 1.21.1 / app 0.21.0 Ready on blue.** Config verified from the
    running pod: `Sources:[traefik-proxy]`, `Provider:aws`, `DomainFilter:
    [tmprl-demo.cloud]`, `Policy:sync`, `Registry:txt`, `TXTOwnerID:blue`,
    `TXTPrefix:extdns-`, `TraefikEnableLegacy:false` (reads Traefik v3 `traefik.io`
    natively), `DryRun:false`. **IRSA proven** (`tmprl-dem-cld-external-dns`): it
    listed + wrote the live zone with zero auth errors.
  - **Gate evidence (throwaway smoke, now torn down):** a whoami Deployment +
    Traefik v3 IngressRoute carrying `external-dns.alpha.kubernetes.io/target:
    live.tmprl-demo.cloud` → external-dns created exactly `p4-smoke → CNAME → live`
    + an ownership TXT at `extdns-cname-p4-smoke` (`owner=blue`, distinct name → no
    clash). `p4-smoke` resolved (authoritative NS) to blue's ELB IPs, and a plain
    `curl` **relying on public DNS** reached the whoami pod (blue VPC 10.20.x) via
    blue's Traefik — the full DNS→ELB→Traefik→pod path. An **unmanaged** host and
    `*` still resolved to the pet ELB (wildcard undisturbed; exact record beats
    wildcard, as designed). Deleting the IngressRoute → external-dns **retracted
    the CNAME + TXT within ~40s** (`policy=sync`) — also proves the P7 demo-teardown
    path. Smoke namespace fully deleted; zone back to steady state.
  - **Pre-apply adversarial review** (workflow `wf_efc8d145-d65`, 6 dims → per-
    finding adversarial verify): 2 medium confirmed, 1 rejected. (1) **Chicken-and-
    egg ordering** — `dns.tf`'s `data.kubernetes_service` reads a *Flux-reconciled*
    Traefik Service Tofu does not create, so a color's DNS can only be applied
    AFTER that color's ELB exists. Blue was safe (P3 built Traefik; P4 adds DNS as a
    separate apply — the correct order). Fixed the misleading `dns.tf` header
    comment (`5e3529a`) + the P6 ordering note below; **did NOT** add a gating
    abstraction (anti-abstraction rule; phased apply already matches reality). (2)
    **Gate needs a smoke artifact** (no annotated IngressRoute on blue until P5) —
    exactly the throwaway above; done + cleaned up. Rejected: `active_color`
    validation checks `var.colors` not the ELB map — fails safe/loud, DX nit.
  - **CROSS-PHASE flags for P5/P6:**
    - **P5:** the operator must stamp `external-dns.alpha.kubernetes.io/target:
      live.tmprl-demo.cloud` on **every** IngressRoute it generates (Traefik v3
      `traefik.io/v1alpha1` + v3 matcher syntax). The smoke test proved this exact
      annotation is the working mechanism. The operator needs **no** Route53 perms
      (external-dns holds them). Without the annotation the traefik source emits
      nothing (spike-confirmed).
    - **P6 ORDERING TRAP (from the review):** bringing up green is **two applies**,
      not one. Apply 1: add the green provider-alias block (`providers.tf`) + green
      `module.flux_*` block (`flux.tf`) + `"green"` to `var.colors` → stands up
      green's cluster + Flux (green NOT yet in `dns.tf`). Wait for green's Traefik
      ELB. Apply 2: add green's `data.kubernetes_service.traefik_green` + map entry
      in `dns.tf` → creates `green.` (and, at the flip, set `active_color=green` to
      repoint `live`). A single from-nothing color apply — or a color destroy+
      rebuild — aborts on the absent Traefik Service. `dns.tf` header documents this.
    - external-dns runs on the **active** color (blue today). At P6 green's tree
      (duplicated from `clusters/blue/`) carries the same addon with
      `txtOwnerId: green`; identical `→ live` records make coexistence benign.

- **P4/P5 DNS + cutover design locked (with external-dns spike).** New model,
  driven by a user requirement to NOT disrupt manual pet deployments: keep the
  `*` wildcard on the pet as the manual catch-all; add a Tofu-managed
  `live.tmprl-demo.cloud` swing record (ALIAS → active color, `active_color`); each
  managed demo gets an explicit `<demo> → CNAME → live` record (exact record beats
  the wildcard — verified live: `registry` resolves to CloudFront, not the pet ELB
  via `*`). Per-demo records are **cluster-agnostic** (always → `live`), so two
  colors' external-dns can't fight over the zone. Full design + record table in
  [DNS + cutover design](#dns--cutover-design-p4p6p7).
  - **external-dns spike (v0.21.0, dry-run/inmemory against blue's live Traefik
    v3) — all confirmed:** reads Traefik v3 `traefik.io/v1alpha1` natively (legacy
    off by default); the traefik source needs a target on the IngressRoute
    (`--default-targets`/`--force-default-targets` do NOT work for it — both
    proven to emit no endpoint); the working mechanism is the per-IngressRoute
    `external-dns.alpha.kubernetes.io/target: live.tmprl-demo.cloud` annotation,
    which produced exactly `<demo> CNAME → live` + an ownership TXT at
    `extdns-cname-<demo>` (different name, no clash). Spike ran dry-run/inmemory →
    zero Route53 writes, resources cleaned up.
  - **Impact:** P4 adds the external-dns addon + `tmprl-dem-cld-external-dns` IRSA
    role + the `live`/`blue.`/`green.` records (pet wildcard untouched). P5 adds
    one operator change: stamp the target annotation on generated IngressRoutes
    (no Route53 perms on the operator). Cutover (P6) = swing the single `live`
    record. Classic ELB (P3) stands; no NLB. Pet can persist as the manual
    wildcard catch-all past P7.

- **P3 GATE PASSED — Flux live on blue, full platform layer reconciled.** Tofu
  installs Flux; Flux syncs `clusters/blue/` from the feature branch. All 10
  platform Kustomizations `Ready` at `feat/full-e2e-iac@2498f55`; 7 HelmReleases
  installed clean; **Traefik Classic ELB provisioned + serving** (curl → Traefik
  404 for unmatched host, i.e. LB→pod path works). **IRSA proven on blue end to
  end**: external-secrets `ClusterSecretStore platform-secrets` = Valid/ReadWrite
  (the `tmprl-dem-cld-external-secrets` role reads Secrets Manager), both
  cert-manager `ClusterIssuer`s Ready (the `tmprl-dem-cld-cert-manager` role
  registered ACME accounts via DNS-01 Route53). **`tofu apply`: 5 added, 0
  changed, 0 destroyed; re-`plan` = "No changes"** (idempotent — pet + all P1/P2
  resources untouched). Commits `0d89241` (blue tree) + `2498f55` (Tofu), pushed.
  - **Decisions locked with the user:** (1) **full duplication** — `clusters/blue/`
    is a self-contained copy of the pet tree, never an overlay (zero indirection;
    matches the anti-abstraction rule; lets blue/green run different versions as
    the upgrade vehicle intends); (2) **latest-GA versions** (upgrade-vehicle
    doctrine), NOT pet parity; (3) **reuse the read-only PAT over HTTPS** — the
    `…/github-temporal-sa-readonly` secret is `{username,password}` JSON, exactly
    Flux's basic-auth shape; Tofu `jsondecode`s it into the git-auth Secret. No new
    deploy key, no repo-admin step.
  - **Latest-GA versions deployed (differ from the pet by design):** flux2 chart
    `2.19.0`/Flux `2.9.1`, cert-manager `v1.21.0`, external-secrets `2.7.0`,
    Traefik chart `41.0.2`/**Traefik v3.7.6** (major v2→v3 from the pet), cluster-
    autoscaler chart `9.58.0`/image `v1.36.0` (matched to k8s 1.36), temporal-
    worker-controller `0.27.0`/app `1.8.0`, metrics-server `3.13.1`,
    k8s-dns-node-cache `1.26.8`. Every value reconciled against each chart's
    latest schema + `helm template`-validated before apply (workflow
    `wf_051453b3-71a`); notable value-schema fixes: cert-manager `installCRDs`→
    `crds.enabled`/`keep`; Traefik `logs.general.level`→`log.level`
    (serviceAccountAnnotations stays top-level); flux2 key is
    `imageReflectionController` (not …Reflector…).
  - **Tofu structure:** `helm`/`kubernetes`/`kubectl`(alekc) providers, aliased
    per color (`*.blue`), exec `aws eks get-token` auth. `modules/flux`:
    `helm_release flux2` + `kubernetes_secret` git-auth + `GitRepository` + root
    `Kustomization` via `kubectl_manifest` (`server_side_apply` → clean Flux
    co-ownership, no drift; no plan-time CRD dry-run). Root objects carry no Helm
    owner so Flux ADOPTS the byte-identical committed `source.yaml`/
    `kustomizations.yaml` — mirrors the pet's hand-bootstrap. **Providers can't be
    `for_each`'d** like `module.cluster`, so **green at P6 = a second provider-
    alias block (providers.tf) + a second `module` block (flux.tf)** — the one
    place adding a color is not a pure `var.colors` edit.
  - **Live blue Flux facts** (read from `tofu output flux` / the cluster; ELB
    rotates on rebuild — do not hardcode): GitRepository `cluster-gitops-config`
    (HTTPS, branch `feat/full-e2e-iac`), git-auth Secret `cluster-gitops-config-auth`;
    Traefik Classic ELB `a65d2de6…-…​.us-west-1.elb.amazonaws.com` (ports 80/443).
  - **CROSS-PHASE flags the next phases MUST honor:**
    - **Traefik v3 CRD group** changed `traefik.containo.us`→`traefik.io`. Porting
      instruqt-litellm + the operator-generated `IngressRoute`/`Middleware` (P5)
      MUST use `traefik.io/v1alpha1` + v3 matcher syntax, or v3 silently ignores
      them.
    - **Flux image CRDs are `image.toolkit.fluxcd.io/v1`-only** in 2.9. The pet's
      `instruqt-litellm/install/image-automation.yaml` uses `v1beta2` → rewrite to
      `v1` when porting. (All other repo Flux apiVersions are fine.)
    - **worker-controller `certmanager.install=false`** on blue (pet had `true`);
      `install: true` deploys a whole *second* cert-manager subchart. Blue relies
      on its own cert-manager addon, so worker-controller `dependsOn: cert-manager`.
    - **node-local-dns** DaemonSet is Ready but only actually caches if the kubelet
      `--cluster-dns` is the link-local `169.254.20.10` (a P1 launch-template
      concern) — verify the cache is truly in-path on blue (not a gate blocker).
    - **LE prod rate limits:** do NOT pre-warm blue against hostnames the pet
      already serves; use `letsencrypt-staging` or distinct hosts until cutover.
  - **Deferred to later phases (NOT wired in blue yet):** instruqt-litellm (app
    layer), registry layer (CRDs/operator/projects/instruqt-courses; operator runs
    scaled-to-zero until P6), and the `registry-operator` IRSA role (P6).
  - **P4 pre-decision (user asked):** to test blue behind its ELB before the DNS
    swing, add a per-color test record independent of the wildcard
    (e.g. `blue.tmprl-demo.cloud` → blue ELB) + a Traefik `IngressRoute` matching
    that host; cert-manager DNS-01 can pre-mint its cert with zero inbound traffic.
    For demo hostnames use `curl --connect-to <host>:443:<blue-elb>:443`. Raw ELB
    name alone 404s (Traefik is Host-routed) — confirmed live.
  - **GOTCHA — local aws CLI default region is us-west-2.** Bare `aws` calls
    silently hit the wrong region (a Secrets Manager lookup 404s as
    ResourceNotFound). Always pass `--region us-west-1` / export `AWS_REGION`.
    (providers.tf already pins the Tofu aws provider to var.region for this reason.)
    Also: this env's `kubectl` rejects flags before the subcommand — use the
    `KUBECONFIG` env var, not `--kubeconfig`.

- **P2 GATE PASSED — identity layer live on blue, shared substrate adopted.**
  `tofu apply`: **18 imported, 13 added, 41 changed, 0 destroyed** (every change
  tag-only). Adversarial diff review ran first (fixed: CI role needed
  `iam:Tag/UntagPolicy` + `route53:ListTagsForResource`); no other issues.
  - **Roles assumable (proven on blue via throwaway IRSA pods, now cleaned up):**
    all five fresh roles assumed their exact `assumed-role/tmprl-dem-cld-<role>`
    identity and their scoped perms worked — external-secrets → `describe-secret`
    on `…/platform/temporal-cloud-bootstrap`; cert-manager → Route53
    `list-resource-record-sets` on the zone; cluster-autoscaler →
    `describe-auto-scaling-groups`; instruqt-litellm-broker → `dynamodb:GetItem/PutItem`
    `allowed` (iam simulate); traefik → assume only (trust-only by design).
  - **Cert validated:** `data.aws_acm_certificate.wildcard` resolved to the ISSUED
    `…certificate/a6daea0d-6517-4f54-9f34-d82165f22949`.
  - **Live P2 ARNs** (from `tofu output`): CI role
    `arn:aws:iam::429214323166:role/tmprl-dem-cld-ci-tofu` (durable cluster admin on
    blue); IRSA roles `tmprl-dem-cld-{external-secrets,cert-manager,cluster-autoscaler,
    traefik,instruqt-litellm-broker}`. Read current values from `tofu output
    irsa_role_arns` / `ci_tofu_role_arn` — do not hardcode.
  - **Adopted in place (import + prevent_destroy, +default_tags only):** boundary
    policy, CodeBuild project + role (+2 inline policies), 10 `…/platform/*` secret
    containers (values NOT managed), 3 operator ECR repos. Pet untouched throughout.
  - **P3 MUST DO:** the fresh IRSA role names differ from the pet's, so the new
    `clusters/<color>/` overlays must annotate the NEW names — external-secrets SA
    → `tmprl-dem-cld-external-secrets` (pet used `tmprl-demo-cloud-external-secrets`),
    cert-manager → `tmprl-dem-cld-cert-manager`, cluster-autoscaler →
    `tmprl-dem-cld-cluster-autoscaler` (+ set `autoDiscovery.clusterName` /
    `awsRegion` for blue), traefik → `tmprl-dem-cld-traefik`, litellm-broker →
    `tmprl-dem-cld-instruqt-litellm-broker`. Also install cert-manager + the DNS-01
    `letsencrypt`/`letsencrypt-staging` ClusterIssuers into Flux (out-of-band today).
  - **DEFERRED to P6:** the `registry-operator` IRSA role (pet holds the clean name;
    blue operator scaled-to-zero until cutover). **DEFERRED (user):** secret values
    (out of band) and ECR lifecycle expiry.
  - **GOTCHA fixed — subnet tag stripping.** The `Repo` default_tags change made
    `aws_subnet` reconcile its full tag set and strip the per-color
    `kubernetes.io/cluster/<name>=shared` tags added by the separate `aws_ec2_tag`
    resources (the classic aws_subnet/aws_ec2_tag coexistence conflict) — which
    would break LB subnet discovery in P4. Fixed with `lifecycle { ignore_changes =
    [tags, tags_all] }` on both subnet resources in `modules/network` (commit
    `0053a7b`); tags restored, `tofu plan` now **clean/idempotent**. Any future
    resource that adds tags to these shared subnets must go through `aws_ec2_tag`,
    not the subnet's inline `tags`.

- **P2 STARTED — approach locked** after an 8-way live-AWS + GitOps discovery
  sweep (account 429214323166, us-west-1). Repo GitHub slug is
  **`temporal-sa/tmprl-demo-cloud-registry`** (not the local dir name). Blue is
  live now (49 tofu state resources). Decisions, all confirmed with the user:
  - **IRSA roles → greenfield-fresh, pet untouched.** Create NEW blue/green
    roles whose trust is a `union` over `var.colors` against
    `module.cluster[*].oidc_provider_arn/url` (never a literal OIDC id — it
    rotates). Leave every hand-built pet IRSA role alone (honors "never import the
    pet" + "change nothing live"). The clean `tmprl-dem-cld-*` name is FREE for
    all of these: `external-secrets`, `cert-manager`, `cluster-autoscaler`,
    `traefik` (trust-only — Traefik's Classic ELB comes from the in-tree provider,
    no AWS perms), `instruqt-litellm-broker`. Policy bodies reproduced verbatim
    from the pet roles; cert-manager Route53 scoped tighter to the one zone;
    cluster-autoscaler tag condition retargeted per color
    (`k8s.io/cluster-autoscaler/tmprl-dem-cld-<color>=owned`).
  - **`registry-operator` IRSA DEFERRED to cutover (P6).** Its clean name
    `tmprl-dem-cld-registry-operator` is held by the live pet, and blue's operator
    runs **scaled-to-zero until cutover**, so it isn't needed in P2. Minting an
    interim distinct-named role now would force a rename later. The "roles
    assumable" gate is proven by the other five roles.
  - **Shared platform substrate → in-place adoption (`tofu import`), no
    mutation.** Import with `prevent_destroy`, config matched exactly so `plan` is
    clean: CodeBuild project `tmprl-dem-cld-build` + role `tmprl-dem-cld-codebuild`
    (NO_SOURCE, `standard:7.0`, privileged, 2 inline policies), the
    `tmprl-dem-cld-serverless-worker-boundary` managed policy (v2), the 10
    `…/platform/*` secret **containers**, and the 3
    `…/registry-operator/{web,worker,temporal-cloud-proxy}` ECR repos
    (MUTABLE/AES256/scanOnPush). These are shared/color-agnostic singletons the
    operator addresses by fixed name; the operator's ECR verify branch is
    tag/policy-blind so Tofu ownership won't fight it (`registry.yaml` uses
    `finalization-policy: retain`). **One consequence:** adoption adds the provider
    `default_tags` (ManagedBy/Project/Repo) to these live resources — the only
    change; functional config untouched.
  - **DEFERRED per user (no live override now):** secret **values** (Tofu owns
    containers only; values stay out-of-band, **never** in Tofu state) and **ECR
    lifecycle expiry** (no net-new policy on the live operator repos).
  - **Data sources (never created — singletons):** ACM wildcard
    `…certificate/a6daea0d-6517-4f54-9f34-d82165f22949` (ISSUED, DNS-validated,
    InUseBy empty), Route53 zone `Z08792403SH7ADUAH7X5X`, GitHub OIDC provider
    `token.actions.githubusercontent.com` (exists since 2025-01-04).
  - **CI Tofu role (new):** `tmprl-dem-cld-ci-tofu` — GitHub OIDC, trust
    `StringLike sub` = [`repo:temporal-sa/tmprl-demo-cloud-registry:ref:refs/heads/main`,
    `…:ref:refs/heads/feat/full-e2e-iac`], `aud=sts.amazonaws.com`; least-priv
    apply surface incl. S3 state bucket (bucket policy needs no change — same-acct
    IAM governs). Register its ARN in `cluster_admin_principal_arns` (resolves the
    P1 churning-SSO-admin finding — blue's only human admin today is the churning
    `_a19b698aaece921e`). Also fix `providers.tf` `default_tags.Repo`
    (`cluster-gitops-config` → `temporal-sa/tmprl-demo-cloud-registry`).
  - **SAFETY — no destroy-gate in P2+.** Unlike P1, the root stack now owns
    adopted live-prod resources; `prevent_destroy` guards them and we do NOT run a
    destroy/recreate cycle while the pet is live. Verify by clean `plan` + IRSA
    assume checks instead.
  - Verbatim policy bodies captured during discovery; the committed `infra/*.tf`
    files are the durable record (workflow run `wf_2eea2aaf-f2b`).

- **P1 gate PASSED — blue is live** for P2 to build on. Full cycle run:
  `apply` (42 res) → 4 nodes Ready via emitted `kubeconfig-blue.yaml` (v1.36.2,
  AL2023, private-only) → `destroy` (42 destroyed, **zero orphans**: VPC/ENI/NAT/
  EIP/IAM/OIDC/log group all gone, KMS key → PendingDeletion, empty state) →
  `apply` again (42 res, no name collisions — the Tofu-owned log group + fresh
  OIDC/KMS alias prove clean re-create). Pet `sa-demo` stayed ACTIVE throughout
  (structurally impossible to touch — separate VPC, separate names, and it is
  not in this Tofu state). Hardening confirmed on live nodes: IMDSv2 required,
  encrypted gp3 root volumes, CMK secret envelope encryption.
  - **Live blue facts for P2** (recreated during re-apply, so different from the
    first apply): VPC `vpc-052e367ac19c5ce90`, cluster `tmprl-dem-cld-blue`
    (endpoint `https://69916084F4D99A6F859FED98F848601D.sk1.us-west-1.eks.amazonaws.com`),
    OIDC provider `oidc.eks.us-west-1.amazonaws.com/id/69916084F4D99A6F859FED98F848601D`
    (arn `arn:aws:iam::429214323166:oidc-provider/…/69916084F4D99A6F859FED98F848601D`),
    node role `tmprl-dem-cld-blue-node`. Read current values any time from
    `tofu output clusters` — do not hardcode; a destroy/re-create rotates the
    OIDC id (IRSA trust policies must consume `module.cluster[*].oidc_provider_*`
    outputs, never a literal id). The first apply's KMS key `6e78a433…` is in
    PendingDeletion (7-day window) and self-cleans; the live key is a new one.
  - **P2 first task (carried from review):** set a stable principal in
    `cluster_admin_principal_arns` (root var already wired) so kubectl admin
    survives SSO permission-set re-provisioning. Today only the bootstrap-creator
    (churning SSO role) is admin. The **CI assume-role** (see "Execution model"
    in Open items) is the intended durable entry — build it in P2 and register it
    here, no separate break-glass role needed.
  - **GOTCHA — Tofu vs SSO creds (interim; goes away with CI):** while running
    locally, `tofu` (AWS SDK) can fail to refresh SSO role creds mid-session
    (`GetRoleCredentials 403 / No valid credential sources`) even while the `aws`
    CLI still works. Bridge it: prefix tofu commands with
    `eval "$(aws configure export-credentials --format env)"` (exports the CLI's
    working temp creds into the process). Run long `apply`/`destroy` in the
    background — each captures static creds at launch and is immune to the
    interactive session expiring. Use `tofu -chdir=infra …` to avoid `cd`. Once
    Tofu runs in the GitHub Action (see "Execution model" in Open items), it
    assumes the CI role directly and this bridge is no longer needed.

- **P1 code authored + reviewed** (commits `330c484` network,
  `2641d68` cluster). Two shallow local modules: `infra/modules/network` (ONE
  shared VPC for all colors) and `infra/modules/cluster` (per-color, instantiated
  by `for_each = toset(var.colors)`, default `["blue"]`; adding `"green"` later
  is conflict-free with zero state moves — verified in review). Raw AWS
  resources, no community EKS module (keeps it readable per the coding rules).
  - **Version strategy (decision, per user):** a fresh stack is the *upgrade
    vehicle* — it always deploys the latest GA EKS (`kubernetes_version` default
    `1.36`) with addon versions auto-resolved to latest-for-that-version; you
    never in-place upgrade the pet. Upgrade = stand up a new latest-version
    color, validate, roll DNS to it.
  - **Network:** VPC `10.20.0.0/16` (distinct from pet `10.0.0.0/16` so both live
    during P7), 2 AZs (us-west-1b/1c — only ones this account exposes), per-AZ
    NAT (HA, matches pet), S3 gateway endpoint, and the k8s subnet role tags the
    pet lacked. Per-cluster `kubernetes.io/cluster/<name>=shared` tags applied by
    the cluster module via `aws_ec2_tag` (shared subnets, no ownership fight).
  - **Cluster (per color, name `tmprl-dem-cld-<color>`):** fresh IAM roles,
    `authentication_mode=API` + bootstrap-creator admin, Tofu-owned CloudWatch
    log group (clean destroy + no re-create clash), IRSA OIDC provider (issuer
    thumbprint via `data.tls_certificate`), node group matching the pet
    (t3.large, min3/desired4/max10) but AL2023 in **private** subnets, core
    addons (vpc-cni/kube-proxy/coredns) at auto-latest, ASG cluster-autoscaler
    discovery tags. Emits `infra/kubeconfig-<color>.yaml` (exec auth, gitignored).
  - **Pre-apply adversarial review** (5 dims → per-finding verify; 7 confirmed /
    9 rejected) drove hardening that is now in the code: node **launch template**
    forcing encrypted gp3 root volumes + IMDSv2 required (account has
    EBS-default-encryption OFF); **KMS envelope encryption** for Secrets
    (`enable_secrets_encryption`, default on); and root knobs
    `endpoint_public_access_cidrs`, `cluster_admin_principal_arns`,
    `single_nat_gateway`. blue/green forward-compat dimension passed clean.
  - **Known follow-up (review finding, medium):** the only durable cluster-admin
    is the SSO AdministratorAccess role, whose ARN suffix *churns* on Identity
    Center re-provisioning (the pet accreted ~54 stale entries). Set a stable
    break-glass/CI role ARN in `cluster_admin_principal_arns` in **P2** so kubectl
    admin survives re-provisioning; the root knob is already wired.
  - Gate run and passed — see the newer entry above. Note
    `endpoint_public_access_cidrs` defaults `0.0.0.0/0` for bring-up (auth still
    IAM-gated) — tighten to an operator allowlist via tfvars later.

- **P0 done** (branch `feat/full-e2e-iac`). `infra/bootstrap/` created the
  dedicated state bucket `tmprl-dem-cld-tofu-state-429214323166-us-west-1`
  (versioning, SSE-S3, public access blocked, TLS-only policy, 90d noncurrent
  expiry, `prevent_destroy`) with its own state migrated into it. Root `infra/`
  stack: aws provider pinned to us-west-1 with an `allowed_account_ids` guard,
  S3 backend (native lockfile, no DynamoDB), default_tags, and input variables
  defaulting to the live env (region, account, `name_prefix=tmprl-dem-cld`,
  domain, zone id, bootstrap secret names). `tofu init/validate/plan/apply` all
  clean — 0 AWS resources beyond the state bucket. An adversarial review pass
  drove the fresh-bootstrap runbook fix (committed `backend.tf` breaks a
  brand-new `init`; see `infra/bootstrap/README.md`) plus TLS-only/account-guard
  hardening.
- Live env confirmed for later phases: account **429214323166**, EKS `sa-demo`
  cp 1.30 in `vpc-00bb9a602ae4bad2c` (10.0.0.0/16), OIDC id `A19A2DDF…`,
  Route53 zone `Z08792403SH7ADUAH7X5X`, ACM wildcard
  `arn:aws:acm:us-west-1:429214323166:certificate/a6daea0d-6517-4f54-9f34-d82165f22949`
  (ISSUED), CodeBuild `tmprl-dem-cld-build`, roles `tmprl-dem-cld-codebuild` /
  `tmprl-dem-cld-registry-operator`, bootstrap secrets under
  `tmprl-dem-cld/platform/`. Two operator-code corrections for P5: k8s client is
  in-cluster-*first* with a `load_kube_config()` fallback (kube.py:1223), not
  strictly in-cluster; multi-cluster facts otherwise confirmed (task queue
  `registry-management` config.py:79, workflow id `demo-project/<name>`
  watch.py:662, sha256 reconcile key resources.py:601).
- Plan authored: environment inspected, decisions locked, operator
  multi-cluster problem confirmed against code, phases defined. No code yet.
</content>
</invoke>
