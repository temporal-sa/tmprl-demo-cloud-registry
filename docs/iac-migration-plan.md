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
| Cutover mechanism | **Declarative** — a Tofu `active_color` variable repoints the wildcard record. No imperative scripts. |
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
- **P4 — Routing + DNS.** Tofu reads the Traefik LB hostname back and writes the
  wildcard record. Decide classic ELB vs NLB-via-annotation here. *Gate: DNS
  resolves to the cluster; a demo is reachable.*
- **P5 — Operator cluster dimension.** Implement the color dimension above.
  *Gate: two clusters, no activity cross-routing, green reconciles its demos
  independently of blue.*
- **P6 — Blue/green cutover.** `active_color` variable swings the wildcard.
  *Gate: stand up green, flip, verify, roll back.*
- **P7 — Migrate + retire the pet.** Point the new stack at real `projects/`,
  validate demos, swing DNS off the old `sa-demo` ELB, decommission it.

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
- **cert-manager challenge type.** Promotion issues a Let's Encrypt cert per
  route. If the issuer uses **HTTP-01**, a standby green cluster cannot obtain
  certs before the DNS swing (challenge needs traffic), which breaks pre-warming.
  Verify the issuer; if HTTP-01, move to **DNS-01 (Route53)** so green can get
  valid TLS before cutover. Resolve during P3/P4.
- **Classic ELB vs NLB.** The in-tree classic ELB works but is deprecated.
  Prefer letting k8s create an **NLB via service annotations** and having Tofu
  data-source its hostname, avoiding the need to install the AWS LB Controller.
  Decide in P4.
- **Node group sizing.** Match current capacity (4 nodes) as a starting point;
  cluster-autoscaler tuning carries over from the existing addon.

## Phase status

| Phase | Status |
| --- | --- |
| P0 — Foundations | ✅ Done |
| P1 — Network + cluster | ✅ Done (gate passed: nodes Ready + clean destroy + clean re-create) |
| P2 — Identity + AWS platform | Not started |
| P3 — Flux bootstrap | Not started |
| P4 — Routing + DNS | Not started |
| P5 — Operator cluster dimension | Not started |
| P6 — Blue/green cutover | Not started |
| P7 — Migrate + retire the pet | Not started |

## Progress log

Newest first. One entry per meaningful step so a fresh session can resume.

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
