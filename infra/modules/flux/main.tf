# Install Flux and apply the initial root sync into one cluster. The caller
# passes the aliased helm/kubernetes/kubectl providers for the target color.
#
# This mirrors the pet's bootstrap exactly: install the flux2 controllers, then
# apply the two root-sync objects (GitRepository + flux-system Kustomization)
# with NO Helm owner, so Flux cleanly ADOPTS the byte-identical objects that
# clusters/<color>/flux-system/{source,kustomizations}.yaml define, and
# self-manages everything from there.

resource "kubernetes_namespace" "flux_system" {
  metadata {
    name = var.flux_namespace
    labels = {
      "app.kubernetes.io/managed-by" = "opentofu"
      "app.kubernetes.io/part-of"    = "flux"
    }
  }
}

# Flux controllers (source, kustomize, helm, notification, image-reflector,
# image-automation). installCRDs defaults true, so this release OWNS the Flux
# CRDs. Values enable the two image controllers explicitly — see flux2-values.yaml.
resource "helm_release" "flux" {
  name       = "flux2"
  namespace  = kubernetes_namespace.flux_system.metadata[0].name
  repository = "https://fluxcd-community.github.io/helm-charts"
  chart      = "flux2"
  version    = var.flux_chart_version

  create_namespace = false
  values           = [file("${path.module}/flux2-values.yaml")]
}

# HTTPS basic-auth for the GitRepository, sourced from the read-only PAT secret
# ({username,password} JSON) by the caller. The key never lands in git; it does
# transit Tofu state, which lives in the locked-down S3 state bucket.
resource "kubernetes_secret" "git_auth" {
  metadata {
    name      = var.git_secret_name
    namespace = kubernetes_namespace.flux_system.metadata[0].name
  }
  type = "Opaque"
  data = {
    username = var.git_username
    password = var.git_password
  }
}

# Root-sync objects. server_side_apply so Tofu owns only the fields it sets and
# Flux owns the rest (status, its own labels) without a field-manager tug — a
# re-apply stays a no-op once Flux is reconciling. depends_on the helm release so
# the source/kustomize CRDs exist before these CRs are applied (kubectl_manifest
# does no plan-time dry-run, so there is no CRD chicken/egg at plan).
resource "kubectl_manifest" "git_repository" {
  server_side_apply = true
  depends_on        = [helm_release.flux, kubernetes_secret.git_auth]

  yaml_body = yamlencode({
    apiVersion = "source.toolkit.fluxcd.io/v1"
    kind       = "GitRepository"
    metadata = {
      name      = var.git_repository_name
      namespace = var.flux_namespace
    }
    spec = {
      interval  = "1m"
      url       = var.git_url
      ref       = { branch = var.git_branch }
      secretRef = { name = var.git_secret_name }
    }
  })
}

resource "kubectl_manifest" "root_kustomization" {
  server_side_apply = true
  depends_on        = [kubectl_manifest.git_repository]

  yaml_body = yamlencode({
    apiVersion = "kustomize.toolkit.fluxcd.io/v1"
    kind       = "Kustomization"
    metadata = {
      name      = "flux-system"
      namespace = var.flux_namespace
    }
    spec = {
      interval = "1m"
      path     = var.root_kustomization_path
      prune    = true
      sourceRef = {
        kind = "GitRepository"
        name = var.git_repository_name
      }
      timeout = "5m"
    }
  })
}
