# Routing + DNS (P4). Tofu owns exactly three kinds of record here:
#
#   live.<domain>    ALIAS A -> the ACTIVE color's Traefik ELB. The single swing
#                    point: managed demos CNAME `live` (via external-dns), so
#                    flipping var.active_color repoints every demo at once. An
#                    ELB alias inherits the ELB's ~60s DNS TTL, so cutover is fast
#                    without a TTL knob.
#   <color>.<domain> ALIAS A -> that color's own ELB. Stable per-color names for
#                    pre-cutover validation (curl/browser) and DNS-01 cert warmup.
#
# The pet's `*.<domain>` wildcard is NOT managed here (it is not in this state at
# all), so it can never be moved — manual pet traffic is undisturbed. Per-demo
# records are written by external-dns, never by Tofu.
#
# Providers cannot be for_each'd the way module.cluster is, so each color's ELB
# hostname is read through that color's aliased kubernetes provider explicitly.
#
# ORDERING (important): the data source below reads a LIVE Traefik Service that
# Flux reconciles inside the cluster — Tofu does not create it. So a color's DNS
# records can only be applied AFTER that color's Traefik ELB exists. That is why
# blue's DNS lands here in P4, a separate apply after P1-P3 stood up the cluster
# and Traefik. Adding green at P6 is therefore NOT a single from-nothing apply:
# apply 1 stands up green's cluster + Flux (green NOT yet in this file's data
# source/map), then once green's Traefik ELB is up, apply 2 adds green's data
# source + map entry to wire green.<domain> (and, at the flip, active_color).
# A single apply that both creates a fresh color and reads its Traefik Service
# would abort ("services \"traefik\" not found"); the same applies to a color
# destroy+rebuild. This is documented in the migration plan's P6 notes.

# Canonical hosted-zone id for Classic ELBs in this region — the alias target zone
# for every record below (all colors' Traefik LBs are Classic ELBs, P3).
data "aws_elb_hosted_zone_id" "main" {}

# Each color's live Traefik LoadBalancer Service, read from the cluster so the ELB
# hostname is never hardcoded (it rotates when a color is rebuilt).
data "kubernetes_service" "traefik_blue" {
  provider = kubernetes.blue
  metadata {
    name      = "traefik"
    namespace = "traefik"
  }
}

locals {
  # color -> that color's Traefik ELB DNS name. One entry per color; extend when
  # green is added.
  traefik_elb_hostnames = {
    blue = data.kubernetes_service.traefik_blue.status[0].load_balancer[0].ingress[0].hostname
  }
}

# The swing record. Repointed by flipping var.active_color, then `tofu apply`.
resource "aws_route53_record" "live" {
  zone_id = data.aws_route53_zone.this.zone_id
  name    = "live.${var.domain}"
  type    = "A"

  alias {
    name                   = local.traefik_elb_hostnames[var.active_color]
    zone_id                = data.aws_elb_hosted_zone_id.main.id
    evaluate_target_health = false
  }
}

# Stable per-color validation names (blue.<domain>, later green.<domain>). Always
# point at their own color's ELB regardless of which color is live.
resource "aws_route53_record" "color" {
  for_each = local.traefik_elb_hostnames

  zone_id = data.aws_route53_zone.this.zone_id
  name    = "${each.key}.${var.domain}"
  type    = "A"

  alias {
    name                   = each.value
    zone_id                = data.aws_elb_hosted_zone_id.main.id
    evaluate_target_health = false
  }
}
