# Routing + DNS (P4). Tofu owns exactly three kinds of record here:
#
#   live.<domain>    ALIAS A -> the ACTIVE color's Traefik ELB. The single swing
#                    point: flipping var.active_color repoints every demo at once.
#                    An ELB alias inherits the ELB's ~60s DNS TTL, so cutover is
#                    fast without a TTL knob.
#   *.<domain>       CNAME -> live.<domain>. Every host without its own record
#                    lands on the active color's Traefik, which routes by Host and
#                    serves the wildcard cert (even on a 404). There are no
#                    per-demo DNS records: new demos and crashable environments
#                    route as soon as their IngressRoute exists.
#   <color>.<domain> ALIAS A -> that color's own ELB. Stable per-color names for
#                    pre-cutover validation (curl/browser) and DNS-01 cert warmup.
#
# The legacy pet cluster (sa-demo) used to own the wildcard. Its remaining hosts
# are pinned with explicit CNAMEs to the pet's ELB, created by hand and NOT
# managed here, so retiring the pet means deleting those records, not this file.
# The hosted zone is capped at 50 records, so keep per-host records rare.
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

# A green cutover sibling adds its own data.kubernetes_service.traefik_green (via the
# green kubernetes provider) + a green entry in the map below — but only in Apply 2,
# AFTER green's cluster + Flux + Traefik ELB exist. Reading it before green's Traefik
# Service is provisioned aborts with "services \"traefik\" not found" (the two-apply
# ordering the header documents). P6 validated this, then removed it before destroying
# green (this data source + the green map entry come out BEFORE the cluster).

locals {
  # color -> that color's Traefik ELB DNS name. One entry per color.
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

# The catch-all. Adopted from the pre-existing (formerly pet-owned) record.
import {
  to = aws_route53_record.wildcard
  id = "Z08792403SH7ADUAH7X5X_*.tmprl-demo.cloud_CNAME"
}

resource "aws_route53_record" "wildcard" {
  zone_id = data.aws_route53_zone.this.zone_id
  name    = "*.${var.domain}"
  type    = "CNAME"
  ttl     = 300
  records = [aws_route53_record.live.fqdn]
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
