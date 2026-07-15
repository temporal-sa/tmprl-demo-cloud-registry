# DNS + TLS primitives the platform reuses. All pre-exist and are shared across
# colors; Tofu reads them, it does not create them (the zone is registered out of
# band, and the wildcard cert is already ISSUED/DNS-validated). The wildcard DNS
# record itself is written in P4, not here.

# Existing Route53 hosted zone for var.domain. cert-manager's DNS-01 solver and
# (later) the P4 wildcard record both target this zone id.
data "aws_route53_zone" "this" {
  zone_id = var.route53_zone_id
}

# Existing ACM wildcard cert (*.tmprl-demo.cloud). Looked up by domain rather than
# a hardcoded ARN so a cert re-issue does not break the reference; the region only
# holds one cert for this domain so the match is unambiguous. P4 attaches it; P2
# just proves it resolves (the "cert validated" gate — it is already ISSUED).
data "aws_acm_certificate" "wildcard" {
  domain      = "*.${var.domain}"
  statuses    = ["ISSUED"]
  most_recent = true
}
