# LiteLLM shared Postgres (P7). One RDS instance in the Tofu-managed VPC, shared by
# every color's litellm gateway (blue + green). It is restored ONCE from a snapshot
# of the legacy hand-built RDS (temporal-instruqt-litellm, which lives in the pet's
# VPC with no path from here) so the LiteLLM master key + virtual API keys + config
# migrate intact. The LITELLM_MASTER_KEY / LITELLM_SALT_KEY stay in the shared
# Secrets Manager secret (tmprl-dem-cld/platform/instruqt-litellm/runtime) unchanged,
# so the restored rows remain decryptable; only DATABASE_URL's host is repointed.
#
# Migration runbook (live steps, run at go-time):
#   1. Snapshot the legacy DB:
#      aws rds create-db-snapshot --region us-west-1 \
#        --db-instance-identifier temporal-instruqt-litellm \
#        --db-snapshot-identifier <snap>            # wait until "available"
#   2. tofu apply -var litellm_db_snapshot_identifier=<snap>  (creates this instance)
#   3. Update the SM secret's DATABASE_URL host -> aws_db_instance.litellm endpoint
#      (user/password/dbname unchanged — preserved by the snapshot).
#   4. Re-wire clusters/blue/addons/instruqt-litellm and reconcile.
# Engine/version/username/password/db_name are inherited from the snapshot.

resource "aws_db_subnet_group" "litellm" {
  name       = "${var.name_prefix}-litellm"
  subnet_ids = module.network.private_subnet_ids
  tags       = { Name = "${var.name_prefix}-litellm" }
}

resource "aws_security_group" "litellm_db" {
  name        = "${var.name_prefix}-litellm-db"
  description = "LiteLLM Postgres - ingress 5432 from each cluster color EKS SG"
  vpc_id      = module.network.vpc_id
  tags        = { Name = "${var.name_prefix}-litellm-db" }
}

# Nodes/pods reach Postgres via each color's EKS cluster security group. for_each
# over module.cluster means green's rule is added automatically when green exists.
resource "aws_security_group_rule" "litellm_db_from_clusters" {
  for_each                 = module.cluster
  type                     = "ingress"
  from_port                = 5432
  to_port                  = 5432
  protocol                 = "tcp"
  security_group_id        = aws_security_group.litellm_db.id
  source_security_group_id = each.value.cluster_security_group_id
  description              = "litellm Postgres from ${each.key} cluster"
}

# Gated on the snapshot id so `tofu plan` stays clean until the migration runs.
resource "aws_db_instance" "litellm" {
  count = var.litellm_db_snapshot_identifier == "" ? 0 : 1

  identifier                = "${var.name_prefix}-litellm"
  snapshot_identifier       = var.litellm_db_snapshot_identifier
  instance_class            = "db.t4g.micro"
  allocated_storage         = 20
  storage_type              = "gp3"
  db_subnet_group_name      = aws_db_subnet_group.litellm.name
  vpc_security_group_ids    = [aws_security_group.litellm_db.id]
  multi_az                  = false
  publicly_accessible       = false
  deletion_protection       = true
  skip_final_snapshot       = false
  final_snapshot_identifier = "${var.name_prefix}-litellm-final"

  # This instance was seeded from a snapshot, so its encryption (storage_encrypted
  # + kms_key_id) and DB identity (engine/version/db_name/username) are inherited
  # and immutable — not declared in config above. Ignore them (and snapshot_identifier,
  # which only matters at creation) so Tofu treats it as a normal managed DB and never
  # plans a replacement on later applies.
  lifecycle {
    ignore_changes = [
      snapshot_identifier,
      storage_encrypted,
      kms_key_id,
      engine,
      engine_version,
      db_name,
      username,
    ]
  }

  tags = { Name = "${var.name_prefix}-litellm" }
}
