# Core EKS-managed addons. Versions resolve to the latest compatible with the
# control-plane version (no hardcoded pins to rot, and no repeat of the pet's
# two-minors-behind kube-proxy). All three wait for the node group: coredns is a
# Deployment that needs schedulable nodes, and letting the vpc-cni/kube-proxy
# addons adopt (OVERWRITE) the bootstrap self-managed copies after nodes join
# avoids any CNI gap during node bring-up.
locals {
  core_addons = ["vpc-cni", "kube-proxy", "coredns"]
}

data "aws_eks_addon_version" "core" {
  for_each = toset(local.core_addons)

  addon_name         = each.key
  kubernetes_version = aws_eks_cluster.this.version
  most_recent        = true
}

resource "aws_eks_addon" "core" {
  for_each = toset(local.core_addons)

  cluster_name                = aws_eks_cluster.this.name
  addon_name                  = each.key
  addon_version               = data.aws_eks_addon_version.core[each.key].version
  resolve_conflicts_on_create = "OVERWRITE"
  resolve_conflicts_on_update = "OVERWRITE"

  depends_on = [aws_eks_node_group.this]
}
