locals {
  private_subnet_ids = values(var.private_subnets)
  public_subnet_ids  = values(var.public_subnets)

  # Control plane gets ENIs across every subnet for AZ reach; nodes are private.
  control_plane_subnet_ids = concat(local.private_subnet_ids, local.public_subnet_ids)

  # Static-keyed map for per-subnet ownership tags: keys (role-az) are known at
  # plan time, ids fill in at apply, so for_each is stable.
  cluster_subnets = merge(
    { for az, id in var.private_subnets : "private-${az}" => id },
    { for az, id in var.public_subnets : "public-${az}" => id },
  )
}

# EKS auto-creates this log group on first use and never deletes it, which
# breaks a re-create after teardown. Owning it here keeps destroy clean.
resource "aws_cloudwatch_log_group" "cluster" {
  name              = "/aws/eks/${var.name}/cluster"
  retention_in_days = var.log_retention_days
}

resource "aws_eks_cluster" "this" {
  name     = var.name
  role_arn = aws_iam_role.cluster.arn
  version  = var.kubernetes_version

  enabled_cluster_log_types = var.enabled_cluster_log_types

  access_config {
    authentication_mode = "API"
    # The SSO admin running `apply` gets cluster-admin; extra principals come
    # from var.cluster_admin_principal_arns (see iam.tf).
    bootstrap_cluster_creator_admin_permissions = true
  }

  # Envelope-encrypt Kubernetes Secrets in etcd with a customer-managed CMK
  # (kms.tf). Best done at creation; gated so it can be turned off if needed.
  dynamic "encryption_config" {
    for_each = var.enable_secrets_encryption ? [1] : []
    content {
      provider {
        key_arn = aws_kms_key.secrets[0].arn
      }
      resources = ["secrets"]
    }
  }

  vpc_config {
    subnet_ids              = local.control_plane_subnet_ids
    endpoint_private_access = true
    endpoint_public_access  = true
    public_access_cidrs     = var.endpoint_public_access_cidrs
  }

  tags = {
    Color = var.color
  }

  depends_on = [
    aws_iam_role_policy_attachment.cluster,
    aws_cloudwatch_log_group.cluster,
  ]
}

# Minimal launch template so nodes get an encrypted root volume (the account has
# EBS-encryption-by-default off) and IMDSv2 is required (blocks the classic
# SSRF-steals-node-credentials path). image_id/instance type stay unset so EKS
# keeps managing the AL2023 AMI and bootstrap for var.node_ami_type.
resource "aws_launch_template" "node" {
  name_prefix = "${var.name}-node-"

  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_size = var.node_disk_size
      volume_type = "gp3"
      encrypted   = true # AWS-managed aws/ebs key: no KMS cost, clean destroy
    }
  }

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required"
    http_put_response_hop_limit = 2
  }

  tag_specifications {
    resource_type = "instance"
    tags = {
      Name  = "${var.name}-node"
      Color = var.color
    }
  }
}

resource "aws_eks_node_group" "this" {
  cluster_name    = aws_eks_cluster.this.name
  node_group_name = var.name
  node_role_arn   = aws_iam_role.node.arn
  subnet_ids      = local.private_subnet_ids
  version         = aws_eks_cluster.this.version # track the control plane; no n-1 skew

  capacity_type  = "ON_DEMAND"
  instance_types = var.node_instance_types
  ami_type       = var.node_ami_type

  launch_template {
    id      = aws_launch_template.node.id
    version = aws_launch_template.node.latest_version
  }

  scaling_config {
    min_size     = var.node_min_size
    desired_size = var.node_desired_size
    max_size     = var.node_max_size
  }

  update_config {
    max_unavailable = 1
  }

  tags = {
    Color = var.color
  }

  depends_on = [aws_iam_role_policy_attachment.node]

  lifecycle {
    # cluster-autoscaler owns the live desired count after creation.
    ignore_changes = [scaling_config[0].desired_size]
  }
}

# cluster-autoscaler auto-discovers node ASGs by these tags. Managed node groups
# do not propagate them, so tag the underlying ASG directly (P3 installs the
# autoscaler; tagging now is harmless and forward-looking).
resource "aws_autoscaling_group_tag" "autoscaler_enabled" {
  autoscaling_group_name = aws_eks_node_group.this.resources[0].autoscaling_groups[0].name

  tag {
    key                 = "k8s.io/cluster-autoscaler/enabled"
    value               = "true"
    propagate_at_launch = false
  }
}

resource "aws_autoscaling_group_tag" "autoscaler_owned" {
  autoscaling_group_name = aws_eks_node_group.this.resources[0].autoscaling_groups[0].name

  tag {
    key                 = "k8s.io/cluster-autoscaler/${var.name}"
    value               = "owned"
    propagate_at_launch = false
  }
}

# Per-cluster subnet ownership tag. Set as individual tags (not on the subnet
# resource) because the subnets are shared: blue and green each stamp their own
# kubernetes.io/cluster/<name> key without fighting over one tag block.
resource "aws_ec2_tag" "cluster_subnet" {
  for_each = local.cluster_subnets

  resource_id = each.value
  key         = "kubernetes.io/cluster/${var.name}"
  value       = "shared"
}
