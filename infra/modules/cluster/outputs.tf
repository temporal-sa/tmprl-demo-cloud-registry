output "cluster_name" {
  description = "EKS cluster name."
  value       = aws_eks_cluster.this.name
}

output "cluster_endpoint" {
  description = "Kubernetes API server endpoint."
  value       = aws_eks_cluster.this.endpoint
}

output "cluster_ca" {
  description = "Base64 cluster CA data (for kubeconfig certificate-authority-data)."
  value       = aws_eks_cluster.this.certificate_authority[0].data
}

output "cluster_version" {
  description = "EKS control-plane version actually running."
  value       = aws_eks_cluster.this.version
}

output "cluster_security_group_id" {
  description = "EKS-managed cluster security group id."
  value       = aws_eks_cluster.this.vpc_config[0].cluster_security_group_id
}

output "oidc_provider_arn" {
  description = "IRSA OIDC provider ARN (for P2 IRSA role trust policies)."
  value       = aws_iam_openid_connect_provider.this.arn
}

output "oidc_provider_url" {
  description = "IRSA OIDC provider URL, without scheme (for IRSA sub/aud conditions)."
  value       = replace(aws_eks_cluster.this.identity[0].oidc[0].issuer, "https://", "")
}

output "node_role_arn" {
  description = "Managed node group IAM role ARN."
  value       = aws_iam_role.node.arn
}
