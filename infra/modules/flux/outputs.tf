output "namespace" {
  description = "Namespace Flux is installed into."
  value       = kubernetes_namespace.flux_system.metadata[0].name
}

output "git_repository_name" {
  description = "Name of the root GitRepository object Flux adopts from git."
  value       = var.git_repository_name
}
