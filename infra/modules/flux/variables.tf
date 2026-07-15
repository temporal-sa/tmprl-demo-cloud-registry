variable "flux_namespace" {
  description = "Namespace for the Flux controllers and the root sync objects."
  type        = string
  default     = "flux-system"
}

variable "flux_chart_version" {
  description = "fluxcd-community/flux2 chart version (pin exactly)."
  type        = string
}

variable "git_url" {
  description = "HTTPS clone URL of the gitops repo for the root GitRepository."
  type        = string
}

variable "git_branch" {
  description = "Branch the root GitRepository tracks."
  type        = string
}

variable "git_repository_name" {
  description = "Name of the Flux GitRepository object. Must match the committed source.yaml so Flux adopts it."
  type        = string
  default     = "cluster-gitops-config"
}

variable "git_secret_name" {
  description = "Name of the git-auth Secret. Must match the committed source.yaml secretRef."
  type        = string
  default     = "cluster-gitops-config-auth"
}

variable "git_username" {
  description = "HTTPS basic-auth username for the gitops repo (from the read-only PAT secret)."
  type        = string
  sensitive   = true
}

variable "git_password" {
  description = "HTTPS basic-auth password/token for the gitops repo (from the read-only PAT secret)."
  type        = string
  sensitive   = true
}

variable "root_kustomization_path" {
  description = "Repo path the root flux-system Kustomization reconciles (e.g. ./clusters/blue/flux-system)."
  type        = string
}
