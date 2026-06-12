# Cluster GitOps Config

Shared onboarding configuration for projects deployed into Temporal demo
clusters through Flux.

Project owners add a reviewed file under `projects/`. The platform owns the
generators, Flux installation, cluster add-ons, and external resource
provisioning needed to turn that project intent into managed infrastructure.

## Goals

- Project owners do not need direct Kubernetes API access for routine deploys.
- Projects provide a containerized source repository and deployment intent.
- The platform creates and manages the Kubernetes namespace, Flux wiring,
  Traefik route, Temporal Cloud namespace/API key, ECR repository, and secret
  injection.
- Flux applies Kubernetes desired state from inside the cluster network.

## Project Contract

Each project config must specify:

- a unique Kubernetes namespace;
- a source repository in the `temporal-sa` GitHub organization;
- container build context and Dockerfile;
- runtime pods/services;
- one explicit ingress target service and port;
- a subdomain only. The platform always appends `tmprl-demo.cloud`;
- Temporal namespace intent and the environment variable names the platform
  should inject.

See [projects/temporal-agent-harness.yaml](projects/temporal-agent-harness.yaml)
and [schemas/project.schema.json](schemas/project.schema.json).

## Generated Assets

The platform provisioner is expected to generate or maintain:

- Kubernetes `Namespace`;
- namespace-scoped Flux `Kustomization` resources;
- namespace-scoped service accounts and RBAC;
- ECR repository and build/push workflow wiring;
- Temporal Cloud namespace, service account, and API key;
- AWS Secrets Manager entries for generated sensitive values;
- External Secrets resources for Kubernetes secret sync;
- Traefik `IngressRoute` for the requested subdomain;
- Temporal worker controller resources when workers are declared.

Deletion of destructive external resources, especially Temporal namespaces,
should require an explicit reviewed removal flow rather than accidental deletion
from a routine edit.
