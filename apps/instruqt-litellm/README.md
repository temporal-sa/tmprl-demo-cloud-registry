# Instruqt LiteLLM

This app directory is the ownership boundary for the shared Instruqt LiteLLM
gateway and its token broker:

- `broker/` contains the broker service, tests, and container build.
- `deploy/` contains the Flux-reconciled Kubernetes resources for the gateway,
  Redis, broker, ingress, and autoscaling.

The root Flux `instruqt-litellm` Kustomization points directly at `deploy/`.
GitHub Actions builds broker changes, pushes the image to ECR, and commits the
resulting digest to `deploy/broker.yaml`. Flux only reads this repository; it
does not need a Git write credential.

Stable AWS resources for this app are managed in `infra/`: RDS and runtime IAM
live with the rest of the platform definitions, while ECR, DynamoDB, and the
GitHub Actions build role are grouped in `infra/litellm_delivery.tf`.
