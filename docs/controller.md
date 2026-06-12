

  # Versioned Worker Rollouts With Temporal Worker Controller

  ## Summary

  Use Temporal Worker Controller as a normal platform capability for any DemoProject worker component that opts into versioning. Flux installs the shared controller once in namespace tmprl-dem-cld-worker-controller; the registry operator renders
  project-local Connection and WorkerDeployment resources and mirrors rollout status back to DemoProject.status.

  The operator may build its own successor images, but activation flows through Git/Flux and Worker Controller. The operator rollout is gated by a dedicated e2e canary workflow that validates create, deploy, promote, finalize, and namespace deletion
  before the new operator version becomes current.

  ## Key Changes

  - Add Flux-managed platform install for Temporal Worker Controller:
      - namespace: tmprl-dem-cld-worker-controller
      - CRDs, controller HelmRelease/Kustomization, webhook TLS/cert-manager dependency, and controller RBAC
      - Flux health checks so versioned worker projects wait for controller readiness

  - Extend DemoProject worker config:
      - worker: true keeps current simple Deployment behavior
      - worker.versioning.enabled: true renders temporal.io/v1alpha1 WorkerDeployment
      - support user-defined strategy: AllAtOnce, Manual, Progressive
      - support user-defined gate.workflowType, gate.input, or gate.inputFrom
      - support optional sunset.scaledownDelay and sunset.deleteDelay

  - For each versioned worker component, the operator renders into the project namespace:
      - Connection pointing at the project Temporal namespace
      - runtime Temporal API-key Secret/ExternalSecret as today
      - WorkerDeployment with the resolved immutable image
      - Services only when the worker component exposes ports
      - optional future WorkerResourceTemplate for HPA/PDB attachments

  - Update worker runtime contract:
      - Worker Controller injects TEMPORAL_ADDRESS, TEMPORAL_NAMESPACE, TEMPORAL_DEPLOYMENT_NAME, and TEMPORAL_WORKER_BUILD_ID
      - worker code must enable SDK Worker Versioning using those values
      - platform-derived build/version values are not user-authored env vars

  - Operator self-management:
      - model the operator worker as a normal versioned worker component once support exists
      - split the singleton Kubernetes watch loop out of versioned worker pods, or configure only one non-versioned watcher
      - let the current operator build/publish next operator images
      - let the registry DemoProject reconcile the operator WorkerDeployment target image
      - let Worker Controller gate, promote, drain, and roll back versions

  ## Operator Canary Gate

  - Add a dedicated RegistryOperatorCanaryWorkflow included in the operator worker image and referenced from the operator WorkerDeployment.rollout.gate.
  - Gate input includes a unique canary project name/prefix, timeout settings, and cleanup policy. It should not pin the expected operator image/source SHA in Git; the target version is identified by the Worker Controller-injected deployment name and build ID.
  - The canary workflow runs on the target operator version before it becomes current and validates:
      - create/apply a temporary DemoProject with a tiny known-good source/build/smoke configuration
      - wait for the entity workflow to provision external resources, build image, deploy candidate resources, pass readiness, pass smoke check, and promote
      - verify expected Kubernetes namespace and promoted workload resources exist
      - request finalization by deleting or marking the temporary DemoProject
      - wait for finalizer removal and workflow completion
      - verify the canary Kubernetes namespace is deleted
      - verify associated source-watch resources, catalog/status entries, and managed external resources are removed or reported finalized

  - The canary must always attempt cleanup on failure and return failure if cleanup cannot prove the namespace was deleted.
  - Failed gate keeps the previous operator version current; no new entity workflow traffic is routed to the target version.

  ## Rollout Semantics

  - Default recommendation for entity-heavy workers: AllAtOnce plus a gate.
  - Progressive remains available for high-volume workers where percentage routing is meaningful.
  - Gate workflows are the primary validation mechanism. Temporal Worker Controller runs gate workflows on the target version before routing traffic/current-version promotion.
  - Failed pre-promotion gates leave the previous current version untouched.
  - Post-promotion rollback is performed by reverting the WorkerDeployment image/spec to a previous known-good version through Git/Flux or an operator-controlled rollback action.

  ## Test Plan

  - Unit-test schema parsing and validation for versioned worker config, including invalid strategy/gate combinations.
  - Render tests:
      - plain worker: true still renders existing Deployment
      - worker.versioning.enabled: true renders Connection and WorkerDeployment
      - gate, rollout, sunset, image, namespace, and secret references are rendered correctly

  - Reconcile tests:
      - operator reads WorkerDeployment.status and mirrors current/target/deprecated/gate condition data into DemoProject.status
      - failed gate produces project rollout failure without promoting public app traffic

  - Canary tests:
      - canary success path creates, promotes, finalizes, and verifies namespace deletion
      - canary failure path attempts cleanup and fails closed if namespace deletion cannot be confirmed
      - canary uses unique names so repeated or concurrent rollout attempts do not collide

  - Cluster acceptance test:
      - install Worker Controller in tmprl-dem-cld-worker-controller
      - deploy a canary versioned worker project
      - verify target version registers, gate runs, current version advances, old version drains
      - verify operator canary gate blocks a bad operator image and passes a good one

  ## Assumptions

  - One Worker Controller installation per cluster, in tmprl-dem-cld-worker-controller.
  - Project-level WorkerDeployment and Connection resources are namespaced per demo.
  - Flux owns controller installation and platform dependencies; the operator does not install CRDs/controllers.
  - The operator canary project uses a small source/build fixture that is safe to create and delete repeatedly.
  - Temporal Worker Controller behavior is based on its documented WorkerDeployment, gate workflow, rollout strategy, and status-condition support.
