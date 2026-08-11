"""External operator-canary project templates.

Each ``*.yaml`` file in this package is a self-contained canary definition:
``{name, authMode, demoProjectTemplate}``. The ``RegistryOperatorCanaryWorkflow``
loads them all (via the ``load_canary_templates`` activity) and launches one branch
per template in parallel. Add a new canary "type" (e.g. worker-versioning,
serverless, or Nexus) by dropping a new YAML file here — no workflow/registry.yaml
changes.
"""
