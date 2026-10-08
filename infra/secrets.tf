# Platform Secrets Manager CONTAINERS (P2). Adopted in place — the operator and
# external-secrets only READ these; the containers are hand-created and their
# VALUES are populated out of band (never by Tofu, so no aws_secretsmanager_secret_version
# here and no value ever enters Tofu state). Names/descriptions/tags reproduced to
# match live so the import is clean; prevent_destroy so adoption cannot delete a
# live credential. Per-project (<prefix>/<project>/*) secrets are operator-owned and
# deliberately NOT managed here.
locals {
  # key = path under <name_prefix>/platform/ ; import_arn = live suffixed ARN.
  platform_secrets = {
    "temporal-cloud-bootstrap" = {
      description = "Temporal Cloud bootstrap (Cloud Ops API) credential for the registry operator"
      tags        = {}
      import_arn  = "arn:aws:secretsmanager:us-west-1:429214323166:secret:tmprl-dem-cld/platform/temporal-cloud-bootstrap-AGsINt"
    }
    "github-temporal-sa-readonly" = {
      description = "Readonly key for temporal-sa github repo - no account or org level permissions, only has read on repositories."
      tags        = {}
      import_arn  = "arn:aws:secretsmanager:us-west-1:429214323166:secret:tmprl-dem-cld/platform/github-temporal-sa-readonly-Vsn97h"
    }
    "instruqt-cli" = {
      description = null
      tags        = {}
      import_arn  = "arn:aws:secretsmanager:us-west-1:429214323166:secret:tmprl-dem-cld/platform/instruqt-cli-56A2I0"
    }
    "registry-worker-temporal" = {
      description = "Registry operator control-plane Temporal credentials"
      tags        = { managed-by = "registry-operator-bootstrap" }
      import_arn  = "arn:aws:secretsmanager:us-west-1:429214323166:secret:tmprl-dem-cld/platform/registry-worker-temporal-W0rS5a"
    }
    "shared-auth" = {
      description = null
      tags        = {}
      import_arn  = "arn:aws:secretsmanager:us-west-1:429214323166:secret:tmprl-dem-cld/platform/shared-auth-WwIJ8S"
    }
    "temporal-cloud-mtls-ca" = {
      description = "Temporal Cloud mTLS platform client CA"
      tags        = { managed-by = "demo-registry" }
      import_arn  = "arn:aws:secretsmanager:us-west-1:429214323166:secret:tmprl-dem-cld/platform/temporal-cloud-mtls-ca-fYhw3c"
    }
    "instruqt-litellm/runtime" = {
      description = "Shared runtime secrets for Instruqt LiteLLM"
      tags        = { app = "instruqt-litellm", managed-by = "codex", cluster = "shared" }
      import_arn  = "arn:aws:secretsmanager:us-west-1:429214323166:secret:tmprl-dem-cld/platform/instruqt-litellm/runtime-DorO0z"
    }
    "instruqt-litellm/broker" = {
      description = "Shared broker secrets for Instruqt LiteLLM"
      tags        = { app = "instruqt-litellm", managed-by = "codex", cluster = "shared" }
      import_arn  = "arn:aws:secretsmanager:us-west-1:429214323166:secret:tmprl-dem-cld/platform/instruqt-litellm/broker-fvSKOg"
    }
    "instruqt-litellm/provider/anthropic" = {
      description = "Anthropic provider API key for shared Instruqt LiteLLM"
      tags        = { app = "instruqt-litellm", cluster = "shared", secret-scope = "provider", managed-by = "codex" }
      import_arn  = "arn:aws:secretsmanager:us-west-1:429214323166:secret:tmprl-dem-cld/platform/instruqt-litellm/provider/anthropic-Jupm53"
    }
    "instruqt-litellm/provider/openai" = {
      description = "OpenAI provider API key for shared Instruqt LiteLLM"
      tags        = { app = "instruqt-litellm", cluster = "shared", secret-scope = "provider", managed-by = "codex" }
      import_arn  = "arn:aws:secretsmanager:us-west-1:429214323166:secret:tmprl-dem-cld/platform/instruqt-litellm/provider/openai-Ya8q1P"
    }
    "shared/anthropic" = {
      description = "Platform-managed shared anthropic credentials for opt-in demos"
      tags        = { managed-by = "platform", secret-scope = "shared" }
      import_arn  = "arn:aws:secretsmanager:us-west-1:429214323166:secret:tmprl-dem-cld/platform/shared/anthropic-4UvLL0"
    }
    "shared/openai" = {
      description = "Platform-managed shared openai credentials for opt-in demos"
      tags        = { managed-by = "platform", secret-scope = "shared" }
      import_arn  = "arn:aws:secretsmanager:us-west-1:429214323166:secret:tmprl-dem-cld/platform/shared/openai-FoIoqU"
    }
    "shared/gemini" = {
      description = "Platform-managed shared gemini credentials for opt-in demos"
      tags        = { managed-by = "platform", secret-scope = "shared" }
      import_arn  = "arn:aws:secretsmanager:us-west-1:429214323166:secret:tmprl-dem-cld/platform/shared/gemini-fgbh4N"
    }
  }
}

resource "aws_secretsmanager_secret" "platform" {
  for_each = local.platform_secrets

  name        = "${var.name_prefix}/platform/${each.key}"
  description = each.value.description
  tags        = each.value.tags

  lifecycle {
    prevent_destroy = true
  }
}

import {
  for_each = local.platform_secrets

  to = aws_secretsmanager_secret.platform[each.key]
  id = each.value.import_arn
}
