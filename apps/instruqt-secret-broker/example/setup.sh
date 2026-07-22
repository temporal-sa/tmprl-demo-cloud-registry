#!/bin/bash
# ABOUTME: Minimal Instruqt track setup example for minting a LiteLLM key.
set -euo pipefail

# Instruqt injects TEMPORAL_LITELLM_BROKER_SECRET and participant metadata into
# this lifecycle. Omitting --model allows every model exposed by the gateway.
secret-broker litellm \
  --duration=1d \
  --budget=5

# Make the new values available to later commands in this setup lifecycle.
# Future root shells source this file automatically through /root/.bashrc.
# shellcheck disable=SC1091
. /root/.litellm-env
