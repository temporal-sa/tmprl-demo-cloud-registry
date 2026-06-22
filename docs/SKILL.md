---
name: temporal-python-rules
description: >-
  Strong opinionated guidance for Python Temporal applications and reusable
  Temporal workflow design patterns. MUST be used before writing or editing any
  Python code that imports temporalio, defines or calls Temporal workflows,
  activities, workers, signals, updates, queries, activity payloads, retry
  policies, heartbeats, sagas, child workflows, entity workflows, polling,
  approvals, callbacks, rate limiting, batch workflows, or other Temporal
  application code. Use when designing, implementing, reviewing, or refactoring
  Temporal Python code or choosing language-neutral Temporal workflow patterns.
---

# Temporal Python Rules

Use this skill for Python Temporal application work where code shape,
workflow/activity boundaries, retry design, heartbeats, sagas, or reusable
Temporal workflow patterns matter.

## Required references

- Read `references/temporal-python-rules.md` before writing, refactoring, or
  reviewing any Python code that imports `temporalio` or implements Temporal
  workflows, activities, workers, signals, updates, queries, payloads, retry
  policies, heartbeats, sagas, or child workflows.
- When choosing a reusable workflow design pattern, read
  `references/pattern-catalog.md` first. Then read only the matching pattern
  family file listed there.
- After selecting a pattern, use the relevant Temporal SDK documentation or
  project-local examples for language-specific API syntax.

## Agent support

This directory is a complete skill package for Claude Code and other agents
that consume `SKILL.md` directly. Codex-specific UI metadata lives in
`agents/openai.yaml`; it must stay consistent with this file's frontmatter and
reference layout.

## Operating rules

- Apply the Python rules as strong defaults. Deviate only when the user's
  explicit project guidance or a hard SDK/runtime constraint requires it.
- Prefer the narrowest pattern reference that matches the problem. Do not load
  every pattern family by default.
- Keep workflows deterministic and orchestration-focused. Keep activities
  side-effect-focused, retryable, heartbeat-aware, and easy for a human to trace.
- If the task is review-oriented, lead with violations of these rules and the
  concrete correction.
