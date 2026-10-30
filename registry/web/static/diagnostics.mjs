// Standalone, opt-in diagnostic view. This file is intentionally NOT imported
// by the live catalog. Its future caller must obtain a canonical project UID
// and installation scope from reviewed trusted discovery, not a catalog name.
// Client enablement is not authorization: the server must verify the session
// and current project policy on every read. No polling, persistence or links.

export const MAX_DIAGNOSTIC_BYTES = 32 * 1024;
const ID = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
const OPERATION_ID = /^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,127}$/;
const STATES = new Set(["Pending", "Running", "Waiting", "Retrying", "Succeeded", "Failed", "Cancelled", "Finalizing", "Finalized"]);
const FRESHNESS = new Set(["Current", "Reconciling", "Stale", "Unknown"]);
const OUTCOMES = new Set(["Running", "Completed", "Failed", "Retained"]);
const DELIVERY = new Set(["Pending", "Succeeded", "Failed", "Unknown"]);
const RETRIES = new Set(["Scheduled", "Exhausted", "NotRetryable"]);
const CREATE_KINDS = new Set(["Namespace", "ConfigMap", "Deployment", "Service", "ServiceAccount"]);
const CREATE_OUTCOMES = new Set(["CanonicalReceiptRecorded", "Rejected", "OutcomeUnconfirmed"]);

function invalid() { return new Error("InvalidDiagnosticView"); }
function record(value) { return value !== null && typeof value === "object" && !Array.isArray(value); }
function integer(value) { return Number.isSafeInteger(value) && value >= 0 ? value : null; }
function exactKeys(value, keys) { return record(value) && Object.keys(value).sort().join(",") === [...keys].sort().join(","); }
function createHistory(value) {
  if (!exactKeys(value, ["scope", "groups"]) || value.scope !== "HistoricalCreateRecords" || !Array.isArray(value.groups) || !value.groups.length || value.groups.length > 15) throw invalid();
  const seen = new Set(); let total = 0;
  const groups = value.groups.map(group => {
    if (!exactKeys(group, ["kind", "outcome", "count"]) || !CREATE_KINDS.has(group.kind) || !CREATE_OUTCOMES.has(group.outcome) || !Number.isSafeInteger(group.count) || group.count <= 0 || group.count > 1024 - total) throw invalid();
    const key = `${group.kind}/${group.outcome}`;
    if (seen.has(key)) throw invalid();
    seen.add(key); total += group.count;
    return { kind: group.kind, outcome: group.outcome, count: group.count };
  });
  return { scope: "HistoricalCreateRecords", groups };
}
function target(value) {
  if (!record(value) || ![value.environment, value.installation, value.projectUID].every(v => typeof v === "string" && ID.test(v))) throw invalid();
  return Object.freeze({ environment: value.environment, installation: value.installation, projectUID: value.projectUID });
}
function sameTarget(a, b) { return a.environment === b.environment && a.installation === b.installation && a.projectUID === b.projectUID; }
function freeze(value) {
  if (value && typeof value === "object") { for (const child of Object.values(value)) freeze(child); Object.freeze(value); }
  return value;
}
function text(value, limit = 512, bounds) {
  if (typeof value !== "string") return "";
  const safe = value.replace(/[\u0000-\u001f\u007f-\u009f\u202a-\u202e\u2066-\u2069]/g, " ");
  if (safe.length <= limit) return safe;
  if (bounds) bounds.partial = true;
  return Array.from(safe).slice(0, limit - 1).join("") + "…";
}
function time(value) {
  if (typeof value !== "string" || value.length > 64 || value.startsWith("0001-01-01T00:00:00") || !/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,9})?(?:Z|[+-]\d\d:\d\d)$/.test(value)) return null;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString() : null;
}
function list(value, limit, bounds) {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value)) throw invalid();
  if (value.length > limit) bounds.partial = true;
  return value.slice(0, limit);
}
function failure(raw, bounds) {
  if (!record(raw)) return null;
  const value = { code: text(raw.code, 96, bounds) || "UnknownFailure", category: text(raw.category, 48, bounds) || "Unknown", message: text(raw.message, 1024, bounds) || "No human-readable cause was recorded.", step: text(raw.step, 128, bounds), occurredAt: time(raw.occurredAt), nextAction: text(raw.nextAction, 512, bounds), partial: raw.partial === true };
  bounds.partial ||= value.partial;
  // Execution/provider references, raw causes, resource locators and URLs are
  // deliberately not copied into this presentation model.
  return value;
}

// Accept ONLY the authorized Go schema1 read model. This is presentation, not a
// sanitizer for raw workflow/provider errors or a project authorization check.
// It copies an allowlist; opaque explanation/evidence locators never survive.
export function normalizeDiagnosticView(raw, expectedTarget, options = {}) {
  const expected = target(expectedTarget);
  let bytes;
  try { bytes = new TextEncoder().encode(JSON.stringify(raw)).length; } catch { throw invalid(); }
  if (bytes > MAX_DIAGNOSTIC_BYTES || !record(raw) || raw.schemaVersion !== 1 || !sameTarget(target(raw.target), expected) || !record(raw.source) || !["Materialized", "Archived"].includes(raw.source.origin)) throw invalid();
  const now = options.now === undefined ? Date.now() : options.now;
  const maxRouteAgeMs = options.maxRouteAgeMs === undefined ? 120000 : options.maxRouteAgeMs;
  if (!Number.isFinite(now) || !Number.isFinite(maxRouteAgeMs) || maxRouteAgeMs <= 0) throw invalid();
  const bounds = { partial: raw.partial === true };
  const source = { origin: raw.source.origin, readAt: time(raw.source.readAt), recordedAt: raw.source.persistenceTimeKnown === true ? time(raw.source.recordedAt) : null, desiredObservedAt: time(raw.source.desiredObservedAt), persistenceTimeKnown: raw.source.persistenceTimeKnown === true && time(raw.source.recordedAt) !== null };
  const freshnessRaw = record(raw.freshness) ? raw.freshness : {};
  const freshness = { state: FRESHNESS.has(freshnessRaw.state) ? freshnessRaw.state : "Unknown", observedReconcileKey: text(freshnessRaw.observedReconcileKey, 128, bounds), desiredReconcileKey: text(freshnessRaw.desiredReconcileKey, 128, bounds), observedGeneration: integer(freshnessRaw.observedGeneration), desiredGeneration: integer(freshnessRaw.desiredGeneration), lastProgressAt: time(freshnessRaw.lastProgressAt), evaluatedAt: time(freshnessRaw.evaluatedAt) };
  if (!source.persistenceTimeKnown || !source.desiredObservedAt || !freshness.evaluatedAt) freshness.state = "Unknown";
  if (freshness.state === "Current" && (!freshness.observedReconcileKey || !freshness.desiredReconcileKey || freshness.observedGeneration === null || freshness.desiredGeneration === null)) freshness.state = "Unknown";
  if (freshness.state === "Current" && (freshness.observedReconcileKey !== freshness.desiredReconcileKey || freshness.observedGeneration !== freshness.desiredGeneration)) freshness.state = "Reconciling";
  const base = { schemaVersion: 1, target: expected, source, freshness, archived: source.origin === "Archived", status: raw.status, partial: bounds.partial || !source.persistenceTimeKnown, operation: null, createHistory: null };
  if (raw.status === "NotObserved") {
    if (raw.operation !== undefined && raw.operation !== null || Object.hasOwn(raw, "createHistory")) throw invalid();
    base.freshness.state = "Unknown";
    return freeze(base);
  }
  const p = raw.operation;
  if (!record(p) || p.schemaVersion !== 1 || !STATES.has(p.state) || p.state !== raw.status || !record(p.identity) || !sameTarget(target(p.identity), expected) || ![p.identity.projectName, p.identity.operationID, p.identity.reconcileKey].every(v => typeof v === "string" && OPERATION_ID.test(v)) || integer(p.sequence) === null || integer(p.identity.generation) === null) throw invalid();
  if (Object.hasOwn(raw, "createHistory")) base.createHistory = createHistory(raw.createHistory);
  if (freshness.state === "Current" && (freshness.observedGeneration !== p.identity.generation || freshness.observedReconcileKey !== p.identity.reconcileKey)) freshness.state = "Unknown";
  const progressedAt = time(p.progressedAt);
  const retryRaw = record(p.retry) ? p.retry : {};
  const observedAt = time(retryRaw.observedAt);
  const authoritative = retryRaw.authoritative === true && observedAt !== null && progressedAt !== null && Date.parse(observedAt) <= Date.parse(progressedAt) && Date.parse(observedAt) <= now && RETRIES.has(retryRaw.status);
  const retry = { status: authoritative ? retryRaw.status : "Unknown", authoritative, observedAt: authoritative ? observedAt : null, attempt: authoritative ? integer(retryRaw.attempt) : null, maximumAttempts: authoritative ? integer(retryRaw.maximumAttempts) : null, nextAttemptAt: null };
  const next = time(retryRaw.nextAttemptAt);
  if (retry.status === "Scheduled" && next && Date.parse(next) >= Date.parse(observedAt)) retry.nextAttemptAt = next;
  const routeRaw = record(p.availability) ? p.availability : {};
  const route = { activeRevision: text(routeRaw.activeRevision, 128, bounds), candidateRevision: text(routeRaw.candidateRevision, 128, bounds), state: "Unconfirmed", confirmedAt: time(routeRaw.confirmedAt) };
  if (!base.archived && route.activeRevision && route.confirmedAt && ["ActivePreserved", "Promoted"].includes(routeRaw.routeState) && Date.parse(route.confirmedAt) <= now && now - Date.parse(route.confirmedAt) <= maxRouteAgeMs) route.state = routeRaw.routeState;
  const recovery = list(p.recovery, 8, bounds).map(value => {
    if (!record(value)) throw invalid();
    return { step: text(value.step, 128, bounds) || "Recovery", state: OUTCOMES.has(value.state) ? value.state : "Unknown", failure: failure(value.failure, bounds), retainedCount: Array.isArray(value.retainedResources) ? value.retainedResources.length : 0 };
  });
  const evidence = list(p.evidence, 8, bounds).map(value => {
    if (!record(value)) throw invalid();
    bounds.partial ||= value.partial === true;
    return { id: text(value.id, 96, bounds), kind: text(value.kind, 48, bounds), state: text(value.state, 48, bounds) || "Unknown", partial: value.partial === true };
  });
  const publication = list(p.publication, 8, bounds).map(value => {
    if (!record(value)) throw invalid();
    const seq = integer(value.sourceSequence);
    const observedAt = time(value.observedAt);
    return { sink: text(value.sink, 64, bounds) || "Status sink", state: seq !== null && seq <= p.sequence && observedAt !== null && Date.parse(observedAt) <= now && DELIVERY.has(value.state) ? value.state : "Unknown", sourceSequence: seq, behind: seq !== null && seq < p.sequence, observedAt, failure: failure(value.failure, bounds) };
  });
  base.operation = { state: p.state, step: text(p.step, 128, bounds), projectName: text(p.identity.projectName, 128, bounds), operationID: text(p.identity.operationID, 128, bounds), reconcileKey: text(p.identity.reconcileKey, 128, bounds), generation: p.identity.generation, sequence: p.sequence, progressedAt, failure: failure(p.failure, bounds), retry, route, recovery, evidence, publication, warnings: list(p.warnings, 4, bounds).map(value => failure(value, bounds)).filter(Boolean) };
  base.partial ||= bounds.partial || p.partial === true;
  return freeze(base);
}

function node(document, tag, value) {
  const element = document.createElement(tag);
  if (value !== undefined) element.textContent = String(value);
  return element;
}
function paragraph(document, parent, text) { parent.appendChild(node(document, "p", text)); }
function rows(document, parent, items) {
  const dl = node(document, "dl");
  for (const [label, value] of items) { const row = node(document, "div"); row.appendChild(node(document, "dt", label)); row.appendChild(node(document, "dd", value === null || value === "" ? "Unknown" : value)); dl.appendChild(row); }
  parent.appendChild(dl);
}
function section(document, parent, title) { const element = node(document, "section"); element.appendChild(node(document, "h4", title)); parent.appendChild(element); return element; }
function renderFailure(document, parent, value) {
  paragraph(document, parent, `${value.code}: ${value.message}`);
  if (value.step) paragraph(document, parent, `Step: ${value.step}`);
  if (value.occurredAt) paragraph(document, parent, `Cause observed: ${value.occurredAt}`);
  if (value.nextAction) paragraph(document, parent, `Next action: ${value.nextAction}`);
}

// Only textContent and DOM construction are used. No innerHTML, URL assignment,
// raw workflow errors, evidence links or retained provider-resource locators.
export function renderDiagnosticView(document, view) {
  const root = node(document, "section"); root.className = "diagnostic-panel"; root.setAttribute("aria-label", "Operator diagnostics");
  root.appendChild(node(document, "h3", "Operator diagnostics"));
  paragraph(document, root, `${view.status} · Freshness: ${view.freshness.state}`);
  if (view.archived) paragraph(document, root, "Archived operation or retired source. This record does not confirm what is serving now.");
  if (view.partial) paragraph(document, root, "Partial diagnostic record. Some detail or freshness evidence was not recorded.");
  if (view.status === "NotObserved") paragraph(document, root, "No operation status has been recorded for this authorized project.");
  const p = view.operation;
  if (p) {
    const cause = section(document, root, p.failure && ["Succeeded", "Finalized"].includes(p.state) ? "Original cause retained from an earlier failure" : "Original cause");
    if (p.failure) renderFailure(document, cause, p.failure);
    else paragraph(document, cause, p.state === "Failed" ? "The original failure cause has not been recorded." : "No original failure cause is recorded.");
    if (view.createHistory) {
      const history = section(document, root, "Historical create records");
      paragraph(document, history, "A recorded creation receipt does not confirm current existence, readiness, or serving availability.");
      const labels = { CanonicalReceiptRecorded: "Canonical creation receipt recorded", Rejected: "Create rejected", OutcomeUnconfirmed: "Create outcome unconfirmed" };
      for (const group of view.createHistory.groups) paragraph(document, history, `${group.kind}: ${labels[group.outcome]} (${group.count}).`);
      if (view.createHistory.groups.some(group => group.outcome === "OutcomeUnconfirmed")) paragraph(document, history, "An unconfirmed outcome does not establish whether a create was dispatched or applied.");
    }
    const serving = section(document, root, "Serving and candidate revisions");
    rows(document, serving, [["Active revision (last observed)", p.route.activeRevision], ["Candidate revision", p.route.candidateRevision], ["Route confirmation", p.route.state === "ActivePreserved" ? "Previous active route preserved" : p.route.state === "Promoted" ? "Promotion confirmed" : "Unconfirmed"], ["Route observed at", p.route.confirmedAt]]);
    if (p.route.state === "Unconfirmed") paragraph(document, serving, "Operation state does not establish serving availability. Verify the current route before taking action.");
    const retry = section(document, root, "Automatic retry");
    const retryText = { Scheduled: "An automatic retry was authoritatively scheduled.", Exhausted: "Automatic retries were authoritatively reported exhausted.", NotRetryable: "The authoritative retry state reports no automatic retry.", Unknown: "Automatic retry status is unknown. A retryable error alone does not prove another attempt is scheduled." }[p.retry.status];
    paragraph(document, retry, retryText);
    if (p.retry.authoritative) rows(document, retry, [["Retry observed at", p.retry.observedAt], ["Observed attempt", p.retry.attempt], ["Configured maximum", p.retry.maximumAttempts], ["Next scheduled attempt", p.retry.nextAttemptAt]]);
    const recovery = section(document, root, "Recovery and cleanup");
    if (!p.recovery.length) paragraph(document, recovery, "No recovery or cleanup outcome was recorded.");
    for (const outcome of p.recovery) { paragraph(document, recovery, `${outcome.step}: ${outcome.state}${outcome.retainedCount ? `; ${outcome.retainedCount} retained resource reference(s)` : ""}`); if (outcome.failure) renderFailure(document, recovery, outcome.failure); }
    const evidence = section(document, root, "Evidence capture");
    if (!p.evidence.length) paragraph(document, evidence, "No evidence capture result was recorded.");
    for (const item of p.evidence) paragraph(document, evidence, `${item.kind || "Evidence"} ${item.id}: ${item.state}${item.partial ? " (partial)" : ""}`);
    const publication = section(document, root, "Status publication");
    if (!p.publication.length) paragraph(document, publication, "No publication outcome was recorded.");
    for (const item of p.publication) { paragraph(document, publication, `${item.sink}: ${item.state}. Publication is independent of the original operation outcome.`); if (item.state === "Unknown") paragraph(document, publication, "Delivery acknowledgement is unknown. Verify the recorded status proposal and repair publication separately."); if (item.behind) paragraph(document, publication, `This delivery covers observation ${item.sourceSequence}; the operation has progressed to ${p.sequence}.`); if (item.observedAt) paragraph(document, publication, `Publication observed: ${item.observedAt}`); if (item.failure) renderFailure(document, publication, item.failure); }
    if (p.warnings.length) { const warnings = section(document, root, "Additional warnings"); for (const value of p.warnings) renderFailure(document, warnings, value); }
  }
  const freshness = section(document, root, "Observation and freshness");
  rows(document, freshness, [["Desired generation", view.freshness.desiredGeneration], ["Observed generation", view.freshness.observedGeneration], ["Desired reconcile", view.freshness.desiredReconcileKey], ["Observed reconcile", view.freshness.observedReconcileKey], ["Last operation progress", p ? p.progressedAt : view.freshness.lastProgressAt], ["Desired state independently observed", view.source.desiredObservedAt], ["Status persisted at", view.source.recordedAt], ["Diagnostic source read at", view.source.readAt], ["Freshness assessed at", view.freshness.evaluatedAt]]);
  if (!view.source.persistenceTimeKnown) paragraph(document, freshness, "Persistence time was not recorded. A recent read does not establish recent progress or persistence.");
  if (p) rows(document, freshness, [["Operation", p.operationID], ["Operation step", p.step], ["Operation observation", p.sequence]]);
  return root;
}

async function boundedJSON(response, signal) {
  const length = response.headers.get("content-length");
  if (length !== null && (!/^\d+$/.test(length) || Number(length) > MAX_DIAGNOSTIC_BYTES)) throw invalid();
  if (!/^application\/json(?:\s*;|$)/i.test(response.headers.get("content-type") || "") || !response.body || typeof response.body.getReader !== "function") throw invalid();
  const reader = response.body.getReader(); const chunks = []; let total = 0;
  const abort = () => { void reader.cancel().catch(() => {}); };
  signal.addEventListener("abort", abort, { once: true });
  try {
    if (signal.aborted) throw invalid();
    while (true) { const { done, value } = await reader.read(); if (done) break; if (!(value instanceof Uint8Array) || (total += value.byteLength) > MAX_DIAGNOSTIC_BYTES) throw invalid(); chunks.push(value); }
    if (signal.aborted) throw invalid();
  } catch { await reader.cancel().catch(() => {}); throw invalid(); }
  finally { signal.removeEventListener("abort", abort); reader.releaseLock(); }
  const bytes = new Uint8Array(total); let offset = 0; for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  try { return JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes)); } catch { throw invalid(); }
}

// No fetch occurs at construction, even when enabled. The trusted caller opts
// in with a canonical target and mounts this outside the live app until actual
// identity discovery/feature policy/API routing are independently integrated.
export function mountDiagnosticPanel(container, options = {}) {
  const document = options.document || container.ownerDocument;
  const binding = options.binding;
  let expected = null;
  if (binding && binding.enabled === true) expected = target(binding.target);
  const fetch = options.fetch || globalThis.fetch;
  const origin = options.origin === undefined ? globalThis.location?.origin : options.origin;
  if (expected) { try { if (typeof fetch !== "function" || typeof origin !== "string" || new URL(origin).origin !== origin || !/^https?:$/.test(new URL(origin).protocol)) throw invalid(); } catch { throw invalid(); } }
  const timers = options.timers || globalThis;
  const timeoutMs = options.timeoutMs === undefined ? 10000 : options.timeoutMs;
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0 || timeoutMs > 30000 || typeof timers.setTimeout !== "function" || typeof timers.clearTimeout !== "function") throw invalid();
  const endpoint = expected ? `/api/v2/projects/${encodeURIComponent(expected.projectUID)}/diagnostics` : null;
  let generation = 0, alive = true, pending = null, view = null, timer = null;
  function clearTimer() { if (timer !== null) timers.clearTimeout(timer); timer = null; }
  function paint(message) {
    const root = view ? renderDiagnosticView(document, view) : node(document, "section");
    if (!view) { root.setAttribute("aria-label", "Operator diagnostics"); root.appendChild(node(document, "h3", "Operator diagnostics")); paragraph(document, root, message); }
    const button = node(document, "button", view ? "Refresh diagnostics" : "Load diagnostics"); button.type = "button"; button.disabled = !expected || !alive || pending !== null; button.addEventListener("click", refresh); root.appendChild(button); container.replaceChildren(root);
  }
  function clearProtectedState() { generation++; clearTimer(); pending?.abort(); pending = null; view = null; if (alive) paint("Diagnostic access must be verified again."); }
  async function refresh() {
    if (!alive || !expected) return;
    const requestGeneration = ++generation; clearTimer(); pending?.abort(); const controller = new AbortController(); pending = controller; view = null; paint("Loading authorized diagnostics…");
    timer = timers.setTimeout(() => { if (alive && generation === requestGeneration) { generation++; timer = null; controller.abort(); pending = null; view = null; paint("The diagnostic read timed out. No current authorized view is available."); } }, timeoutMs);
    try {
      const response = await fetch(endpoint, { method: "GET", credentials: "same-origin", mode: "same-origin", cache: "no-store", redirect: "error", headers: { Accept: "application/json" }, signal: controller.signal });
      if (!alive || generation !== requestGeneration) return;
      if ([401, 403, 404].includes(response.status)) { clearTimer(); pending = null; view = null; if (response.body && typeof response.body.cancel === "function") void response.body.cancel().catch(() => {}); paint(response.status === 401 ? "Sign in again to view diagnostics." : "Diagnostics are unavailable or access is not permitted."); return; }
      if (!response.ok) throw new Error("DiagnosticServiceUnavailable");
      const responseURL = new URL(response.url);
      if (response.redirected || responseURL.origin !== origin || responseURL.pathname !== endpoint || responseURL.search || responseURL.hash || !/(?:^|,)\s*no-store\s*(?:,|$)/i.test(response.headers.get("cache-control") || "")) throw invalid();
      const raw = await boundedJSON(response, controller.signal);
      if (!alive || generation !== requestGeneration) return;
      view = normalizeDiagnosticView(raw, expected, { now: options.now ? options.now() : Date.now(), maxRouteAgeMs: options.maxRouteAgeMs }); clearTimer(); pending = null; paint();
    } catch {
      if (!alive || generation !== requestGeneration) return;
      clearTimer(); controller.abort(); pending = null; view = null; paint("The diagnostic service is unavailable or returned an invalid view. The previous protected view was cleared.");
    }
  }
  paint(expected ? "Diagnostics are available on request. Load a recorded view to inspect this project." : "Diagnostics are not enabled with a trusted project identity.");
  return Object.freeze({ refresh, clearProtectedState, destroy() { alive = false; generation++; clearTimer(); pending?.abort(); pending = null; view = null; container.replaceChildren(); } });
}
