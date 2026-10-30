import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

// No real DOM, timers, credentials, server or network is used. A forgotten fetch
// injection fails instead of reaching the machine's network/session context.
let ambientRequests = 0;
globalThis.fetch = async () => { ambientRequests++; throw new Error("NetworkForbiddenInPureTest"); };
const { normalizeDiagnosticView, renderDiagnosticView, mountDiagnosticPanel, MAX_DIAGNOSTIC_BYTES } = await import("../static/diagnostics.mjs");
const now = Date.parse("2026-10-30T12:00:00Z");
const target = { environment: "lab", installation: "installation-one", projectUID: "canonical-uid-one" };
const origin = "https://catalog.example.invalid";
const endpoint = "/api/v2/projects/canonical-uid-one/diagnostics";

function fixture() {
  return {
    schemaVersion: 1, target: { ...target }, status: "Failed", partial: false,
    source: { origin: "Materialized", recordedAt: "2026-10-30T11:58:00Z", desiredObservedAt: "2026-10-30T11:59:30Z", readAt: "2026-10-30T12:00:00Z", persistenceTimeKnown: true },
    freshness: { state: "Current", observedReconcileKey: "revision-two", desiredReconcileKey: "revision-two", observedGeneration: 2, desiredGeneration: 2, lastProgressAt: "2026-10-30T11:59:00Z", evaluatedAt: "2026-10-30T12:00:00Z" },
    explanation: "DO_NOT_COPY_RAW_EXPLANATION s3://private-workflow-artifact",
    operation: {
      schemaVersion: 1, identity: { ...target, projectName: "demo", operationID: "operation-two", reconcileKey: "revision-two", generation: 2 }, sequence: 4, state: "Failed", step: "smoke", progressedAt: "2026-10-30T11:59:00Z",
      failure: { code: "SmokeFailed", category: "Smoke", message: "Candidate smoke check failed", step: "smoke", occurredAt: "2026-10-30T11:58:30Z", nextAction: "Inspect the candidate configuration.", resources: [{ id: "DO_NOT_COPY_PRIVATE_RESOURCE" }], execution: { workflowID: "DO_NOT_COPY_PRIVATE_WORKFLOW" } },
      retry: { status: "Exhausted", authoritative: true, observedAt: "2026-10-30T11:59:00Z", attempt: 3, maximumAttempts: 3 },
      availability: { activeRevision: "active-one", candidateRevision: "candidate-two", routeState: "ActivePreserved", confirmedAt: "2026-10-30T11:59:00Z" },
      recovery: [{ step: "cleanup", state: "Retained", retainedResources: [{ id: "PRIVATE_RETAINED_LOCATOR" }], failure: { code: "CleanupFailed", message: "Candidate was retained after cleanup failed" } }],
      evidence: [{ id: "capture-one", kind: "PodLogs", state: "Failed", locator: "s3://PRIVATE_BUCKET/key?token=PRIVATE_TOKEN", partial: true }],
      publication: [{ sink: "catalog", state: "Failed", sourceSequence: 3, observedAt: "2026-10-30T11:59:45Z", failure: { code: "PublishFailed", message: "Status delivery failed" } }],
      warnings: [{ code: "EvidenceUnavailable", message: "Evidence capture did not complete" }],
    },
  };
}

function historicalFixture() {
  return { ...fixture(), createHistory: { scope: "HistoricalCreateRecords", groups: [
    { kind: "Namespace", outcome: "CanonicalReceiptRecorded", count: 1 },
    { kind: "ConfigMap", outcome: "Rejected", count: 2 },
    { kind: "Deployment", outcome: "OutcomeUnconfirmed", count: 2 },
    { kind: "Service", outcome: "CanonicalReceiptRecorded", count: 1 },
    { kind: "ServiceAccount", outcome: "CanonicalReceiptRecorded", count: 1 },
  ] } };
}

class Element {
  constructor(document, tag) { this.ownerDocument = document; this.tagName = tag; this.children = []; this.attributes = {}; this.listeners = {}; this.value = ""; }
  set textContent(value) { this.value = String(value); this.children = []; }
  get textContent() { return [this.value, ...this.children.map(child => child.textContent)].join(" "); }
  set innerHTML(_) { throw new Error("HTMLInterpolationForbidden"); }
  setAttribute(key, value) { assert.ok(!/^(href|src|style|on)/i.test(key), "no locators/HTML attributes"); this.attributes[key] = String(value); }
  appendChild(child) { assert.ok(child instanceof Element); this.children.push(child); return child; }
  replaceChildren(...children) { this.children = []; this.value = ""; for (const child of children) this.appendChild(child); }
  addEventListener(key, callback) { this.listeners[key] = callback; }
}
function dom() { const document = { createElement(tag) { assert.ok(!["script", "iframe", "img", "a", "form"].includes(tag)); return new Element(document, tag); } }; return { document, container: document.createElement("div") }; }
function fakeTimers() {
  let next = 0; const jobs = new Map();
  return { jobs, setTimeout(fn, ms) { const id = ++next; jobs.set(id, { fn, ms }); return id; }, clearTimeout(id) { jobs.delete(id); }, fire() { const work = [...jobs.values()]; jobs.clear(); for (const item of work) item.fn(); } };
}
function response(raw = fixture(), options = {}) {
  const headers = new Headers({ "content-type": "application/json; charset=utf-8", "cache-control": "no-store, private", Vary: "Authorization, Cookie", ...options.headers });
  const body = options.body || new ReadableStream({ start(controller) { controller.enqueue(new TextEncoder().encode(typeof raw === "string" ? raw : JSON.stringify(raw))); controller.close(); } });
  const status = options.status || 200;
  return { status, ok: status >= 200 && status < 300, url: options.url || origin + endpoint, redirected: options.redirected || false, headers, body };
}
function mounted(fetch, options = {}) {
  const { document, container } = dom(); const timers = fakeTimers();
  const panel = mountDiagnosticPanel(container, { document, fetch, timers, origin, binding: { enabled: true, target }, now: () => now, ...options });
  return { panel, container, timers };
}
function deferred() { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; }

test("actual Go Model.Read JSON fixture preserves the cross-language wire contract", () => {
  // scripts/verify_diagnostic_ui_contract.py checks this public synthetic file
  // against the current Go model. It is not a real session or provider proof.
  const raw = JSON.parse(readFileSync(new URL("./fixtures/go-diagnostic-view-v1.json", import.meta.url), "utf8"));
  const view = normalizeDiagnosticView(raw, target, { now });
  assert.equal(view.status, "Failed"); assert.equal(view.freshness.state, "Current");
  assert.equal(view.operation.failure.code, "SmokeFailed"); assert.equal(view.operation.recovery[0].failure.code, "CleanupFailed");
  assert.equal(view.operation.publication[0].failure.code, "PublishFailed"); assert.equal(view.operation.publication[0].behind, true);
  assert.equal(view.operation.route.state, "ActivePreserved"); assert.equal(view.operation.retry.status, "Exhausted");
  assert.equal(view.source.recordedAt, "2026-10-30T11:58:00.000Z"); assert.equal(view.operation.progressedAt, "2026-10-30T11:59:00.000Z");
  assert.doesNotMatch(JSON.stringify(view), /PRIVATE_LOCATOR|locator/);
  assert.deepEqual(view.createHistory, { scope: "HistoricalCreateRecords", groups: [
    { kind: "Namespace", outcome: "CanonicalReceiptRecorded", count: 1 },
    { kind: "ConfigMap", outcome: "Rejected", count: 1 },
    { kind: "Deployment", outcome: "OutcomeUnconfirmed", count: 1 },
  ] });
  const { document } = dom(); const rendered = renderDiagnosticView(document, view).textContent;
  assert.match(rendered, /Historical create records/);
  assert.match(rendered, /Namespace: Canonical creation receipt recorded \(1\)/);
  assert.match(rendered, /ConfigMap: Create rejected \(1\)/);
  assert.match(rendered, /Deployment: Create outcome unconfirmed \(1\)/);
});

test("import and disabled/unbound panels perform zero requests or timers", async () => {
  for (const binding of [undefined, { enabled: false, target }, { enabled: "true", target }]) {
    const { container, document } = dom(); const timers = fakeTimers(); let calls = 0;
    const panel = mountDiagnosticPanel(container, { document, binding, timers, fetch: async () => { calls++; throw new Error("unexpected"); } });
    await panel.refresh(); assert.equal(calls, 0); assert.equal(timers.jobs.size, 0); assert.match(container.textContent, /not enabled/);
    panel.destroy(); assert.equal(container.children.length, 0);
  }
  assert.equal(ambientRequests, 0);
});

test("cause, cleanup, evidence and publication remain separate with private references dropped", () => {
  const input = historicalFixture(); const view = normalizeDiagnosticView(input, target, { now }); const { document } = dom(); const rendered = renderDiagnosticView(document, view);
  const text = rendered.textContent;
  assert.match(text, /Original cause/); assert.match(text, /SmokeFailed: Candidate smoke check failed/);
  assert.match(text, /Recovery and cleanup/); assert.match(text, /CleanupFailed/); assert.match(text, /Evidence capture/); assert.match(text, /PodLogs capture-one: Failed \(partial\)/);
  assert.match(text, /Status publication/); assert.match(text, /Publication is independent/); assert.match(text, /observation 3; the operation has progressed to 4/);
  assert.match(text, /Previous active route preserved/); assert.match(text, /active-one/); assert.match(text, /candidate-two/); assert.equal(view.partial, true);
  assert.doesNotMatch(JSON.stringify(view), /PRIVATE|s3:|DO_NOT_COPY/); assert.doesNotMatch(text, /PRIVATE|s3:|DO_NOT_COPY/);
  for (const kind of ["Namespace", "ConfigMap", "Deployment", "Service", "ServiceAccount"]) assert.ok(text.includes(`${kind}:`));
  assert.match(text, /A recorded creation receipt does not confirm current existence, readiness, or serving availability/);
  assert.match(text, /An unconfirmed outcome does not establish whether a create was dispatched or applied/);
  const legacy = normalizeDiagnosticView(fixture(), target, { now });
  assert.equal(legacy.createHistory, null); assert.doesNotMatch(renderDiagnosticView(document, legacy).textContent, /Historical create records/);
  const archived = historicalFixture(); archived.source.origin = "Archived";
  const archivedView = normalizeDiagnosticView(archived, target, { now });
  assert.deepEqual(archivedView.createHistory, view.createHistory); assert.equal(archivedView.operation.route.state, "Unconfirmed");
  input.createHistory.groups[0].count = 999; assert.equal(view.createHistory.groups[0].count, 1);
  assert.throws(() => { view.createHistory.groups.push({ kind: "Pod" }); }, TypeError);
  const uncertain = fixture(); uncertain.operation.publication[0].state = "Unknown";
  const uncertainty = normalizeDiagnosticView(uncertain, target, { now });
  assert.equal(uncertainty.operation.publication[0].state, "Unknown");
  const uncertaintyText = renderDiagnosticView(document, uncertainty).textContent;
  assert.match(uncertaintyText, /Delivery acknowledgement is unknown/);
  assert.match(uncertaintyText, /repair publication separately/);
  assert.equal(uncertainty.operation.state, view.operation.state);
  assert.deepEqual(uncertainty.operation.failure, view.operation.failure);
  assert.equal(uncertainty.operation.progressedAt, view.operation.progressedAt);
  for (const change of [p => { p.state = "PRIVATE_IMPOSSIBLE_STATE"; }, p => { p.observedAt = "2099-01-01T00:00:00Z"; }, p => { p.observedAt = null; }]) {
    const invalid = fixture(); change(invalid.operation.publication[0]);
    const normalized = normalizeDiagnosticView(invalid, target, { now });
    assert.equal(normalized.operation.publication[0].state, "Unknown");
    assert.doesNotMatch(renderDiagnosticView(document, normalized).textContent, /PRIVATE_IMPOSSIBLE_STATE/);
  }
  input.operation.failure.message = "changed after normalization"; assert.equal(view.operation.failure.message, "Candidate smoke check failed");
  assert.throws(() => { view.operation.failure.message = "alias"; }, TypeError);
});

test("schema1 current, reconciling, stale, unknown and not-observed contracts", () => {
  for (const state of ["Current", "Reconciling", "Stale", "Unknown"]) { const raw = fixture(); raw.freshness.state = state; assert.equal(normalizeDiagnosticView(raw, target, { now }).freshness.state, state); }
  const mismatched = fixture(); mismatched.freshness.desiredGeneration = 3; mismatched.freshness.desiredReconcileKey = "revision-three"; assert.equal(normalizeDiagnosticView(mismatched, target, { now }).freshness.state, "Reconciling");
  const raw = fixture(); raw.source.persistenceTimeKnown = false; const view = normalizeDiagnosticView(raw, target, { now }); assert.equal(view.freshness.state, "Unknown"); assert.equal(view.source.recordedAt, null);
  const { document } = dom(); assert.match(renderDiagnosticView(document, view).textContent, /recent read does not establish recent progress or persistence/);
  const empty = fixture(); empty.status = "NotObserved"; delete empty.operation; const unobserved = normalizeDiagnosticView(empty, target, { now }); assert.equal(unobserved.operation, null); assert.equal(unobserved.freshness.state, "Unknown"); assert.match(renderDiagnosticView(document, unobserved).textContent, /No operation status has been recorded/);
});

test("archived, stale or future route evidence never confirms current serving", () => {
  for (const change of [raw => { raw.source.origin = "Archived"; }, raw => { raw.operation.availability.confirmedAt = "2026-10-30T11:00:00Z"; }, raw => { raw.operation.availability.confirmedAt = "2026-10-30T12:01:00Z"; }, raw => { raw.operation.availability.confirmedAt = "0001-01-01T00:00:00Z"; }]) {
    const raw = fixture(); change(raw); const view = normalizeDiagnosticView(raw, target, { now }); assert.equal(view.operation.route.state, "Unconfirmed"); const { document } = dom(); assert.match(renderDiagnosticView(document, view).textContent, /Operation state does not establish serving availability/);
    if (view.archived) assert.match(renderDiagnosticView(document, view).textContent, /Archived operation or retired source/);
  }
  const succeeded = fixture(); succeeded.status = succeeded.operation.state = "Succeeded"; const { document } = dom(); assert.match(renderDiagnosticView(document, normalizeDiagnosticView(succeeded, target, { now })).textContent, /Original cause retained from an earlier failure/);
});

test("retry certainty requires authoritative observation; category/configuration never invent schedule", () => {
  for (const change of [raw => { raw.operation.retry.authoritative = false; }, raw => { delete raw.operation.retry.observedAt; }, raw => { raw.operation.retry.observedAt = "2026-10-30T12:01:00Z"; }, raw => { raw.operation.retry.status = "Retryable"; }]) {
    const raw = fixture(); raw.operation.failure.category = "Transient"; change(raw); const view = normalizeDiagnosticView(raw, target, { now }); assert.equal(view.operation.retry.status, "Unknown"); assert.equal(view.operation.retry.authoritative, false);
  }
  const raw = fixture(); raw.operation.retry.status = "Scheduled"; raw.operation.retry.nextAttemptAt = "2026-10-30T12:03:00Z"; const view = normalizeDiagnosticView(raw, target, { now }); assert.equal(view.operation.retry.status, "Scheduled"); assert.equal(view.operation.retry.nextAttemptAt, "2026-10-30T12:03:00.000Z");
});

test("timestamps retain independent observation meanings", () => {
  const view = normalizeDiagnosticView(fixture(), target, { now }); const { document } = dom(); const rendered = renderDiagnosticView(document, view).textContent;
  assert.equal(view.source.readAt, "2026-10-30T12:00:00.000Z"); assert.equal(view.source.recordedAt, "2026-10-30T11:58:00.000Z"); assert.equal(view.operation.progressedAt, "2026-10-30T11:59:00.000Z"); assert.equal(view.source.desiredObservedAt, "2026-10-30T11:59:30.000Z");
  for (const label of ["Last operation progress", "Desired state independently observed", "Status persisted at", "Diagnostic source read at", "Freshness assessed at", "Observed generation", "Desired generation"]) assert.ok(rendered.includes(label));
});

test("scope/version/type/size mismatch fails closed and no project-name UID substitution exists", () => {
  for (const change of [raw => { raw.schemaVersion = 2; }, raw => { raw.target.projectUID = "demo"; }, raw => { raw.operation.identity.installation = "different"; }, raw => { raw.operation.sequence = Number.MAX_SAFE_INTEGER + 1; }, raw => { raw.status = "Succeeded"; }, raw => { raw.operation.identity.operationID = ""; }, raw => { raw.source.origin = "Unknown"; }, raw => { raw.explanation = "x".repeat(MAX_DIAGNOSTIC_BYTES); }]) { const raw = fixture(); change(raw); assert.throws(() => normalizeDiagnosticView(raw, target, { now }), /InvalidDiagnosticView/); }
  assert.throws(() => normalizeDiagnosticView(fixture(), { ...target, projectUID: "../escape" }, { now }), /InvalidDiagnosticView/);
  const valid = historicalFixture().createHistory;
  const changedGroup = change => ({ ...valid, groups: [{ ...valid.groups[0], ...change }] });
  const invalidHistory = [null, [], {}, { ...valid, scope: "CurrentHealth" }, { ...valid, resourceUID: "PRIVATE_UID" }, { ...valid, groups: [] }, changedGroup({ kind: "Pod" }), changedGroup({ outcome: "AppliedAndReady" }), changedGroup({ count: 0 }), changedGroup({ count: -1 }), changedGroup({ count: 1.5 }), changedGroup({ count: "1" }), changedGroup({ count: true }), changedGroup({ count: 1025 }), changedGroup({ count: Number.MAX_SAFE_INTEGER + 1 }), changedGroup({ locator: "PRIVATE_URL" }), { ...valid, groups: [valid.groups[0], valid.groups[0]] }, { ...valid, groups: [{ ...valid.groups[0], count: 1024 }, valid.groups[1]] }];
  for (const createHistory of invalidHistory) assert.throws(() => normalizeDiagnosticView({ ...fixture(), createHistory }, target, { now }), /InvalidDiagnosticView/);
  const all = { scope: "HistoricalCreateRecords", groups: ["Namespace", "ConfigMap", "Deployment", "Service", "ServiceAccount"].flatMap(kind => ["CanonicalReceiptRecorded", "Rejected", "OutcomeUnconfirmed"].map(outcome => ({ kind, outcome, count: 1 }))) };
  assert.equal(normalizeDiagnosticView({ ...fixture(), createHistory: all }, target, { now }).createHistory.groups.length, 15);
  assert.throws(() => normalizeDiagnosticView({ ...fixture(), createHistory: { ...all, groups: [...all.groups, all.groups[0]] } }, target, { now }), /InvalidDiagnosticView/);
  assert.equal(normalizeDiagnosticView({ ...fixture(), createHistory: changedGroup({ count: 1024 }) }, target, { now }).createHistory.groups[0].count, 1024);
  const unobserved = historicalFixture(); unobserved.status = "NotObserved"; delete unobserved.operation;
  assert.throws(() => normalizeDiagnosticView(unobserved, target, { now }), /InvalidDiagnosticView/);
});

test("untrusted text renders literally with no HTML or locator attributes", () => {
  const raw = fixture(); const attack = '<img src=x onerror="globalThis.owned=true"><script>alert(1)</script>';
  raw.operation.failure.message = attack; raw.operation.failure.nextAction = attack; raw.operation.recovery[0].step = attack;
  const { document } = dom(); const rendered = renderDiagnosticView(document, normalizeDiagnosticView(raw, target, { now })); assert.ok(rendered.textContent.includes(attack));
  function walk(element) { assert.ok(!["img", "script", "iframe", "a"].includes(element.tagName)); for (const child of element.children) walk(child); } walk(rendered);
  assert.equal(globalThis.owned, undefined);
});

test("manual client uses fixed same-origin endpoint, safe fetch options and no polling", async () => {
  const calls = []; const { panel, container, timers } = mounted(async (path, options) => { calls.push({ path, options }); return response(); });
  assert.equal(calls.length, 0); assert.equal(timers.jobs.size, 0); await panel.refresh(); assert.equal(calls.length, 1);
  const { path, options } = calls[0]; assert.equal(path, endpoint); assert.equal(options.credentials, "same-origin"); assert.equal(options.mode, "same-origin"); assert.equal(options.method, "GET"); assert.equal(options.cache, "no-store"); assert.equal(options.redirect, "error"); assert.equal(options.headers.Accept, "application/json");
  assert.match(container.textContent, /SmokeFailed/); assert.equal(timers.jobs.size, 0); panel.destroy();
});

test("authorization failures clear old protected state and do not claim project existence", async () => {
  for (const status of [401, 403, 404]) {
    let calls = 0; const { panel, container } = mounted(async () => ++calls === 1 ? response(historicalFixture()) : response({ error: "PRIVATE_ERROR_BODY" }, { status }));
    await panel.refresh(); assert.match(container.textContent, /SmokeFailed/); assert.match(container.textContent, /Historical create records/); await panel.refresh(); assert.doesNotMatch(container.textContent, /SmokeFailed|Candidate smoke|PRIVATE_ERROR_BODY|Historical create records|Canonical creation receipt/);
    assert.match(container.textContent, status === 401 ? /Sign in again/ : /unavailable or access is not permitted/); panel.destroy();
  }
});

test("late success cannot restore protected state after a newer denial or explicit clear", async () => {
  const old = deferred(); let calls = 0; const { panel, container } = mounted(async () => ++calls === 1 ? old.promise : response({}, { status: 401 }));
  const first = panel.refresh(); await panel.refresh(); old.resolve(response(historicalFixture())); await first; assert.match(container.textContent, /Sign in again/); assert.doesNotMatch(container.textContent, /SmokeFailed|Historical create records/);
  const pending = deferred(); const next = mounted(async () => pending.promise); const load = next.panel.refresh(); next.panel.clearProtectedState(); pending.resolve(response(historicalFixture())); await load; assert.doesNotMatch(next.container.textContent, /SmokeFailed|Historical create records/); assert.match(next.container.textContent, /must be verified again/); next.panel.destroy(); panel.destroy();
});

test("cross-origin/redirect/cacheable/non-JSON/cross-scope/unavailable sources fail without raw error rendering", async () => {
  const wrong = fixture(); wrong.target.projectUID = "foreign";
  for (const res of [response({}, { status: 503 }), response(fixture(), { url: "https://foreign.invalid" + endpoint }), response(fixture(), { redirected: true }), response(fixture(), { headers: { "cache-control": "public, max-age=60" } }), response(fixture(), { headers: { "content-type": "text/html" } }), response(wrong)]) {
    const { panel, container } = mounted(async () => res); await panel.refresh(); assert.doesNotMatch(container.textContent, /SmokeFailed|PRIVATE|foreign.invalid/); assert.match(container.textContent, /unavailable or returned an invalid view/); panel.destroy();
  }
});

test("body size is bounded before decoding; announced overflow and malformed UTF-8 are refused", async () => {
  let cancelled = false;
  const oversized = new ReadableStream({ start(controller) { controller.enqueue(new Uint8Array(MAX_DIAGNOSTIC_BYTES + 1)); }, cancel() { cancelled = true; } });
  for (const res of [response(fixture(), { body: oversized }), response(fixture(), { headers: { "content-length": String(MAX_DIAGNOSTIC_BYTES + 1) } }), response(fixture(), { body: new ReadableStream({ start(controller) { controller.enqueue(new Uint8Array([0xff, 0xfe])); controller.close(); } }) })]) {
    const { panel, container } = mounted(async () => res); await panel.refresh(); assert.doesNotMatch(container.textContent, /SmokeFailed/); assert.match(container.textContent, /invalid view/); panel.destroy();
  }
  assert.equal(cancelled, true);
});

test("manual read deadline aborts and clears a stalled body; destruction prevents late rendering", async () => {
  const stream = new ReadableStream({ start(controller) { controller.enqueue(new TextEncoder().encode("{")); } });
  const { panel, container, timers } = mounted(async () => response({}, { body: stream })); const pending = panel.refresh(); await Promise.resolve(); await Promise.resolve(); assert.equal(timers.jobs.size, 1); timers.fire(); await pending; assert.match(container.textContent, /timed out/); assert.doesNotMatch(container.textContent, /SmokeFailed/); panel.destroy();
  const request = deferred(); const second = mounted(async () => request.promise); const loading = second.panel.refresh(); second.panel.destroy(); request.resolve(response()); await loading; assert.equal(second.container.children.length, 0); assert.equal(second.timers.jobs.size, 0);
});

test("untrusted config cannot select an arbitrary diagnostic endpoint or leak invalid origin text", () => {
  const { document, container } = dom();
  for (const options of [{ binding: { enabled: true, target: { ...target, projectUID: "../other" } }, origin }, { binding: { enabled: true, target }, origin: "not-a-url-PRIVATE_TOKEN" }, { binding: { enabled: true, target }, origin: origin + "/other" }]) assert.throws(() => mountDiagnosticPanel(container, { document, ...options }), error => error.message === "InvalidDiagnosticView");
  assert.equal(ambientRequests, 0);
});
