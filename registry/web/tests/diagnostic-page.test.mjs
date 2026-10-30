import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

// Injected DOM/fetch/timers prove composition contracts, not a real browser or
// kernel network isolation. The isolated runner is responsible for that tier.
let ambientRequests = 0;
globalThis.fetch = async () => { ambientRequests++; throw new Error("NetworkForbiddenInPureTest"); };
const { mountDiagnosticPage, BINDING_ENDPOINT, MAX_BINDING_BYTES } = await import("../static/diagnostic-page.mjs");
const origin = "https://catalog.example.invalid";
const target = { environment: "lab", installation: "installation-one", projectUID: "canonical-uid-one" };
const endpoint = "/api/v2/projects/canonical-uid-one/diagnostics";
const now = Date.parse("2026-10-30T12:00:00Z");
function fixture() { return JSON.parse(readFileSync(new URL("./fixtures/go-diagnostic-view-v1.json", import.meta.url), "utf8")); }
function trustedBinding() { return { enabled: true, target: { ...target } }; }

class Events {
  constructor() { this.listeners = new Map(); }
  addEventListener(name, fn) { if (!this.listeners.has(name)) this.listeners.set(name, new Set()); this.listeners.get(name).add(fn); }
  removeEventListener(name, fn) { this.listeners.get(name)?.delete(fn); }
  emit(name) { return Promise.all([...(this.listeners.get(name) || [])].map(fn => fn({ type: name }))); }
}
class Element extends Events {
  constructor(document, tag) { super(); this.ownerDocument = document; this.tagName = tag; this.children = []; this.attributes = {}; this.value = ""; }
  set textContent(value) { this.value = String(value); this.children = []; }
  get textContent() { return [this.value, ...this.children.map(child => child.textContent)].join(" "); }
  set innerHTML(_) { throw new Error("HTMLInterpolationForbidden"); }
  setAttribute(key, value) { assert.ok(!/^(href|src|style|on)/i.test(key)); this.attributes[key] = String(value); }
  appendChild(child) { assert.ok(child instanceof Element); this.children.push(child); return child; }
  replaceChildren(...children) { this.children = []; this.value = ""; for (const child of children) this.appendChild(child); }
}
function fakeTimers() {
  let next = 0; const jobs = new Map();
  return { jobs, setTimeout(fn, ms) { const id = ++next; jobs.set(id, { fn, ms }); return id; }, clearTimeout(id) { jobs.delete(id); }, fire() { const work = [...jobs.values()]; jobs.clear(); for (const { fn } of work) fn(); } };
}
function mounted(fetch, options = {}) {
  const window = new Events(); window.location = { origin, href: origin + "/diagnostics/" };
  const document = new Events(); document.defaultView = window; document.hidden = false;
  document.createElement = tag => { assert.ok(!["script", "iframe", "img", "a", "form"].includes(tag)); return new Element(document, tag); };
  const container = document.createElement("div"); const timers = fakeTimers();
  const page = mountDiagnosticPage(container, { document, window, fetch, timers, origin, enabled: true, now: () => now, ...options });
  return { page, container, timers, document, window };
}
function response(raw, path = BINDING_ENDPOINT, options = {}) {
  const status = options.status || 200;
  const headers = new Headers({ "content-type": "application/json; charset=utf-8", "cache-control": "no-store, private", ...options.headers });
  const body = options.body || new ReadableStream({ start(controller) { controller.enqueue(new TextEncoder().encode(typeof raw === "string" ? raw : JSON.stringify(raw))); controller.close(); } });
  return { status, ok: status >= 200 && status < 300, url: options.url || origin + path, redirected: options.redirected || false, headers, body };
}
function success(path) { return path === BINDING_ENDPOINT ? response(trustedBinding()) : response(fixture(), endpoint); }
function deferred() { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; }
function textHasCause(container) { return container.textContent.includes("SmokeFailed"); }

test("standalone page import, construction and disabled mode make no requests", async () => {
  for (const enabled of [false, undefined, "true"]) {
    let calls = 0; const { page, container, timers } = mounted(async () => { calls++; throw new Error("unexpected"); }, { enabled });
    await page.refresh(); assert.equal(calls, 0); assert.equal(timers.jobs.size, 0); assert.match(container.textContent, /not enabled/);
    page.destroy(); assert.equal(container.children.length, 0);
  }
  const { page, timers } = mounted(async () => { throw new Error("construction fetched"); });
  assert.equal(timers.jobs.size, 0); page.destroy(); assert.equal(ambientRequests, 0);
});

test("one explicit load composes fixed authorized binding and real diagnostic panel", async () => {
  const calls = []; const { page, container, timers } = mounted(async (path, options) => { calls.push({ path, options }); return success(path); });
  assert.equal(calls.length, 0);
  await container.children[1].emit("click"); assert.deepEqual(calls.map(call => call.path), [BINDING_ENDPOINT, endpoint]);
  for (const { options } of calls) {
    assert.equal(options.method, "GET"); assert.equal(options.credentials, "same-origin"); assert.equal(options.mode, "same-origin"); assert.equal(options.cache, "no-store"); assert.equal(options.redirect, "error"); assert.equal(options.headers.Accept, "application/json"); assert.ok(options.signal instanceof AbortSignal);
  }
  assert.match(container.textContent, /SmokeFailed/); assert.match(container.textContent, /CleanupFailed/); assert.match(container.textContent, /PublishFailed/); assert.match(container.textContent, /Next action/);
  assert.doesNotMatch(container.textContent, /PRIVATE_LOCATOR|s3:|token=/); assert.equal(timers.jobs.size, 0);
  page.clearProtectedState(); assert.equal(textHasCause(container), false); assert.equal(calls.length, 2); page.destroy();
});

test("binding scope shape and feature flag reject substitutions before diagnostic fetch", async () => {
  for (const raw of [
    { enabled: false }, { enabled: "true", target }, { enabled: true }, { enabled: true, target: { ...target, projectUID: "../name" } },
    { enabled: true, target: { ...target, projectUID: "" } }, { enabled: true, target: { ...target, projectName: "name" } },
    { enabled: true, target, endpoint: "https://foreign.invalid/diagnostics" }, { enabled: true, target: { environment: target.environment, installation: target.installation, name: target.projectUID } },
    { enabled: true, target, schemaVersion: 99 }, { enabled: false, target },
  ]) {
    const paths = []; const { page, container } = mounted(async path => { paths.push(path); return response(raw); });
    await page.refresh(); assert.deepEqual(paths, [BINDING_ENDPOINT]); assert.equal(textHasCause(container), false); page.destroy();
  }
  const wrong = fixture(); wrong.target.projectUID = "name-is-not-uid";
  const { page, container } = mounted(async path => path === BINDING_ENDPOINT ? response(trustedBinding()) : response(wrong, endpoint));
  await page.refresh(); assert.equal(textHasCause(container), false); assert.match(container.textContent, /invalid view/); page.destroy();
});

test("discovery transport and body bounds fail closed without private error rendering", async () => {
  const cases = [
    { status: 401 }, { status: 403 }, { status: 404 }, { status: 503 }, { redirected: true },
    { url: "https://foreign.invalid" + BINDING_ENDPOINT }, { url: origin + BINDING_ENDPOINT + "?scope=foreign" },
    { url: origin + BINDING_ENDPOINT + "#private" }, { url: origin + "/api/catalog" },
    { headers: { "cache-control": "private" } }, { headers: { "content-type": "text/html" } },
    { headers: { "content-length": String(MAX_BINDING_BYTES + 1) } }, { headers: { "content-length": "invalid" } },
  ];
  for (const options of cases) {
    const raw = options.status ? { rawError: "PRIVATE_ERROR_SENTINEL" } : trustedBinding();
    let calls = 0; const { page, container, timers } = mounted(async () => { calls++; return response(raw, BINDING_ENDPOINT, options); });
    await page.refresh(); assert.equal(calls, 1); assert.equal(timers.jobs.size, 0); assert.equal(textHasCause(container), false); assert.doesNotMatch(container.textContent, /PRIVATE_ERROR_SENTINEL/); page.destroy();
  }
  for (const raw of ["{", JSON.stringify(trustedBinding()) + "{}", " ".repeat(MAX_BINDING_BYTES + 1)]) {
    const { page, container } = mounted(async () => response(raw)); await page.refresh(); assert.equal(textHasCause(container), false); assert.match(container.textContent, /invalid binding/); page.destroy();
  }
  const body = new ReadableStream({ start(controller) { controller.enqueue(Uint8Array.from([255, 255])); controller.close(); } });
  const { page, container } = mounted(async () => response("", BINDING_ENDPOINT, { body })); await page.refresh(); assert.match(container.textContent, /invalid binding/); page.destroy();
});

test("authorization failure on rediscovery or diagnostic refresh clears an earlier view", async () => {
  for (const phase of ["binding", "diagnostic"]) for (const status of [401, 403, 404]) {
    let denied = false; const paths = [];
    const { page, container } = mounted(async path => { paths.push(path); return denied && ((phase === "binding") === (path === BINDING_ENDPOINT)) ? response({}, path, { status }) : success(path); });
    await page.refresh(); assert.equal(textHasCause(container), true);
    denied = true; await page.refresh(); assert.equal(textHasCause(container), false);
    assert.equal(paths.length, phase === "binding" ? 3 : 4); page.destroy();
  }
});

test("mounted page pins first enabled canonical scope through clear and lifecycle", async () => {
  for (const field of ["environment", "installation", "projectUID"]) {
    let changed = false; const paths = [];
    const { page, container, document, window } = mounted(async path => {
      paths.push(path);
      if (path === BINDING_ENDPOINT) return response({ enabled: true, target: { ...target, ...(changed ? { [field]: "different-canonical-id" } : {}) } });
      return response(fixture(), endpoint);
    });
    await page.refresh(); assert.equal(textHasCause(container), true);
    if (field === "environment") page.clearProtectedState();
    else if (field === "installation") { document.hidden = true; await document.emit("visibilitychange"); document.hidden = false; await document.emit("visibilitychange"); }
    else { await window.emit("pagehide"); await window.emit("pageshow"); }
    changed = true; await page.refresh();
    assert.deepEqual(paths, [BINDING_ENDPOINT, endpoint, BINDING_ENDPOINT]); assert.equal(textHasCause(container), false); assert.match(container.textContent, /invalid binding/);
    changed = false; await page.refresh(); assert.equal(textHasCause(container), true); assert.equal(paths.length, 5); page.destroy();
  }
  // A newly constructed controller may accept a separately authorized binding;
  // the old controller cannot be reopened after explicit destruction.
  const replacement = { ...target, projectUID: "replacement-canonical-uid" };
  const raw = fixture(); raw.target = replacement; Object.assign(raw.operation.identity, replacement);
  const paths = []; const { page, container } = mounted(async path => { paths.push(path); return path === BINDING_ENDPOINT ? response({ enabled: true, target: replacement }) : response(raw, "/api/v2/projects/replacement-canonical-uid/diagnostics"); });
  await page.refresh(); assert.equal(textHasCause(container), true); page.destroy(); await page.refresh(); assert.equal(paths.length, 2); assert.equal(container.children.length, 0);
});

test("discovery close, replacement and timeout cannot mount a late protected view", async () => {
  for (const stop of ["clear", "destroy", "timeout"]) {
    const waiting = deferred(); let signal; let calls = 0;
    const { page, container, timers } = mounted(async (_path, options) => { signal = options.signal; calls++; return waiting.promise; });
    const pending = page.refresh();
    if (stop === "clear") page.clearProtectedState(); else if (stop === "destroy") page.destroy(); else timers.fire();
    assert.equal(signal.aborted, true); waiting.resolve(response(trustedBinding())); await pending;
    assert.equal(calls, 1); assert.equal(textHasCause(container), false); assert.equal(timers.jobs.size, 0);
    if (stop !== "destroy") page.destroy();
  }
  const waiting = deferred(); let calls = 0; const paths = [];
  const { page, container } = mounted(async path => { paths.push(path); if (++calls === 1) return waiting.promise; return success(path); });
  const old = page.refresh(); await page.refresh(); assert.equal(textHasCause(container), true);
  waiting.resolve(response({ enabled: false })); await old;
  assert.equal(textHasCause(container), true); assert.deepEqual(paths, [BINDING_ENDPOINT, BINDING_ENDPOINT, endpoint]); page.destroy();
});

test("page lifecycle clears and aborts without polling or automatic resume", async () => {
  for (const event of ["pagehide", "pageshow", "popstate", "hashchange", "diagnostic-session-cleared", "visibilitychange"]) {
    let calls = 0; const { page, container, document, window } = mounted(async path => { calls++; return success(path); });
    await page.refresh(); assert.equal(textHasCause(container), true);
    if (event === "visibilitychange") { document.hidden = true; await document.emit(event); await page.refresh(); document.hidden = false; await document.emit(event); } else await window.emit(event);
    assert.equal(textHasCause(container), false); assert.equal(calls, 2);
    await page.refresh(); assert.equal(textHasCause(container), true); assert.equal(calls, 4);
    page.destroy(); await window.emit(event); assert.equal(container.children.length, 0); assert.equal(calls, 4);
  }
  const waiting = deferred(); let signal; let calls = 0;
  const { page, container, window } = mounted(async (path, options) => { calls++; if (path === BINDING_ENDPOINT) return success(path); signal = options.signal; return waiting.promise; });
  const loading = page.refresh(); await Promise.resolve(); await Promise.resolve();
  // Wait for the injected binding's bounded stream to reach the diagnostic read.
  for (let i = 0; i < 20 && !signal; i++) await Promise.resolve();
  assert.ok(signal); await window.emit("pagehide"); assert.equal(signal.aborted, true);
  waiting.resolve(response(fixture(), endpoint)); await loading; assert.equal(calls, 2); assert.equal(textHasCause(container), false); page.destroy();
  const guard = mounted(async () => { throw new Error("wrong page fetched"); });
  for (const path of ["/", "/diagnostics/?project=foreign", "/diagnostics/#foreign"]) { guard.window.location.href = origin + path; await guard.page.refresh(); assert.match(guard.container.textContent, /configured diagnostic page/); }
  guard.page.destroy();
});

test("standalone HTML uses only the fixed module and exposes no bootstrap identity", () => {
  const html = readFileSync(new URL("../static/diagnostic-page.html", import.meta.url), "utf8");
  assert.match(html, /src="\/diagnostics\/diagnostic-page\.mjs"/); assert.match(html, /data-diagnostic-page/);
  assert.match(html, /href="\/diagnostics\/diagnostic-page\.css"/);
  assert.doesNotMatch(html, /projectUID|installation|temporal_demo_auth|fetch\(|onclick=|http-equiv|<style|<iframe|<form/);
  assert.equal((html.match(/<script\b/g) || []).length, 1); assert.equal(ambientRequests, 0);
});
