// Separate owned-local page; the live catalog does not import this module.
// A binding enables presentation only. The existing diagnostic handler must
// independently verify the session and current project policy on every read.
import { mountDiagnosticPanel } from "./diagnostics.mjs";

export const BINDING_ENDPOINT = "/api/v2/diagnostics/binding";
export const MAX_BINDING_BYTES = 1024;
const ID = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

function invalid() { return new Error("InvalidDiagnosticBinding"); }
function record(value) { return value !== null && typeof value === "object" && !Array.isArray(value); }
function keys(value, expected) { return record(value) && Object.keys(value).sort().join(",") === [...expected].sort().join(","); }
function binding(raw) {
  if (keys(raw, ["enabled"]) && raw.enabled === false) return Object.freeze({ enabled: false });
  if (!keys(raw, ["enabled", "target"]) || raw.enabled !== true || !keys(raw.target, ["environment", "installation", "projectUID"]) || !Object.values(raw.target).every(value => typeof value === "string" && ID.test(value))) throw invalid();
  return Object.freeze({ enabled: true, target: Object.freeze({ environment: raw.target.environment, installation: raw.target.installation, projectUID: raw.target.projectUID }) });
}
function cancel(response) { if (response?.body && typeof response.body.cancel === "function") void response.body.cancel().catch(() => {}); }
async function readBinding(response, signal) {
  if (!/^application\/json(?:\s*;|$)/i.test(response.headers.get("content-type") || "")) throw invalid();
  const length = response.headers.get("content-length");
  if (length !== null && (!/^\d+$/.test(length) || Number(length) > MAX_BINDING_BYTES)) throw invalid();
  if (!response.body || typeof response.body.getReader !== "function") throw invalid();
  const reader = response.body.getReader();
  const chunks = []; let total = 0;
  const abort = () => { void reader.cancel().catch(() => {}); };
  signal.addEventListener("abort", abort, { once: true });
  try {
    if (signal.aborted) throw invalid();
    for (;;) {
      const { value, done } = await reader.read();
      if (signal.aborted) throw invalid();
      if (done) break;
      if (!(value instanceof Uint8Array) || (total += value.byteLength) > MAX_BINDING_BYTES) throw invalid();
      chunks.push(value);
    }
  } catch { await reader.cancel().catch(() => {}); throw invalid(); }
  finally { signal.removeEventListener("abort", abort); reader.releaseLock(); }
  const bytes = new Uint8Array(total); let offset = 0;
  for (const chunk of chunks) { bytes.set(chunk, offset); offset += chunk.byteLength; }
  try { return binding(JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes))); } catch { throw invalid(); }
}

// enabled=true is an explicit caller opt-in, not a permission. Construction and
// lifecycle events never fetch. The one user Load action discovers a fixed
// authorized binding, then loads the existing panel once, without polling.
export function mountDiagnosticPage(container, options = {}) {
  const document = options.document || container.ownerDocument;
  const window = options.window || document.defaultView;
  const fetch = options.fetch || globalThis.fetch;
  const origin = options.origin === undefined ? window?.location?.origin : options.origin;
  const enabled = options.enabled === true;
  const timers = options.timers || globalThis;
  const timeoutMs = options.timeoutMs === undefined ? 10000 : options.timeoutMs;
  if (typeof fetch !== "function" || typeof origin !== "string" || new URL(origin).origin !== origin || !/^https?:$/.test(new URL(origin).protocol) || !Number.isFinite(timeoutMs) || timeoutMs <= 0 || timeoutMs > 30000 || typeof timers.setTimeout !== "function" || typeof timers.clearTimeout !== "function") throw invalid();
  let alive = true, generation = 0, pending = null, timer = null, panel = null, pinnedTarget = null;
  const message = document.createElement("p"); message.setAttribute("role", "status");
  const load = document.createElement("button"); load.type = "button"; load.textContent = "Load diagnostics"; load.disabled = !enabled;
  const clear = document.createElement("button"); clear.type = "button"; clear.textContent = "Clear view";
  const host = document.createElement("div");
  container.replaceChildren(message, load, clear, host);
  function say(value) { if (alive) message.textContent = value; }
  function stopTimer() { if (timer !== null) timers.clearTimeout(timer); timer = null; }
  function clearProtectedState() {
    generation++; stopTimer(); pending?.abort(); pending = null;
    panel?.destroy(); panel = null; host.replaceChildren();
    if (alive) { load.disabled = !enabled; say(enabled ? "The view was cleared. Load again to verify access." : "Diagnostics are not enabled for this page."); }
  }
  function pageAllowed() {
    if (!window?.location?.href) return true; // explicit pure-test DOM without a browsing context
    try { const url = new URL(window.location.href); return url.origin === origin && url.pathname === "/diagnostics/" && !url.search && !url.hash; } catch { return false; }
  }
  async function refresh() {
    if (!alive || !enabled) return;
    clearProtectedState();
    if (document.hidden) return;
    if (!pageAllowed()) { say("Open the configured diagnostic page to load a view."); return; }
    const requestGeneration = ++generation;
    const controller = new AbortController(); pending = controller; load.disabled = true;
    say("Verifying diagnostic access…");
    timer = timers.setTimeout(() => {
      if (alive && generation === requestGeneration) { clearProtectedState(); say("The diagnostic binding read timed out. No authorized view is available."); }
    }, timeoutMs);
    try {
      const response = await fetch(BINDING_ENDPOINT, { method: "GET", credentials: "same-origin", mode: "same-origin", cache: "no-store", redirect: "error", headers: { Accept: "application/json" }, signal: controller.signal });
      if (!alive || generation !== requestGeneration) { cancel(response); return; }
      if ([401, 403, 404].includes(response.status)) {
        cancel(response); clearProtectedState(); say(response.status === 401 ? "Sign in again to view diagnostics." : "Diagnostics are unavailable or access is not permitted."); return;
      }
      if (!response.ok) { cancel(response); throw invalid(); }
      const url = new URL(response.url);
      if (response.redirected || url.origin !== origin || url.pathname !== BINDING_ENDPOINT || url.search || url.hash || !/(?:^|,)\s*no-store\s*(?:,|$)/i.test(response.headers.get("cache-control") || "")) { cancel(response); throw invalid(); }
      const trustedBinding = await readBinding(response, controller.signal);
      if (!alive || generation !== requestGeneration) return;
      stopTimer(); pending = null; load.disabled = false;
      if (!trustedBinding.enabled) { say("Diagnostics are not enabled for this page."); return; }
      // Clearing a view does not authorize switching the mounted page to a
      // different canonical project or installation. Replacement is explicit:
      // destroy this controller and construct another from trusted discovery.
      if (pinnedTarget && ["environment", "installation", "projectUID"].some(key => pinnedTarget[key] !== trustedBinding.target[key])) throw invalid();
      pinnedTarget ||= trustedBinding.target;
      say("");
      const mounted = mountDiagnosticPanel(host, { document, fetch, origin, timers, timeoutMs, binding: trustedBinding, now: options.now, maxRouteAgeMs: options.maxRouteAgeMs });
      panel = mounted;
      await mounted.refresh();
    } catch {
      if (!alive || generation !== requestGeneration) return;
      clearProtectedState(); say("The diagnostic service is unavailable or returned an invalid binding. The protected view was cleared.");
    }
  }
  const registrations = [];
  function listen(target, name, callback) {
    if (target && typeof target.addEventListener === "function") { target.addEventListener(name, callback); registrations.push([target, name, callback]); }
  }
  listen(load, "click", refresh);
  listen(clear, "click", clearProtectedState);
  listen(document, "visibilitychange", () => { if (document.hidden) clearProtectedState(); });
  for (const name of ["pagehide", "pageshow", "popstate", "hashchange", "diagnostic-session-cleared"]) listen(window, name, clearProtectedState);
  say(enabled ? "Load a recorded diagnostic view. Access is verified for each read." : "Diagnostics are not enabled for this page.");
  return Object.freeze({ refresh, clearProtectedState, destroy() {
    clearProtectedState(); alive = false;
    for (const [target, name, callback] of registrations) target.removeEventListener(name, callback);
    container.replaceChildren();
  } });
}

// The fixed standalone HTML is the only automatic mount. Importing this module
// elsewhere does not enable the live catalog or discover a project by name.
if (typeof globalThis.document !== "undefined") {
  const container = globalThis.document.querySelector("[data-diagnostic-page]");
  if (container) mountDiagnosticPage(container, { enabled: true });
}
