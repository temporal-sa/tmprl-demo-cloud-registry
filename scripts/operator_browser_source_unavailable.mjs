// Separate owned source-connector outage proof. Import performs no probes,
// driver loading, stdin reads or browser launch. Runtime failures are finite.
import assert from "node:assert/strict";
import { closeSync, mkdirSync, openSync, readFileSync, readSync, statSync, writeFileSync } from "node:fs";
import { connect } from "node:net";
import { networkInterfaces } from "node:os";
import { spawnSync } from "node:child_process";
import { createInterface } from "node:readline";
import { pathToFileURL } from "node:url";
import { verifyInternalSandbox, verifyRendererKernel } from "./operator_browser_assertions.mjs";
import { CHILD_FAILURE_CODE, pageBootstrap } from "./operator_browser_page.mjs";

export const SOURCE_CASES = Object.freeze(["native-network", "chromium-sandbox", "initial-authorized-view", "source-disconnected", "unavailable-cleared", "evidence-and-no-repost", "browser-closed"]);
function invalid() { throw new Error("BrowserSourceProtocolInvalid"); }
function exact(value, keys) { return value !== null && typeof value === "object" && !Array.isArray(value) && Object.keys(value).sort().join(",") === [...keys].sort().join(","); }

export function sourcePhaseRecord(value, phase) {
  const disconnected = ["phase", "sourceReads", "proxyClosed", "evidenceRetained"];
  const finished = [...disconnected, "managedPosts", "writerExited", "serverClosed"];
  if (phase !== "source-disconnected" && phase !== "finished" || !exact(value, phase === "finished" ? finished : disconnected) || value.phase !== phase || !Number.isSafeInteger(value.sourceReads) || value.sourceReads !== (phase === "finished" ? 2 : 1) || value.proxyClosed !== true || value.evidenceRetained !== true) invalid();
  if (phase === "finished" && (value.managedPosts !== 1 || value.writerExited !== true || value.serverClosed !== true)) invalid();
  return Object.freeze({ ...value });
}

// This NEW process/container proves its own native boundary before importing
// the fixed driver. No PID/kernel evidence is borrowed from another execution.
async function nativeBoundary(input) {
  assert.equal(process.platform, "linux"); assert.equal(process.getuid(), 1001); assert.equal(process.getgid(), 1001);
  assert.ok(process.getgroups().every(group => group === 1001));
  const status = readFileSync("/proc/self/status", "utf8");
  for (const key of ["CapEff", "CapPrm", "CapBnd"]) assert.match(status, new RegExp(`^${key}:\\s+0+$`, "m"));
  assert.match(status, /^NoNewPrivs:\s+1$/m); assert.match(status, /^Seccomp:\s+2$/m);
  const mounts = readFileSync("/proc/self/mountinfo", "utf8").trim().split("\n").map(row => row.split(" "));
  assert.ok(mounts.find(row => row[4] === "/")[5].split(",").includes("ro"));
  for (const path of ["/tmp", "/dev/shm"]) for (const flag of ["rw", "noexec", "nosuid", "nodev"]) assert.ok(mounts.find(row => row[4] === path)[5].split(",").includes(flag));
  const addresses = [];
  for (const entries of Object.values(networkInterfaces())) for (const address of entries) if (!address.internal) addresses.push(address.address);
  assert.deepEqual(addresses, [input.browserIP]);
  for (const row of readFileSync("/proc/net/route", "utf8").trim().split("\n").slice(1)) assert.notEqual(row.trim().split(/\s+/)[1], "00000000");
  for (const key of Object.keys(process.env)) assert.ok(["PATH", "LANG", "LC_ALL", "PLAYWRIGHT_BROWSERS_PATH", "HOME", "TMPDIR", "XDG_CACHE_HOME", "HOSTNAME"].includes(key));
  assert.equal(process.env.HOME, "/tmp/home"); assert.equal(process.env.TMPDIR, "/tmp"); assert.equal(process.env.XDG_CACHE_HOME, "/tmp/cache");
  for (const home of ["/root", "/home/pwuser", "/tmp/home"]) for (const path of [".aws/credentials", ".kube/config", ".docker/config.json", ".netrc", ".npmrc", ".ssh/id_rsa", ".ssh/id_ed25519", ".ssh/authorized_keys"]) assert.throws(() => statSync(home + "/" + path), error => ["ENOENT", "EACCES"].includes(error.code));
  for (const path of ["/connection.json", "/var/run/docker.sock", "/run/docker.sock", "/run/secrets/kubernetes.io/serviceaccount/token"]) assert.throws(() => statSync(path), error => error.code === "ENOENT");
  assert.throws(() => writeFileSync("/reviewed/denied-probe", "synthetic", { flag: "wx" }), error => ["EROFS", "EACCES"].includes(error.code));
  mkdirSync("/tmp/home"); mkdirSync("/tmp/cache"); writeFileSync("/tmp/owned-probe", "synthetic", { flag: "wx" });
  const chroot = spawnSync("/usr/sbin/chroot", ["/", "/usr/bin/true"], { env: { ...process.env, LANG: "C", LC_ALL: "C" }, encoding: "utf8", timeout: 3000 });
  assert.equal(chroot.error, undefined); assert.equal(chroot.signal, null); assert.equal(chroot.status, 125); assert.equal(chroot.stdout, "");
  assert.equal(chroot.stderr, "/usr/sbin/chroot: cannot change root directory to '/': Operation not permitted\n");
  async function denied(host, port) {
    await new Promise((resolve, reject) => {
      const socket = connect({ host, port }); socket.setTimeout(1500);
      socket.once("connect", () => { socket.destroy(); reject(new Error("NativeNetworkReachable")); });
      socket.once("timeout", () => { socket.destroy(); reject(new Error("NativeNetworkNotKernelDenied")); });
      socket.once("error", error => { socket.destroy(); ["ENETUNREACH", "EHOSTUNREACH", "EAFNOSUPPORT"].includes(error.code) ? resolve() : reject(new Error("NativeNetworkWrongFailure")); });
    });
  }
  for (const [host, port] of [[input.nodeIP, 6443], [input.hostGateway, 9], ["192.0.2.1", 443], ["2001:db8::1", 443]]) await denied(host, port);
  const descriptor = openSync("/proc/sys/kernel/cap_last_cap", "r");
  let cap;
  try {
    const bytes = Buffer.alloc(32); const count = readSync(descriptor, bytes, 0, bytes.length, 0);
    assert.ok(count > 0 && count < bytes.length); const value = bytes.subarray(0, count).toString("ascii");
    assert.match(value, /^[0-9]{1,2}\n$/); cap = Number(value.trim()); assert.ok(Number.isSafeInteger(cap) && cap >= 0 && cap <= 63);
  } finally { closeSync(descriptor); }
  return { status, uidMap: readFileSync("/proc/self/uid_map", "utf8"), cap };
}

async function verifyLiveRenderers(session, outer) {
  const renderers = (await session.send("SystemInfo.getProcessInfo")).processInfo.filter(value => value.type === "renderer");
  assert.ok(renderers.length >= 1 && renderers.length <= 16);
  for (const process of renderers) {
    assert.ok(Number.isSafeInteger(process.id) && process.id > 0);
    verifyRendererKernel(readFileSync(`/proc/${process.id}/status`, "utf8"), readFileSync(`/proc/${process.id}/uid_map`, "utf8"), outer.status, outer.uidMap, process.id, outer.cap);
  }
}

async function main() {
  let step = "bootstrap", browser, failed = false;
  const lines = createInterface({ input: process.stdin, crlfDelay: Infinity })[Symbol.asyncIterator]();
  async function receive() {
    const next = await lines.next(); if (next.done || typeof next.value !== "string" || next.value.length > 8192) invalid();
    try { return JSON.parse(next.value); } catch { invalid(); }
  }
  const passed = name => console.log("PASS browser-source " + name);
  try {
    const input = pageBootstrap(await receive());
    step = "native-network"; const outer = await nativeBoundary(input); passed(step);
    step = "chromium-sandbox";
    assert.equal(JSON.parse(readFileSync("/reviewed/package/package.json", "utf8")).version, "1.56.0");
    const { chromium } = await import("/reviewed/package/index.mjs");
    browser = await chromium.launch({ executablePath: "/ms-playwright/chromium-1194/chrome-linux/chrome", headless: true, chromiumSandbox: true, timeout: 20000, env: process.env });
    assert.equal(browser.version(), "141.0.7390.37");
    const probe = await browser.newPage(); await probe.goto("chrome://sandbox/", { timeout: 10000 });
    const rows = await probe.locator("tr").evaluateAll(elements => elements.map(element => [...element.querySelectorAll("td")].map(cell => cell.textContent.trim())).filter(value => value.length === 2));
    const report = (await probe.locator("body").innerText()).split("\n").map(row => row.trim()).filter(row => row === "You are adequately sandboxed.");
    assert.equal(report.length, 1); verifyInternalSandbox(rows, report[0]);
    const session = await browser.newBrowserCDPSession(); await verifyLiveRenderers(session, outer); await probe.close(); passed(step);

    const context = await browser.newContext({ viewport: { width: 1100, height: 900 }, serviceWorkers: "block" });
    await context.addCookies([{ name: "temporal_demo_auth", value: input.cookie, url: input.origin, httpOnly: true, secure: false, sameSite: "Strict" }]);
    const endpoint = `/api/v2/projects/${input.target.projectUID}/diagnostics`;
    const binding = "/api/v2/diagnostics/binding";
    const allowed = new Set(["/diagnostics/", "/diagnostics/diagnostic-page.mjs", "/diagnostics/diagnostic-page.css", "/diagnostics/diagnostics.mjs", binding, endpoint]);
    const counts = new Map(); let unexpected = 0;
    await context.route("**/*", route => {
      const url = new URL(route.request().url());
      if (url.origin !== input.origin || !allowed.has(url.pathname) || url.search || url.hash || route.request().method() !== "GET") { unexpected++; return route.abort(); }
      counts.set(url.pathname, (counts.get(url.pathname) || 0) + 1); return route.continue();
    });
    const page = await context.newPage(); page.setDefaultTimeout(10000);
    await page.goto(input.origin + "/diagnostics/", { waitUntil: "networkidle", timeout: 15000 });
    assert.equal(counts.get(binding) || 0, 0); assert.equal(counts.get(endpoint) || 0, 0);
    function loadResponse(path) { return page.waitForResponse(response => response.url() === input.origin + path && response.request().method() === "GET", { timeout: 15000 }); }
    async function manualLoad() {
      const responses = Promise.all([loadResponse(binding), loadResponse(endpoint)]);
      // If the click itself fails, later waiter rejection must not become an
      // unhandled Playwright exception containing private URL/context details.
      void responses.catch(() => {});
      await page.getByRole("button", { name: "Load diagnostics", exact: true }).click();
      return responses;
    }
    step = "initial-authorized-view";
    let [bound, diagnostics] = await manualLoad();
    assert.equal(bound.status(), 200); assert.equal(diagnostics.status(), 200);
    assert.deepEqual(await bound.json(), { enabled: true, target: input.target });
    for (const response of [bound, diagnostics]) assert.match(response.headers()["cache-control"], /(?:^|,)\s*no-store\s*(?:,|$)/i);
    await page.getByRole("heading", { name: "Original cause", exact: true }).waitFor();
    const text = await page.locator("main").innerText();
    assert.ok(text.includes(`${CHILD_FAILURE_CODE}:`) && text.includes("The Kubernetes API denied access."));
    assert.ok(!text.includes(input.cookie) && !text.includes("SYNTHETIC_PRIVATE"));
    assert.equal(counts.get(binding), 1); assert.equal(counts.get(endpoint), 1); assert.equal(unexpected, 0);
    await verifyLiveRenderers(session, outer); passed(step);

    step = "source-disconnected";
    console.log('SOURCE-BROWSER-PHASE {"phase":"disconnect-source"}');
    sourcePhaseRecord(await receive(), "source-disconnected"); passed(step);
    step = "unavailable-cleared";
    [bound, diagnostics] = await manualLoad();
    assert.equal(bound.status(), 200); assert.deepEqual(await bound.json(), { enabled: true, target: input.target });
    // The real Go handler witness requires the exact DiagnosticSourceUnavailable
    // JSON before finish. The UI intentionally aborts an unsuccessful fetch;
    // reading that fetch's body here can race its cancellation in Playwright.
    assert.equal(diagnostics.status(), 503);
    for (const response of [bound, diagnostics]) assert.match(response.headers()["cache-control"], /(?:^|,)\s*no-store\s*(?:,|$)/i);
    await page.getByText("The diagnostic service is unavailable or returned an invalid view. The previous protected view was cleared.", { exact: true }).waitFor();
    assert.equal(await page.getByRole("heading", { name: "Original cause", exact: true }).count(), 0);
    const cleared = await page.locator("main").innerText();
    assert.ok(!cleared.includes(`${CHILD_FAILURE_CODE}:`) && !cleared.includes("The Kubernetes API denied access.") && !cleared.includes(input.cookie) && !cleared.includes("SYNTHETIC_PRIVATE"));
    assert.equal(counts.get(binding), 2); assert.equal(counts.get(endpoint), 2); assert.equal(unexpected, 0);
    await verifyLiveRenderers(session, outer); passed(step);
    step = "evidence-and-no-repost";
    console.log('SOURCE-BROWSER-PHASE {"phase":"finish"}');
    sourcePhaseRecord(await receive(), "finished"); passed(step);
  } catch {
    failed = true;
    console.log("FAIL browser-source " + (SOURCE_CASES.includes(step) || step === "bootstrap" ? step : "unknown") + " AssertionOrBoundaryFailed");
  } finally {
    try { if (browser) await browser.close(); } catch { failed = true; console.log("FAIL browser-source browser-closed CloseUnconfirmed"); }
  }
  if (!failed) passed("browser-closed");
  process.exitCode = failed ? 1 : 0;
  process.stdin.destroy();
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) await main();
