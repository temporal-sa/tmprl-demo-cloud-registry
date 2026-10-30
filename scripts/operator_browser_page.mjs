// Finite owned HTTP page proof only. No CLI origin, command or auth override.
// Runtime errors MUST NOT print thrown objects, DOM, cookies, pins or headers.
import assert from "node:assert/strict";
import { closeSync, mkdirSync, openSync, readFileSync, readSync, statSync, writeFileSync } from "node:fs";
import { connect, isIP } from "node:net";
import { networkInterfaces } from "node:os";
import { spawnSync } from "node:child_process";
import { createInterface } from "node:readline";
import { pathToFileURL } from "node:url";
import { verifyInternalSandbox, verifyRendererKernel } from "./operator_browser_assertions.mjs";

export const PAGE_CASES = Object.freeze(["native-network", "chromium-sandbox", "static-no-read", "session-denial", "cause-and-warning", "clear-view", "logout-and-scope", "policy-revocation", "no-repost-and-privacy", "browser-closed"]);
export const CHILD_FAILURE_CODE = "kubernetes_child_unavailable";
const ID = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;
function invalid() { throw new Error("BrowserProtocolInvalid"); }
function exact(value, keys) { return value !== null && typeof value === "object" && !Array.isArray(value) && Object.keys(value).sort().join(",") === [...keys].sort().join(","); }
function privateIPv4(value) {
  if (typeof value !== "string" || isIP(value) !== 4) return false;
  const [a, b] = value.split(".").map(Number);
  return a === 10 || a === 172 && b >= 16 && b <= 31 || a === 192 && b === 168;
}

export function pageBootstrap(value) {
  if (!exact(value, ["phase", "run", "origin", "target", "cookie", "writerExited", "frontendIP", "browserIP", "nodeIP", "hostGateway"]) || value.phase !== "ready" || value.writerExited !== true || !/^operator-local-[a-f0-9]{12}$/.test(value.run)) invalid();
  if (![value.frontendIP, value.browserIP, value.nodeIP, value.hostGateway].every(privateIPv4) || new Set([value.frontendIP, value.browserIP, value.nodeIP, value.hostGateway]).size !== 4 || value.origin !== `http://${value.frontendIP}:8080`) invalid();
  if (!exact(value.target, ["environment", "installation", "projectUID"]) || !Object.values(value.target).every(text => typeof text === "string" && ID.test(text))) invalid();
  if (typeof value.cookie !== "string" || value.cookie.length > 4096 || !/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(value.cookie)) invalid();
  return Object.freeze({ ...value, target: Object.freeze({ ...value.target }) });
}

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
  mkdirSync("/tmp/home"); mkdirSync("/tmp/cache");
  writeFileSync("/tmp/owned-probe", "synthetic", { flag: "wx" });
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

async function main() {
  let step = "bootstrap", browser, failed = false;
  const lines = createInterface({ input: process.stdin, crlfDelay: Infinity })[Symbol.asyncIterator]();
  async function receive() {
    const next = await lines.next(); if (next.done || typeof next.value !== "string" || next.value.length > 8192) invalid();
    try { return JSON.parse(next.value); } catch { invalid(); }
  }
  const passed = name => console.log("PASS browser-page " + name);
  try {
    const input = pageBootstrap(await receive());
    step = "native-network";
    const outer = await nativeBoundary(input); passed(step);
    step = "chromium-sandbox";
    assert.equal(JSON.parse(readFileSync("/reviewed/package/package.json", "utf8")).version, "1.56.0");
    const { chromium } = await import("/reviewed/package/index.mjs");
    browser = await chromium.launch({ executablePath: "/ms-playwright/chromium-1194/chrome-linux/chrome", headless: true, chromiumSandbox: true, timeout: 20000, env: process.env });
    assert.equal(browser.version(), "141.0.7390.37");
    const probe = await browser.newPage(); await probe.goto("chrome://sandbox/", { timeout: 10000 });
    const rows = await probe.locator("tr").evaluateAll(elements => elements.map(element => [...element.querySelectorAll("td")].map(cell => cell.textContent.trim())).filter(value => value.length === 2));
    const report = (await probe.locator("body").innerText()).split("\n").map(row => row.trim()).filter(row => row === "You are adequately sandboxed.");
    assert.equal(report.length, 1); verifyInternalSandbox(rows, report[0]);
    const session = await browser.newBrowserCDPSession();
    const renderers = (await session.send("SystemInfo.getProcessInfo")).processInfo.filter(value => value.type === "renderer");
    assert.ok(renderers.length >= 1 && renderers.length <= 16);
    for (const process of renderers) verifyRendererKernel(readFileSync(`/proc/${process.id}/status`, "utf8"), readFileSync(`/proc/${process.id}/uid_map`, "utf8"), outer.status, outer.uidMap, process.id, outer.cap);
    await probe.close(); passed(step);

    const context = await browser.newContext({ viewport: { width: 1100, height: 900 }, serviceWorkers: "block" });
    const endpoint = `/api/v2/projects/${input.target.projectUID}/diagnostics`;
    const foreign = "/api/v2/projects/foreign-project/diagnostics";
    const allowed = new Set(["/diagnostics/", "/diagnostics/diagnostic-page.mjs", "/diagnostics/diagnostic-page.css", "/diagnostics/diagnostics.mjs", "/api/v2/diagnostics/binding", endpoint, foreign]);
    const counts = new Map(); let unexpected = 0;
    await context.route("**/*", route => {
      const url = new URL(route.request().url());
      if (url.origin !== input.origin || !allowed.has(url.pathname) || url.search || url.hash || route.request().method() !== "GET") { unexpected++; return route.abort(); }
      counts.set(url.pathname, (counts.get(url.pathname) || 0) + 1); return route.continue();
    });
    const page = await context.newPage(); page.setDefaultTimeout(10000);
    step = "static-no-read";
    await page.goto(input.origin + "/diagnostics/", { waitUntil: "networkidle", timeout: 15000 });
    assert.equal(counts.get("/api/v2/diagnostics/binding") || 0, 0); assert.equal(counts.get(endpoint) || 0, 0);
    assert.equal(await page.getByRole("button", { name: "Load diagnostics", exact: true }).count(), 1); passed(step);
    step = "session-denial";
    const unauth = await page.evaluate(async paths => Promise.all(paths.map(async path => { const response = await fetch(path, { credentials: "same-origin", cache: "no-store" }); return response.status; })), [endpoint, foreign]);
    assert.deepEqual(unauth, [401, 401]);
    await context.addCookies([{ name: "temporal_demo_auth", value: input.cookie, url: input.origin, httpOnly: true, secure: false, sameSite: "Strict" }]);
    assert.equal(await page.evaluate(async path => (await fetch(path, { credentials: "same-origin", cache: "no-store" })).status, foreign), 404); passed(step);
    async function loaded() {
      await page.getByRole("button", { name: "Load diagnostics", exact: true }).click();
      await page.getByRole("heading", { name: "Original cause", exact: true }).waitFor();
    }
    const causeSection = () => page.locator("section").filter({ has: page.getByRole("heading", { name: "Original cause", exact: true }) }).last();
    step = "cause-and-warning"; await loaded();
    const cause = await causeSection().innerText();
    assert.ok(cause.includes(`${CHILD_FAILURE_CODE}:`) && cause.includes("The Kubernetes API denied access.") && cause.includes("Step: runtime.provider.observe") && cause.includes("Next action:"));
    const warning = await page.locator("section").filter({ has: page.getByRole("heading", { name: "Additional warnings", exact: true }) }).last().innerText();
    assert.ok(warning.includes("The Kubernetes API reported the requested resource was not found."));
    const historyHeading = () => page.getByRole("heading", { name: "Historical create records", exact: true });
    assert.equal(await historyHeading().count(), 1);
    const history = page.locator("section").filter({ has: historyHeading() }).last();
    assert.deepEqual(await history.locator("p").allTextContents(), [
      "A recorded creation receipt does not confirm current existence, readiness, or serving availability.",
      "Namespace: Canonical creation receipt recorded (1).",
      "Service: Canonical creation receipt recorded (1).",
    ]);
    let text = await page.locator("main").innerText();
    assert.ok(text.includes("Automatic retry status is unknown.") && text.includes("Unconfirmed") && text.includes("Persistence time was not recorded.") && text.includes("Partial diagnostic record."));
    assert.ok(!text.includes(input.cookie) && !text.includes("SYNTHETIC_PRIVATE"));
    passed(step);
    step = "clear-view";
    await page.getByRole("button", { name: "Clear view", exact: true }).click();
    assert.equal(await page.getByRole("heading", { name: "Original cause", exact: true }).count(), 0); assert.equal(await historyHeading().count(), 0); await loaded(); passed(step);
    step = "logout-and-scope";
    await context.clearCookies(); await page.evaluate(() => window.dispatchEvent(new Event("diagnostic-session-cleared")));
    assert.equal(await page.getByRole("heading", { name: "Original cause", exact: true }).count(), 0);
    assert.equal(await historyHeading().count(), 0);
    await page.getByRole("button", { name: "Load diagnostics", exact: true }).click(); await page.getByText("Sign in again to view diagnostics.", { exact: true }).waitFor(); assert.equal(await historyHeading().count(), 0);
    await context.addCookies([{ name: "temporal_demo_auth", value: input.cookie, url: input.origin, httpOnly: true, secure: false, sameSite: "Strict" }]); await loaded();
    await page.evaluate(() => { window.location.hash = "scope-cleared"; });
    await page.getByText("The view was cleared. Load again to verify access.", { exact: true }).waitFor();
    assert.equal(await page.getByRole("heading", { name: "Original cause", exact: true }).count(), 0);
    assert.equal(await historyHeading().count(), 0);
    await page.goto(input.origin + "/diagnostics/", { waitUntil: "networkidle" }); await loaded(); passed(step);
    step = "policy-revocation";
    console.log('BROWSER-PHASE {"phase":"revoke"}');
    const revoked = await receive();
    assert.ok(exact(revoked, ["phase", "sourceReads"]) && revoked.phase === "revoked" && revoked.sourceReads === 4);
    await page.getByRole("button", { name: "Refresh diagnostics", exact: true }).click();
    await page.getByText("Diagnostics are unavailable or access is not permitted.", { exact: true }).waitFor();
    assert.equal(await page.getByRole("heading", { name: "Original cause", exact: true }).count(), 0); assert.equal(await historyHeading().count(), 0); passed(step);
    step = "no-repost-and-privacy";
    text = await page.locator("main").innerText();
    assert.ok(!text.includes(input.cookie) && !text.includes("SYNTHETIC_PRIVATE") && unexpected === 0);
    console.log('BROWSER-PHASE {"phase":"finish"}');
    const finished = await receive();
    assert.ok(exact(finished, ["phase", "sourceReads", "managedPosts", "writerExited", "serverClosed"]) && finished.phase === "finished" && finished.sourceReads === revoked.sourceReads && finished.managedPosts === 1 && finished.writerExited === true && finished.serverClosed === true); passed(step);
  } catch {
    failed = true;
    // All runtime diagnostics use a fixed enum step and finite code only.
    console.log("FAIL browser-page " + (PAGE_CASES.includes(step) || step === "bootstrap" ? step : "unknown") + " AssertionOrBoundaryFailed");
  } finally {
    try { if (browser) await browser.close(); } catch { failed = true; console.log("FAIL browser-page browser-closed CloseUnconfirmed"); }
  }
  if (!failed) passed("browser-closed");
  process.exitCode = failed ? 1 : 0;
  process.stdin.destroy();
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) await main();
