// Execute ONLY inside the owned, inspected network=none browser container.
// Driver import/Chromium launch occur after native kernel/environment probes.
import assert from "node:assert/strict";
import { closeSync, mkdirSync, openSync, readFileSync, readSync, statSync, writeFileSync } from "node:fs";
import { networkInterfaces } from "node:os";
import { connect, isIP } from "node:net";
import { spawnSync } from "node:child_process";
import { rendererFailureMetadata, verifyInternalSandbox, verifyRendererKernel } from "./operator_browser_assertions.mjs";

assert.equal(process.platform, "linux");
assert.ok(["arm64", "x64"].includes(process.arch));
assert.equal(process.getuid(), 1001);
assert.equal(process.getgid(), 1001);
assert.ok(process.getgroups().every(value => value === 1001));
const outerStatus = readFileSync("/proc/self/status", "utf8");
const outerUIDMap = readFileSync("/proc/self/uid_map", "utf8");
const capabilityFile = openSync("/proc/sys/kernel/cap_last_cap", "r");
let lastCapability;
try {
  const bytes = Buffer.alloc(32);
  const count = readSync(capabilityFile, bytes, 0, bytes.length, 0);
  assert.ok(count > 0 && count < bytes.length);
  const value = bytes.subarray(0, count).toString("ascii");
  assert.match(value, /^[0-9]{1,2}\n$/);
  lastCapability = Number(value.trim());
  assert.ok(Number.isSafeInteger(lastCapability) && lastCapability >= 0 && lastCapability <= 63);
} finally { closeSync(capabilityFile); }
for (const name of ["CapEff", "CapPrm", "CapBnd"]) assert.match(outerStatus, new RegExp(`^${name}:\\s+0+$`, "m"));
assert.match(outerStatus, /^NoNewPrivs:\s+1$/m);
assert.match(outerStatus, /^Seccomp:\s+2$/m);
const mounts = readFileSync("/proc/self/mountinfo", "utf8").trim().split("\n").map(line => line.split(" "));
assert.ok(mounts.find(row => row[4] === "/")[5].split(",").includes("ro"));
for (const name of ["/tmp", "/dev/shm"]) {
  const mount = mounts.find(row => row[4] === name);
  assert.ok(mount);
  for (const flag of ["rw", "nosuid", "nodev", "noexec"]) assert.ok(mount[5].split(",").includes(flag));
}
for (const addresses of Object.values(networkInterfaces())) for (const address of addresses) assert.equal(address.internal, true);
for (const line of readFileSync("/proc/net/route", "utf8").trim().split("\n").slice(1)) assert.notEqual(line.trim().split(/\s+/)[1], "00000000");
const allowed = ["PATH", "LANG", "LC_ALL", "PLAYWRIGHT_BROWSERS_PATH", "HOME", "TMPDIR", "XDG_CACHE_HOME", "HOSTNAME", "OPERATOR_UNIT_HOST_GATEWAY"];
for (const name of Object.keys(process.env)) assert.ok(allowed.includes(name));
assert.equal(process.env.HOME, "/tmp/home");
assert.equal(process.env.TMPDIR, "/tmp");
assert.equal(process.env.XDG_CACHE_HOME, "/tmp/cache");
for (const home of ["/root", "/home/pwuser", "/tmp/home"]) for (const path of [".aws/credentials", ".kube/config", ".docker/config.json", ".netrc", ".npmrc"]) assert.throws(() => statSync(`${home}/${path}`), error => ["ENOENT", "EACCES"].includes(error.code));
for (const home of ["/root", "/home/pwuser", "/tmp/home"]) for (const path of ["id_rsa", "id_ed25519", "authorized_keys", "config"]) assert.throws(() => statSync(`${home}/.ssh/${path}`), error => ["ENOENT", "EACCES"].includes(error.code));
for (const path of ["/var/run/docker.sock", "/run/docker.sock", "/run/secrets/kubernetes.io/serviceaccount/token"]) assert.throws(() => statSync(path), error => error.code === "ENOENT");
assert.throws(() => writeFileSync("/reviewed/denied-write-probe", "synthetic", { flag: "wx" }), error => ["EROFS", "EACCES"].includes(error.code));
mkdirSync("/tmp/home");
mkdirSync("/tmp/cache");
writeFileSync("/tmp/owned-write-probe", "synthetic", { flag: "wx" });
const gateway = process.env.OPERATOR_UNIT_HOST_GATEWAY;
assert.equal(isIP(gateway), 4);
async function denied(host) {
  await new Promise((resolve, reject) => {
    const socket = connect({ host, port: 443 });
    socket.setTimeout(1500);
    socket.once("connect", () => { socket.destroy(); reject(new Error("Unexpected native network reachability")); });
    socket.once("timeout", () => { socket.destroy(); reject(new Error("Expected immediate kernel route denial")); });
    socket.once("error", error => { socket.destroy(); if (["ENETUNREACH", "EHOSTUNREACH", "EAFNOSUPPORT"].includes(error.code)) resolve(); else reject(new Error("Unexpected native denial category")); });
  });
}
for (const host of [gateway, "192.0.2.1", "2001:db8::1"]) await denied(host);
// Fixed pinned-base coreutils flow calls chroot("/") before exec. Exact EPERM
// diagnostic distinguishes kernel refusal from executable/argument failure.
// New userns-local authority used during Chromium initialization is separate
// from this outer UID1001/cap0 process; live renderers must drop caps again.
const chroot = spawnSync("/usr/sbin/chroot", ["/", "/usr/bin/true"], { env: { ...process.env, LANG: "C", LC_ALL: "C" }, encoding: "utf8", timeout: 3000 });
assert.equal(chroot.error, undefined);
assert.equal(chroot.signal, null);
assert.equal(chroot.status, 125); // coreutils EXIT_CANCELED: chroot itself failed.
assert.equal(chroot.stdout, "");
assert.equal(chroot.stderr, "/usr/sbin/chroot: cannot change root directory to '/': Operation not permitted\n");
console.log("PASS browser native-boundary");

// Deliberately full Chromium, not headless-shell: chrome://sandbox reports
// expected startup sandbox state, complemented by live renderer /proc evidence.
const driver = JSON.parse(readFileSync("/reviewed/package/package.json", "utf8"));
assert.equal(driver.name, "playwright-core");
assert.equal(driver.version, "1.56.0");
const { chromium } = await import("/reviewed/package/index.mjs");
let browser;
try {
  browser = await chromium.launch({ executablePath: "/ms-playwright/chromium-1194/chrome-linux/chrome", headless: true, chromiumSandbox: true, timeout: 20000, env: process.env });
  assert.equal(browser.version(), "141.0.7390.37");
  console.log("PASS browser chromium-version");
  const page = await browser.newPage({ viewport: { width: 800, height: 600 } });
  await page.goto("chrome://sandbox/", { waitUntil: "load", timeout: 10000 });
  const rows = await page.locator("tr").evaluateAll(elements => elements.map(element => [...element.querySelectorAll("td")].map(cell => cell.textContent.trim())).filter(cells => cells.length === 2));
  const evaluation = await page.locator("body").innerText();
  const matching = evaluation.split("\n").map(line => line.trim()).filter(line => line === "You are adequately sandboxed.");
  assert.equal(matching.length, 1);
  verifyInternalSandbox(rows, matching[0]);
  console.log("PASS browser internal-sandbox-status");
  const ordinary = await browser.newPage();
  await ordinary.setContent("<!doctype html><title>Owned finite sandbox probe</title><p>synthetic</p>");
  assert.equal(await ordinary.title(), "Owned finite sandbox probe");
  const cdp = await browser.newBrowserCDPSession();
  const processes = await cdp.send("SystemInfo.getProcessInfo");
  const renderers = processes.processInfo.filter(value => value.type === "renderer");
  assert.ok(renderers.length >= 1 && renderers.length <= 16);
  for (const renderer of renderers) {
    assert.ok(Number.isSafeInteger(renderer.id) && renderer.id > 0);
    const status = readFileSync(`/proc/${renderer.id}/status`, "utf8");
    const uidMap = readFileSync(`/proc/${renderer.id}/uid_map`, "utf8");
    try {
      verifyRendererKernel(status, uidMap, outerStatus, outerUIDMap, renderer.id, lastCapability);
    } catch (error) {
      console.log("RENDERER-DIAGNOSTIC " + JSON.stringify(rendererFailureMetadata(status, uidMap, renderer.id)));
      throw error;
    }
  }
  console.log("PASS browser renderer-kernel-sandbox");
} finally {
  if (browser) await browser.close();
}
console.log("PASS browser browser-closed");
