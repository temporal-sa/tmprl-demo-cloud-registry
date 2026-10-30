// Native boundary proof for the SAME no-network container which runs the pure
// contracts below. No user credentials, browser, real diagnostic API or provider
// is used. The host harness independently verifies image contents/config first.
import assert from "node:assert/strict";
import { readFileSync, statSync } from "node:fs";
import { networkInterfaces } from "node:os";
import { connect, isIP } from "node:net";
import { spawnSync } from "node:child_process";

assert.equal(process.version, "v26.13.0");
assert.equal(process.platform, "linux");
assert.ok(["arm64", "x64"].includes(process.arch));
assert.equal(process.getuid(), 65532);
assert.equal(process.getgid(), 65532);
assert.ok(process.getgroups().every(value => value === 65532));
const status = readFileSync("/proc/self/status", "utf8");
for (const key of ["CapEff", "CapPrm", "CapBnd"]) assert.match(status, new RegExp(`^${key}:\\s+0+$`, "m"));
assert.match(status, /^NoNewPrivs:\s+1$/m);
const mounts = readFileSync("/proc/self/mountinfo", "utf8").trim().split("\n").map(row => row.split(" "));
assert.ok(mounts.find(row => row[4] === "/")[5].split(",").includes("ro"));
const tmp = mounts.find(row => row[4] === "/tmp");
for (const flag of ["rw", "nosuid", "nodev", "noexec"]) assert.ok(tmp[5].split(",").includes(flag));
for (const addresses of Object.values(networkInterfaces())) for (const entry of addresses) assert.equal(entry.internal, true);
for (const row of readFileSync("/proc/net/route", "utf8").trim().split("\n").slice(1)) assert.notEqual(row.trim().split(/\s+/)[1], "00000000");
for (const key of Object.keys(process.env)) assert.ok(["PATH", "NODE_VERSION", "YARN_VERSION", "HOSTNAME", "HOME", "TZ", "OPERATOR_UNIT_HOST_GATEWAY"].includes(key));
assert.equal(process.env.HOME, "/tmp/home");
// Root home may be inaccessible to this UID. Exact path absence is also checked
// by the host's unstarted-image export scan; this process must never access it.
for (const path of ["/var/run/docker.sock", "/root/.aws/credentials", "/root/.kube/config", "/home/node/.aws/credentials", "/home/node/.kube/config", "/run/secrets/kubernetes.io/serviceaccount/token"]) assert.throws(() => statSync(path), error => ["ENOENT", "EACCES"].includes(error.code));
const gateway = process.env.OPERATOR_UNIT_HOST_GATEWAY;
assert.equal(isIP(gateway), 4);
async function denied(host) {
  await new Promise((resolve, reject) => {
    const socket = connect({ host, port: 443 });
    socket.setTimeout(1500);
    socket.once("connect", () => { socket.destroy(); reject(new Error("Unexpected network reachability")); });
    socket.once("timeout", () => { socket.destroy(); reject(new Error("Expected immediate kernel denial")); });
    socket.once("error", error => { socket.destroy(); if (["ENETUNREACH", "EHOSTUNREACH", "EAFNOSUPPORT"].includes(error.code)) resolve(); else reject(new Error("Unexpected network failure category")); });
  });
}
for (const host of ["192.0.2.1", "2001:db8::1", gateway]) await denied(host);
console.log(`Verified native Node ${process.version} ${process.arch}: read-only nonroot, no capabilities, no host/external route, inaccessible credential/control paths.`);
// Fixed explicit files, in lexical order, with serial execution: no caller
// target/environment override can omit page, component, pure browser evidence
// validators or page/source protocols. Importing these launches no browser/provider
// and does not read stdin or invoke native probes.
const result = spawnSync(process.execPath, ["--test", "--test-reporter=tap", "--test-concurrency=1", "/reviewed/registry/web/tests/diagnostic-page.test.mjs", "/reviewed/registry/web/tests/diagnostics.test.mjs", "/reviewed/scripts/tests/browser_assertions.test.mjs", "/reviewed/scripts/tests/browser_page_protocol.test.mjs", "/reviewed/scripts/tests/browser_source_unavailable_protocol.test.mjs"], { stdio: "inherit", env: process.env, timeout: 30000 });
assert.equal(result.error, undefined);
assert.equal(result.signal, null);
assert.equal(result.status, 0);
console.log("Verified Node boundary and diagnostic contracts completed in the same container.");
