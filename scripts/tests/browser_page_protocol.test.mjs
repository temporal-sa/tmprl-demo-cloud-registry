// Pure protocol tests: importing runtime module cannot read stdin/credentials,
// acquire anything, invoke native probes, load the driver or launch a browser.
import test from "node:test";
import assert from "node:assert/strict";
import { PAGE_CASES, pageBootstrap } from "../operator_browser_page.mjs";

const fixture = () => ({ phase: "ready", run: "operator-local-012345abcdef", origin: "http://172.30.0.1:8080", target: { environment: "local", installation: "owned", projectUID: "actual-project-uid" }, cookie: "synthetic.claims.signature", writerExited: true, frontendIP: "172.30.0.1", browserIP: "172.30.0.2", nodeIP: "172.29.0.1", hostGateway: "172.17.0.1" });

test("page module imports without runtime prerequisites or native side effects", () => {
  assert.equal(PAGE_CASES.length, 10); assert.ok(Object.isFrozen(PAGE_CASES));
  const value = pageBootstrap(fixture());
  assert.ok(Object.isFrozen(value) && Object.isFrozen(value.target));
});

test("bootstrap origin is exact private frontend IP and fixed owned port", () => {
  for (const origin of ["https://172.30.0.1:8080", "http://172.30.0.1:8081", "http://foreign.example", "http://172.30.0.1:8080/?cookie=PRIVATE", "http://127.0.0.1:8080"]) assert.throws(() => pageBootstrap({ ...fixture(), origin }));
  for (const frontendIP of ["127.0.0.1", "0.0.0.0", "169.254.169.254", "203.0.113.1", "::1"]) assert.throws(() => pageBootstrap({ ...fixture(), frontendIP, origin: `http://${frontendIP}:8080` }));
  assert.throws(() => pageBootstrap({ ...fixture(), nodeIP: fixture().browserIP }));
});

test("bootstrap cannot select commands aliases arbitrary scopes or unverified writer", () => {
  for (const changed of [{ command: "PRIVATE_COMMAND" }, { phase: "ready-again" }, { run: "production-context" }, { writerExited: false }, { writerExited: "true" }, { target: { ...fixture().target, projectName: "not-an-identity" } }, { target: { ...fixture().target, projectUID: "../foreign" } }]) assert.throws(() => pageBootstrap({ ...fixture(), ...changed }));
});

test("malformed synthetic cookies fail with only finite privacy-safe error", () => {
  for (const cookie of ["PRIVATE_COOKIE", "a.b.c\nPRIVATE_COOKIE", "a".repeat(4097), {}, null]) {
    let failure;
    try { pageBootstrap({ ...fixture(), cookie }); } catch (error) { failure = error; }
    assert.equal(failure?.message, "BrowserProtocolInvalid");
    assert.ok(!String(failure).includes("PRIVATE_COOKIE"));
  }
});
