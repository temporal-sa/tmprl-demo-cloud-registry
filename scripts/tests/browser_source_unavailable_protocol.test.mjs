// Pure import/protocol contracts only; no browser, driver, probes or stdin.
import test from "node:test";
import assert from "node:assert/strict";
import { SOURCE_CASES, sourcePhaseRecord } from "../operator_browser_source_unavailable.mjs";

const disconnected = () => ({ phase: "source-disconnected", sourceReads: 1, proxyClosed: true, evidenceRetained: true });
const finished = () => ({ phase: "finished", sourceReads: 2, proxyClosed: true, evidenceRetained: true, managedPosts: 1, writerExited: true, serverClosed: true });

test("source outage module imports without runtime or driver side effects", () => {
  assert.deepEqual(SOURCE_CASES, ["native-network", "chromium-sandbox", "initial-authorized-view", "source-disconnected", "unavailable-cleared", "evidence-and-no-repost", "browser-closed"]);
  assert.ok(Object.isFrozen(SOURCE_CASES));
});

test("source outage phase requires exact completed first read and actual close evidence", () => {
  assert.ok(Object.isFrozen(sourcePhaseRecord(disconnected(), "source-disconnected")));
  for (const changed of [{ sourceReads: 0 }, { sourceReads: 2 }, { sourceReads: true }, { proxyClosed: false }, { proxyClosed: "true" }, { evidenceRetained: false }, { command: "PRIVATE_COMMAND" }, { phase: "closed" }]) assert.throws(() => sourcePhaseRecord({ ...disconnected(), ...changed }, "source-disconnected"));
});

test("source outage finish refuses polling provider repost and unclosed resources", () => {
  assert.ok(Object.isFrozen(sourcePhaseRecord(finished(), "finished")));
  for (const changed of [{ sourceReads: 1 }, { sourceReads: 3 }, { sourceReads: true }, { managedPosts: 0 }, { managedPosts: 2 }, { writerExited: false }, { serverClosed: false }, { proxyClosed: false }, { evidenceRetained: false }]) assert.throws(() => sourcePhaseRecord({ ...finished(), ...changed }, "finished"));
});

test("source outage unknown phases and private fields yield only finite failure", () => {
  for (const [value, phase] of [[{ ...disconnected(), cookie: "PRIVATE_COOKIE" }, "source-disconnected"], [{ ...finished(), endpoint: "PRIVATE_LOCATOR" }, "finished"], [disconnected(), "resume-source"], [null, "finished"], [[], "finished"]]) {
    let failure;
    try { sourcePhaseRecord(value, phase); } catch (error) { failure = error; }
    assert.equal(failure?.message, "BrowserSourceProtocolInvalid");
    assert.ok(!String(failure).includes("PRIVATE_"));
  }
});
