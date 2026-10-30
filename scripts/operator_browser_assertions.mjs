// Pure, finite evidence validators. No process, filesystem, browser or network
// access occurs on import; the isolated boundary runner supplies observed data.
import assert from "node:assert/strict";

export function verifyInternalSandbox(rows, evaluation) {
  assert.ok(Array.isArray(rows) && rows.length <= 24);
  const evidence = new Map();
  for (const row of rows) {
    assert.ok(Array.isArray(row) && row.length === 2 && row.every(item => typeof item === "string" && item.length < 256));
    assert.ok(!evidence.has(row[0]));
    evidence.set(row[0], row[1]);
  }
  assert.equal(evidence.get("Layer 1 Sandbox"), "Namespace");
  for (const name of ["PID namespaces", "Network namespaces", "Seccomp-BPF sandbox", "Seccomp-BPF sandbox supports TSYNC"]) assert.equal(evidence.get(name), "Yes");
  assert.equal(evaluation, "You are adequately sandboxed.");
}

export function verifyRendererKernel(status, uidMap, outerStatus, outerUIDMap, expectedPID, lastCapability) {
  const field = (source, name) => {
    assert.ok(typeof source === "string" && source.length <= 65536);
    const lines = source.split("\n").filter(line => line.startsWith(name + ":"));
    assert.equal(lines.length, 1);
    return lines[0].split(":")[1].trim();
  };
  assert.ok(Number.isSafeInteger(expectedPID) && expectedPID > 0);
  assert.equal(field(status, "Pid"), String(expectedPID));
  for (const name of ["CapEff", "CapPrm", "CapInh", "CapAmb"]) assert.match(field(status, name), /^0+$/, `renderer ${name}`);
  assert.equal(field(status, "NoNewPrivs"), "1");
  assert.equal(field(status, "Seccomp"), "2");
  const filters = Number(field(status, "Seccomp_filters"));
  const outerFilters = Number(field(outerStatus, "Seccomp_filters"));
  assert.ok(Number.isSafeInteger(filters) && Number.isSafeInteger(outerFilters) && filters > outerFilters && outerFilters >= 1);
  const pids = field(status, "NSpid").split(/\s+/);
  const outerPids = field(outerStatus, "NSpid").split(/\s+/);
  assert.ok(pids.every(value => /^[1-9][0-9]*$/.test(value)) && pids.length > outerPids.length);
  assert.ok(typeof uidMap === "string" && uidMap.length <= 4096 && uidMap.trim() !== outerUIDMap.trim());
  const mapping = uidMap.trim().split(/\s+/);
  // Pinned Chromium141 NamespaceUtils::WriteToIdMapFile maps getuid() to
  // itself, not to namespace root. No alternative/arbitrary mapping is valid.
  assert.deepEqual(mapping, ["1001", "1001", "1"]);
  // New userns resets the bounding ceiling to CAP_FULL_SET. Chromium drops
  // actual E/P/I/A authority with capset; that does not clear this ceiling.
  // Outer container CapBnd must still be zero (proved before driver import).
  assert.ok(Number.isSafeInteger(lastCapability) && lastCapability >= 0 && lastCapability <= 63);
  const full = ((1n << BigInt(lastCapability + 1)) - 1n).toString(16).padStart(16, "0");
  assert.equal(field(status, "CapBnd"), full, "renderer namespace bounding ceiling");
}

// Failure-only public fixture evidence: never return raw status, argv, paths,
// environment or unknown keys. Malformed/missing fields become null.
export function rendererFailureMetadata(status, uidMap, expectedPID) {
  const validSource = typeof status === "string" && status.length <= 65536;
  const fields = {};
  for (const name of ["Pid", "CapEff", "CapPrm", "CapInh", "CapAmb", "CapBnd", "NoNewPrivs", "Seccomp", "Seccomp_filters", "NSpid"]) {
    const lines = validSource ? status.split("\n").filter(line => line.startsWith(name + ":")) : [];
    const raw = lines.length === 1 ? lines[0].split(":")[1].trim().replace(/\s+/g, " ") : "";
    const pattern = name.startsWith("Cap") ? /^[0-9a-f]{1,16}$/ : name === "NSpid" ? /^[1-9][0-9]{0,9}(?: [1-9][0-9]{0,9}){0,5}$/ : /^[0-9]{1,10}$/;
    fields[name] = pattern.test(raw) ? raw : null;
  }
  const mapping = typeof uidMap === "string" && uidMap.length <= 4096 ? uidMap.trim().replace(/\s+/g, " ") : "";
  return { cdpPID: Number.isSafeInteger(expectedPID) && expectedPID > 0 ? expectedPID : null, fields, uidMap: /^[0-9]{1,10} [0-9]{1,10} [0-9]{1,10}$/.test(mapping) ? mapping : null };
}
