// Pure synthetic evidence only; importing validators never launches a browser.
import test from "node:test";
import assert from "node:assert/strict";
import { rendererFailureMetadata, verifyInternalSandbox, verifyRendererKernel } from "../operator_browser_assertions.mjs";

const rows = [["Layer 1 Sandbox", "Namespace"], ["PID namespaces", "Yes"], ["Network namespaces", "Yes"], ["Seccomp-BPF sandbox", "Yes"], ["Seccomp-BPF sandbox supports TSYNC", "Yes"]];
const outer = "Pid:\t1\nCapEff:\t0000\nCapPrm:\t0000\nCapInh:\t0000\nCapAmb:\t0000\nCapBnd:\t0000\nNoNewPrivs:\t1\nSeccomp:\t2\nSeccomp_filters:\t1\nNSpid:\t1\n";
const renderer = outer.replace("Pid:\t1", "Pid:\t22").replace("CapBnd:\t0000", "CapBnd:\t000001ffffffffff").replace("Seccomp_filters:\t1", "Seccomp_filters:\t2").replace("NSpid:\t1", "NSpid:\t22\t2");
const outerMap = "0 0 4294967295\n";
const innerMap = "1001 1001 1\n";

test("exact internal namespace and seccomp report is required", () => {
  verifyInternalSandbox(rows, "You are adequately sandboxed.");
  for (let index = 0; index < rows.length; index++) {
    const changed = rows.map(row => [...row]); changed[index][1] = "No";
    assert.throws(() => verifyInternalSandbox(changed, "You are adequately sandboxed."));
  }
  assert.throws(() => verifyInternalSandbox([...rows, rows[0]], "You are adequately sandboxed."));
  assert.throws(() => verifyInternalSandbox(rows, "You are NOT adequately sandboxed."));
});

test("failure metadata contains only finite selected numeric kernel fields", () => {
  const value = rendererFailureMetadata(renderer + "Secret:\tprivate-marker\n", innerMap, 22);
  assert.equal(value.cdpPID, 22);
  assert.equal(value.fields.Pid, "22");
  assert.equal(value.fields.NSpid, "22 2");
  assert.equal(value.uidMap, "1001 1001 1");
  assert.ok(!JSON.stringify(value).includes("private-marker"));
  assert.equal(rendererFailureMetadata(renderer.replace("CapEff:\t0000", "CapEff:\tprivate-marker"), innerMap, 22).fields.CapEff, null);
  assert.equal(rendererFailureMetadata(renderer, "private-marker", 22).uidMap, null);
  assert.equal(rendererFailureMetadata(renderer + "Pid:\t23\n", innerMap, 22).fields.Pid, null);
});

test("live renderer needs additional seccomp and PID and UID namespace evidence", () => {
  verifyRendererKernel(renderer, innerMap, outer, outerMap, 22, 40);
  for (const status of [outer, renderer.replace("Seccomp:\t2", "Seccomp:\t0"), renderer.replace("NoNewPrivs:\t1", "NoNewPrivs:\t0"), renderer.replace("Seccomp_filters:\t2", "Seccomp_filters:\t1"), renderer + "NSpid:\t30\n", ...["CapEff", "CapPrm", "CapInh", "CapAmb"].map(key => renderer.replace(`${key}:\t0000`, `${key}:\t0001`))]) assert.throws(() => verifyRendererKernel(status, innerMap, outer, outerMap, 22, 40));
  assert.throws(() => verifyRendererKernel(renderer, outerMap, outer, outerMap, 22, 40));
  assert.throws(() => verifyRendererKernel(renderer, "0 0 1", outer, outerMap, 22, 40));
  assert.throws(() => verifyRendererKernel(renderer, "0 1001 1", outer, outerMap, 22, 40));
  assert.throws(() => verifyRendererKernel(renderer, "1001 1001 2", outer, outerMap, 22, 40));
  assert.throws(() => verifyRendererKernel(renderer, innerMap, outer, outerMap, 23, 40));
});

test("namespace bounding ceiling must equal exact bounded current kernel full set", () => {
  for (const value of [-1, 64, 40.5, "40", NaN, undefined]) assert.throws(() => verifyRendererKernel(renderer, innerMap, outer, outerMap, 22, value));
  for (const value of ["0000000000000000", "000000ffffffffff", "000003ffffffffff", "ffffffffffffffff"]) assert.throws(() => verifyRendererKernel(renderer.replace("000001ffffffffff", value), innerMap, outer, outerMap, 22, 40));
  verifyRendererKernel(renderer.replace("000001ffffffffff", "ffffffffffffffff"), innerMap, outer, outerMap, 22, 63);
});
