#!/usr/bin/env python3
"""Compare the public UI fixture with an actual Go Model.Read().JSON() result.

The owned helper uses a synthetic journal Cell, real Repository/JournalSource,
synthetic grant and fixed freshness clocks; it makes no provider calls and
authenticates no real session. Compile only on the host,
offline/CGO-disabled from existing explicit module caches; package initialization
and Model.Read run only inside a verified owned no-network/read-only/nonroot
scratch container with no host mounts/credentials. The separate Node tier copies
only the reviewed public JSON fixture, module and tests. No browser/API claim.
"""
from __future__ import annotations

import argparse
import json
import io
import ipaddress
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import tarfile
import uuid

from operator_node_test_contract import Docker
from run_operator_node_tests import LABEL, check_container

ROOT = Path(__file__).resolve().parents[1]
MODULE = ROOT / "operator-v2"
FIXTURE = ROOT / "registry/web/tests/fixtures/go-diagnostic-view-v1.json"
GO_PROGRAM = r'''package main
import (
  "context"
  "encoding/json"
  "os"
  "strconv"
  "strings"
  "time"
  c "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/catalogdiagnostics"
  d "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/diagnostics"
  j "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/journal"
  r "github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/rollout"
)
type source struct{ value c.Snapshot }
func(s source)Read(context.Context,c.ReadGrant)(c.Snapshot,error){return s.value,nil}
type cell struct{data []byte; version j.Version}
func(s *cell)Read(context.Context)([]byte,j.Version,error){return append([]byte(nil),s.data...),s.version,nil}
func(s *cell)CompareAndSwap(_ context.Context,v j.Version,b []byte,_ string)error{
  if v!=s.version{return j.ErrConflict};n,_:=strconv.Atoi(v.ResourceVersion);s.version.ResourceVersion=strconv.Itoa(n+1);s.data=append([]byte(nil),b...);return nil
}
type desired struct{value c.DesiredObservation}
func(s desired)ObserveDesired(context.Context,c.Target)(c.DesiredObservation,error){return s.value,nil}
func update(repo *j.Repository,change func(*j.Envelope)){
  saved,err:=repo.Load(context.Background());check(err);raw,err:=json.Marshal(saved.Envelope);check(err)
  var next j.Envelope;check(json.Unmarshal(raw,&next));change(&next);_,err=repo.Commit(context.Background(),saved,next);check(err)
}
func main(){
  now:=time.Date(2026,10,30,12,0,0,0,time.UTC)
  target:=c.Target{Environment:"lab",Installation:"installation-one",ProjectUID:"canonical-uid-one"}
  identity:=d.Identity{Environment:target.Environment,Installation:target.Installation,ProjectUID:target.ProjectUID,ProjectName:"demo",OperationID:"operation-two",ReconcileKey:"revision-two",Generation:2}
  p,err:=d.NewProjection(identity);check(err)
  cause:=d.DomainFailure{ID:"smoke-one",Code:"SmokeFailed",Category:d.Smoke,Message:"Candidate smoke check failed",Step:"smoke",OccurredAt:now.Add(-time.Minute),NextAction:"Inspect the candidate configuration."}
  cleanup:=d.DomainFailure{ID:"cleanup-one",Code:"CleanupFailed",Category:d.Cleanup,Message:"Candidate was retained after cleanup failed",Step:"cleanup",OccurredAt:now.Add(-time.Minute)}
  p,err=p.Apply(d.OperationEvent{Identity:identity,Sequence:4,OccurredAt:now.Add(-time.Minute),State:d.Failed,Step:"smoke",Failure:&cause,Retry:&d.RetryInfo{Status:d.RetryExhausted,Authoritative:true,ObservedAt:now.Add(-time.Minute),Attempt:3,MaximumAttempts:3},Availability:&d.Availability{ActiveRevision:"active-one",CandidateRevision:"candidate-two",RouteState:d.RouteActivePreserved,ConfirmedAt:now.Add(-time.Minute)},Recovery:[]d.RecoveryOutcome{{ID:"cleanup-one",Step:"cleanup",State:d.OutcomeRetained,Failure:&cleanup}},Evidence:[]d.EvidenceRef{{ID:"capture-one",Kind:"PodLogs",State:"Failed",Locator:"PRIVATE_LOCATOR_NEVER_PUBLISHED",Partial:true}},Partial:true});check(err)
  delivery:=d.DomainFailure{ID:"publication-one",Code:"PublishFailed",Category:d.Publication,Message:"Status delivery failed",OccurredAt:now}
  p,err=p.WithPublication(d.SinkHealth{Sink:"catalog",State:d.DeliveryFailed,SourceSequence:3,ObservedAt:now,Failure:&delivery});check(err)
  grant,err:=c.IssueReadGrant("synthetic-verified-subject","synthetic-policy-v1",target,now.Add(-time.Minute),now.Add(time.Minute));check(err)
  initial,err:=j.Initial(r.Subject{Environment:target.Environment,Installation:target.Installation,ProjectUID:target.ProjectUID},1);check(err)
  raw,err:=initial.JSON();check(err);owned:=&cell{raw,j.Version{UID:"synthetic-pinned-cell",ResourceVersion:"1"}}
  repo,err:=j.New(owned,initial.Subject,owned.version.UID);check(err)
  update(repo,func(e *j.Envelope){e.Attempts=[]j.Attempt{{Number:1,OperationID:identity.OperationID,NamespaceName:"synthetic-candidate",Revision:"git-source-revision",DesiredDigest:strings.Repeat("a",64),State:j.Reserved}};e.Projection=&p})
  update(repo,func(e *j.Envelope){e.Writes=append(e.Writes,j.Write{ID:"namespace-create",OperationID:identity.OperationID,WriterEpoch:1,State:j.Pending,Preconditions:j.Preconditions{Kind:"Namespace",Action:"Create",Name:"synthetic-candidate",Fingerprint:strings.Repeat("b",64),TargetRevision:"git-source-revision",ProviderOperationID:identity.OperationID}})})
  update(repo,func(e *j.Envelope){e.Attempts[0].State=j.Observed;e.Attempts[0].NamespaceUID="PRIVATE-namespace-uid";e.Writes[0].State=j.Committed;e.Writes[0].ResultUID="PRIVATE-namespace-uid";e.Writes[0].ResultVersion="PRIVATE-historical-version"})
  for _,kind:=range []string{"ConfigMap","Deployment"}{
    update(repo,func(e *j.Envelope){e.Writes=append(e.Writes,j.Write{ID:kind+"-create",OperationID:identity.OperationID,WriterEpoch:1,State:j.Pending,Preconditions:j.Preconditions{Kind:kind,Action:"Create",Name:"synthetic-child",ParentUID:"PRIVATE-namespace-uid",Fingerprint:strings.Repeat("c",64),TargetRevision:"git-source-revision",ProviderOperationID:kind+"-create"}})})
    update(repo,func(e *j.Envelope){w:=&e.Writes[len(e.Writes)-1];w.State=j.Rejected;if kind=="Deployment"{w.State=j.Unknown}})
  }
  durable,err:=c.NewJournalSource(target.Environment,target.Installation,[]c.JournalBinding{{Target:target,Repository:repo,Origin:c.Materialized}},desired{c.DesiredObservation{Desired:d.DesiredState{ReconcileKey:identity.ReconcileKey,Generation:identity.Generation},Known:true,ObservedAt:now.Add(-30*time.Second)}},func()time.Time{return now});check(err)
  observed,err:=durable.Read(context.Background(),grant);check(err)
  // Synthetic independently injected persistence clock preserves this fixture's
  // freshness case. Real schema1 JournalSource never invents this timestamp.
  observed.RecordedAt=now.Add(-2*time.Minute)
  model,err:=c.NewModel(source{observed},c.Config{Now:func()time.Time{return now},MaxProgressAge:5*time.Minute,MaxDesiredAge:2*time.Minute,MaxAvailabilityAge:2*time.Minute});check(err)
  view,err:=model.Read(context.Background(),grant,target);check(err)
  data,err:=view.JSON();check(err)
  _,err=os.Stdout.Write(data);check(err)
}
func check(err error){if err!=nil{panic("owned diagnostic contract fixture failed")}}
'''


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write-reviewed-fixture", action="store_true", help="Explicitly update only the public checked-in synthetic fixture")
    parser.add_argument("--docker-socket", default="/var/run/docker.sock")
    args = parser.parse_args()
    go = shutil.which("go")
    if not go:
        parser.error("Existing Go required; this script never acquires a toolchain")
    with tempfile.TemporaryDirectory(prefix=".diagnostic-contract-", dir=MODULE) as helper, tempfile.TemporaryDirectory(prefix="diagnostic-contract-home-") as home:
        Path(helper, "main.go").write_text(GO_PROGRAM)
        docker = Docker(args.docker_socket, Path(home))
        env = {"PATH": os.defpath, "HOME": home, "GOENV": "off", "GOWORK": "off", "GOFLAGS": "", "GOTOOLCHAIN": "local", "GOPROXY": "off", "GOSUMDB": "off", "GOTELEMETRY": "off", "CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": docker.arch, "GOCACHE": str(ROOT / ".venv/operator-go-cache"), "GOMODCACHE": str(ROOT / ".venv/operator-go-modules")}
        binary, proof = Path(home) / "fixture", Path(home) / "proof"
        for output, source in ((binary, "./" + Path(helper).name), (proof, str(ROOT / "scripts/operator_unit_boundary.go"))):
            result = subprocess.run([go, "build", "-mod=readonly", "-trimpath", "-buildvcs=false", "-o", str(output), source], cwd=MODULE, env=env, capture_output=True, timeout=180, check=False)
            if result.returncode:
                raise RuntimeError("Owned Go diagnostic fixture compilation failed; no code executed on host")
        run_id = "diagnostic-wire-" + uuid.uuid4().hex[:12]
        image = "operator-local-wire:" + run_id
        archive_path = Path(home) / "wire.tar"
        with tarfile.open(archive_path, "w") as archive:
            for source in (binary, proof):
                content = source.read_bytes()
                item = tarfile.TarInfo(source.name)
                item.size, item.mode = len(content), 0o555
                archive.addfile(item, io.BytesIO(content))
        gateway = json.loads(docker.command(["network", "inspect", "bridge"]))[0]["IPAM"]["Config"][0]["Gateway"]
        ipaddress.IPv4Address(gateway)
        attempted = False
        containers = []
        primary = None
        generated = None
        try:
            attempted = True
            docker.command(["image", "import", "--platform", "linux/" + docker.arch, "--change", "USER 65532:65532", "--change", f"LABEL {LABEL}={run_id}", archive_path, image])
            staged = docker.inspect_optional("image", image)
            if staged is None or staged["Config"].get("Labels", {}).get(LABEL) != run_id or staged["Architecture"] != docker.arch or staged["Os"] != "linux":
                raise RuntimeError("Owned Go fixture image identity mismatch")
            for index, entrypoint in enumerate(("/proof", "/fixture")):
                name = run_id + "-" + str(index)
                containers.append(name)
                docker.command(["create", "--pull=never", "--name", name, "--label", f"{LABEL}={run_id}", "--network=none", "--read-only", "--cap-drop=ALL", "--security-opt", "no-new-privileges", "--user", "65532:65532", "--pids-limit", "64", "--memory", "256m", "--cpus", "1", "--tmpfs", "/tmp:rw,noexec,nosuid,nodev,size=32m", "--env", "HOME=/tmp/home", "--env", "OPERATOR_UNIT_HOST_GATEWAY=" + gateway, staged["Id"], entrypoint])
                owned = docker.inspect_optional("container", name)
                check_container(owned, run_id, staged["Id"])
                if owned["State"]["Status"] != "created":
                    raise RuntimeError("Go fixture executed before boundary configuration verification")
                output = docker.command(["start", "--attach", name], timeout=60)
                completed = docker.inspect_optional("container", name)
                check_container(completed, run_id, staged["Id"])
                if completed["State"]["Running"] or completed["State"]["ExitCode"] != 0 or completed["State"].get("OOMKilled"):
                    raise RuntimeError("Isolated Go fixture failed")
                if index == 0:
                    if "Verified only loopback is active" not in output:
                        raise RuntimeError("Native Go boundary proof missing")
                else:
                    generated = output
        except BaseException as error:
            primary = error
            raise
        finally:
            cleanup = []
            for name in containers:
                try:
                    owned = docker.inspect_optional("container", name)
                    if owned is not None:
                        if owned["Config"].get("Labels", {}).get(LABEL) != run_id:
                            raise RuntimeError("Go fixture container ownership changed")
                        docker.command(["rm", "--force", name])
                        if docker.inspect_optional("container", name) is not None:
                            raise RuntimeError("Go fixture container removal unconfirmed")
                except Exception:
                    cleanup.append("owned Go fixture container cleanup unconfirmed")
            if attempted:
                try:
                    owned = docker.inspect_optional("image", image)
                    if owned is not None:
                        if owned["Config"].get("Labels", {}).get(LABEL) != run_id:
                            raise RuntimeError("Go fixture image ownership changed")
                        docker.command(["image", "rm", image])
                        if docker.inspect_optional("image", image) is not None:
                            raise RuntimeError("Go fixture image removal unconfirmed")
                except Exception:
                    cleanup.append("owned Go fixture image cleanup unconfirmed")
            if cleanup:
                message = "; ".join(cleanup)
                if primary is None:
                    raise RuntimeError(message)
                primary.add_note(message)
        if generated is None or len(generated.encode()) > 32768:
            raise RuntimeError("Isolated public Go fixture output exceeds its contract")
        data = json.loads(generated)
        if "PRIVATE_LOCATOR" in generated:
            raise RuntimeError("Private evidence locator escaped the actual read model")
        if args.write_reviewed_fixture:
            FIXTURE.parent.mkdir(parents=True, exist_ok=True)
            FIXTURE.write_text(json.dumps(data, sort_keys=True, indent=2) + "\n")
        elif json.loads(FIXTURE.read_text()) != data:
            raise RuntimeError("Go diagnostic wire schema changed; review the frontend contract and public fixture")
    print("Isolated actual Go JournalSource + Model.Read().JSON() matches the reviewed public UI fixture (synthetic Cell/grant/clocks only).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
