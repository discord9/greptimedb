#!/usr/bin/env python3
# Copyright 2023 Greptime Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Kubernetes E2E executor for ``workload_scheduler_distributed``.

This is intentionally an entry point of query-regression-run.py, rather than a
second benchmark product.  It uses only kubectl and the standard library: a
cached tool-image is used as a sleeping transport container and the selected
Greptime binary/configuration is copied into every role pod.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))
import workload_scheduler_benchmark as local
import workload_scheduler_report as report

DEFAULT_IMAGE = "docker.io/greptime/greptime-tool:20250606-04e3c7d"
ROLES = ("metasrv", "datanode-0", "datanode-1", "datanode-2", "frontend", "loadgen")


class Kubectl:
    def __init__(self, binary: str, context: str | None, namespace: str, log: list[dict[str, Any]]):
        self.binary, self.context, self.namespace, self.log = binary, context, namespace, log

    def run(self, args: list[str], *, input_text: str | None = None, check: bool = True, namespace: bool = True) -> str:
        cmd = [self.binary]
        if self.context:
            cmd += ["--context", self.context]
        if namespace:
            cmd += ["-n", self.namespace]
        cmd += args
        p = subprocess.run(cmd, input=input_text, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        self.log.append({"command": cmd, "returncode": p.returncode, "stdout": p.stdout[-4000:], "stderr": p.stderr[-4000:]})
        if check and p.returncode:
            raise RuntimeError(f"kubectl failed ({p.returncode}): {' '.join(cmd)}\n{p.stderr[-2000:]}")
        return p.stdout

    def json(self, args: list[str], *, namespace: bool = True) -> Any:
        return json.loads(self.run(args + ["-o", "json"], namespace=namespace))

    def cp(self, source: Path, pod: str, destination: str) -> None:
        self.run(["cp", str(source), f"{pod}:{destination}"])

    def exec(self, pod: str, command: str, *, check: bool = True) -> str:
        return self.run(["exec", pod, "--", "sh", "-ceu", command], check=check)


def safe_name(value: str) -> str:
    value = re.sub("[^a-z0-9-]+", "-", value.lower()).strip("-")
    return value[:45] or "scheduler"


def load_plan(generator: Path, case: Path) -> dict[str, Any]:
    return local.load_plan(generator, case)


def require_plan(plan: dict[str, Any]) -> dict[str, Any]:
    if plan.get("schema_version") != 1 or plan.get("scenario", {}).get("kind") != "workload_scheduler_distributed":
        raise ValueError("planner did not return workload_scheduler_distributed schema_version 1")
    s = plan["scenario"]
    topology = s.get("topology")
    expected = {"frontend": (1, "1", "2Gi"), "metasrv": (1, "1", "2Gi"), "datanode": (3, "2", "4Gi"), "loadgen": (1, "2", "4Gi")}
    if not isinstance(topology, dict):
        raise ValueError("normalized plan is missing topology")
    for role, contract in expected.items():
        actual = topology.get(role, {})
        if (actual.get("replicas"), actual.get("cpu"), actual.get("memory")) != contract:
            raise ValueError(f"normalized topology contract for {role} is not {contract}")
    if s.get("scheduler", {}).get("query_weight") != 2 or s.get("scheduler", {}).get("write_weight") != 8:
        raise ValueError("normalized scheduler must use fixed 2:8 weights")
    if s.get("target_order") != ["baseline", "scheduled", "scheduled", "baseline"]:
        raise ValueError("normalized target order must be explicit A/B/B/A")
    return s


def resources(cpu: str, memory: str) -> dict[str, Any]:
    return {"requests": {"cpu": cpu, "memory": memory}, "limits": {"cpu": cpu, "memory": memory}}


def pod_manifest(name: str, role: str, image: str, node: str | None, res: dict[str, Any]) -> dict[str, Any]:
    spec: dict[str, Any] = {"restartPolicy": "Never", "containers": [{"name": "main", "image": image, "imagePullPolicy": "Never", "command": ["sh", "-c", "trap : TERM INT; sleep infinity & wait"], "resources": res, "volumeMounts": [{"name": "work", "mountPath": "/work"}, {"name": "data", "mountPath": "/data"}, {"name": "logs", "mountPath": "/logs"}]}], "volumes": [{"name": "work", "emptyDir": {}}, {"name": "data", "emptyDir": {}}, {"name": "logs", "emptyDir": {}}]}
    if node:
        spec["nodeName"] = node
    return {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": name, "labels": {"app": "query-regression-scheduler", "role": role, "query-regression-owned": "true"}}, "spec": spec}


def service_manifest(name: str, selector: str, port: int) -> dict[str, Any]:
    return {"apiVersion": "v1", "kind": "Service", "metadata": {"name": name, "labels": {"query-regression-owned": "true"}}, "spec": {"selector": {"role": selector}, "ports": [{"port": port, "targetPort": port}]}}


def select_nodes(k: Kubectl) -> tuple[list[str], str | None]:
    data = k.json(["get", "nodes"], namespace=False)
    nodes = []
    for item in data.get("items", []):
        unsched = item.get("spec", {}).get("unschedulable", False)
        conditions = {x.get("type"): x.get("status") for x in item.get("status", {}).get("conditions", [])}
        if not unsched and conditions.get("Ready") == "True":
            nodes.append(item["metadata"]["name"])
    if len(nodes) < 3:
        raise ValueError("invalid environment: need at least three Ready schedulable worker nodes for datanodes")
    return nodes[:3], nodes[3] if len(nodes) > 3 else None


def config_for(s: dict[str, Any], target: str, node_id: int | None = None) -> str:
    scheduler = s["scheduler"]
    if node_id is not None:
        return f'''node_id = {node_id}
[http]
addr = "0.0.0.0:4000"
[grpc]
bind_addr = "0.0.0.0:3001"
server_addr = "datanode-{node_id - 1}:3001"
[meta_client]
metasrv_addrs = ["metasrv:3002"]
[storage]
data_home = "/data"
[runtime]
global_rt_size = {s['runtime']['global']}
compact_rt_size = {s['runtime']['compact']}
query_rt_size = {s['runtime']['query']}
ingest_rt_size = {s['runtime']['ingest']}
[runtime.experimental_workload_scheduler]
enable = {str(target == 'scheduled').lower()}
max_concurrent_polls = {scheduler['max_concurrent_polls']}
query_weight = 2
write_weight = 8
'''
    return '''[http]
addr = "0.0.0.0:4000"
[grpc]
bind_addr = "0.0.0.0:4001"
server_addr = "frontend:4001"
[internal_grpc]
bind_addr = "0.0.0.0:4010"
server_addr = "frontend:4010"
[meta_client]
metasrv_addrs = ["metasrv:3002"]
'''


def role_identity(k: Kubectl, pod: str) -> dict[str, Any]:
    item = k.json(["get", "pod", pod])
    c = item.get("spec", {}).get("containers", [{}])[0]
    return {"pod": pod, "uid": item.get("metadata", {}).get("uid"), "node": item.get("spec", {}).get("nodeName"), "image": c.get("image"), "resources": c.get("resources")}


def wait_pod(k: Kubectl, pod: str, timeout: int = 120) -> None:
    k.run(["wait", "--for=condition=Ready", f"pod/{pod}", f"--timeout={timeout}s"])


def write_worker_script(path: Path) -> None:
    # Use the existing stdlib request/event implementation.  The coordinator
    # performs each datanode scrape at the same absolute slots; raw streams are
    # endpoint scoped rather than a silently aggregated frontend scrape.
    path.write_text(r'''import json,sys,threading,time
from pathlib import Path
import workload_scheduler_runner as w
cfg=json.load(open(sys.argv[1])); out=Path(sys.argv[2]); out.mkdir(parents=True,exist_ok=True)
client=w.SqlClient('http://frontend:4000',cfg['database'],cfg['http_timeout'])
w.setup_table(client,cfg['data']['seed_rows'],cfg['data']['seed_batch_size'],cfg['data']['shards'],cfg['tables']['query']['name'],cfg['tables']['write']['name'],cfg['tables']['query']['partitions'],cfg['tables']['write']['partitions'],cfg['data']['seed_timestamp_millis'])
seq=w.Sequence(cfg['data']['write_sequence_start_millis'])
ok,_,body=client.sql(cfg['placement_sql'])
if not ok: raise RuntimeError('placement query failed: %s'%body)
# Greptime JSON output shape varies by protocol version; extract only peer-id
# values from returned rows and preserve original response for audit.
def ids(v):
 if isinstance(v,dict):
  return sum((ids(x) for x in v.values()),[])
 if isinstance(v,list): return sum((ids(x) for x in v),[])
 return [v] if isinstance(v,int) else []
Path(out/'placement.json').write_text(json.dumps({'query':cfg['placement_sql'],'response':body,'peer_ids':ids(body)},sort_keys=True))
for phase in cfg['phases']:
  sample=out/phase['name']; sample.mkdir(parents=True,exist_ok=True); start=time.monotonic()+cfg['warmup_seconds']; records={}
  def scrape(name):
    endpoint='http://%s:4000/metrics'%name; rows=[]
    for i in range(cfg['duration_seconds']+1):
      target=start+i; now=time.monotonic()
      if now<target: time.sleep(target-now)
      begun=time.monotonic()-start
      try:
       import urllib.request
       text=urllib.request.urlopen(endpoint,timeout=.25).read().decode(); done=time.monotonic()-start; status='success'; err=None
      except Exception as e: text=None; done=None; status='error'; err=str(e)
      d=sample/'metrics'/name; d.mkdir(parents=True,exist_ok=True); prom=d/('scrape-%03d.prom'%i)
      if text is not None: prom.write_text(text)
      rows.append({'offset':i,'start':begun,'completion':done,'status':status,'http_status':200 if text is not None else None,'error':err,'text':text,'path':str(prom.relative_to(sample)) if text is not None else None,'endpoint':endpoint})
    with open(sample/'metrics'/name/'scrapes.jsonl','w') as f:
      for row in rows: f.write(json.dumps(row,sort_keys=True)+'\n')
    records[name]=rows
  ts=[threading.Thread(target=scrape,args=(x,)) for x in cfg['datanodes']]
  [x.start() for x in ts]
  result=w.run_phase(client,phase['name'],cfg['duration_seconds'],cfg['warmup_seconds'],phase['query_workers'],phase['write_workers'],cfg['write']['batch_size'],phase['write_delay_seconds'],seq,'disabled',cfg['drain_timeout_seconds'],metrics_dir=sample,query_sql=cfg['query']['sql'],write_table=cfg['tables']['write']['name'],shards=cfg['data']['shards'])
  [x.join() for x in ts]
  result.update({'mode':cfg['target'],'target':cfg['target'],'phase':phase['name'],'iteration':cfg['iteration'],'artifact_dir':str(sample),'datanodes':cfg['datanodes'],'endpoint_scrapes':records})
  w.write_sample_json(sample,result)
''')


def evaluate_target(root: Path, s: dict[str, Any], target: str, iteration: int, identities: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], report.SchedulerEvaluation, report.RequestEvaluation]:
    samples: list[dict[str, Any]] = []
    local_gates: list[report.SchedulerEvaluation] = []
    thresholds = report.SchedulerGateThresholds(max_active_polls=s["scheduler"]["max_concurrent_polls"], max_failure_rate=s["gates"]["max_failure_rate"], max_outstanding_requests=s["gates"]["max_outstanding_requests"], write_share_min=s["gates"]["dual_backlog_lower"], write_share_max=s["gates"]["dual_backlog_upper"], min_dual_backlog_interval_fraction=s["gates"]["min_dual_backlog_interval_fraction"], min_dual_backlog_polls_per_class=s["gates"]["min_dual_backlog_polls_per_class"], single_class_purity_min_share=s["gates"]["min_single_class_active_purity"], min_light_write_query_share=s["gates"]["min_light_write_query_share"])
    for phase in s["phases"]:
        sample_dir = root / phase["name"]
        raw = json.loads((sample_dir / "sample.json").read_text())
        raw["datanode_identity"] = identities
        samples.append(raw)
        if target == "scheduled":
            phases_by_node: dict[str, report.SchedulerSummary | None] = {}
            for ident in identities:
                p = sample_dir / "metrics" / ident["pod"] / "scrapes.jsonl"
                if not p.exists():
                    phases_by_node[ident["pod"]] = None
                    continue
                snaps = [report.parse_scheduler_metrics(json.loads(x)["text"]) for x in p.read_text().splitlines() if x and json.loads(x).get("status") == "success"]
                phases_by_node[ident["pod"]] = report.summarize_scheduler_metrics(snaps) if len(snaps) == s["expected_scrape_count"] else None
            # Keep complete phase maps per node so one failing datanode cannot
            # be masked by aggregate admission counts.
            for node, summary in phases_by_node.items():
                node_maps = {p["name"]: None for p in s["phases"]}
                node_maps[phase["name"]] = summary
                # Accumulate below in sample metadata.
                raw.setdefault("node_phase_summaries", {})[node] = {phase["name"]: summary}
    if target != "scheduled":
        mechanism = report.SchedulerEvaluation("passed", True, 0, ())
    else:
        failures: list[str] = []
        for ident in identities:
            phases: dict[str, report.SchedulerSummary | None] = {}
            for phase in s["phases"]:
                p = root / phase["name"] / "metrics" / ident["pod"] / "scrapes.jsonl"
                if not p.exists(): phases[phase["name"]] = None; continue
                rows=[json.loads(x) for x in p.read_text().splitlines() if x]
                if len(rows) != s["expected_scrape_count"] or any(x.get("status") != "success" for x in rows): phases[phase["name"]]=None; continue
                phases[phase["name"]]=report.summarize_scheduler_metrics([report.parse_scheduler_metrics(x["text"]) for x in rows])
            ev=report.evaluate_scheduler_report(phases, thresholds)
            if ev.status != "passed": failures.extend(f"{ident['pod']}: {x}" for x in ev.failures)
        mechanism = report.SchedulerEvaluation("failed" if failures else "passed", not failures, 1 if failures else 0, tuple(failures))
    req=report.evaluate_request_validity(samples, s["gates"]["max_failure_rate"], s["gates"]["max_outstanding_requests"])
    return samples, mechanism, req


def run_target(k: Kubectl, root: Path, s: dict[str, Any], target: str, iteration: int, binary: Path, image: str) -> dict[str, Any]:
    dns = {"metasrv": "metasrv", "frontend": "frontend", "datanode-0": "datanode-0", "datanode-1": "datanode-1", "datanode-2": "datanode-2"}
    dn_nodes, load_node = select_nodes(k)
    manifests = root / "manifests"; manifests.mkdir(parents=True, exist_ok=True)
    t = s["topology"]
    objects = [service_manifest("metasrv", "metasrv", 3002), service_manifest("frontend", "frontend", 4000)]
    for i in range(3): objects.append(service_manifest(f"datanode-{i}", f"datanode-{i}", 4000))
    objects += [pod_manifest("metasrv", "metasrv", image, None, resources("1", "2Gi"))]
    objects += [pod_manifest(f"datanode-{i}", f"datanode-{i}", image, dn_nodes[i], resources("2", "4Gi")) for i in range(3)]
    objects += [pod_manifest("frontend", "frontend", image, None, resources("1", "2Gi")), pod_manifest("loadgen", "loadgen", image, load_node, resources("2", "4Gi"))]
    rendered = "---\n".join(json.dumps(x) for x in objects)
    (manifests / "cluster.json").write_text(rendered)
    k.run(["apply", "-f", "-"], input_text=rendered)
    for pod in ROLES: wait_pod(k, pod)
    for pod in ROLES[:-1]:
        k.cp(binary, pod, "/work/greptime")
        k.exec(pod, "chmod +x /work/greptime")
    cfg = root / "configs"; cfg.mkdir(exist_ok=True)
    for i in range(3):
        p=cfg/f"datanode-{i}.toml"; p.write_text(config_for(s,target,i+1)); k.cp(p,f"datanode-{i}","/work/config.toml")
    p=cfg/"frontend.toml"; p.write_text(config_for(s,target)); k.cp(p,"frontend","/work/config.toml")
    k.exec("metasrv", "/work/greptime metasrv start --backend memory --data-home /data --grpc-bind-addr 0.0.0.0:3002 --grpc-server-addr metasrv:3002 --http-addr 0.0.0.0:4000 > /logs/greptime.log 2>&1 &")
    time.sleep(2)
    for i in range(3): k.exec(f"datanode-{i}", "/work/greptime datanode start --config-file /work/config.toml --data-home /data > /logs/greptime.log 2>&1 &")
    time.sleep(4)
    k.exec("frontend", "/work/greptime frontend start --config-file /work/config.toml > /logs/greptime.log 2>&1 &")
    time.sleep(4)
    identities=[role_identity(k,f"datanode-{i}") for i in range(3)]
    if len({x["uid"] for x in identities}) != 3 or len({x["node"] for x in identities}) != 3:
        raise ValueError("invalid environment: datanode pod UID/node identities are not three stable distinct values")
    # Persist cgroup/Kubernetes pressure diagnostics without turning ambient
    # noise into an undeclared gate.
    for ident in identities:
        diag = root / "environment" / ident["pod"]
        diag.mkdir(parents=True, exist_ok=True)
        (diag / "cgroup.txt").write_text(k.exec(ident["pod"], "cat /sys/fs/cgroup/cpu.stat; cat /sys/fs/cgroup/memory.current", check=False))
    (root / "environment").mkdir(exist_ok=True)
    (root / "environment" / "kubectl-top.txt").write_text(k.run(["top", "pod"], check=False))
    worker=root/"distributed_loadgen.py"; write_worker_script(worker); k.cp(HERE/"workload_scheduler_runner.py","loadgen","/work/workload_scheduler_runner.py"); k.cp(worker,"loadgen","/work/distributed_loadgen.py")
    job={**s,"target":target,"iteration":iteration,"http_timeout":60,"datanodes":[x["pod"] for x in identities]}
    # The SQL result is retained as placement evidence.  The coordinator
    # invalidates if its JSON response does not prove all three local node ids.
    placement_sql = "SELECT DISTINCT peer_id FROM information_schema.region_peers WHERE table_name IN ('%s','%s')" % (s["tables"]["query"]["name"], s["tables"]["write"]["name"])
    job["placement_sql"] = placement_sql
    local_cfg=root/"loadgen.json"; local_cfg.write_text(json.dumps(job)); k.cp(local_cfg,"loadgen","/work/loadgen.json")
    k.exec("loadgen", "cd /work && python3 distributed_loadgen.py loadgen.json /work/artifacts")
    output=root/"samples"; output.mkdir(exist_ok=True); k.run(["cp", "loadgen:/work/artifacts/.", str(output)])
    samples, mechanism, req=evaluate_target(output,s,target,iteration,identities)
    placement = output / "placement.json"
    if not placement.exists():
        raise ValueError("invalid placement evidence: load generator did not persist region ownership")
    placement_data = json.loads(placement.read_text())
    peers = {int(x) for x in placement_data.get("peer_ids", []) if isinstance(x, (int, float))}
    if not {1, 2, 3}.issubset(peers):
        raise ValueError(f"invalid placement evidence: all three datanodes must own regions, got {sorted(peers)}")
    for pod in ROLES:
        for what, suffix in [("/logs/greptime.log","log"), ("","describe")]:
            dest=root/"logs"/pod; dest.mkdir(parents=True,exist_ok=True)
            if suffix == "log": k.run(["cp",f"{pod}:{what}",str(dest/f"{pod}.log")],check=False)
            else: (dest/f"{pod}.describe.txt").write_text(k.run(["describe","pod",pod],check=False))
    return {"target":target,"iteration":iteration,"samples":samples,"mechanism":mechanism,"request":req,"identities":identities,"node_assignments":{"datanodes":dn_nodes,"loadgen":load_node}}


def parse_args() -> argparse.Namespace:
    p=argparse.ArgumentParser(description="Integrated distributed workload scheduler query-regression executor")
    p.add_argument("--case",type=Path,required=True); p.add_argument("--fixture-generator",type=Path,required=True); p.add_argument("--base-bin",type=Path,required=True); p.add_argument("--candidate-bin",type=Path,required=True); p.add_argument("--work-dir",type=Path,required=True)
    p.add_argument("--kubectl",default=os.environ.get("KUBECTL","kubectl")); p.add_argument("--context",default=os.environ.get("KUBE_CONTEXT")); p.add_argument("--namespace-prefix",default=os.environ.get("QUERY_REGRESSION_NAMESPACE_PREFIX","query-regression-scheduler")); p.add_argument("--base-image",default=os.environ.get("QUERY_REGRESSION_BASE_IMAGE",DEFAULT_IMAGE)); p.add_argument("--same-binary-ab",action="store_true"); p.add_argument("--keep-namespace-on-failure",action="store_true"); return p.parse_args()


def main() -> int:
    a=parse_args(); a.work_dir.mkdir(parents=True,exist_ok=True); commands=[]; namespace=f"{safe_name(a.namespace_prefix)}-{uuid.uuid4().hex[:10]}"; k=Kubectl(a.kubectl,a.context,namespace,commands); started=time.time(); result: dict[str,Any]={"case_path":str(a.case.resolve()),"query_mode":"workload_scheduler_distributed","namespace":namespace,"context":a.context,"status":"error","targets":[],"kubectl_commands":commands,"base_image":a.base_image,"same_binary_ab":a.same_binary_ab}
    cleanup_error=None
    try:
        s=require_plan(load_plan(a.fixture_generator,a.case)); result["scenario"]=s
        # A/B/B/A is represented by pair iterations: even B/A, odd A/B.  Each
        # target gets a fresh owned namespace/cluster; clusters never overlap.
        all_samples=[]; mechanisms=[]; requests=[]; perf_by_iteration=[]
        for iteration in range(s["iterations"]):
            order=["baseline","scheduled"] if iteration%2==0 else ["scheduled","baseline"]
            cells={}
            for target in order:
                root=a.work_dir/f"iteration-{iteration+1:02d}"/target; root.mkdir(parents=True,exist_ok=True)
                binary=a.candidate_bin if a.same_binary_ab or target=="scheduled" else a.base_bin
                target_namespace = f"{safe_name(a.namespace_prefix)}-{iteration + 1}-{target[:3]}-{uuid.uuid4().hex[:8]}"
                k.namespace = target_namespace
                k.run(["create", "namespace", target_namespace], namespace=False)
                target_failed = False
                try:
                    entry=run_target(k,root,s,target,iteration+1,binary,a.base_image)
                    result["targets"].append({"name":target,"iteration":iteration+1,"namespace":target_namespace,"binary":str(binary),"scheduler_enabled":target=="scheduled","datanodes":entry["identities"],"node_assignments":entry["node_assignments"]})
                    all_samples += entry["samples"]; mechanisms.append(entry["mechanism"]); requests.append(entry["request"])
                    cells[target]={x["name"]:{"query_rps":x["requests"]["query"]["successful_rps"],"write_rps":x["requests"]["write"]["successful_rps"]} for x in entry["samples"]}
                except Exception:
                    target_failed = True
                    raise
                finally:
                    if not (a.keep_namespace_on_failure and target_failed):
                        try:
                            k.run(["delete", "namespace", target_namespace, "--wait=true"], namespace=False)
                        except Exception as e:
                            cleanup_error = repr(e)
            perf_by_iteration.append((iteration+1,{phase:{"baseline":cells["baseline"].get(phase,{}),"scheduled":cells["scheduled"].get(phase,{})} for phase in [x["name"] for x in s["phases"]]}))
        mech_fail=[f for e in mechanisms for f in e.failures]; mechanism=report.SchedulerEvaluation("failed" if mech_fail else "passed",not mech_fail,1 if mech_fail else 0,tuple(mech_fail))
        req_fail=[f for e in requests for f in e.failures]; req_status="invalid" if any(e.status=="invalid" for e in requests) else "failed" if req_fail else "passed"; req=report.RequestEvaluation(req_status,tuple(req_fail))
        perf=report.evaluate_performance(perf_by_iteration,[x["name"] for x in s["phases"]],s["gates"]["max_capacity_normalized_regression_pct"])
        canonical=report.build_report(config={"same_binary_ab":a.same_binary_ab,"code_version_confound":not a.same_binary_ab,"scheduler_role":"datanode_only"},samples=all_samples,request_eval=req,mechanism_eval=mechanism,performance_evals=perf)
        result.update(canonical); result["status"]={"passed":"ok","failed":"failed","invalid":"invalid","error":"error"}[canonical["status"]]
    except Exception as e:
        result["error"]=repr(e); result["status"]="error"
    finally:
        if cleanup_error: result["cleanup_error"]=cleanup_error; result["status"]="error"
        result["elapsed_seconds"]=time.time()-started
        (a.work_dir/"query-regression-report.json").write_text(json.dumps(result,indent=2,sort_keys=True)+"\n")
        print(json.dumps(result,indent=2,sort_keys=True))
    return 0 if result["status"] == "ok" else 1

if __name__ == "__main__": raise SystemExit(main())
