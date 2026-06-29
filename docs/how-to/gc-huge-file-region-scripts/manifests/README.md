# GC huge-file stress lab manifests

This directory contains **review-only** Kubernetes/Helm manifests for the
`gc-stress-test` namespace.

Do not apply these resources until the plan is reviewed and explicitly approved.

## Target namespace

- Namespace: `gc-stress-test`
- Purpose: isolated GreptimeDB GC huge-file stress lab
- Object-store bucket: `gc-stress-bucket`
- Object-store root prefix: `gc-hf-lab`
- MinIO endpoint for GreptimeDB: `http://minio.gc-stress-test.svc.cluster.local:9000`

## Files

- `00-namespace-labels.yaml` — labels/annotations for the already-created namespace.
- `01-resourcequota.yaml` — guardrails for CPU/memory/PVC/storage/pod count.
- `02-limitrange.yaml` — default requests/limits for containers and PVCs.
- `minio-values.yaml` — Helm values for a single-pod standalone MinIO release using the Greptime registry's mirrored Bitnami MinIO chart.
- `etcd-values.yaml` — Helm values for a single-pod etcd release.
- `greptimedb-cluster-values.yaml` — Helm values for a minimal non-HA GreptimeDB cluster using MinIO object storage and GC enabled for the lab.
- `commands.md` — commands to run only after explicit approval.

## Review checklist before apply

- Confirm `gc-stress-test` is the intended namespace.
- Confirm the resource quota fits the office cluster. The current draft assumes
  a larger lab envelope for high file-count pressure: 24 CPU / 96Gi memory limit
  quota and 700Gi storage quota.
- Confirm `gc-stress-bucket` and `gc-hf-lab` are acceptable isolated names.
- Replace `CHANGE_ME_GC_STRESS_MINIO_ROOT_PASSWORD` before installing, or inject
  an equivalent lab-only credential with Helm `--set`/Kubernetes secret tooling.
- Confirm GreptimeDB image/tag defaults are acceptable or choose explicit image values.
- Confirm the GreptimeDB cluster chart plan is acceptable:
  - chart: `greptime/greptimedb-cluster`, version `0.8.21`
  - reviewed local fallback chart: `/home/discord9/.cache/helm/repository/greptimedb-cluster-0.8.21.tgz`
  - default image rendered by this chart: `docker.io/greptime/greptimedb:v1.1.1`
  - datanode local PVC: `20Gi`, `local-path`
  - `flownode` disabled for this GC lab
- Monitoring is enabled through chart `monitoring.standalone`, but the current
  shared operator CRD rejects the chart's rendered `spec.monitoring.ttl` field.
  Use `scripts/drop_monitoring_ttl_post_renderer.py` with Helm until the CRD is
  upgraded or the chart template is fixed.
- Confirm using the Greptime registry MinIO chart/image is acceptable:
  - chart: `oci://greptime-registry.cn-hangzhou.cr.aliyuncs.com/charts/minio`, version `16.0.10`
  - reviewed local fallback chart: `/home/discord9/.cache/helm/repository/minio-16.0.10.tgz`
  - image: `greptime-registry.cn-hangzhou.cr.aliyuncs.com/bitnami/minio:2025.4.22-debian-12-r1`
- Confirm pinning MinIO to `minipc-6` is acceptable. Read-only PV allocation
  summary showed `minipc-6` had the lowest currently requested local-path PV
  total among the listed nodes, but this is not the same as live filesystem free
  space.
- Confirm no shared/prod MinIO or bucket is referenced.
- Do not install or upgrade the cluster-shared GreptimeDB operator as part of
  this isolated lab; perform read-only checks at most.
