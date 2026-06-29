# Commands for review only

Do **not** run these commands until explicitly approved.

All commands target the dedicated namespace `gc-stress-test` except the shared
GreptimeDB operator install in `greptimedb-admin`.

```bash
# 0. Read-only sanity checks
kubectl get namespace gc-stress-test
kubectl -n gc-stress-test get all,pvc,secret,configmap,resourcequota,limitrange

# 1. Namespace labels and guardrails
kubectl apply -f docs/how-to/gc-huge-file-region-scripts/manifests/00-namespace-labels.yaml
kubectl apply -f docs/how-to/gc-huge-file-region-scripts/manifests/01-resourcequota.yaml
kubectl apply -f docs/how-to/gc-huge-file-region-scripts/manifests/02-limitrange.yaml

# 2. Helm repositories
helm repo add greptime https://greptimeteam.github.io/helm-charts/
helm repo update greptime

# 3. Single-pod MinIO in gc-stress-test
helm upgrade --install minio oci://greptime-registry.cn-hangzhou.cr.aliyuncs.com/charts/minio \
  -n gc-stress-test \
  --values docs/how-to/gc-huge-file-region-scripts/manifests/minio-values.yaml \
  --version 16.0.10 \
  --wait \
  --timeout 10m

# If the OCI registry times out after the chart has already been cached locally,
# use the reviewed cached chart tarball instead of changing values/images:
helm upgrade --install minio /home/discord9/.cache/helm/repository/minio-16.0.10.tgz \
  -n gc-stress-test \
  --values docs/how-to/gc-huge-file-region-scripts/manifests/minio-values.yaml \
  --wait \
  --timeout 10m

# 4. Single-pod etcd in gc-stress-test
helm upgrade --install etcd oci://registry-1.docker.io/bitnamicharts/etcd \
  -n gc-stress-test \
  --values docs/how-to/gc-huge-file-region-scripts/manifests/etcd-values.yaml \
  --version 12.0.8 \
  --wait

# 5. GreptimeDB operator is cluster-shared; do not install it for this lab.
# Optional read-only checks only, if needed:
kubectl -n greptimedb-admin get deploy,pod
kubectl get crd | grep greptime

# 6. Minimal GreptimeDB lab cluster in gc-stress-test
helm upgrade --install gc-stress-greptimedb greptime/greptimedb-cluster \
  -n gc-stress-test \
  --values docs/how-to/gc-huge-file-region-scripts/manifests/greptimedb-cluster-values.yaml \
  --version 0.8.21 \
  --post-renderer docs/how-to/gc-huge-file-region-scripts/scripts/drop_monitoring_ttl_post_renderer.py \
  --wait \
  --timeout 10m \
  --wait-for-jobs

# If GitHub chart download times out after the chart has already been cached locally,
# use the reviewed cached chart tarball instead of changing values/images:
helm upgrade --install gc-stress-greptimedb /home/discord9/.cache/helm/repository/greptimedb-cluster-0.8.21.tgz \
  -n gc-stress-test \
  --values docs/how-to/gc-huge-file-region-scripts/manifests/greptimedb-cluster-values.yaml \
  --post-renderer docs/how-to/gc-huge-file-region-scripts/scripts/drop_monitoring_ttl_post_renderer.py \
  --wait \
  --timeout 10m \
  --wait-for-jobs

# 7. Read-only post-install checks
kubectl -n gc-stress-test get all,pvc,secret,configmap,resourcequota,limitrange
kubectl -n gc-stress-test get gtc
kubectl -n gc-stress-test get pods --show-labels
```

## Stop conditions

- Any command points outside `gc-stress-test` except the operator install in
  `greptimedb-admin`.
- MinIO endpoint is not `http://minio.gc-stress-test.svc.cluster.local:9000`.
- Object storage bucket/root is not `gc-stress-bucket` / `gc-hf-lab`.
- ResourceQuota does not fit the office cluster.
- `local-path` pinning to `minipc-6` is not acceptable or the node is unhealthy.
- Any command attempts to install or upgrade the shared GreptimeDB operator.
