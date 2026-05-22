#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="quant-titans"
NAMESPACE="quant-titans"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
HELM_DIR="${SCRIPT_DIR}/../helm/platform"
VALUES_FILE="${SCRIPT_DIR}/local-values.yaml"
CLUSTER_CONFIG="${SCRIPT_DIR}/cluster.yaml"
LOCAL_REGISTRY="quant-titans"
SERVICES=(sandbox botfleet telemetry leaderboard)

echo "==> [1/6] Creating kind cluster '${CLUSTER_NAME}'..."
if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
  echo "  (cluster already exists — skipping create)"
else
  kind create cluster --name "${CLUSTER_NAME}" --config "${CLUSTER_CONFIG}"
fi

echo "==> [2/6] Labelling worker nodes (general x2, sandbox x1)..."
WORKERS=($(kubectl get nodes --no-headers -o custom-columns=':metadata.name' \
  | grep -v control-plane))
if [ "${#WORKERS[@]}" -lt 3 ]; then
  echo "ERROR: expected 3 worker nodes, got ${#WORKERS[@]}" >&2
  exit 1
fi
kubectl label node --overwrite "${WORKERS[0]}" role=general
kubectl label node --overwrite "${WORKERS[1]}" role=general
kubectl label node --overwrite "${WORKERS[2]}" role=sandbox
echo "  general: ${WORKERS[0]}, ${WORKERS[1]}"
echo "  sandbox: ${WORKERS[2]}"

echo "==> [3/6] Building service images and loading into kind..."
for svc in "${SERVICES[@]}"; do
  echo "  building ${LOCAL_REGISTRY}/${svc}:latest ..."
  docker build -t "${LOCAL_REGISTRY}/${svc}:latest" "${REPO_ROOT}/${svc}/"
done

# Load only locally-built service images; infra images (redpanda, timescaledb, redis)
# are pulled by kubelet directly from Docker Hub — kind load fails on multi-platform
# manifest indexes when only one platform's blobs are cached locally (Apple Silicon).
kind load docker-image \
  "${LOCAL_REGISTRY}/sandbox:latest" \
  "${LOCAL_REGISTRY}/botfleet:latest" \
  "${LOCAL_REGISTRY}/telemetry:latest" \
  "${LOCAL_REGISTRY}/leaderboard:latest" \
  --name "${CLUSTER_NAME}"
echo "  ✓ service images loaded (infra images pulled by kubelet on demand)"

echo "==> [4/6] Adding Redpanda Helm repo..."
helm repo add redpanda https://charts.redpanda.com 2>/dev/null || true
helm repo update --fail-on-repo-update-fail 2>/dev/null || helm repo update

echo "==> [5/6] Updating Helm chart dependencies..."
helm dependency update "${HELM_DIR}"

echo "==> [6/6] Deploying platform (namespace: ${NAMESPACE})..."
helm upgrade --install "${CLUSTER_NAME}" "${HELM_DIR}" \
  --namespace "${NAMESPACE}" --create-namespace \
  -f "${VALUES_FILE}" \
  --wait --timeout 20m

echo ""
echo "════════════════════════════════════════════════"
echo "  ✓ Platform live on kind cluster"
echo ""
echo "  Leaderboard  → http://localhost:8082"
echo "  Sandbox API  → http://localhost:8080"
echo "  Botfleet     → http://localhost:9091"
echo ""
echo "  kubectl get pods -n ${NAMESPACE}"
echo "════════════════════════════════════════════════"
