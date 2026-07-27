#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${KIND_CLUSTER_NAME:-kube-applier-dev}"
NAMESPACE="kube-applier-system"
KIND_KUBECONFIG="${KIND_KUBECONFIG:-/tmp/${CLUSTER_NAME}.kubeconfig}"

if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
  echo "Kind cluster '${CLUSTER_NAME}' already exists, reusing it."
else
  echo "Creating Kind cluster '${CLUSTER_NAME}'..."
  kind create cluster --name "${CLUSTER_NAME}" --kubeconfig "${KIND_KUBECONFIG}" --wait 60s
fi

# Always export so kubectl uses the isolated kubeconfig, not ~/.kube/config.
export KUBECONFIG="${KIND_KUBECONFIG}"

kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f "$(dirname "$0")/manifests/rbac.yaml"

echo ""
echo "Kind cluster '${CLUSTER_NAME}' is ready."
echo "Kubeconfig: ${KIND_KUBECONFIG}"
echo "Kubeconfig context: kind-${CLUSTER_NAME}"
echo "Leader election namespace: ${NAMESPACE}"
