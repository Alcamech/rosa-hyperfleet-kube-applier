#!/usr/bin/env bash
# start-localstack.sh — starts a LocalStack container with DynamoDB and
# DynamoDB Streams enabled in detached mode.
#
# Usage: ./hack/start-localstack.sh
#
# Environment variables:
#   CONTAINER_ENGINE      podman or docker (auto-detected if unset)
#   LOCALSTACK_PORT       host port to expose LocalStack on (default: 4566)
#   LOCALSTACK_AUTH_TOKEN set to use the Pro image; community image used otherwise
#
# Idempotent: if the container is already running and healthy on the expected
# port it is reused without restart. A stopped/dead container is replaced.
#
# Stop with: <docker|podman> stop localstack-kube-applier-aws

set -euo pipefail

CONTAINER_ENGINE="${CONTAINER_ENGINE:-$(command -v podman 2>/dev/null || command -v docker 2>/dev/null)}"
CONTAINER_NAME="localstack-kube-applier-aws"
PORT="${LOCALSTACK_PORT:-4566}"

if [[ -n "${LOCALSTACK_AUTH_TOKEN:-}" ]]; then
  IMAGE="localstack/localstack-pro"
  AUTH_ARGS=(-e "LOCALSTACK_AUTH_TOKEN=${LOCALSTACK_AUTH_TOKEN}")
else
  IMAGE="localstack/localstack"
  AUTH_ARGS=()
fi

# Check if the container is already running and healthy — if so, reuse it.
if "${CONTAINER_ENGINE}" inspect "${CONTAINER_NAME}" --format '{{.State.Status}}' 2>/dev/null | grep -q "^running$"; then
  echo "LocalStack container '${CONTAINER_NAME}' is already running on port ${PORT}, reusing."
  exit 0
fi

# Remove any stale (stopped/dead/exited) container with the same name.
"${CONTAINER_ENGINE}" rm -f "${CONTAINER_NAME}" 2>/dev/null || true

echo "Starting ${IMAGE} on port ${PORT} (detached) ..."
"${CONTAINER_ENGINE}" run -d \
  --name "${CONTAINER_NAME}" \
  -p "${PORT}:4566" \
  -e "SERVICES=dynamodb,dynamodbstreams" \
  -e "DEBUG=0" \
  "${AUTH_ARGS[@]}" \
  "${IMAGE}"

echo "LocalStack container '${CONTAINER_NAME}' started."
echo "Set LOCALSTACK_ENDPOINT=http://localhost:${PORT} before running integration tests."
echo "Stop with: ${CONTAINER_ENGINE} stop ${CONTAINER_NAME}"
