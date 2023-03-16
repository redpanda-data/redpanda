#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail
set -x

# This script tries to create sample cluster and waits until it succeeds
# this way we are able to wait until webhook is ready to serve our content

if ! command -v kubectl &>/dev/null; then
  echo "required kubectl command NOT found"
  exit 1
fi

MAX=50
CURRENT=0

until kubectl create -f ./config/samples/one_node_cluster.yaml >/dev/null 2>&1; do
  CURRENT=$((CURRENT + 1))
  printf '.'
  sleep 5

  if [ "$CURRENT" -gt "$MAX" ]; then
    echo "FAILED: Webhook not ready, giving up."
    exit 1
  fi
done

echo "Webhook is ready!"

kubectl delete -f ./config/samples/one_node_cluster.yaml
