#!/usr/bin/env bash
set -xeuo pipefail

KIND_CLUSTER_NAME="$1"
kubectl annotate "node/${KIND_CLUSTER_NAME}-worker" topology.kubernetes.io/zone=rack1
kubectl annotate "node/${KIND_CLUSTER_NAME}-worker2" topology.kubernetes.io/zone=rack2
kubectl annotate "node/${KIND_CLUSTER_NAME}-worker3" topology.kubernetes.io/zone=rack3
kubectl annotate "node/${KIND_CLUSTER_NAME}-worker4" topology.kubernetes.io/zone=rack4
