#!/bin/bash

set -x

script_namespace="${1:-$NAMESPACE}"

if [[ -z $script_namespace ]]; then
  echo "Error: no NAMESPACE env defined nor script first argument"
  exit 1
fi

ARTIFACTS_PATH=../../_e2e_artifacts/$TEST_NAME
mkdir -p $ARTIFACTS_PATH
kubectl -n $script_namespace get pods -o yaml >$ARTIFACTS_PATH/pods.yaml

for cl in $(kubectl -n $script_namespace get cluster --output=jsonpath='{.items..metadata.name}'); do
  replication_factor=$(kubectl -n $script_namespace get cluster $cl --output=jsonpath='{.spec.replicas}')

  tls_enabled=$(kubectl -n $script_namespace get cluster $cl --output=jsonpath='{.spec.configuration.adminApi[0].tls.enabled}')
  curl_arguments="-s http"
  if [ $tls_enabled = "true" ]; then
    curl_arguments="-sk https"
  fi

  mtls_enabled=$(kubectl -n $script_namespace get cluster $cl --output=jsonpath='{.spec.configuration.adminApi[0].tls.requireClientAuth}')
  if [ $mtls_enabled = "true" ]; then
    curl_arguments="-sk --cert /etc/tls/certs/admin/tls.crt --key /etc/tls/certs/admin/tls.key https"
  fi

  kubectl -n $script_namespace get $cl -o yaml >$ARTIFACTS_PATH/$cl.yaml

  i=0
  while [[ $i -lt $replication_factor ]]; do
    kubectl -n $script_namespace exec -c redpanda $cl-$i -- curl $curl_arguments://$cl-$i.$cl.$script_namespace.svc.cluster.local.:9644/v1/brokers >$ARTIFACTS_PATH/brokers-from-pod-$i.json
    kubectl -n $script_namespace exec -c redpanda $cl-$i -- curl $curl_arguments://$cl-$i.$cl.$script_namespace.svc.cluster.local.:9644/v1/cluster_config/status >$ARTIFACTS_PATH/config-status-from-pod-$i.json
    kubectl -n $script_namespace logs -c redpanda $cl-$i >$ARTIFACTS_PATH/logs-from-pod-$i.txt
    kubectl -n $script_namespace logs -c redpanda $cl-$i -p >$ARTIFACTS_PATH/logs-from-previous-pod-$i.txt
    ((i = i + 1))
  done
done

mkdir -p $ARTIFACTS_PATH/exported-logs/
kind export logs --name kind $ARTIFACTS_PATH/exported-logs/
