#!/bin/bash

set -e

cluster=$1
message=$2

if [[ $# -ne 2 ]]; then
  echo "Usage: verify-condition-message.sh cluster message"
  exit 1
fi

if [[ -z $NAMESPACE ]]; then
  echo "Error: no NAMESPACE env defined"
  exit 1
fi

kubectl -n $NAMESPACE get cluster $cluster -o jsonpath='{.status.conditions[*].message}' | grep $message >/dev/null 2>&1
