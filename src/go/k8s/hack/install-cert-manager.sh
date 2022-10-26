#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

VERSION="0.2.0"

# TODO: support more OS/architectures as they are needed
if [ "$(kubectl get deploy --sort-by=.metadata.name --namespace cert-manager -o=jsonpath='{.items[*].metadata.name}')" = "cert-manager cert-manager-cainjector cert-manager-webhook" ]; then
  echo "cert manager already installed"
  exit 0
fi

mkdir -p ./bin

if [ "$(uname)" == 'Darwin' ]; then
  curl -L https://github.com/alenkacz/cert-manager-verifier/releases/download/v"${VERSION}"/cert-manager-verifier_"${VERSION}"_Darwin_x86_64.tar.gz | tar -xvf - -C ./bin
else
  curl -L https://github.com/alenkacz/cert-manager-verifier/releases/download/v"${VERSION}"/cert-manager-verifier_"${VERSION}"_Linux_x86_64.tar.gz | tar -xzvf - -C ./bin
fi

kubectl apply -f https://github.com/jetstack/cert-manager/releases/download/v1.4.4/cert-manager.yaml
./bin/cm-verifier --timeout 5m
