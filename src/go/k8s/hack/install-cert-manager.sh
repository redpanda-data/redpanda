#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

CERT_MANAGER_VERSION="v1.11.0"
CERT_MANAGER_VERIFIER_VERSION="0.3.0"

# TODO: support more OS/architectures as they are needed
if [ "$(kubectl get deploy --sort-by=.metadata.name --namespace cert-manager -o=jsonpath='{.items[*].metadata.name}')" = "cert-manager cert-manager-cainjector cert-manager-webhook" ]; then
  echo "cert manager already installed"
  exit 0
fi

mkdir -p ./bin

CURL_OUTPUT="${1:-.}"

if [ "$(uname)" == 'Darwin' ]; then
  curl -Lv https://github.com/alenkacz/cert-manager-verifier/releases/download/v"${CERT_MANAGER_VERIFIER_VERSION}"/cert-manager-verifier_"${CERT_MANAGER_VERIFIER_VERSION}"_Darwin_x86_64.tar.gz 2>"${CURL_OUTPUT}"/cert-manager-verifier-download-output.txt | tar -xvf - -C ./bin
else
  curl -Lv https://github.com/alenkacz/cert-manager-verifier/releases/download/v"${CERT_MANAGER_VERIFIER_VERSION}"/cert-manager-verifier_"${CERT_MANAGER_VERIFIER_VERSION}"_Linux_x86_64.tar.gz 2>"${CURL_OUTPUT}"/cert-manager-verifier-download-output.txt | tar -xzvf - -C ./bin
fi

HOME=$(mktemp -d)
helm repo add jetstack https://charts.jetstack.io
helm repo update
helm install cert-manager jetstack/cert-manager --version "${CERT_MANAGER_VERSION}" --set installCRDs=true --namespace cert-manager --create-namespace
./bin/cm-verifier --timeout 5m
