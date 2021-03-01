#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail

VERSION="0.1.0"

# TODO: support more OS/architectures as they are needed
if [ "$(uname)" == 'Darwin' ]; then
  curl -L https://github.com/alenkacz/cert-manager-verifier/releases/download/v"${VERSION}"/cert-manager-verifier_"${VERSION}"_Darwin_x86_64.tar.gz | tar -xvf - -C ./bin
else
  curl -L https://github.com/alenkacz/cert-manager-verifier/releases/download/v"${VERSION}"/cert-manager-verifier_"${VERSION}"_Linux_x86_64.tar.gz | tar -xzvf - -C ./bin
fi

kubectl apply -f https://github.com/jetstack/cert-manager/releases/download/v1.2.0/cert-manager.yaml
./bin/cm-verifier
