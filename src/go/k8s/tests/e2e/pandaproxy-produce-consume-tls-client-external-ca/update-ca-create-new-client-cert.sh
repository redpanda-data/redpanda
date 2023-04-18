#!/bin/bash

set -e

KEY_FILE=/tmp/client.key.$$
CRT_FILE=/tmp/client.crt.$$
CN=e2e-test-proy-client1
SECRET_NAME="client-cert1"

# Create self signed cert and key
openssl req -x509 -newkey rsa:2048 -sha256 -days 36500 -nodes -keyout $KEY_FILE -out $CRT_FILE -subj "/CN=$CN.redpanda.com"
kubectl create -n $NAMESPACE secret generic $SECRET_NAME --from-file=client\.key=$KEY_FILE --from-file=client\.crt=$CRT_FILE

# Update the external CA cert secret with the new self signed CA
SECRET_NAME="ca-cert"
kubectl create -n $NAMESPACE secret generic $SECRET_NAME --from-file=ca\.crt=$CRT_FILE --save-config --dry-run=client -o yaml | kubectl apply -f -

rm $KEY_FILE $CRT_FILE
