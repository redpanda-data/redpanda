#!/bin/bash

set -e

CLUSTER_NAME=$1
KEY_FILE=/tmp/client.key.$$
CRT_FILE=/tmp/client.crt.$$
CN=e2e-test-schema-client
SECRET_NAME="client-cert-key"

# Create self signed cert and key
openssl req -x509 -newkey rsa:2048 -sha256 -days 36500 -nodes -keyout $KEY_FILE -out $CRT_FILE -subj "/CN=$CN.redpanda.com"
kubectl create -n $NAMESPACE secret generic $SECRET_NAME --from-file=tls\.key=$KEY_FILE --from-file=tls\.crt=$CRT_FILE

# Create the external CA cert with the self signed CA
SECRET_NAME="ca-cert"
kubectl create -n $NAMESPACE secret generic $SECRET_NAME --from-file=ca\.crt=$CRT_FILE
kubectl annotate secret -n $NAMESPACE $SECRET_NAME operator.redpanda.com/external-ca="true"
kubectl label secret -n $NAMESPACE $SECRET_NAME app.kubernetes.io/component=redpanda app.kubernetes.io/name=redpanda app.kubernetes.io/instance=$CLUSTER_NAME

rm $KEY_FILE $CRT_FILE
