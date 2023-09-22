#!/bin/bash

# Remove the preamble from each CI file
for FILE in helm-charts/charts/redpanda/ci/*.yaml; do 
    echo $FILE
    tail -n +16 $FILE > operator-tests/files/$(basename $FILE)
done

# Remove the following CI files as they cannot pass right now
# CRD for servicemonitor missing
rm operator-tests/files/15-*.yaml
# CRD for servicemonitor missing
rm operator-tests/file/14-*.yaml
# Needs external-tls secret, tests lb
rm operator-tests/file/13-*.yaml
# Needs external-tls secret, tests nodeport
rm operator-tests/file/12-*.yaml




