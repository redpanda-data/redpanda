# Utility script that fetches GCB artifacts for a given git reference.
# The artifacts are copied to the local folders used by the ducktape tests.

#!/bin/bash
set -e

if [ $# -eq 0 ]; then
  echo "No arguments supplied"
  exit 15
fi

ref=$1

gsutil cp gs://vectorizedio/rp/release/clang/$ref/*.tar.gz . # fetch redpanda & pandaproxy packages

# copy artifacts to the release directory currently used by the ducktape tests
mkdir -p build/release/clang/dist/local/redpanda &&
  tar -xzvf redpanda-*_$ref.tar.gz -C build/release/clang/dist/local/redpanda

mkdir -p build/release/clang/dist/local/pandaproxy &&
  tar -xzvf pandaproxy-*_$ref.tar.gz -C build/release/clang/dist/local/pandaproxy

# workaround for rpk needing a config file
cp build/release/clang/dist/local/redpanda/conf/redpanda.yaml .
