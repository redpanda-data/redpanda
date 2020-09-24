#!/usr/bin/env bash

set -e

mkdir -p /mnt/vectorized/redpanda
mkdir -p /mnt/vectorized/back
rm -rf /mnt/vectorized/redpanda/* || true
rm -rf /mnt/vectorized/back/* || true
rm -rf /home/admin/redpanda.log || true
