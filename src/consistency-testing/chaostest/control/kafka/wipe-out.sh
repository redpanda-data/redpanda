#!/usr/bin/env bash

set -e

mkdir -p /mnt/vectorized/front
mkdir -p /mnt/vectorized/back
rm -rf /mnt/vectorized/front/* || true
rm -rf /mnt/vectorized/back/* || true
