#!/usr/bin/env bash

set -e

mkdir -p /mnt/kvell
mkdir -p /mnt/vectorized/back
rm -rf /mnt/kvell/* || true
rm -rf /mnt/vectorized/back/* || true
rm -rf /home/admin/kvell.log || true
