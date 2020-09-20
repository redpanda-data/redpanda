#!/usr/bin/env bash

set -e

rm -rf /mnt/kvell || true
rm -rf /mnt/vectorized/back || true
rm -rf /home/admin/kvell.log || true
mkdir /mnt/kvell
mkdir /mnt/vectorized/back
