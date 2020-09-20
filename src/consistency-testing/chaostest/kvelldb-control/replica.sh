#!/usr/bin/env bash

set -e

/opt/redpanda/bin/kvelldb --ip 0.0.0.0 --workdir /mnt/kvell --node-id $1 --port 33145 --httpport 9092 --peers $2,$3:33145 --peers $4,$5:33145 --cpus 1 2>&1
