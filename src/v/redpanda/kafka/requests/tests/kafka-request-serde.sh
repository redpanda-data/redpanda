#!/bin/bash
set -e

gold=$(mktemp)
check=$(mktemp)
trap "rm ${gold} ${check}" EXIT

kafka-python-api-serde.py | tee ${gold} | kafka_api_serde_tool --output ${check}

if ! cmp ${gold} ${check}; then
    echo "error: serde test failed"
    exit 1
fi
