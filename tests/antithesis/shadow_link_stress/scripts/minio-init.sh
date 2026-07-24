#!/usr/bin/env bash
# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
# ==================================================================
#
# One-shot init: creates the two object storage buckets (source and target
# cluster each upload to their own; shadow linking replicates over the Kafka
# protocol, never through shared storage). Services wait on it via
# depends_on: service_completed_successfully.

set -euo pipefail

: "${MINIO_URL:?}"
: "${BUCKETS:?}"
: "${ACCESS_KEY:?}"
: "${SECRET_KEY:?}"

mc alias set local "$MINIO_URL" "$ACCESS_KEY" "$SECRET_KEY"
for b in $BUCKETS; do
    mc mb --ignore-existing "local/$b"
    echo "bucket $b ready"
done
