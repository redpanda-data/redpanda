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
# One-shot init: creates the object storage bucket Redpanda is
# configured with. Services wait on it via
# depends_on: service_completed_successfully.

set -euo pipefail

: "${MINIO_URL:?}"
: "${BUCKET:?}"
: "${ACCESS_KEY:?}"
: "${SECRET_KEY:?}"

mc alias set local "$MINIO_URL" "$ACCESS_KEY" "$SECRET_KEY"
mc mb --ignore-existing "local/$BUCKET"

echo "bucket $BUCKET ready"
