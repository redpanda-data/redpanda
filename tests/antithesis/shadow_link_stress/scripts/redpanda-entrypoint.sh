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

set -euo pipefail

# Copy the role-specific bootstrap config off its read-only bind mount onto
# the container's writable layer, then start Redpanda. Under Antithesis a
# bind mount is backed by overlay-over-squashfs, which mishandles O_DIRECT;
# Redpanda opens config files with O_DIRECT and would crash reading it there.
# See seastar issue 3518.
: "${CLUSTER_ROLE:?CLUSTER_ROLE must be 'source' or 'target'}"
cp "/scripts/bootstrap-${CLUSTER_ROLE}.yaml" /etc/redpanda/.bootstrap.yaml

exec rpk "$@"
