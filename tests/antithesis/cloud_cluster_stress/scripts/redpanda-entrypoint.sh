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

# Copy the bootstrap config off its read-only bind mount onto the container's
# writable layer, then start Redpanda. Under Antithesis a bind mount is backed
# by overlay-over-squashfs, which mishandles O_DIRECT (close() returns EINVAL);
# Redpanda opens config files with O_DIRECT and would crash reading it there.
# The writable layer handles O_DIRECT, so a plain (buffered) copy sidesteps it.
# See seastar issue 3518.
cp /scripts/bootstrap.yaml /etc/redpanda/.bootstrap.yaml

exec rpk "$@"
