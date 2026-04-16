#!/usr/bin/env bash
#
# ==================================================================
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
# Entrypoint for the Antithesis ducktape runner container.
#
# 1. Starts sshd (ducktape uses SSH internally)
# 2. Emits the Antithesis setup_complete signal
# 3. Sleeps forever (Antithesis invokes the singleton driver separately)
#

set -euo pipefail

# Start sshd for ducktape's internal use.
service ssh start

# Emit setup_complete — Antithesis begins fault injection after this signal.
if [ -n "${ANTITHESIS_OUTPUT_DIR:-}" ]; then
  mkdir -p "$ANTITHESIS_OUTPUT_DIR"
  printf '{"antithesis_setup": {"status": "complete", "details": null}}\n' \
    >>"$ANTITHESIS_OUTPUT_DIR/sdk.jsonl"
fi

exec sleep infinity
