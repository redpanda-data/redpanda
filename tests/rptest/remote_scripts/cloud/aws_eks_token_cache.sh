#!/bin/bash
# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

# EKS token caching wrapper - caches aws eks get-token output to avoid
# repeated AWS API calls on every kubectl invocation.
# Tokens are cached until 10 seconds before expiry.

CACHE_DIR="$HOME/.kube/cache/eks-tokens"
mkdir -p "$CACHE_DIR" 2>/dev/null
CACHE_FILE="$CACHE_DIR/$(echo "$@" | md5sum | cut -d' ' -f1)"

# Check if cached token is valid and has >10s remaining
check_cache() {
  [[ -f $CACHE_FILE ]] || return 1
  python3 -c "
import json, sys
from datetime import datetime
with open('$CACHE_FILE') as f:
    data = json.load(f)
if data.get('kind') != 'ExecCredential':
    sys.exit(1)
expiry = data['status']['expirationTimestamp']
exp_time = datetime.fromisoformat(expiry.replace('Z', '+00:00'))
remaining = (exp_time - datetime.now(exp_time.tzinfo)).total_seconds()
sys.exit(0 if remaining > 10 else 1)
" 2>/dev/null
}

if check_cache; then
  cat "$CACHE_FILE"
  exit 0
fi

# Often there will be parallel invocations of this script. Acqiure a lock so that only
# one of these instances updates the token, rather than all of them.
exec 200>"$CACHE_FILE.lock"
flock -w 30 200 || exec aws "$@"

# Re-check as another process could've acquired the lock first and updated the token.
if check_cache; then
  cat "$CACHE_FILE"
  exit 0
fi

# Get fresh token and cache it. The tmp file + mv is so that readers never
# read a partial token.
TOKEN=$(aws "$@")
EXIT_CODE=$?

if [[ $EXIT_CODE -eq 0 ]]; then
  echo "$TOKEN" >"$CACHE_FILE.tmp" && mv "$CACHE_FILE.tmp" "$CACHE_FILE" 2>/dev/null
fi

echo "$TOKEN"
exit $EXIT_CODE
