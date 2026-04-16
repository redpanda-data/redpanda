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
# Singleton driver for Antithesis ducktape tests.
# Antithesis invokes this script to run the test suite.
#
# Test selection and ducktape parameters are controlled via container
# environment variables set in the docker-compose.yaml.
#

set -euo pipefail

# Pause fault injection if requested. ANTITHESIS_STOP_FAULTS is injected
# by the Antithesis platform; DUCKTAPE_DISABLE_FAULTS is set by the packager.
if [ "${DUCKTAPE_DISABLE_FAULTS:-0}" = "1" ] && [ -n "${ANTITHESIS_STOP_FAULTS:-}" ]; then
  "${ANTITHESIS_STOP_FAULTS}" 86400
fi

pushd /root/tests

ducktape \
  --cluster=ducktape.cluster.json.JsonCluster \
  --cluster-file=/root/.ducktape/cluster.json \
  --results-root=/build/tests/results \
  --max-parallel="${DUCKTAPE_MAX_PARALLEL:-1}" \
  --test-runner-timeout="${DUCKTAPE_TEST_TIMEOUT:-1800000}" \
  --globals=/root/.ducktape/globals.json \
  ${DUCKTAPE_TEST_ARGS:?DUCKTAPE_TEST_ARGS must be set}

popd
