#!/usr/bin/env bash
set -euo pipefail

sleep_time="${RPK_SHIM_SLEEP_TIME:-5}"

echo "$@" | tee results.txt

sleep "${sleep_time}"
