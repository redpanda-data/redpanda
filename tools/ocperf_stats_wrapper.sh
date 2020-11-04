#!/bin/bash
set -o errexit
set -o nounset
set -o pipefail
echo "Usage: ocperf_stats_wrapper.sh -p $(pgrep redpanda)"
# TODO(agallego) Missing 'Memory' group, but perf crashes
exec ocperf.py stat --metrics TLB,IPC,DSB,Frontend -d -d -d $@
