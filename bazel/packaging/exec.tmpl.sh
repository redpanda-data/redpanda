#!/usr/bin/env bash
set -e
export LD_LIBRARY_PATH="/opt/redpanda/lib"
export PATH="/opt/redpanda/bin:${PATH}"
exec -a "$0" "/opt/redpanda/libexec/%%BINARY%%" "$@"
