#!/usr/bin/env bash
set -o errexit
set -o nounset
set -o pipefail

# we assume bootstraph.sh lives in v/tools/
vroot="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. ${vroot}/tools/base_script.sh

# use our clang-tidy first!
PATH=${vroot}/build/llvm/llvm-bin/bin:$PATH
log "Static analyzing redpanda"

# to use release settings, we must be in the release/clang directory
(set -x && cd "${vroot}/build/release/clang" && clang-tidy "${vroot}/src/v/redpanda/main.cc")

log "To run automatic fixes, run:"
log "clang-tidy ${vroot}/src/v/redpanda/main.cc --fix"
