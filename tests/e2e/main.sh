#!/bin/bash
set -e

if [[ $# != 1 ]]; then
  echo "Expecting one argument, binary tarball to test"
  echo "Example:"
  echo "./main.sh file:///home/agallego/workspace/v/./build/release/clang/dist/tar/redpanda-0.0-dev_77a35d7.tar.gz"
  exit 1
fi

this_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source $this_dir/../../tools/base_script.sh
source $this_dir/../../tools/alias.sh

function clean_exit() {
  bamboo session destroy --sname leia
}

# after the sourcing
set -x

log "running end to end tests"
log "creating bamboo session"
bamboo session init --sname leia
# make sure we clean up
trap clean_exit EXIT
bamboo session add-node --sname leia --package $1
tests=($(find $this_dir -maxdepth 1 -type d))
for d in "${tests[@]:1}"; do
  log "running test: $d"
  bamboo session run-test --sname leia --path $d
done
