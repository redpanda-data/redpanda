#!/usr/bin/env bash
# Copyright 2020 Vectorized, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md

py="python3"

command -v python3 >/dev/null 2>&1

if [[ $? -ne 0 ]]; then
  # python3 isn't installed. Check for python -v.
  version=$(python -V)
  if [[ $? -ne 0 ]]; then
    echo "python is not installed"
    exit 1
  fi

  if [[ $version =~ "Python 3" ]]; then
    $py="python"
  else
    echo "python 3 is not installed"
    exit 1
  fi
fi

echo "Using ${py}"

root=$(git rev-parse --show-toplevel)
cd "$root"/tools/ts-generator/types &&
  $py types_gen_js.py \
    --entities-define-file "$root"/src/js/modules/domain/generatedRpc/entitiesDefinition.json \
    --output-file "$root"/src/js/modules/domain/generatedRpc/generatedClasses.ts

cd "$root"/tools/ts-generator/types &&
  $py types_gen_js.py \
    --entities-define-file "$root"/src/js/modules/domain/generatedRpc/enableDisableCoproc.json \
    --output-file "$root"/src/js/modules/domain/generatedRpc/enableDisableCoprocClasses.ts

cd "$root"/tools/ts-generator/rpc &&
  $py rpc_gen_js.py \
    --server-define-file "$root"/src/v/coproc/gen.json \
    --output-file "$root"/src/js/modules/rpc/serverAndClients/rpcServer.ts
