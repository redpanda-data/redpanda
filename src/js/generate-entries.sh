#!/usr/bin/env bash
# Copyright 2020 Vectorized, Inc.
#
# Licensed as a Redpanda Enterprise file under the Redpanda Community
# License (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md

root=$(git rev-parse --show-toplevel)
cd "$root"/tools/ts-generator/types &&
  python3 types_gen_js.py \
    --entities-define-file "$root"/src/js/modules/domain/generatedRpc/entitiesDefinition.json \
    --output-file "$root"/src/js/modules/domain/generatedRpc/generatedClasses.ts

cd "$root"/tools/ts-generator/types &&
  python3 types_gen_js.py \
    --entities-define-file "$root"/src/js/modules/domain/generatedRpc/enableDisableCoproc.json \
    --output-file "$root"/src/js/modules/domain/generatedRpc/enableDisableCoprocClasses.ts

cd "$root"/tools/ts-generator/rpc &&
  python3 rpc_gen_js.py \
    --server-define-file "$root"/src/idl/coproc_idl.json \
    --output-file "$root"/src/js/modules/rpc/serverAndClients/server.ts

cd "$root"/tools/ts-generator/rpc &&
  python3 rpc_gen_js.py \
    --server-define-file "$root"/src/idl/process_batch_coproc.json \
    --output-file "$root"/src/js/modules/rpc/serverAndClients/processBatch.ts
