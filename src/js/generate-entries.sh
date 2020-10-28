#!/usr/bin/env bash
root=$(git rev-parse --show-toplevel)
cd "$root"/tools/ts-generator/types &&
  python types_gen_js.py \
    --entities-define-file "$root"/src/js/modules/domain/generatedRpc/entitiesDefinition.json \
    --output-file "$root"/src/js/modules/domain/generatedRpc/generatedClasses.ts

cd "$root"/tools/ts-generator/types &&
  python types_gen_js.py \
    --entities-define-file "$root"/src/js/modules/domain/generatedRpc/enableDisableCoproc.json \
    --output-file "$root"/src/js/modules/domain/generatedRpc/enableDisableCoprocClasses.ts

cd "$root"/tools/ts-generator/rpc &&
  python rpc_gen_js.py \
    --server-define-file "$root"/src/idl/coproc_idl.json \
    --output-file "$root"/src/js/modules/rpc/serverAndClients/server.ts

cd "$root"/tools/ts-generator/rpc &&
  python rpc_gen_js.py \
    --server-define-file "$root"/src/idl/process_batch_coproc.json \
    --output-file "$root"/src/js/modules/rpc/serverAndClients/processBatch.ts
