#!/usr/bin/env bash
root=$(git rev-parse --show-toplevel)
cd "$root"/tools/ts-generator &&
  python rpcgen_js.py \
    --entities-define-file "$root"/src/js/modules/domain/generatedRpc/entitiesDefinition.json \
    --output-file "$root"/src/js/modules/domain/generatedRpc/generatedClasses.ts
