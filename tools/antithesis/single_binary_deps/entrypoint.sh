#!/usr/bin/sh
if [ -n "$ANTITHESIS_OUTPUT_DIR" ]; then
  mkdir -p "$ANTITHESIS_OUTPUT_DIR"
  printf '{"antithesis_setup": {"status": "complete", "details": {"message": "ready"}}}\n' \
    >>"$ANTITHESIS_OUTPUT_DIR/sdk.jsonl"
fi
exec sleep infinity
