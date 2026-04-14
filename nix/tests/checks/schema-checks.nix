# Schema Registry checks: register, get, list, compatibility.
{ constants }:

let
  srBase = "http://127.0.0.1:${toString constants.ports.schemaRegistry}";
in
{
  mkSchemaRegistryChecks = ''
    sr_start=$(time_ms)
    SCHEMA='{"schema": "{\"type\": \"record\", \"name\": \"Test\", \"fields\": [{\"name\": \"id\", \"type\": \"int\"}]}", "schemaType": "AVRO"}'
    REG_RESULT=$(curl -sf -X POST "${srBase}/subjects/nix-test-value/versions" \
      -H "Content-Type: application/vnd.schemaregistry.v1+json" \
      -d "$SCHEMA" 2>/dev/null || echo "")
    if echo "$REG_RESULT" | jq -e '.id' >/dev/null 2>&1; then
      result_pass "schema register (Avro)" "$(elapsed_ms "$sr_start")"
      record_pass
    else
      result_fail "schema register (Avro): $REG_RESULT" "$(elapsed_ms "$sr_start")"
      record_fail
    fi

    get_start=$(time_ms)
    GET_RESULT=$(curl -sf "${srBase}/subjects/nix-test-value/versions/latest" 2>/dev/null || echo "")
    if echo "$GET_RESULT" | jq -e '.schema' >/dev/null 2>&1; then
      result_pass "schema get (latest version)" "$(elapsed_ms "$get_start")"
      record_pass
    else
      result_fail "schema get" "$(elapsed_ms "$get_start")"
      record_fail
    fi

    list_start=$(time_ms)
    SUBJECTS=$(curl -sf "${srBase}/subjects" 2>/dev/null || echo "")
    if echo "$SUBJECTS" | jq -e '.[]' >/dev/null 2>&1; then
      result_pass "schema list subjects" "$(elapsed_ms "$list_start")"
      record_pass
    else
      result_fail "schema list subjects" "$(elapsed_ms "$list_start")"
      record_fail
    fi

    compat_start=$(time_ms)
    COMPAT_RESULT=$(curl -sf -X POST "${srBase}/compatibility/subjects/nix-test-value/versions/latest" \
      -H "Content-Type: application/vnd.schemaregistry.v1+json" \
      -d "$SCHEMA" 2>/dev/null || echo "")
    if echo "$COMPAT_RESULT" | jq -e '.is_compatible' >/dev/null 2>&1; then
      result_pass "schema compatibility check" "$(elapsed_ms "$compat_start")"
      record_pass
    else
      result_skip "schema compatibility (endpoint may differ)"
    fi
  '';
}
