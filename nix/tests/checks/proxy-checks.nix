# Pandaproxy (HTTP API) checks.
{ rpkDrv, constants }:

let
  rpk = "${rpkDrv}/bin/rpk";
  proxyBase = "http://127.0.0.1:${toString constants.ports.pandaproxy}";
  brokers = "--brokers 127.0.0.1:${toString constants.ports.kafka}";
in
{
  mkPandaproxyChecks = ''
    ${rpk} topic create nix-proxy-test ${brokers} 2>/dev/null || true

    proxy_start=$(time_ms)
    PRODUCE_RESULT=$(curl -sf -X POST "${proxyBase}/topics/nix-proxy-test" \
      -H "Content-Type: application/vnd.kafka.json.v2+json" \
      -d '{"records": [{"value": {"message": "hello-from-pandaproxy"}}]}' 2>/dev/null || echo "")
    if echo "$PRODUCE_RESULT" | jq -e '.offsets[0].offset' >/dev/null 2>&1; then
      result_pass "pandaproxy HTTP produce" "$(elapsed_ms "$proxy_start")"
      record_pass
    else
      result_fail "pandaproxy HTTP produce: $PRODUCE_RESULT" "$(elapsed_ms "$proxy_start")"
      record_fail
    fi

    topics_start=$(time_ms)
    TOPICS_RESULT=$(curl -sf "${proxyBase}/topics" 2>/dev/null || echo "")
    if echo "$TOPICS_RESULT" | grep -q "nix-proxy-test"; then
      result_pass "pandaproxy list topics" "$(elapsed_ms "$topics_start")"
      record_pass
    else
      result_fail "pandaproxy list topics" "$(elapsed_ms "$topics_start")"
      record_fail
    fi
  '';
}
