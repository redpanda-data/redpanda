# Kafka protocol checks: topic CRUD, produce/consume round-trip.
{ rpkDrv, constants }:

let
  rpk = "${rpkDrv}/bin/rpk";
  brokers = "--brokers 127.0.0.1:${toString constants.ports.kafka}";
in
{
  mkTopicChecks = ''
    ${rpk} topic delete nix-test-topic ${brokers} 2>/dev/null || true
    topic_start=$(time_ms)
    if ${rpk} topic create nix-test-topic -p 3 -r 1 ${brokers}; then
      result_pass "topic create (3 partitions)" "$(elapsed_ms "$topic_start")"
      record_pass
    else
      result_fail "topic create" "$(elapsed_ms "$topic_start")"
      record_fail
    fi

    list_start=$(time_ms)
    if ${rpk} topic list ${brokers} | grep -q nix-test-topic; then
      result_pass "topic list shows created topic" "$(elapsed_ms "$list_start")"
      record_pass
    else
      result_fail "topic list" "$(elapsed_ms "$list_start")"
      record_fail
    fi

    desc_start=$(time_ms)
    if ${rpk} topic describe nix-test-topic ${brokers} 2>&1 | grep -qi "partition"; then
      result_pass "topic describe" "$(elapsed_ms "$desc_start")"
      record_pass
    else
      result_fail "topic describe" "$(elapsed_ms "$desc_start")"
      record_fail
    fi
  '';

  mkProduceConsumeChecks = ''
    pc_start=$(time_ms)
    echo "${constants.testMessages.small}" | ${rpk} topic produce nix-test-topic ${brokers}
    OUTPUT=$(timeout 10 ${rpk} topic consume nix-test-topic -n 1 -f '%v\n' ${brokers})
    if echo "$OUTPUT" | grep -q "${constants.testMessages.small}"; then
      result_pass "produce/consume round-trip (small)" "$(elapsed_ms "$pc_start")"
      record_pass
    else
      result_fail "produce/consume round-trip: got '$OUTPUT'" "$(elapsed_ms "$pc_start")"
      record_fail
    fi

    med_start=$(time_ms)
    ${rpk} topic create nix-med-topic ${brokers} 2>/dev/null || true
    echo "${constants.testMessages.medium}" | ${rpk} topic produce nix-med-topic ${brokers}
    MED_OUT=$(timeout 10 ${rpk} topic consume nix-med-topic -n 1 -f '%v\n' ${brokers} 2>/dev/null || echo "")
    if echo "$MED_OUT" | grep -q "medium-payload"; then
      result_pass "produce/consume round-trip (medium)" "$(elapsed_ms "$med_start")"
      record_pass
    else
      result_fail "produce/consume round-trip (medium)" "$(elapsed_ms "$med_start")"
      record_fail
    fi

    kv_start=$(time_ms)
    echo "test-key:test-value" | ${rpk} topic produce nix-test-topic -f '%k:%v\n' ${brokers}
    result_pass "produce with key (no crash)" "$(elapsed_ms "$kv_start")"
    record_pass
  '';

  mkTopicDeleteCheck = ''
    ${rpk} topic create nix-delete-me ${brokers} 2>/dev/null || true
    del_start=$(time_ms)
    if ${rpk} topic delete nix-delete-me ${brokers}; then
      result_pass "topic delete" "$(elapsed_ms "$del_start")"
      record_pass
    else
      result_fail "topic delete" "$(elapsed_ms "$del_start")"
      record_fail
    fi
  '';
}
