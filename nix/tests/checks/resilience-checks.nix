# Resilience checks: graceful shutdown, data persistence, restart recovery.
{ rpkDrv, constants }:

let
  rpk = "${rpkDrv}/bin/rpk";
  brokers = "--brokers 127.0.0.1:${toString constants.ports.kafka}";
in
{
  mkGracefulShutdownCheck = ''
    shutdown_start=$(time_ms)
    info "  Sending SIGTERM to Redpanda (pid $RP_PID)..."
    kill -TERM "$RP_PID" 2>/dev/null || true
    _shutdown_ok=false
    elapsed=0
    while [[ $elapsed -lt ${toString constants.timeouts.shutdown} ]]; do
      if ! kill -0 "$RP_PID" 2>/dev/null; then
        _shutdown_ok=true
        break
      fi
      sleep 1
      elapsed=$((elapsed + 1))
    done
    if [[ "$_shutdown_ok" == "true" ]]; then
      result_pass "graceful shutdown (SIGTERM)" "$(elapsed_ms "$shutdown_start")"
      record_pass
      RP_PID=""
    else
      result_fail "graceful shutdown (did not exit in ${toString constants.timeouts.shutdown}s)" "$(elapsed_ms "$shutdown_start")"
      record_fail
      kill -9 "$RP_PID" 2>/dev/null || true
      RP_PID=""
    fi
  '';

  mkDataPersistenceCheck = ''
    persist_start=$(time_ms)
    info "  Producing 100 unique messages before restart..."
    ${rpk} topic create nix-persist-test -p 1 ${brokers} 2>/dev/null || true

    # Generate unique messages and save expected output
    EXPECTED="$WORK/expected-messages.txt"
    for i in $(seq 1 100); do
      echo "persist-msg-$i-$(date +%s%N)-$$"
    done > "$EXPECTED"
    ${rpk} topic produce nix-persist-test ${brokers} < "$EXPECTED"

    info "  Shutting down for persistence test..."
    kill -TERM "$RP_PID" 2>/dev/null || true
    wait "$RP_PID" 2>/dev/null || true
    sleep 2

    info "  Restarting Redpanda..."
    start_redpanda "$CONFIG"
    if wait_for_ready; then
      ACTUAL="$WORK/actual-messages.txt"
      timeout 15 ${rpk} topic consume nix-persist-test -n 100 -f '%v\n' ${brokers} 2>/dev/null | sort > "$ACTUAL"
      EXPECTED_SORTED="$WORK/expected-sorted.txt"
      sort "$EXPECTED" > "$EXPECTED_SORTED"

      if diff -q "$EXPECTED_SORTED" "$ACTUAL" >/dev/null 2>&1; then
        result_pass "data persistence: 100/100 messages match exactly" "$(elapsed_ms "$persist_start")"
        record_pass
      else
        MISSING=$(comm -23 "$EXPECTED_SORTED" "$ACTUAL" | wc -l)
        EXTRA=$(comm -13 "$EXPECTED_SORTED" "$ACTUAL" | wc -l)
        result_fail "data persistence: content mismatch ($MISSING missing, $EXTRA unexpected)" "$(elapsed_ms "$persist_start")"
        # Show first few diffs for debugging
        diff "$EXPECTED_SORTED" "$ACTUAL" | head -20 >&2 || true
        record_fail
      fi
    else
      result_fail "data persistence: Redpanda failed to restart" "$(elapsed_ms "$persist_start")"
      record_fail
    fi
  '';

  mkRestartRecoveryCheck = ''
    restart_start=$(time_ms)
    info "  Restarting Redpanda..."
    kill -TERM "$RP_PID" 2>/dev/null || true
    wait "$RP_PID" 2>/dev/null || true
    sleep 1

    start_redpanda "$CONFIG"
    if wait_for_ready; then
      result_pass "restart recovery" "$(elapsed_ms "$restart_start")"
      record_pass
    else
      result_fail "restart recovery (not healthy)" "$(elapsed_ms "$restart_start")"
      record_fail
    fi
  '';
}
