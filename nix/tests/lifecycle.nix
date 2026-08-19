# nix/tests/lifecycle.nix
#
# Layer 3: Lifecycle test — restart, data persistence, graceful shutdown.
# Run with: nix run .#test-lifecycle
{ pkgs, redpandaDrv, rpkDrv }:

let
  testLib = import ./lib.nix { inherit redpandaDrv rpkDrv; };
  constants = testLib.constants;
  resilienceChecks = import ./checks/resilience-checks.nix { inherit rpkDrv constants; };
in
pkgs.writeShellApplication {
  name = "test-lifecycle";
  runtimeInputs = with pkgs; [
    coreutils
    gnugrep
    gnused
    curl
    jq
    redpandaDrv
    rpkDrv
  ];
  text = ''
    set +e

    ${testLib.colorHelpers}
    ${testLib.timingHelpers}
    ${testLib.counterHelpers}
    ${testLib.processHelpers}
    ${testLib.assertionHelpers}

    TOTAL_START=$(time_ms)

    WORK=$(mktemp -d)
    DATA_DIR="$WORK/data"
    CONFIG="$WORK/redpanda.yaml"
    mkdir -p "$DATA_DIR"

    cleanup() {
      if [[ -n "''${RP_PID:-}" ]]; then
        kill -TERM "$RP_PID" 2>/dev/null || true
        wait "$RP_PID" 2>/dev/null || true
      fi
      rm -rf "$WORK"
    }
    trap cleanup EXIT

    cat > "$CONFIG" <<YAML
    ${constants.mkRedpandaYaml { }}
    YAML
    sed -i "s|DATA_DIR_PLACEHOLDER|$DATA_DIR|g" "$CONFIG"

    bold "========================================="
    bold "  Redpanda Lifecycle Test"
    bold "========================================="

    # --- Phase 1: Initial start ---
    phase_header 1 "Initial Start" ${toString constants.timeouts.startup}
    start_redpanda "$CONFIG"
    if wait_for_ready; then
      result_pass "initial startup" "0"
      record_pass
    else
      result_fail "initial startup" "0"
      record_fail
      exit 1
    fi

    # --- Phase 2: Data persistence ---
    phase_header 2 "Data Persistence" 90
    ${resilienceChecks.mkDataPersistenceCheck}

    # --- Phase 3: Restart recovery ---
    phase_header 3 "Restart Recovery" 60
    ${resilienceChecks.mkRestartRecoveryCheck}

    # --- Phase 4: Graceful shutdown ---
    phase_header 4 "Graceful Shutdown" ${toString constants.timeouts.shutdown}
    ${resilienceChecks.mkGracefulShutdownCheck}

    # --- Summary ---
    ${testLib.summaryBlock}
  '';
}
