# nix/tests/single-node.nix
#
# Layer 2: Single-node integration test.
# Starts a real Redpanda in developer mode and exercises all APIs.
# Run with: nix run .#test-single-node
{ pkgs, redpandaDrv, rpkDrv }:

let
  testLib = import ./lib.nix { inherit redpandaDrv rpkDrv; };
  constants = testLib.constants;
  kafkaChecks = import ./checks/kafka-checks.nix { inherit rpkDrv constants; };
  adminChecks = import ./checks/admin-checks.nix { inherit constants; };
  schemaChecks = import ./checks/schema-checks.nix { inherit constants; };
  proxyChecks = import ./checks/proxy-checks.nix { inherit rpkDrv constants; };
  rpkChecks = import ./checks/rpk-checks.nix { inherit rpkDrv constants; };
in
pkgs.writeShellApplication {
  name = "test-single-node";
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
      info "Cleaning up..."
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
    bold "  Redpanda Single-Node Integration Test"
    bold "========================================="

    # --- Phase 1: Start ---
    phase_header 1 "Start" ${toString constants.timeouts.startup}
    start_phase=$(time_ms)
    start_redpanda "$CONFIG"
    if wait_for_ready; then
      result_pass "Redpanda started and healthy" "$(elapsed_ms "$start_phase")"
      record_pass
    else
      result_fail "Redpanda failed to start" "$(elapsed_ms "$start_phase")"
      record_fail
      error "FATAL: cannot continue without running Redpanda"
      exit 1
    fi

    # --- Phase 2: Admin API ---
    phase_header 2 "Admin API" 30
    ${adminChecks.mkHealthCheck}
    ${adminChecks.mkClusterConfigCheck}
    ${adminChecks.mkBrokersCheck}
    ${adminChecks.mkStatusCheck}

    # --- Phase 3: Kafka Protocol ---
    phase_header 3 "Kafka Protocol" 30
    ${kafkaChecks.mkTopicChecks}
    ${kafkaChecks.mkProduceConsumeChecks}
    ${kafkaChecks.mkTopicDeleteCheck}

    # --- Phase 4: Schema Registry ---
    phase_header 4 "Schema Registry" 30
    ${schemaChecks.mkSchemaRegistryChecks}

    # --- Phase 5: Pandaproxy ---
    phase_header 5 "Pandaproxy (HTTP API)" 30
    ${proxyChecks.mkPandaproxyChecks}

    # --- Phase 6: rpk CLI ---
    phase_header 6 "rpk CLI (live cluster)" 15
    ${rpkChecks.mkRpkLiveChecks}

    # --- Summary ---
    ${testLib.summaryBlock}
  '';
}
