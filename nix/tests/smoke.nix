# nix/tests/smoke.nix
#
# Layer 1: Sandboxed smoke tests for `nix flake check`.
# Verifies binaries exist and basic CLI works without a running cluster.
{ pkgs, redpandaDrv, rpkDrv }:

let
  constants = import ./constants.nix;
  rpkChecks = import ./checks/rpk-checks.nix { inherit rpkDrv constants; };
in
pkgs.runCommand "redpanda-smoke-test" { } ''
  TOTAL_PASSED=0
  TOTAL_FAILED=0
  record_pass() { TOTAL_PASSED=$((TOTAL_PASSED + 1)); }
  record_fail() { TOTAL_FAILED=$((TOTAL_FAILED + 1)); }

  time_ms() { echo $(($(date +%s%N) / 1000000)); }
  elapsed_ms() { local s="$1"; echo $(( $(time_ms) - s )); }
  result_pass() { echo "  PASS: $1 (''${2}ms)"; }
  result_fail() { echo "  FAIL: $1 (''${2}ms)"; }
  result_skip() { echo "  SKIP: $1"; }

  echo "=== Redpanda Nix Smoke Tests ==="

  rp_start=$(time_ms)
  if ${redpandaDrv}/bin/redpanda --help >/dev/null 2>&1; then
    result_pass "redpanda --help" "$(elapsed_ms "$rp_start")"
    record_pass
  else
    result_fail "redpanda --help" "$(elapsed_ms "$rp_start")"
    record_fail
  fi

  ${rpkChecks.mkRpkOfflineChecks}

  echo ""
  echo "=== Results: $TOTAL_PASSED passed, $TOTAL_FAILED failed ==="
  if [[ $TOTAL_FAILED -eq 0 ]]; then
    touch "$out"
  else
    exit 1
  fi
''
