# nix/tests/default.nix
#
# Test orchestrator — exports checks (sandboxed), packages (runnable),
# and apps (Docker-based) for the flake.
#
# Adapted from KwaaiNet's nix/tests/default.nix pattern.
{
  pkgs,
  redpandaDrv,
  rpkDrv,
  mkApp,
}:

let
  smoke = import ./smoke.nix { inherit pkgs redpandaDrv rpkDrv; };
  singleNode = import ./single-node.nix { inherit pkgs redpandaDrv rpkDrv; };
  lifecycle = import ./lifecycle.nix { inherit pkgs redpandaDrv rpkDrv; };
  containers = import ./containers.nix { inherit pkgs mkApp; };

  # Run-all: smoke + single-node + lifecycle in sequence.
  # Container tests are excluded (require Docker daemon).
  testAll = pkgs.writeShellApplication {
    name = "test-all";
    runtimeInputs = [
      singleNode
      lifecycle
      pkgs.coreutils
    ];
    text = ''
      set -euo pipefail

      echo ""
      echo "╔═══════════════════════════════════════════╗"
      echo "║       Redpanda Nix Test Suite             ║"
      echo "╚═══════════════════════════════════════════╝"
      echo ""

      TOTAL_START=$(date +%s)
      FAILED=0

      # Layer 1: Smoke (sandboxed checks are build-time only,
      # so we just verify the binaries exist and respond to --help)
      echo "━━━ Layer 1: Smoke ━━━"
      echo "  (Smoke checks run via 'nix flake check' at build time)"
      echo "  Skipping — run 'nix flake check' separately."
      echo ""

      # Layer 2: Single-node integration
      echo "━━━ Layer 2: Single-Node Integration ━━━"
      if test-single-node; then
        echo ""
        echo "  ✓ Single-node tests passed"
      else
        echo ""
        echo "  ✗ Single-node tests FAILED"
        FAILED=$((FAILED + 1))
      fi
      echo ""

      # Layer 3: Lifecycle
      echo "━━━ Layer 3: Lifecycle ━━━"
      if test-lifecycle; then
        echo ""
        echo "  ✓ Lifecycle tests passed"
      else
        echo ""
        echo "  ✗ Lifecycle tests FAILED"
        FAILED=$((FAILED + 1))
      fi
      echo ""

      # Layer 4: Container tests (informational)
      echo "━━━ Layer 4: Containers ━━━"
      echo "  (Requires Docker — run 'nix run .#test-images' separately)"
      echo ""

      ELAPSED=$(( $(date +%s) - TOTAL_START ))
      echo "╔═══════════════════════════════════════════╗"
      if [[ "$FAILED" -eq 0 ]]; then
        echo "║  ALL TESTS PASSED  (''${ELAPSED}s)              ║"
      else
        echo "║  $FAILED TEST LAYER(S) FAILED  (''${ELAPSED}s)        ║"
      fi
      echo "╚═══════════════════════════════════════════╝"

      exit "$FAILED"
    '';
  };
in
{
  # Sandboxed — runs during `nix flake check`
  checks = {
    redpanda-smoke = smoke;
  };

  # Runnable packages — `nix run .#test-single-node`, `nix run .#test-lifecycle`, `nix run .#test-all`
  packages = {
    test-single-node = singleNode;
    test-lifecycle = lifecycle;
    test-all = testAll;
  };

  # Apps (Docker-based) — `nix run .#test-images`
  apps = {
    test-images = containers;
  };
}
