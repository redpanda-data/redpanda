{ pkgs, mkApp }:

let
  lib = pkgs.lib;

  bazelCacheDir = "/var/cache/bazel-nix";

  mkClear = { clearNix ? false, clearBazel ? false }: lib.concatStrings [
    (lib.optionalString clearNix ''
      date -u +%Y-%m-%dT%H:%M:%S.%NZ > nix/entropy
      echo "[bench] Wrote entropy file (Nix cache invalidated)"
    '')
    (lib.optionalString clearBazel ''
      sudo rm -rf ${bazelCacheDir}/*
      echo "[bench] Cleared persistent Bazel cache at ${bazelCacheDir}"
    '')
  ];

  mkBench = { name, target ? "redpanda-cached", clearNix ? false, clearBazel ? false, repeat ? 1 }:
    mkApp (pkgs.writeShellApplication {
      name = "redpanda-bench-${name}";
      runtimeInputs = [ pkgs.coreutils pkgs.nix ];
      text = ''
        for i in $(seq 1 ${toString repeat}); do
          echo "=== Run $i/${toString repeat}: ${name} ==="
          ${mkClear { inherit clearNix clearBazel; }}
          echo "[bench] Building..."
          time nix build .#${target} --print-build-logs
          echo "[bench] Done"
          echo ""
        done
      '';
    });

  mkClearOnly = { name, clearNix ? false, clearBazel ? false }:
    mkApp (pkgs.writeShellApplication {
      name = "redpanda-${name}";
      runtimeInputs = [ pkgs.coreutils pkgs.nix ];
      text = mkClear { inherit clearNix clearBazel; };
    });

in
{
  # Clear-only targets (no build)
  clear-nix = mkClearOnly {
    name = "clear-nix";
    clearNix = true;
  };

  clear-bazel = mkClearOnly {
    name = "clear-bazel";
    clearBazel = true;
  };

  clear-all = mkClearOnly {
    name = "clear-all";
    clearNix = true;
    clearBazel = true;
  };

  # Single-run benchmarks (use redpanda-cached for persistent Bazel cache)
  bench-warm = mkBench {
    name = "warm";
  };

  bench-cold-nix = mkBench {
    name = "cold-nix";
    clearNix = true;
  };

  bench-cold-bazel = mkBench {
    name = "cold-bazel";
    clearBazel = true;
  };

  bench-cold-all = mkBench {
    name = "cold-all";
    clearNix = true;
    clearBazel = true;
  };

  # 3x repeated benchmarks
  bench-3x-warm = mkBench {
    name = "3x-warm";
    repeat = 3;
  };

  bench-3x-cold-nix = mkBench {
    name = "3x-cold-nix";
    clearNix = true;
    repeat = 3;
  };

  # Full matrix: warm + cold-nix + cold-all
  bench-matrix = mkApp (pkgs.writeShellApplication {
    name = "redpanda-bench-matrix";
    runtimeInputs = [ pkgs.coreutils pkgs.nix ];
    text = ''
      echo "========================================="
      echo "  Benchmark Matrix (9 builds total)"
      echo "========================================="
      echo ""

      echo "--- Phase 1: 3x warm (both caches present) ---"
      for i in 1 2 3; do
        echo "=== Warm run $i/3 ==="
        time nix build .#redpanda-cached --print-build-logs
        echo ""
      done

      echo "--- Phase 2: 3x cold-nix (Bazel cache present) ---"
      for i in 1 2 3; do
        echo "=== Cold-nix run $i/3 ==="
        ${mkClear { clearNix = true; }}
        time nix build .#redpanda-cached --print-build-logs
        echo ""
      done

      echo "--- Phase 3: 3x cold-all (no caches) ---"
      for i in 1 2 3; do
        echo "=== Cold-all run $i/3 ==="
        ${mkClear { clearNix = true; clearBazel = true; }}
        time nix build .#redpanda-cached --print-build-logs
        echo ""
      done

      echo "========================================="
      echo "  Matrix complete"
      echo "========================================="
    '';
  });
}
