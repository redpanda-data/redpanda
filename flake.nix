{
  description = "Redpanda: a Kafka-compatible streaming data platform";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    flake-utils.lib.eachSystem
      [
        "x86_64-linux"
        "aarch64-linux"
      ]
      (
        system:
        let
          pkgs = import nixpkgs {
            inherit system;
            config.allowUnfree = true;
          };

          # ── Base builds ──
          # Default fastbuild — fastest compilation, no optimizations.
          redpanda = pkgs.callPackage ./nix/redpanda.nix { };

          redpanda-cached = pkgs.callPackage ./nix/redpanda.nix {
            bazelCacheDir = "/var/cache/bazel-nix";
          };

          rpk = pkgs.callPackage ./nix/rpk.nix { };

          # ── Optimization tiers ──
          # Nix makes it trivial to offer every combination of optimization
          # level.  Each tier is a one-line parameter change — the build
          # system handles the rest.
          #
          #   Tier      │ Flags                   │ What it adds
          #   ──────────┼─────────────────────────┼──────────────────────
          #   default   │ (fastbuild)             │ fastest compile
          #   release   │ --config=release        │ -O2, secure, stripped
          #   lto       │ --config=lto            │ ThinLTO cross-module
          #   pgo       │ lto + profile data      │ branch/call frequency
          #

          # Release: -O2 optimized, security hardened.
          redpanda-release = pkgs.callPackage ./nix/redpanda.nix {
            optimizationLevel = "release";
          };
          redpanda-release-cached = pkgs.callPackage ./nix/redpanda.nix {
            optimizationLevel = "release";
            bazelCacheDir = "/var/cache/bazel-nix";
          };

          # LTO: ThinLTO cross-module optimization (slower to link).
          redpanda-lto = pkgs.callPackage ./nix/redpanda.nix {
            optimizationLevel = "lto";
          };
          redpanda-lto-cached = pkgs.callPackage ./nix/redpanda.nix {
            optimizationLevel = "lto";
            bazelCacheDir = "/var/cache/bazel-nix";
          };

          # ── PGO (Profile-Guided Optimization) ──
          # Instrumented binary for external profiling workflows.
          redpanda-pgo-instrument = pkgs.callPackage ./nix/redpanda.nix {
            pgoMode = "instrument";
          };

          # Automated PGO: instrument → train (~15k msgs, 5 size tiers) → optimize.
          redpanda-pgo = let
            instrumented = pkgs.callPackage ./nix/redpanda.nix {
              pgoMode = "instrument";
            };
            profile = pkgs.callPackage ./nix/pgo-train.nix {
              redpandaInstrumented = instrumented;
              rpkDrv = rpk;
            };
          in pkgs.callPackage ./nix/redpanda.nix {
            pgoMode = "optimize";
            pgoProfilePath = "${profile}/pgo_profile.profdata";
          };

          # Cached PGO variants for repeat builders.
          redpanda-pgo-cached = let
            instrumented = pkgs.callPackage ./nix/redpanda.nix {
              pgoMode = "instrument";
              bazelCacheDir = "/var/cache/bazel-nix";
            };
            profile = pkgs.callPackage ./nix/pgo-train.nix {
              redpandaInstrumented = instrumented;
              rpkDrv = rpk;
            };
          in pkgs.callPackage ./nix/redpanda.nix {
            pgoMode = "optimize";
            pgoProfilePath = "${profile}/pgo_profile.profdata";
            bazelCacheDir = "/var/cache/bazel-nix";
          };

          # Helper for external profile workflow: build an optimized binary
          # using pre-generated .profdata from the full train_pgo.py pipeline.
          mkRedpandaPgo = profilePath: pkgs.callPackage ./nix/redpanda.nix {
            pgoMode = "optimize";
            pgoProfilePath = profilePath;
          };

          mkApp = drv: {
            type = "app";
            program = "${drv}/bin/${drv.name}";
          };

          bench = import ./nix/bench.nix { inherit pkgs mkApp; };

          tests = import ./nix/tests {
            inherit pkgs mkApp;
            redpandaDrv = redpanda;
            rpkDrv = rpk;
          };

          # Tests using the cached build (faster for repeat testing).
          testsCached = import ./nix/tests {
            inherit pkgs mkApp;
            redpandaDrv = redpanda-cached;
            rpkDrv = rpk;
          };
        in
        {
          packages = {
            inherit
              redpanda
              rpk
              redpanda-cached
              redpanda-release
              redpanda-release-cached
              redpanda-lto
              redpanda-lto-cached
              redpanda-pgo
              redpanda-pgo-instrument
              redpanda-pgo-cached
              ;
            default = redpanda-pgo;

            # OCI container images (use plain redpanda so they work without
            # /var/cache/bazel-nix sandbox passthrough in nix.conf)
            redpanda-image = pkgs.callPackage ./nix/redpanda-image.nix {
              redpandaDrv = redpanda;
            };
            redpanda-image-debug = pkgs.callPackage ./nix/redpanda-image.nix {
              redpandaDrv = redpanda;
              debug = true;
            };
            # Cached variants for repeat builders with /var/cache/bazel-nix configured
            redpanda-image-cached = pkgs.callPackage ./nix/redpanda-image.nix {
              redpandaDrv = redpanda-cached;
            };
            redpanda-image-debug-cached = pkgs.callPackage ./nix/redpanda-image.nix {
              redpandaDrv = redpanda-cached;
              debug = true;
            };
            rpk-image = pkgs.callPackage ./nix/rpk-image.nix {
              rpkDrv = rpk;
            };
          } // tests.packages // {
            # Cached variants for faster iteration (requires /var/cache/bazel-nix)
            test-single-node-cached = testsCached.packages.test-single-node;
            test-lifecycle-cached = testsCached.packages.test-lifecycle;
            test-all = tests.packages.test-all;
            test-all-cached = testsCached.packages.test-all;
          };

          apps = bench // tests.apps;

          devShells.default = pkgs.callPackage ./nix/shell.nix { };

          lib = {
            inherit mkRedpandaPgo;
          };

          checks = {
            rpk-version = pkgs.runCommand "rpk-version-check" { } ''
              ${rpk}/bin/rpk version
              touch $out
            '';
          } // tests.checks;
        }
      );
}
