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

          redpanda = pkgs.callPackage ./nix/redpanda.nix { };

          redpanda-cached = pkgs.callPackage ./nix/redpanda.nix {
            bazelCacheDir = "/var/cache/bazel-nix";
          };

          rpk = pkgs.callPackage ./nix/rpk.nix { };

          # PGO (Profile-Guided Optimization) variants.
          # Instrumented binary for external profiling workflows.
          redpanda-pgo-instrument = pkgs.callPackage ./nix/redpanda.nix {
            pgoMode = "instrument";
          };

          # Automated PGO: instrument -> lightweight training -> optimized build.
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
        in
        {
          packages = {
            inherit
              redpanda
              rpk
              redpanda-cached
              redpanda-pgo
              redpanda-pgo-instrument
              redpanda-pgo-cached
              ;
            default = redpanda;

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
          };

          apps = bench // {
            test-images = import ./nix/test-images.nix { inherit pkgs mkApp; };
          };

          devShells.default = pkgs.callPackage ./nix/shell.nix { };

          lib = {
            inherit mkRedpandaPgo;
          };

          checks = {
            rpk-version = pkgs.runCommand "rpk-version-check" { } ''
              ${rpk}/bin/rpk version
              touch $out
            '';
          };
        }
      );
}
