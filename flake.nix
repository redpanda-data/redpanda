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

          mkApp = drv: {
            type = "app";
            program = "${drv}/bin/${drv.name}";
          };

          bench = import ./nix/bench.nix { inherit pkgs mkApp; };
        in
        {
          packages = {
            inherit redpanda rpk redpanda-cached;
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

          checks = {
            rpk-version = pkgs.runCommand "rpk-version-check" { } ''
              ${rpk}/bin/rpk version
              touch $out
            '';
          };
        }
      );
}
